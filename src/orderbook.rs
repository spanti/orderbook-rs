use std::collections::{BTreeMap, HashMap, VecDeque};
use std::ops::Bound;

use crate::types::{Fill, Order, OrderBookId, OrderType, Side, SubmitResult};

/// Result of matching an incoming order against a single price level.
#[derive(Debug, Default)]
struct MatchOutcome {
    fills: Vec<Fill>,
    filled_order_ids: Vec<OrderBookId>, // 完全成交的，需要从 order_map 移除
}

/// Orders at a single price level in the OrderBook
#[derive(Debug, Clone, PartialEq)]
pub struct PriceLevel {
    pub price: u64,              // Price level in ticks (smallest price increment)
    pub orders: VecDeque<Order>, // Orders at this price level in the OrderBook
    pub total_quantity: u64,     // Total quantity of orders at this price level
}

impl PriceLevel {
    pub fn new(price: u64) -> Self {
        Self {
            price,
            orders: VecDeque::new(),
            total_quantity: 0,
        }
    }

    pub fn add_order(&mut self, order: Order) {
        self.total_quantity += order.quantity;
        self.orders.push_back(order);
    }

    pub fn pop_front(&mut self) -> Option<Order> {
        let removed = self.orders.pop_front()?;
        self.total_quantity -= removed.quantity;
        Some(removed)
    }

    fn push_front(&mut self, order: Order) {
        self.total_quantity += order.quantity;
        self.orders.push_front(order);
    }

    pub fn remove_order(&mut self, order_id: OrderBookId) -> Option<Order> {
        let pos = self.orders.iter().position(|o| o.id == order_id)?;
        let order = self.orders.remove(pos)?;
        self.total_quantity -= order.quantity;
        Some(order)
    }

    fn match_order(&mut self, incoming_order: &mut Order) -> MatchOutcome {
        let mut outcome = MatchOutcome::default();

        while let Some(mut resting) = self.pop_front() {
            let traded = resting.quantity.min(incoming_order.quantity);
            resting.quantity -= traded;
            incoming_order.quantity -= traded;

            outcome.fills.push(Fill {
                maker_order_id: resting.id,
                taker_order_id: incoming_order.id,
                price: resting.price,
                quantity: traded,
                taker_side: incoming_order.side,
            });

            if resting.quantity > 0 {
                // Partially filled, put the remaining back to the front of the level
                self.push_front(resting);
                break;
            }

            outcome.filled_order_ids.push(resting.id);

            if incoming_order.quantity == 0 {
                break;
            }
        }

        outcome
    }

    pub fn total_volume(&self) -> u64 {
        self.total_quantity
    }
}

/// Cursor over the price levels on one book side that an incoming order can
/// match, visited in price-priority order.
///
/// This is the single definition of the matchable-level walk shared by the
/// read-only liquidity probe ([`BookSide::available_quantity`]) and the
/// mutating matcher ([`BookSide::match_order`]):
///
/// * a limit bid can match asks priced at or below its limit, a limit ask can
///   match bids priced at or above its limit, and a market order matches every
///   level (the same prices accepted by [`Order::matches_price`]);
/// * bids are visited best first (highest price), asks best first (lowest
///   price);
/// * the walk ends when the eligible price range is exhausted.
///
/// The cursor yields prices rather than level references, so the matcher can
/// take a mutable borrow of a level between two steps without cloning the book.
#[derive(Debug, Clone, Copy)]
struct MatchWalk {
    /// Resting side whose levels are being walked; decides the direction.
    side: Side,
    /// Inclusive lower bound of the eligible price range.
    start: Bound<u64>,
    /// Inclusive upper bound of the eligible price range.
    end: Bound<u64>,
}

impl MatchWalk {
    fn new(side: Side, order: &Order) -> Self {
        let (start, end) = match (side, order.order_type) {
            (_, OrderType::Market) => (Bound::Unbounded, Bound::Unbounded),
            // Incoming ask: resting bids at or above its limit are eligible.
            (Side::Bid, OrderType::Limit) => (Bound::Included(order.price), Bound::Unbounded),
            // Incoming bid: resting asks at or below its limit are eligible.
            (Side::Ask, OrderType::Limit) => (Bound::Unbounded, Bound::Included(order.price)),
        };

        Self { side, start, end }
    }

    /// Next price to visit, or `None` once the eligible range is exhausted.
    fn next_price(&self, levels: &BTreeMap<u64, PriceLevel>) -> Option<u64> {
        let next = match self.side {
            Side::Bid => levels.range((self.start, self.end)).next_back(),
            Side::Ask => levels.range((self.start, self.end)).next(),
        };
        next.map(|(price, _)| *price)
    }

    /// Move the cursor past `price` so the next step visits another level.
    fn advance_past(&mut self, price: u64) {
        match self.side {
            Side::Bid => self.end = Bound::Excluded(price),
            Side::Ask => self.start = Bound::Excluded(price),
        }
    }
}

// One side of the OrderBook (either Bid or Ask)
#[derive(Debug, Clone, PartialEq)]
pub struct BookSide {
    pub side: Side,
    pub price_levels: BTreeMap<u64, PriceLevel>, // Price levels in the OrderBook side
    pub order_map: HashMap<OrderBookId, (Side, u64)>, // Map from order ID to its price level
}

impl BookSide {
    pub fn new(side: Side) -> Self {
        Self {
            side,
            price_levels: BTreeMap::new(),
            order_map: HashMap::new(),
        }
    }

    pub fn available_quantity(&self, order: &Order) -> u64 {
        let mut walk = MatchWalk::new(self.side, order);
        let mut available = 0u64;

        while let Some(price) = walk.next_price(&self.price_levels) {
            available = available.saturating_add(self.price_levels[&price].total_quantity);
            walk.advance_past(price);

            // Liquidity beyond the requested quantity cannot change the answer.
            if available >= order.quantity {
                break;
            }
        }

        available
    }

    pub fn snapshot(&self, depth: usize) -> Vec<(u64, u64)> {
        match self.side {
            Side::Bid => self
                .price_levels
                .iter()
                .rev()
                .take(depth)
                .map(|(price, level)| (*price, level.total_volume()))
                .collect(),
            Side::Ask => self
                .price_levels
                .iter()
                .take(depth)
                .map(|(price, level)| (*price, level.total_volume()))
                .collect(),
        }
    }

    pub fn depth(&self, level: usize) -> Option<(u64, u64)> {
        // Bid 侧从最高价开始，Ask 侧从最低价开始
        let (price, price_level) = match self.side {
            Side::Bid => self.price_levels.iter().rev().nth(level)?,
            Side::Ask => self.price_levels.iter().nth(level)?,
        };
        Some((*price, price_level.total_volume()))
    }

    // Get best price level (highest for Bid side, lowest for Ask side)
    pub fn best_price_level(&self) -> Option<u64> {
        match self.side {
            Side::Bid => self.price_levels.keys().next_back().copied(),
            Side::Ask => self.price_levels.keys().next().copied(),
        }
    }

    // Insert order into its price level
    pub fn insert_order(&mut self, order: Order) {
        let price_level = self
            .price_levels
            .entry(order.price)
            .or_insert_with(|| PriceLevel::new(order.price));
        self.order_map.insert(order.id, (order.side, order.price));
        price_level.add_order(order);
    }

    // Remove an order by its ID from the book side
    pub fn remove_order(&mut self, order_id: OrderBookId) -> Option<Order> {
        let (_, price) = self.order_map.remove(&order_id)?;

        let level = self.price_levels.get_mut(&price)?;

        let removed = level.remove_order(order_id)?;
        self.remove_level_if_empty(price);
        Some(removed)
    }

    // Remove a whole price level if it becomes empty
    pub fn remove_level_if_empty(&mut self, price: u64) {
        if let Some(price_level) = self.price_levels.get(&price) {
            if price_level.orders.is_empty() {
                self.price_levels.remove(&price);
            }
        }
    }

    // match order
    pub fn match_order(&mut self, order: &mut Order) -> Vec<Fill> {
        let mut filled = Vec::new();
        let mut walk = MatchWalk::new(self.side, order);

        while order.quantity > 0 {
            // The walk only yields prices the incoming order is allowed to match.
            let Some(price) = walk.next_price(&self.price_levels) else {
                break;
            };

            let Some(level) = self.price_levels.get_mut(&price) else {
                break;
            };

            // Match the order with the best price level on the other side
            let outcome = level.match_order(order);
            outcome.filled_order_ids.iter().for_each(|id| {
                self.order_map.remove(id);
            });
            filled.extend(outcome.fills);

            self.remove_level_if_empty(price);
            walk.advance_past(price);
        }
        filled
    }
}

// Structure representing the full OrderBook with both Bid and Ask sides
#[derive(Debug, Clone, PartialEq)]
pub struct OrderBook {
    bids: BookSide,
    asks: BookSide,
}

impl OrderBook {
    pub fn new() -> Self {
        Self {
            bids: BookSide::new(Side::Bid),
            asks: BookSide::new(Side::Ask),
        }
    }

    pub fn can_fill_completely(&self, order: &Order) -> bool {
        let other = match order.side {
            Side::Bid => &self.asks,
            Side::Ask => &self.bids,
        };
        other.available_quantity(order) >= order.quantity
    }

    pub fn snapshot(&self, depth: usize) -> (Vec<(u64, u64)>, Vec<(u64, u64)>) {
        let bids = self.bids.snapshot(depth);
        let asks = self.asks.snapshot(depth);
        (bids, asks)
    }

    pub fn best_bid(&self) -> Option<u64> {
        self.bids.best_price_level()
    }

    pub fn best_ask(&self) -> Option<u64> {
        self.asks.best_price_level()
    }

    pub fn depth(&self, side: Side, level: usize) -> Option<(u64, u64)> {
        let book_side = match side {
            Side::Bid => &self.bids,
            Side::Ask => &self.asks,
        };
        book_side.depth(level)
    }

    // Submit an order: match it against the opposite side, then rest any
    // remainder that the order's type and time-in-force allow.
    pub fn submit(&mut self, mut order: Order) -> SubmitResult {
        let (own_side, other_side) = match order.side {
            Side::Bid => (&mut self.bids, &mut self.asks), // Bid side is own side, Ask side is other side
            Side::Ask => (&mut self.asks, &mut self.bids), // Ask side is own side, Bid side is other side
        };

        // Attempt to match the order against the other side
        let fills = other_side.match_order(&mut order);
        let remaining_quantity = order.quantity;
        let rested = remaining_quantity > 0 && order.rests();

        if rested {
            own_side.insert_order(order);
        }

        SubmitResult {
            remaining_quantity,
            fills,
            rested,
        }
    }

    // cancel limit order by ID; returns true if the order was found and removed
    pub fn cancel_limit_order(&mut self, order_id: OrderBookId) -> Option<Order> {
        if let Some(order) = self.bids.remove_order(order_id) {
            return Some(order);
        }
        if let Some(order) = self.asks.remove_order(order_id) {
            return Some(order);
        }
        None
    }

    pub fn print_order_book(&self) {
        println!("Order Book:");
        println!("Bids:");
        for (price, level) in &self.bids.price_levels {
            let total_quantity: u64 = level.orders.iter().map(|o| o.quantity).sum();
            println!("Price: {}, Quantity: {}", price, total_quantity);
        }
        println!("Asks:");
        for (price, level) in &self.asks.price_levels {
            let total_quantity: u64 = level.orders.iter().map(|o| o.quantity).sum();
            println!("Price: {}, Quantity: {}", price, total_quantity);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TimeInForce;

    fn limit(id: OrderBookId, side: Side, price: u64, quantity: u64) -> Order {
        Order {
            id,
            side,
            price,
            quantity,
            timestamp: id,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::GTC,
        }
    }

    fn ioc_limit(id: OrderBookId, side: Side, price: u64, quantity: u64) -> Order {
        Order {
            time_in_force: TimeInForce::IOC,
            ..limit(id, side, price, quantity)
        }
    }

    fn market(id: OrderBookId, side: Side, quantity: u64) -> Order {
        Order {
            id,
            side,
            price: 0,
            quantity,
            timestamp: id,
            order_type: OrderType::Market,
            time_in_force: TimeInForce::IOC,
        }
    }

    fn book_side(book: &OrderBook, side: Side) -> &BookSide {
        match side {
            Side::Bid => &book.bids,
            Side::Ask => &book.asks,
        }
    }

    fn level_volume(book: &OrderBook, side: Side, price: u64) -> u64 {
        book_side(book, side)
            .price_levels
            .get(&price)
            .map(|level| level.total_volume())
            .unwrap_or(0)
    }

    fn order_ids_at(book: &OrderBook, side: Side, price: u64) -> Vec<OrderBookId> {
        book_side(book, side)
            .price_levels
            .get(&price)
            .map(|level| level.orders.iter().map(|o| o.id).collect())
            .unwrap_or_default()
    }

    #[test]
    fn side_opposite_flips() {
        assert_eq!(Side::Bid.opposite(), Side::Ask);
        assert_eq!(Side::Ask.opposite(), Side::Bid);
    }

    #[test]
    fn best_bid_and_ask_are_none_on_empty_book() {
        let book = OrderBook::new();

        assert_eq!(book.best_bid(), None);
        assert_eq!(book.best_ask(), None);
        assert_eq!(book.depth(Side::Bid, 0), None);
        assert_eq!(book.depth(Side::Ask, 0), None);
    }

    #[test]
    fn best_bid_and_ask_follow_price_priority() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Bid, 99, 1));
        book.submit(limit(2, Side::Bid, 101, 2));
        book.submit(limit(3, Side::Ask, 105, 3));
        book.submit(limit(4, Side::Ask, 103, 4));

        assert_eq!(book.best_bid(), Some(101));
        assert_eq!(book.best_ask(), Some(103));
    }

    #[test]
    fn depth_returns_levels_in_price_priority() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Bid, 99, 1));
        book.submit(limit(2, Side::Bid, 101, 2));
        book.submit(limit(3, Side::Bid, 100, 3));
        book.submit(limit(4, Side::Ask, 105, 4));
        book.submit(limit(5, Side::Ask, 103, 5));
        book.submit(limit(6, Side::Ask, 104, 6));

        // Bid: 从最高价开始
        assert_eq!(book.depth(Side::Bid, 0), Some((101, 2)));
        assert_eq!(book.depth(Side::Bid, 1), Some((100, 3)));
        assert_eq!(book.depth(Side::Bid, 2), Some((99, 1)));
        assert_eq!(book.depth(Side::Bid, 3), None);

        // Ask: 从最低价开始
        assert_eq!(book.depth(Side::Ask, 0), Some((103, 5)));
        assert_eq!(book.depth(Side::Ask, 1), Some((104, 6)));
        assert_eq!(book.depth(Side::Ask, 2), Some((105, 4)));
        assert_eq!(book.depth(Side::Ask, 3), None);
    }

    #[test]
    fn depth_aggregates_orders_at_same_price() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Bid, 100, 2));
        book.submit(limit(2, Side::Bid, 100, 3));

        assert_eq!(book.depth(Side::Bid, 0), Some((100, 5)));
        assert_eq!(book.depth(Side::Bid, 1), None);
    }

    #[test]
    fn depth_reflects_fills_and_cancellations() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 5));
        book.submit(limit(2, Side::Ask, 101, 2));

        // 部分成交后，100 档剩余 2
        book.submit(limit(3, Side::Bid, 100, 3));
        assert_eq!(book.depth(Side::Ask, 0), Some((100, 2)));

        // 撤掉 100 档剩余后，最优卖价变为 101
        assert!(book.cancel_limit_order(1).is_some());
        assert_eq!(book.depth(Side::Ask, 0), Some((101, 2)));
        assert_eq!(book.depth(Side::Ask, 1), None);

        // 市价单吃光后，深度为空
        book.submit(market(4, Side::Bid, 2));
        assert_eq!(book.best_ask(), None);
        assert_eq!(book.depth(Side::Ask, 0), None);
    }

    #[test]
    fn price_level_caches_total_quantity() {
        let mut level = PriceLevel::new(100);
        level.add_order(limit(1, Side::Bid, 100, 2));
        level.add_order(limit(2, Side::Bid, 100, 3));

        assert_eq!(level.total_volume(), 5);
        assert_eq!(level.total_quantity, 5);

        // 吃掉 4：2 来自订单 1，2 来自订单 2
        let mut incoming = limit(3, Side::Ask, 100, 4);
        let outcome = level.match_order(&mut incoming);

        assert_eq!(outcome.fills.len(), 2);
        assert_eq!(level.total_volume(), 1);
        assert_eq!(level.total_quantity, 1);
        assert_eq!(level.orders.len(), 1);
        assert_eq!(level.orders[0].quantity, 1);
    }

    #[test]
    fn price_level_remove_updates_total_quantity() {
        let mut level = PriceLevel::new(100);
        level.add_order(limit(1, Side::Bid, 100, 2));
        level.add_order(limit(2, Side::Bid, 100, 3));

        let removed = level.remove_order(1).unwrap();
        assert_eq!(removed.quantity, 2);
        assert_eq!(level.total_quantity, 3);
        assert_eq!(level.total_volume(), 3);

        assert!(level.remove_order(42).is_none());
        assert_eq!(level.total_quantity, 3);
    }

    #[test]
    fn cancel_updates_level_total_quantity() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Bid, 100, 2));
        book.submit(limit(2, Side::Bid, 100, 3));
        assert_eq!(book.depth(Side::Bid, 0), Some((100, 5)));

        assert!(book.cancel_limit_order(1).is_some());
        assert_eq!(book.depth(Side::Bid, 0), Some((100, 3)));
    }

    #[test]
    fn snapshot_orders_levels_by_price() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Bid, 99, 1));
        book.submit(limit(2, Side::Bid, 101, 2));
        book.submit(limit(3, Side::Bid, 100, 3));
        book.submit(limit(4, Side::Ask, 105, 4));
        book.submit(limit(5, Side::Ask, 103, 5));
        book.submit(limit(6, Side::Ask, 104, 6));

        let (bids, asks) = book.snapshot(10);
        assert_eq!(bids, vec![(101, 2), (100, 3), (99, 1)]);
        assert_eq!(asks, vec![(103, 5), (104, 6), (105, 4)]);

        // depth 截断
        let (bids, asks) = book.snapshot(2);
        assert_eq!(bids, vec![(101, 2), (100, 3)]);
        assert_eq!(asks, vec![(103, 5), (104, 6)]);

        // depth = 0
        let (bids, asks) = book.snapshot(0);
        assert!(bids.is_empty());
        assert!(asks.is_empty());
    }

    #[test]
    fn snapshot_reflects_fills_and_cancellations() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 5));
        book.submit(limit(2, Side::Ask, 101, 2));

        // 部分成交后，100 档剩 2
        book.submit(limit(3, Side::Bid, 100, 3));
        let (_, asks) = book.snapshot(10);
        assert_eq!(asks, vec![(100, 2), (101, 2)]);

        // 撤掉 100 档剩余
        assert!(book.cancel_limit_order(1).is_some());
        let (_, asks) = book.snapshot(10);
        assert_eq!(asks, vec![(101, 2)]);

        // 市价单吃光
        book.submit(market(4, Side::Bid, 2));
        let (bids, asks) = book.snapshot(10);
        assert!(bids.is_empty());
        assert!(asks.is_empty());
    }

    #[test]
    fn gtc_limit_remainder_rests() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 2));

        let result = book.submit(limit(2, Side::Bid, 100, 5));

        assert_eq!(result.remaining_quantity, 3);
        assert!(result.rested);
        assert_eq!(book.best_bid(), Some(100));
        assert_eq!(book.depth(Side::Bid, 0), Some((100, 3)));
    }

    #[test]
    fn ioc_limit_remainder_does_not_rest() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 2));

        let result = book.submit(ioc_limit(2, Side::Bid, 100, 5));

        assert_eq!(result.remaining_quantity, 3);
        assert!(!result.rested);
        assert_eq!(result.fills.len(), 1);
        assert_eq!(book.best_bid(), None);
        assert_eq!(book.best_ask(), None);
    }

    #[test]
    fn submit_reports_rested_only_when_remainder_is_inserted() {
        // GTC limit remainder rests on the book.
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 2));
        let gtc = book.submit(limit(2, Side::Bid, 100, 5));
        assert_eq!(gtc.remaining_quantity, 3);
        assert!(gtc.rested);
        assert_eq!(level_volume(&book, Side::Bid, 100), 3);

        // IOC limit remainder is discarded.
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 2));
        let ioc = book.submit(ioc_limit(2, Side::Bid, 100, 5));
        assert_eq!(ioc.remaining_quantity, 3);
        assert!(!ioc.rested);
        assert_eq!(book.best_bid(), None);

        // Market remainder is discarded.
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 2));
        let market_result = book.submit(market(2, Side::Bid, 5));
        assert_eq!(market_result.remaining_quantity, 3);
        assert!(!market_result.rested);
        assert_eq!(book.best_bid(), None);

        // A fully filled order never rests.
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 5));
        let filled = book.submit(limit(2, Side::Bid, 100, 5));
        assert_eq!(filled.remaining_quantity, 0);
        assert!(!filled.rested);
        assert!(book.bids.price_levels.is_empty());
    }

    #[test]
    fn available_quantity_counts_only_crossing_levels() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 2));
        book.submit(limit(2, Side::Ask, 101, 3));

        let limit_probe = limit(3, Side::Bid, 100, 10);
        let market_probe = market(4, Side::Bid, 10);

        // 限价 100 只能吃到 100 这一档
        assert_eq!(book.asks.available_quantity(&limit_probe), 2);
        // 市价单可以吃穿所有档位
        assert_eq!(book.asks.available_quantity(&market_probe), 5);

        assert!(book.can_fill_completely(&limit(5, Side::Bid, 100, 2)));
        assert!(!book.can_fill_completely(&limit(6, Side::Bid, 100, 3)));
        assert!(book.can_fill_completely(&market(7, Side::Bid, 5)));
        assert!(!book.can_fill_completely(&market(8, Side::Bid, 6)));
    }

    /// Drive the shared walk to exhaustion and check it visited exactly the
    /// levels `Order::matches_price` accepts, in price-priority order.
    fn walk_prices(book_side: &BookSide, order: &Order) -> Vec<u64> {
        let mut walk = MatchWalk::new(book_side.side, order);
        let mut visited = Vec::new();
        while let Some(price) = walk.next_price(&book_side.price_levels) {
            assert!(
                order.matches_price(price),
                "walk visited non-matchable price {price} for {order:?}"
            );
            visited.push(price);
            walk.advance_past(price);
        }

        let expected: Vec<u64> = match book_side.side {
            Side::Bid => book_side
                .price_levels
                .keys()
                .rev()
                .copied()
                .filter(|price| order.matches_price(*price))
                .collect(),
            Side::Ask => book_side
                .price_levels
                .keys()
                .copied()
                .filter(|price| order.matches_price(*price))
                .collect(),
        };
        assert_eq!(visited, expected, "walk diverged for {order:?}");
        visited
    }

    #[test]
    fn match_walk_visits_exactly_the_prices_matches_price_accepts() {
        let mut asks = OrderBook::new();
        for (id, price) in [99u64, 100, 101, 102].into_iter().enumerate() {
            asks.submit(limit(id as u64 + 1, Side::Ask, price, 1));
        }

        // Ask side: eligible asks are <= the incoming bid's limit, lowest first.
        assert_eq!(
            walk_prices(&asks.asks, &limit(10, Side::Bid, 100, 10)),
            vec![99, 100]
        );
        assert_eq!(
            walk_prices(&asks.asks, &limit(11, Side::Bid, 98, 10)),
            Vec::<u64>::new()
        );
        assert_eq!(
            walk_prices(&asks.asks, &market(15, Side::Bid, 10)),
            vec![99, 100, 101, 102]
        );

        let mut bids = OrderBook::new();
        for (id, price) in [98u64, 99, 100, 101].into_iter().enumerate() {
            bids.submit(limit(id as u64 + 20, Side::Bid, price, 1));
        }

        // Bid side: eligible bids are >= the incoming ask's limit, highest first.
        assert_eq!(
            walk_prices(&bids.bids, &limit(12, Side::Ask, 100, 10)),
            vec![101, 100]
        );
        assert_eq!(
            walk_prices(&bids.bids, &limit(13, Side::Ask, 102, 10)),
            Vec::<u64>::new()
        );
        assert_eq!(
            walk_prices(&bids.bids, &market(14, Side::Ask, 10)),
            vec![101, 100, 99, 98]
        );
    }

    #[test]
    fn can_fill_completely_counts_partially_filled_maker_at_last_crossing_level() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 5));
        book.submit(limit(2, Side::Ask, 101, 2));

        // Partially fill the 100 level: maker 1 stays at the front with 2 left.
        book.submit(limit(3, Side::Bid, 100, 3));
        assert_eq!(order_ids_at(&book, Side::Ask, 100), vec![1]);
        assert_eq!(level_volume(&book, Side::Ask, 100), 2);

        // 2 left at 100 + 2 at 101: enough for 4, not for 5.
        assert!(book.can_fill_completely(&limit(4, Side::Bid, 101, 4)));
        assert!(!book.can_fill_completely(&limit(5, Side::Bid, 101, 5)));
        // The limit price still excludes 101 when bidding 100.
        assert!(!book.can_fill_completely(&limit(6, Side::Bid, 100, 3)));

        // The matcher agrees with the probe at the boundary.
        let result = book.submit(limit(7, Side::Bid, 101, 4));
        assert_eq!(result.remaining_quantity, 0);
        assert_eq!(
            result
                .fills
                .iter()
                .map(|fill| (fill.price, fill.quantity))
                .collect::<Vec<_>>(),
            vec![(100, 2), (101, 2)]
        );
        assert!(book.asks.price_levels.is_empty());
    }

    fn dry_run_fills_completely(book: &OrderBook, order: &Order) -> bool {
        let probe = order.clone();
        let mut copy = book.clone();
        copy.submit(probe).remaining_quantity == 0
    }

    #[test]
    fn can_fill_completely_agrees_with_actual_matching() {
        fn check(book: &OrderBook, order: &Order) {
            assert_eq!(
                book.can_fill_completely(order),
                dry_run_fills_completely(book, order),
                "precheck diverged from matcher for {order:?}"
            );
        }

        // 空簿：限价单和市价单都无法成交
        let empty = OrderBook::new();
        check(&empty, &limit(1, Side::Bid, 100, 1));
        check(&empty, &market(2, Side::Bid, 1));

        // 单档：刚好够 / 不够
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 5));
        check(&book, &limit(2, Side::Bid, 100, 5));
        check(&book, &limit(3, Side::Bid, 100, 6));
        check(&book, &market(4, Side::Bid, 5));
        check(&book, &market(5, Side::Bid, 6));

        // 多档：限价决定能吃多远
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 2));
        book.submit(limit(2, Side::Ask, 101, 3));
        check(&book, &limit(3, Side::Bid, 101, 5));
        check(&book, &limit(4, Side::Bid, 101, 6));
        check(&book, &limit(5, Side::Bid, 100, 3));

        // 买方向的镜像
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Bid, 100, 2));
        book.submit(limit(2, Side::Bid, 99, 3));
        check(&book, &limit(3, Side::Ask, 99, 5));
        check(&book, &limit(4, Side::Ask, 100, 3));
        check(&book, &market(5, Side::Ask, 6));

        // 部分成交后：100 档剩 3，101 档剩 2
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 5));
        book.submit(limit(2, Side::Ask, 101, 2));
        book.submit(limit(3, Side::Bid, 100, 2));
        check(&book, &limit(4, Side::Bid, 100, 3));
        check(&book, &limit(5, Side::Bid, 100, 4));
        check(&book, &limit(6, Side::Bid, 101, 5));
    }

    #[test]
    fn best_price_is_highest_bid_and_lowest_ask() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Bid, 97, 1));
        book.submit(limit(2, Side::Bid, 99, 1));
        book.submit(limit(3, Side::Bid, 98, 1));
        book.submit(limit(4, Side::Ask, 102, 1));
        book.submit(limit(5, Side::Ask, 100, 1));
        book.submit(limit(6, Side::Ask, 101, 1));

        assert_eq!(book.bids.best_price_level(), Some(99));
        assert_eq!(book.asks.best_price_level(), Some(100));
    }

    #[test]
    fn limit_order_rests_on_empty_book() {
        let mut book = OrderBook::new();

        let remaining = book.submit(limit(1, Side::Bid, 100, 5)).remaining_quantity;

        assert_eq!(remaining, 5);
        assert_eq!(book.bids.best_price_level(), Some(100));
        assert_eq!(level_volume(&book, Side::Bid, 100), 5);
        assert!(book.asks.price_levels.is_empty());
    }

    #[test]
    fn limit_bid_matches_resting_ask_at_same_price() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 5));

        let remaining = book.submit(limit(2, Side::Bid, 100, 5)).remaining_quantity;

        assert_eq!(remaining, 0);
        assert!(book.asks.price_levels.is_empty());
        assert!(book.bids.price_levels.is_empty());
    }

    #[test]
    fn limit_ask_matches_resting_bid_at_same_price() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Bid, 100, 5));

        let remaining = book.submit(limit(2, Side::Ask, 100, 5)).remaining_quantity;

        assert_eq!(remaining, 0);
        assert!(book.asks.price_levels.is_empty());
        assert!(book.bids.price_levels.is_empty());
    }

    #[test]
    fn limit_bid_below_best_ask_rests() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 5));

        let remaining = book.submit(limit(2, Side::Bid, 99, 5)).remaining_quantity;

        assert_eq!(remaining, 5);
        assert_eq!(level_volume(&book, Side::Ask, 100), 5);
        assert_eq!(level_volume(&book, Side::Bid, 99), 5);
    }

    #[test]
    fn limit_ask_above_best_bid_rests() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Bid, 100, 5));

        let remaining = book.submit(limit(2, Side::Ask, 101, 5)).remaining_quantity;

        assert_eq!(remaining, 5);
        assert_eq!(level_volume(&book, Side::Bid, 100), 5);
        assert_eq!(level_volume(&book, Side::Ask, 101), 5);
    }

    #[test]
    fn limit_order_partial_fill_rests_remainder() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 3));

        let remaining = book.submit(limit(2, Side::Bid, 105, 10)).remaining_quantity;

        assert_eq!(remaining, 7);
        assert!(book.asks.price_levels.is_empty());
        assert_eq!(book.bids.best_price_level(), Some(105));
        assert_eq!(level_volume(&book, Side::Bid, 105), 7);
    }

    #[test]
    fn limit_order_sweeps_levels_in_price_priority() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 2));
        book.submit(limit(2, Side::Ask, 101, 3));

        let remaining = book.submit(limit(3, Side::Bid, 101, 4)).remaining_quantity;

        assert_eq!(remaining, 0);
        assert!(!book.asks.price_levels.contains_key(&100));
        assert_eq!(book.asks.best_price_level(), Some(101));
        assert_eq!(level_volume(&book, Side::Ask, 101), 1);
        assert!(book.bids.price_levels.is_empty());
    }

    #[test]
    fn price_time_priority_fills_oldest_order_first() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 2));
        book.submit(limit(2, Side::Ask, 100, 3));

        let remaining = book.submit(limit(3, Side::Bid, 100, 3)).remaining_quantity;

        assert_eq!(remaining, 0);
        assert_eq!(order_ids_at(&book, Side::Ask, 100), vec![2]);
        assert_eq!(level_volume(&book, Side::Ask, 100), 2);
    }

    #[test]
    fn market_order_ignores_price_and_sweeps_levels() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 2));
        book.submit(limit(2, Side::Ask, 101, 3));

        let remaining = book.submit(market(3, Side::Bid, 4)).remaining_quantity;

        assert_eq!(remaining, 0);
        assert!(!book.asks.price_levels.contains_key(&100));
        assert_eq!(book.asks.best_price_level(), Some(101));
        assert_eq!(level_volume(&book, Side::Ask, 101), 1);
        assert!(book.bids.price_levels.is_empty());
    }

    #[test]
    fn market_order_partial_fill_does_not_rest() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 2));

        let remaining = book.submit(market(2, Side::Bid, 5)).remaining_quantity;

        assert_eq!(remaining, 3);
        assert!(book.asks.price_levels.is_empty());
        assert!(book.bids.price_levels.is_empty());
    }

    #[test]
    fn market_order_on_empty_book_is_unfilled() {
        let mut book = OrderBook::new();

        let remaining = book.submit(market(1, Side::Bid, 5)).remaining_quantity;

        assert_eq!(remaining, 5);
        assert!(book.bids.price_levels.is_empty());
        assert!(book.asks.price_levels.is_empty());
    }

    #[test]
    fn market_sell_hits_highest_bid_first() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Bid, 99, 2));
        book.submit(limit(2, Side::Bid, 100, 2));

        let remaining = book.submit(market(3, Side::Ask, 3)).remaining_quantity;

        assert_eq!(remaining, 0);
        assert!(!book.bids.price_levels.contains_key(&100));
        assert_eq!(book.bids.best_price_level(), Some(99));
        assert_eq!(level_volume(&book, Side::Bid, 99), 1);
    }

    #[test]
    fn cancel_removes_order_and_empty_level() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Bid, 100, 5));

        let canceled = book.cancel_limit_order(1).unwrap();
        assert_eq!(canceled.id, 1);
        assert_eq!(canceled.quantity, 5);
        assert!(book.bids.price_levels.is_empty());
        assert_eq!(book.bids.best_price_level(), None);
    }

    #[test]
    fn cancel_keeps_other_orders_at_same_level() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 2));
        book.submit(limit(2, Side::Ask, 100, 3));

        assert!(book.cancel_limit_order(1).is_some());
        assert_eq!(order_ids_at(&book, Side::Ask, 100), vec![2]);
        assert_eq!(level_volume(&book, Side::Ask, 100), 3);
    }

    #[test]
    fn cancel_unknown_order_returns_false() {
        let mut book = OrderBook::new();

        assert!(book.cancel_limit_order(42).is_none());
    }

    #[test]
    fn order_map_stays_in_sync_through_fill_and_cancel() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 2));
        book.submit(limit(2, Side::Ask, 100, 3));
        assert_eq!(book.asks.order_map.get(&1), Some(&(Side::Ask, 100)));

        // 吃掉订单 1，部分成交订单 2
        let remaining = book.submit(limit(3, Side::Bid, 100, 4)).remaining_quantity;

        assert_eq!(remaining, 0);
        assert!(!book.asks.order_map.contains_key(&1));
        assert_eq!(book.asks.order_map.get(&2), Some(&(Side::Ask, 100)));
        assert_eq!(level_volume(&book, Side::Ask, 100), 1);
        assert!(book.bids.order_map.is_empty());

        assert!(book.cancel_limit_order(2).is_some());
        assert!(book.asks.order_map.is_empty());
    }

    #[test]
    fn price_level_partial_fill_keeps_order_at_front() {
        let mut level = PriceLevel::new(100);
        level.add_order(limit(1, Side::Bid, 100, 2));
        level.add_order(limit(2, Side::Bid, 100, 3));

        let mut incoming = limit(3, Side::Ask, 100, 3);
        let outcome = level.match_order(&mut incoming);

        assert_eq!(outcome.filled_order_ids, vec![1]);
        assert_eq!(outcome.fills.len(), 2);
        assert_eq!(outcome.fills[0].maker_order_id, 1);
        assert_eq!(outcome.fills[0].taker_order_id, 3);
        assert_eq!(outcome.fills[0].price, 100);
        assert_eq!(outcome.fills[0].quantity, 2);
        assert_eq!(outcome.fills[0].taker_side, Side::Ask);
        assert_eq!(outcome.fills[1].maker_order_id, 2);
        assert_eq!(outcome.fills[1].taker_order_id, 3);
        assert_eq!(outcome.fills[1].price, 100);
        assert_eq!(outcome.fills[1].quantity, 1);
        assert_eq!(outcome.fills[1].taker_side, Side::Ask);
        assert_eq!(incoming.quantity, 0);
        assert_eq!(level.orders.len(), 1);
        assert_eq!(level.orders[0].id, 2);
        assert_eq!(level.orders[0].quantity, 2);
        assert_eq!(level.total_volume(), 2);
    }

    #[test]
    fn limit_fills_use_maker_price() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 2));

        let result = book.submit(limit(2, Side::Bid, 105, 2));

        assert_eq!(result.remaining_quantity, 0);
        assert_eq!(result.fills.len(), 1);
        assert_eq!(result.fills[0].maker_order_id, 1);
        assert_eq!(result.fills[0].taker_order_id, 2);
        assert_eq!(result.fills[0].price, 100);
        assert_eq!(result.fills[0].quantity, 2);
        assert_eq!(result.fills[0].taker_side, Side::Bid);
    }

    #[test]
    fn sweeping_multiple_levels_produces_one_fill_per_level() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 2));
        book.submit(limit(2, Side::Ask, 101, 3));

        let result = book.submit(limit(3, Side::Bid, 101, 4));

        assert_eq!(result.remaining_quantity, 0);
        assert_eq!(result.fills.len(), 2);
        assert_eq!(result.fills[0].maker_order_id, 1);
        assert_eq!(result.fills[0].price, 100);
        assert_eq!(result.fills[0].quantity, 2);
        assert_eq!(result.fills[1].maker_order_id, 2);
        assert_eq!(result.fills[1].price, 101);
        assert_eq!(result.fills[1].quantity, 2);
        assert!(result.fills.iter().all(|f| f.taker_order_id == 3));
    }

    #[test]
    fn partial_fill_records_only_traded_quantity() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 3));

        let result = book.submit(limit(2, Side::Bid, 105, 10));

        assert_eq!(result.remaining_quantity, 7);
        assert_eq!(result.fills.len(), 1);
        assert_eq!(result.fills[0].quantity, 3);
        assert_eq!(result.fills[0].price, 100);
    }

    #[test]
    fn market_order_records_fills_at_resting_prices() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 2));
        book.submit(limit(2, Side::Ask, 101, 3));

        let result = book.submit(market(3, Side::Bid, 4));

        assert_eq!(result.remaining_quantity, 0);
        assert_eq!(result.fills.len(), 2);
        assert_eq!(result.fills[0].price, 100);
        assert_eq!(result.fills[0].quantity, 2);
        assert_eq!(result.fills[1].price, 101);
        assert_eq!(result.fills[1].quantity, 2);
        assert!(result.fills.iter().all(|f| f.taker_side == Side::Bid));
    }

    #[test]
    fn non_crossing_order_produces_no_fills() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 5));

        let result = book.submit(limit(2, Side::Bid, 99, 5));

        assert_eq!(result.remaining_quantity, 5);
        assert!(result.fills.is_empty());
    }
}
