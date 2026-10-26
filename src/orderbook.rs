use std::collections::{BTreeMap, HashMap, VecDeque};
use std::ops::Bound;

use crate::types::{Fill, Order, OrderBookId, OrderType, Side, SubmitResult};

/// Result of matching an incoming order against a single price level.
#[derive(Debug, Default)]
struct MatchOutcome {
    fills: Vec<Fill>,
    filled_order_ids: Vec<OrderBookId>, // 完全成交的，需要从 order_map 移除
}

/// Orders at a single price level in the OrderBook.
///
/// Private implementation detail: every observation and mutation goes through
/// [`OrderBook`].
#[derive(Debug, Clone, PartialEq)]
struct PriceLevel {
    price: u64,              // Price level in ticks (smallest price increment)
    orders: VecDeque<Order>, // Orders at this price level in the OrderBook
    total_quantity: u64,     // Total quantity of orders at this price level
}

impl PriceLevel {
    fn new(price: u64) -> Self {
        Self {
            price,
            orders: VecDeque::new(),
            total_quantity: 0,
        }
    }

    fn add_order(&mut self, order: Order) {
        self.total_quantity += order.quantity;
        self.orders.push_back(order);
    }

    fn pop_front(&mut self) -> Option<Order> {
        let removed = self.orders.pop_front()?;
        self.total_quantity -= removed.quantity;
        Some(removed)
    }

    fn push_front(&mut self, order: Order) {
        self.total_quantity += order.quantity;
        self.orders.push_front(order);
    }

    fn remove_order(&mut self, order_id: OrderBookId) -> Option<Order> {
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

    fn total_volume(&self) -> u64 {
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

/// One side of the OrderBook (either Bid or Ask).
///
/// Private implementation detail: every observation and mutation goes through
/// [`OrderBook`].
#[derive(Debug, Clone, PartialEq)]
struct BookSide {
    side: Side,
    price_levels: BTreeMap<u64, PriceLevel>, // Price levels in the OrderBook side
    order_map: HashMap<OrderBookId, (Side, u64)>, // Map from order ID to its price level
}

impl BookSide {
    fn new(side: Side) -> Self {
        Self {
            side,
            price_levels: BTreeMap::new(),
            order_map: HashMap::new(),
        }
    }

    fn available_quantity(&self, order: &Order) -> u64 {
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

    fn snapshot(&self, depth: usize) -> Vec<(u64, u64)> {
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

    fn depth(&self, level: usize) -> Option<(u64, u64)> {
        // Bid 侧从最高价开始，Ask 侧从最低价开始
        let (price, price_level) = match self.side {
            Side::Bid => self.price_levels.iter().rev().nth(level)?,
            Side::Ask => self.price_levels.iter().nth(level)?,
        };
        Some((*price, price_level.total_volume()))
    }

    // Get best price level (highest for Bid side, lowest for Ask side)
    fn best_price_level(&self) -> Option<u64> {
        match self.side {
            Side::Bid => self.price_levels.keys().next_back().copied(),
            Side::Ask => self.price_levels.keys().next().copied(),
        }
    }

    // Insert order into its price level
    fn insert_order(&mut self, order: Order) {
        let price_level = self
            .price_levels
            .entry(order.price)
            .or_insert_with(|| PriceLevel::new(order.price));
        self.order_map.insert(order.id, (order.side, order.price));
        price_level.add_order(order);
    }

    // Remove an order by its ID from the book side
    fn remove_order(&mut self, order_id: OrderBookId) -> Option<Order> {
        let (_, price) = self.order_map.remove(&order_id)?;

        let level = self.price_levels.get_mut(&price)?;

        let removed = level.remove_order(order_id)?;
        self.remove_level_if_empty(price);
        Some(removed)
    }

    // Remove a whole price level if it becomes empty
    fn remove_level_if_empty(&mut self, price: u64) {
        if let Some(price_level) = self.price_levels.get(&price) {
            if price_level.orders.is_empty() {
                self.price_levels.remove(&price);
            }
        }
    }

    // match order
    fn match_order(&mut self, order: &mut Order) -> Vec<Fill> {
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

    /// `(maker_id, price, quantity)` for each fill, in execution order.
    fn fill_summary(fills: &[Fill]) -> Vec<(OrderBookId, u64, u64)> {
        fills
            .iter()
            .map(|fill| (fill.maker_order_id, fill.price, fill.quantity))
            .collect()
    }

    /// One resting ask of quantity 1 at each of 99, 100, 101 and 102.
    fn ask_ladder() -> OrderBook {
        let mut book = OrderBook::new();
        for (id, price) in [(1u64, 99u64), (2, 100), (3, 101), (4, 102)] {
            book.submit(limit(id, Side::Ask, price, 1));
        }
        book
    }

    /// One resting bid of quantity 1 at each of 98, 99, 100 and 101.
    fn bid_ladder() -> OrderBook {
        let mut book = OrderBook::new();
        for (id, price) in [(11u64, 98u64), (12, 99), (13, 100), (14, 101)] {
            book.submit(limit(id, Side::Bid, price, 1));
        }
        book
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
    fn depth_aggregates_quantity_through_partial_fills() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Bid, 100, 2));
        book.submit(limit(2, Side::Bid, 100, 3));
        assert_eq!(book.depth(Side::Bid, 0), Some((100, 5)));

        // 吃掉 4：2 来自订单 1，2 来自订单 2
        let result = book.submit(limit(3, Side::Ask, 100, 4));
        assert_eq!(result.remaining_quantity, 0);
        assert_eq!(fill_summary(&result.fills), vec![(1, 100, 2), (2, 100, 2)]);

        // 只剩订单 2 的 1 个剩余量
        assert_eq!(book.depth(Side::Bid, 0), Some((100, 1)));
        assert_eq!(book.snapshot(10).0, vec![(100, 1)]);
        assert_eq!(book.best_bid(), Some(100));
    }

    #[test]
    fn cancel_updates_level_total_quantity() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Bid, 100, 2));
        book.submit(limit(2, Side::Bid, 100, 3));
        assert_eq!(book.depth(Side::Bid, 0), Some((100, 5)));

        let canceled = book.cancel_limit_order(1).unwrap();
        assert_eq!(canceled.id, 1);
        assert_eq!(canceled.quantity, 2);
        assert_eq!(book.depth(Side::Bid, 0), Some((100, 3)));
        assert_eq!(book.snapshot(10).0, vec![(100, 3)]);
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
        assert_eq!(book.depth(Side::Bid, 0), Some((100, 3)));

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
        assert_eq!(book.best_bid(), None);
        assert_eq!(book.best_ask(), None);
    }

    #[test]
    fn can_fill_completely_counts_only_crossing_levels() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 2));
        book.submit(limit(2, Side::Ask, 101, 3));

        // 限价 100 只能吃到 100 这一档
        assert!(book.can_fill_completely(&limit(3, Side::Bid, 100, 2)));
        assert!(!book.can_fill_completely(&limit(4, Side::Bid, 100, 3)));
        assert!(!book.can_fill_completely(&limit(5, Side::Bid, 100, 10)));

        // 市价单可以吃穿所有档位
        assert!(book.can_fill_completely(&market(6, Side::Bid, 5)));
        assert!(!book.can_fill_completely(&market(7, Side::Bid, 6)));

        // The probe must not mutate the book.
        assert_eq!(book.depth(Side::Ask, 0), Some((100, 2)));
        assert_eq!(book.depth(Side::Ask, 1), Some((101, 3)));
    }

    #[test]
    fn limit_bid_crosses_only_asks_at_or_below_its_limit() {
        // Eligible asks are those at or below the bid's limit, lowest first.
        let mut book = ask_ladder();
        let result = book.submit(limit(10, Side::Bid, 100, 10));
        assert_eq!(result.remaining_quantity, 8);
        assert_eq!(fill_summary(&result.fills), vec![(1, 99, 1), (2, 100, 1)]);
        assert_eq!(book.best_ask(), Some(101));
        assert_eq!(book.depth(Side::Bid, 0), Some((100, 8)));

        // A bid below every ask crosses nothing and rests.
        let mut book = ask_ladder();
        let result = book.submit(limit(11, Side::Bid, 98, 10));
        assert_eq!(result.remaining_quantity, 10);
        assert!(result.fills.is_empty());
        assert!(result.rested);
        assert_eq!(book.best_bid(), Some(98));
        assert_eq!(book.best_ask(), Some(99));
    }

    #[test]
    fn limit_ask_crosses_only_bids_at_or_above_its_limit() {
        // Eligible bids are those at or above the ask's limit, highest first.
        let mut book = bid_ladder();
        let result = book.submit(limit(20, Side::Ask, 100, 10));
        assert_eq!(result.remaining_quantity, 8);
        assert_eq!(
            fill_summary(&result.fills),
            vec![(14, 101, 1), (13, 100, 1)]
        );
        assert_eq!(book.best_bid(), Some(99));
        assert_eq!(book.depth(Side::Ask, 0), Some((100, 8)));

        // An ask above every bid crosses nothing and rests.
        let mut book = bid_ladder();
        let result = book.submit(limit(21, Side::Ask, 102, 10));
        assert_eq!(result.remaining_quantity, 10);
        assert!(result.fills.is_empty());
        assert!(result.rested);
        assert_eq!(book.best_bid(), Some(101));
        assert_eq!(book.best_ask(), Some(102));
    }

    #[test]
    fn market_bid_sweeps_every_ask_level() {
        let mut book = ask_ladder();

        let result = book.submit(market(12, Side::Bid, 10));

        assert_eq!(result.remaining_quantity, 6);
        assert!(!result.rested);
        assert_eq!(
            fill_summary(&result.fills),
            vec![(1, 99, 1), (2, 100, 1), (3, 101, 1), (4, 102, 1)]
        );
        assert_eq!(book.best_ask(), None);
        assert_eq!(book.best_bid(), None);
    }

    #[test]
    fn market_ask_sweeps_every_bid_level() {
        let mut book = bid_ladder();

        let result = book.submit(market(22, Side::Ask, 10));

        assert_eq!(result.remaining_quantity, 6);
        assert!(!result.rested);
        assert_eq!(
            fill_summary(&result.fills),
            vec![(14, 101, 1), (13, 100, 1), (12, 99, 1), (11, 98, 1)]
        );
        assert_eq!(book.best_bid(), None);
        assert_eq!(book.best_ask(), None);
    }

    #[test]
    fn can_fill_completely_counts_partially_filled_maker_at_last_crossing_level() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 5));
        book.submit(limit(2, Side::Ask, 101, 2));

        // Partially fill the 100 level: maker 1 stays with 2 left.
        book.submit(limit(3, Side::Bid, 100, 3));
        assert_eq!(book.depth(Side::Ask, 0), Some((100, 2)));

        // 2 left at 100 + 2 at 101: enough for 4, not for 5.
        assert!(book.can_fill_completely(&limit(4, Side::Bid, 101, 4)));
        assert!(!book.can_fill_completely(&limit(5, Side::Bid, 101, 5)));
        // The limit price still excludes 101 when bidding 100.
        assert!(!book.can_fill_completely(&limit(6, Side::Bid, 100, 3)));

        // The matcher agrees with the probe at the boundary.
        let result = book.submit(limit(7, Side::Bid, 101, 4));
        assert_eq!(result.remaining_quantity, 0);
        assert_eq!(fill_summary(&result.fills), vec![(1, 100, 2), (2, 101, 2)]);
        let (bids, asks) = book.snapshot(10);
        assert!(bids.is_empty());
        assert!(asks.is_empty());
    }

    #[test]
    fn can_fill_completely_agrees_with_actual_matching() {
        fn check(book: &OrderBook, order: &Order) {
            // Dry-run the public matcher on a clone and compare its verdict
            // with the read-only precheck.
            let mut copy = book.clone();
            let matcher_fills_completely = copy.submit(order.clone()).remaining_quantity == 0;
            assert_eq!(
                book.can_fill_completely(order),
                matcher_fills_completely,
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

        assert_eq!(book.best_bid(), Some(99));
        assert_eq!(book.best_ask(), Some(100));
    }

    #[test]
    fn limit_order_rests_on_empty_book() {
        let mut book = OrderBook::new();

        let result = book.submit(limit(1, Side::Bid, 100, 5));

        assert_eq!(result.remaining_quantity, 5);
        assert!(result.rested);
        assert!(result.fills.is_empty());
        assert_eq!(book.best_bid(), Some(100));
        assert_eq!(book.depth(Side::Bid, 0), Some((100, 5)));
        assert_eq!(book.best_ask(), None);
    }

    #[test]
    fn limit_bid_matches_resting_ask_at_same_price() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 5));

        let result = book.submit(limit(2, Side::Bid, 100, 5));

        assert_eq!(result.remaining_quantity, 0);
        assert!(!result.rested);
        assert_eq!(result.fills.len(), 1);
        let (bids, asks) = book.snapshot(10);
        assert!(bids.is_empty());
        assert!(asks.is_empty());
        assert_eq!(book.best_bid(), None);
        assert_eq!(book.best_ask(), None);
    }

    #[test]
    fn limit_ask_matches_resting_bid_at_same_price() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Bid, 100, 5));

        let result = book.submit(limit(2, Side::Ask, 100, 5));

        assert_eq!(result.remaining_quantity, 0);
        assert!(!result.rested);
        assert_eq!(result.fills.len(), 1);
        let (bids, asks) = book.snapshot(10);
        assert!(bids.is_empty());
        assert!(asks.is_empty());
        assert_eq!(book.best_bid(), None);
        assert_eq!(book.best_ask(), None);
    }

    #[test]
    fn limit_bid_below_best_ask_rests() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 5));

        let result = book.submit(limit(2, Side::Bid, 99, 5));

        assert_eq!(result.remaining_quantity, 5);
        assert!(result.rested);
        assert!(result.fills.is_empty());
        assert_eq!(book.depth(Side::Ask, 0), Some((100, 5)));
        assert_eq!(book.depth(Side::Bid, 0), Some((99, 5)));
        assert_eq!(book.best_ask(), Some(100));
        assert_eq!(book.best_bid(), Some(99));
    }

    #[test]
    fn limit_ask_above_best_bid_rests() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Bid, 100, 5));

        let result = book.submit(limit(2, Side::Ask, 101, 5));

        assert_eq!(result.remaining_quantity, 5);
        assert!(result.rested);
        assert!(result.fills.is_empty());
        assert_eq!(book.depth(Side::Bid, 0), Some((100, 5)));
        assert_eq!(book.depth(Side::Ask, 0), Some((101, 5)));
        assert_eq!(book.best_bid(), Some(100));
        assert_eq!(book.best_ask(), Some(101));
    }

    #[test]
    fn limit_order_partial_fill_rests_remainder() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 3));

        let result = book.submit(limit(2, Side::Bid, 105, 10));

        assert_eq!(result.remaining_quantity, 7);
        assert!(result.rested);
        assert_eq!(fill_summary(&result.fills), vec![(1, 100, 3)]);
        assert_eq!(book.best_ask(), None);
        assert_eq!(book.best_bid(), Some(105));
        assert_eq!(book.depth(Side::Bid, 0), Some((105, 7)));
    }

    #[test]
    fn limit_order_sweeps_levels_in_price_priority() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 2));
        book.submit(limit(2, Side::Ask, 101, 3));

        let result = book.submit(limit(3, Side::Bid, 101, 4));

        assert_eq!(result.remaining_quantity, 0);
        assert_eq!(fill_summary(&result.fills), vec![(1, 100, 2), (2, 101, 2)]);
        assert_eq!(book.best_ask(), Some(101));
        assert_eq!(book.depth(Side::Ask, 0), Some((101, 1)));
        assert_eq!(book.depth(Side::Ask, 1), None);
        assert_eq!(book.best_bid(), None);
    }

    #[test]
    fn price_time_priority_fills_oldest_order_first() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 2));
        book.submit(limit(2, Side::Ask, 100, 3));

        let result = book.submit(limit(3, Side::Bid, 100, 3));

        assert_eq!(result.remaining_quantity, 0);
        // Time priority: order 1 trades completely before order 2.
        assert_eq!(fill_summary(&result.fills), vec![(1, 100, 2), (2, 100, 1)]);
        assert_eq!(book.depth(Side::Ask, 0), Some((100, 2)));
        assert_eq!(book.best_ask(), Some(100));
    }

    #[test]
    fn partial_fill_keeps_maker_at_front_of_level() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 2));
        book.submit(limit(2, Side::Ask, 100, 3));

        let first = book.submit(limit(3, Side::Bid, 100, 3));

        assert_eq!(first.remaining_quantity, 0);
        assert_eq!(fill_summary(&first.fills), vec![(1, 100, 2), (2, 100, 1)]);
        assert!(first.fills.iter().all(|fill| fill.taker_order_id == 3));
        assert!(first.fills.iter().all(|fill| fill.taker_side == Side::Bid));
        assert_eq!(book.depth(Side::Ask, 0), Some((100, 2)));

        // The partially filled order 2 is still first in line.
        let second = book.submit(limit(4, Side::Bid, 100, 2));

        assert_eq!(second.remaining_quantity, 0);
        assert_eq!(fill_summary(&second.fills), vec![(2, 100, 2)]);
        assert_eq!(book.best_ask(), None);
    }

    #[test]
    fn market_order_ignores_price_and_sweeps_levels() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 2));
        book.submit(limit(2, Side::Ask, 101, 3));

        let result = book.submit(market(3, Side::Bid, 4));

        assert_eq!(result.remaining_quantity, 0);
        assert_eq!(fill_summary(&result.fills), vec![(1, 100, 2), (2, 101, 2)]);
        assert_eq!(book.best_ask(), Some(101));
        assert_eq!(book.depth(Side::Ask, 0), Some((101, 1)));
        assert_eq!(book.best_bid(), None);
    }

    #[test]
    fn market_order_partial_fill_does_not_rest() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 2));

        let result = book.submit(market(2, Side::Bid, 5));

        assert_eq!(result.remaining_quantity, 3);
        assert!(!result.rested);
        let (bids, asks) = book.snapshot(10);
        assert!(bids.is_empty());
        assert!(asks.is_empty());
    }

    #[test]
    fn market_order_on_empty_book_is_unfilled() {
        let mut book = OrderBook::new();

        let result = book.submit(market(1, Side::Bid, 5));

        assert_eq!(result.remaining_quantity, 5);
        assert!(result.fills.is_empty());
        assert!(!result.rested);
        let (bids, asks) = book.snapshot(10);
        assert!(bids.is_empty());
        assert!(asks.is_empty());
    }

    #[test]
    fn market_sell_hits_highest_bid_first() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Bid, 99, 2));
        book.submit(limit(2, Side::Bid, 100, 2));

        let result = book.submit(market(3, Side::Ask, 3));

        assert_eq!(result.remaining_quantity, 0);
        assert_eq!(fill_summary(&result.fills), vec![(2, 100, 2), (1, 99, 1)]);
        assert!(result.fills.iter().all(|fill| fill.taker_side == Side::Ask));
        assert_eq!(book.best_bid(), Some(99));
        assert_eq!(book.depth(Side::Bid, 0), Some((99, 1)));
        assert_eq!(book.best_ask(), None);
    }

    #[test]
    fn cancel_removes_order_and_empty_level() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Bid, 100, 5));

        let canceled = book.cancel_limit_order(1).unwrap();
        assert_eq!(canceled.id, 1);
        assert_eq!(canceled.quantity, 5);

        assert_eq!(book.best_bid(), None);
        assert_eq!(book.depth(Side::Bid, 0), None);
        let (bids, asks) = book.snapshot(10);
        assert!(bids.is_empty());
        assert!(asks.is_empty());

        // The removed order cannot be canceled twice.
        assert!(book.cancel_limit_order(1).is_none());
    }

    #[test]
    fn cancel_keeps_other_orders_at_same_level() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 2));
        book.submit(limit(2, Side::Ask, 100, 3));

        assert!(book.cancel_limit_order(1).is_some());
        assert_eq!(book.depth(Side::Ask, 0), Some((100, 3)));

        // Order 2 is still matchable at the same level.
        let result = book.submit(limit(3, Side::Bid, 100, 3));
        assert_eq!(result.remaining_quantity, 0);
        assert_eq!(fill_summary(&result.fills), vec![(2, 100, 3)]);
        assert_eq!(book.best_ask(), None);
    }

    #[test]
    fn cancel_unknown_order_returns_none() {
        let mut book = OrderBook::new();

        assert!(book.cancel_limit_order(42).is_none());

        book.submit(limit(1, Side::Bid, 100, 5));
        assert!(book.cancel_limit_order(42).is_none());
        assert_eq!(book.depth(Side::Bid, 0), Some((100, 5)));
    }

    #[test]
    fn filled_and_canceled_orders_disappear_from_cancellation() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 2));
        book.submit(limit(2, Side::Ask, 100, 3));

        // 吃掉订单 1，部分成交订单 2
        let result = book.submit(limit(3, Side::Bid, 100, 4));
        assert_eq!(result.remaining_quantity, 0);
        assert_eq!(book.depth(Side::Ask, 0), Some((100, 1)));

        // The fully filled order 1 is gone, not cancelable.
        assert!(book.cancel_limit_order(1).is_none());
        assert_eq!(book.depth(Side::Ask, 0), Some((100, 1)));

        // The partially filled order 2 can still be canceled for its remainder.
        let canceled = book.cancel_limit_order(2).unwrap();
        assert_eq!(canceled.id, 2);
        assert_eq!(canceled.quantity, 1);
        assert_eq!(book.depth(Side::Ask, 0), None);
        assert_eq!(book.best_ask(), None);
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
