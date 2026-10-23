use std::collections::{BTreeMap, HashMap, VecDeque};

use crate::types::{Fill, MatchOutcome, Order, OrderBookId, OrderType, Side, SubmitResult};

/// Orders at a single price level in the OrderBook
#[derive(Debug, Clone, PartialEq)]
pub struct PriceLevel {
    pub price: u64,              // Price level in ticks (smallest price increment)
    pub orders: VecDeque<Order>, // Orders at this price level in the OrderBook
}

impl PriceLevel {
    pub fn new(price: u64) -> Self {
        Self {
            price,
            orders: VecDeque::new(),
        }
    }

    pub fn add_order(&mut self, order: Order) {
        self.orders.push_back(order);
    }

    pub fn pop_front(&mut self) -> Option<Order> {
        self.orders.pop_front()
    }

    pub fn match_order(&mut self, incoming_order: &mut Order) -> MatchOutcome {
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
                self.orders.push_front(resting);
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
        self.orders.iter().map(|o| o.quantity).sum()
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
        let (_ , price) = self.order_map.remove(&order_id)?;
        
        let level = self.price_levels.get_mut(&price)?;

        let pos = level.orders.iter().position(|o| o.id == order_id)?;

        let removed = level.orders.remove(pos)?;
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

        while order.quantity > 0 {
            // Peek best opposite price
            let Some(best_price) = self.best_price_level() else {
                break;
            };
            
            // Check if the best price is matchable
            if order.order_type == OrderType::Limit && !order.crosses(best_price) {
                break;
            }

            let Some(level) = self.price_levels.get_mut(&best_price) else {
                break;
            };

            // Match the order with the best price level on the other side
            let outcome = level.match_order(order);
            outcome.filled_order_ids.iter().for_each(|id| { self.order_map.remove(id); });
            filled.extend(outcome.fills);
            
            self.remove_level_if_empty(best_price);
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

    // submit a new limit order; returns remaining quantity if not fully filled
    pub fn submit_limit_order(&mut self, mut order: Order) -> SubmitResult {
        debug_assert_eq!(order.order_type, OrderType::Limit);

        let (own_side, other_side) = match order.side {
            Side::Bid => (&mut self.bids, &mut self.asks), // Bid side is own side, Ask side is other side
            Side::Ask => (&mut self.asks, &mut self.bids), // Ask side is own side, Bid side is other side
        };

        // Attempt to match the order against the other side
        let fills = other_side.match_order(&mut order);
        let remaining_quantity = order.quantity;

        if remaining_quantity > 0 {
            own_side.insert_order(order);
        }

        SubmitResult {
            remaining_quantity,
            fills,
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

    // submit market order; market orders match against the best available prices until filled or no more liquidity
    pub fn submit_market_order(&mut self, mut order: Order) -> SubmitResult {
        debug_assert_eq!(order.order_type, OrderType::Market);
        let other_side = match order.side {
            Side::Bid => &mut self.asks, // Bid side is own side, Ask side is other side
            Side::Ask => &mut self.bids, // Ask side is own side, Bid side is other side
        };

        let fills = other_side.match_order(&mut order);
        let remaining_quantity = order.quantity;

        SubmitResult {
            remaining_quantity,
            fills,
        }
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

    fn limit(id: OrderBookId, side: Side, price: u64, quantity: u64) -> Order {
        Order {
            id,
            side,
            price,
            quantity,
            timestamp: id,
            order_type: OrderType::Limit,
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
        book.submit_limit_order(limit(1, Side::Bid, 99, 1));
        book.submit_limit_order(limit(2, Side::Bid, 101, 2));
        book.submit_limit_order(limit(3, Side::Ask, 105, 3));
        book.submit_limit_order(limit(4, Side::Ask, 103, 4));

        assert_eq!(book.best_bid(), Some(101));
        assert_eq!(book.best_ask(), Some(103));
    }

    #[test]
    fn depth_returns_levels_in_price_priority() {
        let mut book = OrderBook::new();
        book.submit_limit_order(limit(1, Side::Bid, 99, 1));
        book.submit_limit_order(limit(2, Side::Bid, 101, 2));
        book.submit_limit_order(limit(3, Side::Bid, 100, 3));
        book.submit_limit_order(limit(4, Side::Ask, 105, 4));
        book.submit_limit_order(limit(5, Side::Ask, 103, 5));
        book.submit_limit_order(limit(6, Side::Ask, 104, 6));

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
        book.submit_limit_order(limit(1, Side::Bid, 100, 2));
        book.submit_limit_order(limit(2, Side::Bid, 100, 3));

        assert_eq!(book.depth(Side::Bid, 0), Some((100, 5)));
        assert_eq!(book.depth(Side::Bid, 1), None);
    }

    #[test]
    fn depth_reflects_fills_and_cancellations() {
        let mut book = OrderBook::new();
        book.submit_limit_order(limit(1, Side::Ask, 100, 5));
        book.submit_limit_order(limit(2, Side::Ask, 101, 2));

        // 部分成交后，100 档剩余 2
        book.submit_limit_order(limit(3, Side::Bid, 100, 3));
        assert_eq!(book.depth(Side::Ask, 0), Some((100, 2)));

        // 撤掉 100 档剩余后，最优卖价变为 101
        assert!(book.cancel_limit_order(1).is_some());
        assert_eq!(book.depth(Side::Ask, 0), Some((101, 2)));
        assert_eq!(book.depth(Side::Ask, 1), None);

        // 市价单吃光后，深度为空
        book.submit_market_order(market(4, Side::Bid, 2));
        assert_eq!(book.best_ask(), None);
        assert_eq!(book.depth(Side::Ask, 0), None);
    }

    #[test]
    fn best_price_is_highest_bid_and_lowest_ask() {
        let mut book = OrderBook::new();
        book.submit_limit_order(limit(1, Side::Bid, 97, 1));
        book.submit_limit_order(limit(2, Side::Bid, 99, 1));
        book.submit_limit_order(limit(3, Side::Bid, 98, 1));
        book.submit_limit_order(limit(4, Side::Ask, 102, 1));
        book.submit_limit_order(limit(5, Side::Ask, 100, 1));
        book.submit_limit_order(limit(6, Side::Ask, 101, 1));

        assert_eq!(book.bids.best_price_level(), Some(99));
        assert_eq!(book.asks.best_price_level(), Some(100));
    }

    #[test]
    fn limit_order_rests_on_empty_book() {
        let mut book = OrderBook::new();

        let remaining = book.submit_limit_order(limit(1, Side::Bid, 100, 5)).remaining_quantity;

        assert_eq!(remaining, 5);
        assert_eq!(book.bids.best_price_level(), Some(100));
        assert_eq!(level_volume(&book, Side::Bid, 100), 5);
        assert!(book.asks.price_levels.is_empty());
    }

    #[test]
    fn limit_bid_matches_resting_ask_at_same_price() {
        let mut book = OrderBook::new();
        book.submit_limit_order(limit(1, Side::Ask, 100, 5));

        let remaining = book.submit_limit_order(limit(2, Side::Bid, 100, 5)).remaining_quantity;

        assert_eq!(remaining, 0);
        assert!(book.asks.price_levels.is_empty());
        assert!(book.bids.price_levels.is_empty());
    }

    #[test]
    fn limit_ask_matches_resting_bid_at_same_price() {
        let mut book = OrderBook::new();
        book.submit_limit_order(limit(1, Side::Bid, 100, 5));

        let remaining = book.submit_limit_order(limit(2, Side::Ask, 100, 5)).remaining_quantity;

        assert_eq!(remaining, 0);
        assert!(book.asks.price_levels.is_empty());
        assert!(book.bids.price_levels.is_empty());
    }

    #[test]
    fn limit_bid_below_best_ask_rests() {
        let mut book = OrderBook::new();
        book.submit_limit_order(limit(1, Side::Ask, 100, 5));

        let remaining = book.submit_limit_order(limit(2, Side::Bid, 99, 5)).remaining_quantity;

        assert_eq!(remaining, 5);
        assert_eq!(level_volume(&book, Side::Ask, 100), 5);
        assert_eq!(level_volume(&book, Side::Bid, 99), 5);
    }

    #[test]
    fn limit_ask_above_best_bid_rests() {
        let mut book = OrderBook::new();
        book.submit_limit_order(limit(1, Side::Bid, 100, 5));

        let remaining = book.submit_limit_order(limit(2, Side::Ask, 101, 5)).remaining_quantity;

        assert_eq!(remaining, 5);
        assert_eq!(level_volume(&book, Side::Bid, 100), 5);
        assert_eq!(level_volume(&book, Side::Ask, 101), 5);
    }

    #[test]
    fn limit_order_partial_fill_rests_remainder() {
        let mut book = OrderBook::new();
        book.submit_limit_order(limit(1, Side::Ask, 100, 3));

        let remaining = book.submit_limit_order(limit(2, Side::Bid, 105, 10)).remaining_quantity;

        assert_eq!(remaining, 7);
        assert!(book.asks.price_levels.is_empty());
        assert_eq!(book.bids.best_price_level(), Some(105));
        assert_eq!(level_volume(&book, Side::Bid, 105), 7);
    }

    #[test]
    fn limit_order_sweeps_levels_in_price_priority() {
        let mut book = OrderBook::new();
        book.submit_limit_order(limit(1, Side::Ask, 100, 2));
        book.submit_limit_order(limit(2, Side::Ask, 101, 3));

        let remaining = book.submit_limit_order(limit(3, Side::Bid, 101, 4)).remaining_quantity;

        assert_eq!(remaining, 0);
        assert!(!book.asks.price_levels.contains_key(&100));
        assert_eq!(book.asks.best_price_level(), Some(101));
        assert_eq!(level_volume(&book, Side::Ask, 101), 1);
        assert!(book.bids.price_levels.is_empty());
    }

    #[test]
    fn price_time_priority_fills_oldest_order_first() {
        let mut book = OrderBook::new();
        book.submit_limit_order(limit(1, Side::Ask, 100, 2));
        book.submit_limit_order(limit(2, Side::Ask, 100, 3));

        let remaining = book.submit_limit_order(limit(3, Side::Bid, 100, 3)).remaining_quantity;

        assert_eq!(remaining, 0);
        assert_eq!(order_ids_at(&book, Side::Ask, 100), vec![2]);
        assert_eq!(level_volume(&book, Side::Ask, 100), 2);
    }

    #[test]
    fn market_order_ignores_price_and_sweeps_levels() {
        let mut book = OrderBook::new();
        book.submit_limit_order(limit(1, Side::Ask, 100, 2));
        book.submit_limit_order(limit(2, Side::Ask, 101, 3));

        let remaining = book.submit_market_order(market(3, Side::Bid, 4)).remaining_quantity;

        assert_eq!(remaining, 0);
        assert!(!book.asks.price_levels.contains_key(&100));
        assert_eq!(book.asks.best_price_level(), Some(101));
        assert_eq!(level_volume(&book, Side::Ask, 101), 1);
        assert!(book.bids.price_levels.is_empty());
    }

    #[test]
    fn market_order_partial_fill_does_not_rest() {
        let mut book = OrderBook::new();
        book.submit_limit_order(limit(1, Side::Ask, 100, 2));

        let remaining = book.submit_market_order(market(2, Side::Bid, 5)).remaining_quantity;

        assert_eq!(remaining, 3);
        assert!(book.asks.price_levels.is_empty());
        assert!(book.bids.price_levels.is_empty());
    }

    #[test]
    fn market_order_on_empty_book_is_unfilled() {
        let mut book = OrderBook::new();

        let remaining = book.submit_market_order(market(1, Side::Bid, 5)).remaining_quantity;

        assert_eq!(remaining, 5);
        assert!(book.bids.price_levels.is_empty());
        assert!(book.asks.price_levels.is_empty());
    }

    #[test]
    fn market_sell_hits_highest_bid_first() {
        let mut book = OrderBook::new();
        book.submit_limit_order(limit(1, Side::Bid, 99, 2));
        book.submit_limit_order(limit(2, Side::Bid, 100, 2));

        let remaining = book.submit_market_order(market(3, Side::Ask, 3)).remaining_quantity;

        assert_eq!(remaining, 0);
        assert!(!book.bids.price_levels.contains_key(&100));
        assert_eq!(book.bids.best_price_level(), Some(99));
        assert_eq!(level_volume(&book, Side::Bid, 99), 1);
    }

    #[test]
    fn cancel_removes_order_and_empty_level() {
        let mut book = OrderBook::new();
        book.submit_limit_order(limit(1, Side::Bid, 100, 5));

        let canceled = book.cancel_limit_order(1).unwrap();
        assert_eq!(canceled.id, 1);
        assert_eq!(canceled.quantity, 5);
        assert!(book.bids.price_levels.is_empty());
        assert_eq!(book.bids.best_price_level(), None);
    }

    #[test]
    fn cancel_keeps_other_orders_at_same_level() {
        let mut book = OrderBook::new();
        book.submit_limit_order(limit(1, Side::Ask, 100, 2));
        book.submit_limit_order(limit(2, Side::Ask, 100, 3));

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
        book.submit_limit_order(limit(1, Side::Ask, 100, 2));
        book.submit_limit_order(limit(2, Side::Ask, 100, 3));
        assert_eq!(book.asks.order_map.get(&1), Some(&(Side::Ask, 100)));

        // 吃掉订单 1，部分成交订单 2
        let remaining = book.submit_limit_order(limit(3, Side::Bid, 100, 4)).remaining_quantity;

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
        book.submit_limit_order(limit(1, Side::Ask, 100, 2));

        let result = book.submit_limit_order(limit(2, Side::Bid, 105, 2));

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
        book.submit_limit_order(limit(1, Side::Ask, 100, 2));
        book.submit_limit_order(limit(2, Side::Ask, 101, 3));

        let result = book.submit_limit_order(limit(3, Side::Bid, 101, 4));

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
        book.submit_limit_order(limit(1, Side::Ask, 100, 3));

        let result = book.submit_limit_order(limit(2, Side::Bid, 105, 10));

        assert_eq!(result.remaining_quantity, 7);
        assert_eq!(result.fills.len(), 1);
        assert_eq!(result.fills[0].quantity, 3);
        assert_eq!(result.fills[0].price, 100);
    }

    #[test]
    fn market_order_records_fills_at_resting_prices() {
        let mut book = OrderBook::new();
        book.submit_limit_order(limit(1, Side::Ask, 100, 2));
        book.submit_limit_order(limit(2, Side::Ask, 101, 3));

        let result = book.submit_market_order(market(3, Side::Bid, 4));

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
        book.submit_limit_order(limit(1, Side::Ask, 100, 5));

        let result = book.submit_limit_order(limit(2, Side::Bid, 99, 5));

        assert_eq!(result.remaining_quantity, 5);
        assert!(result.fills.is_empty());
    }
}
