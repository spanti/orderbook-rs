use std::collections::{BTreeMap, VecDeque};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderType {
    Limit,
    Market,
}
// Unique identifier for the OrderBook system
pub type OrderBookId = u64;

// Side of an order in the OrderBook
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Bid,
    Ask,
}

impl Side {
    pub fn opposite(&self) -> Side {
        match self {
            Side::Bid => Side::Ask,
            Side::Ask => Side::Bid,
        }
    }
}

// Structure representing an order in the OrderBook
#[derive(Debug, Clone)]
pub struct Order {
    pub id: OrderBookId,
    pub side: Side,
    pub price: u64,     // Price in ticks (smallest price increment)
    pub quantity: u64,  // Remaining quantity of the order
    pub timestamp: u64, // Timestamp of the order in milliseconds since epoch
    pub order_type: OrderType,
}

impl Order {
    // Check if the order crosses the given best price on the opposite side of the book
    pub fn crosses(&self, best_price: u64) -> bool {
        match self.side {
            Side::Bid => self.price >= best_price,
            Side::Ask => self.price <= best_price,
        }
    }
}

/// Orders at a single price level in the OrderBook
#[derive(Debug, Clone)]
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

    pub fn match_order(&mut self, incoming_order: &mut Order) -> u64 {
        while let Some(mut resting) = self.pop_front() {
            let traded = resting.quantity.min(incoming_order.quantity);
            resting.quantity -= traded;
            incoming_order.quantity -= traded;

            if resting.quantity > 0 {
                // Partially filled, put the remaining back to the front of the level
                self.orders.push_front(resting);
                break;
            }

            if incoming_order.quantity == 0 {
                break;
            }
        }

        incoming_order.quantity
    }

    pub fn total_volume(&self) -> u64 {
        self.orders.iter().map(|o| o.quantity).sum()
    }
}
// One side of the OrderBook (either Bid or Ask)
pub struct BookSide {
    pub side: Side,
    pub price_levels: BTreeMap<u64, PriceLevel>, // Price levels in the OrderBook side
}

impl BookSide {
    pub fn new(side: Side) -> Self {
        Self {
            side,
            price_levels: BTreeMap::new(),
        }
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
        price_level.add_order(order);
    }

    // Remove an order by its ID from the book side
    pub fn remove_order(&mut self, order_id: OrderBookId) -> Option<Order> {
        let (&price, level) = self.price_levels.iter_mut().find(|(_, level)| {
            level.orders.iter().any(|o| o.id == order_id)
        })?;

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
    pub fn match_order(&mut self, order: &mut Order) -> u64 {
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
            level.match_order(order);
            self.remove_level_if_empty(best_price);
        }
        order.quantity
    }
}

// Structure representing the full OrderBook with both Bid and Ask sides
pub struct OrderBook {
    bids: BookSide,
    asks: BookSide,
    next_order_id: OrderBookId,
}

impl OrderBook {
    pub fn new() -> Self {
        Self {
            bids: BookSide::new(Side::Bid),
            asks: BookSide::new(Side::Ask),
            next_order_id: 1,
        }
    }

    // submit a new limit order; returns remaining quantity if not fully filled
    pub fn submit_limit_order(&mut self, mut order: Order) -> u64 {
        debug_assert_eq!(order.order_type, OrderType::Limit);

        let (own_side, other_side) = match order.side {
            Side::Bid => (&mut self.bids, &mut self.asks), // Bid side is own side, Ask side is other side
            Side::Ask => (&mut self.asks, &mut self.bids), // Ask side is own side, Bid side is other side
        };

        // Attempt to match the order against the other side
        let remaining_quantity = other_side.match_order(&mut order);

        if remaining_quantity > 0 {
            own_side.insert_order(order);
        }

        remaining_quantity
    }

    // cancel limit order by ID; returns true if the order was found and removed
    pub fn cancel_limit_order(&mut self, order_id: OrderBookId) -> bool {
        if self.bids.remove_order(order_id).is_some() {
            return true;
        }
        if self.asks.remove_order(order_id).is_some() {
            return true;
        }
        false
    }

    // submit market order; market orders match against the best available prices until filled or no more liquidity
    pub fn submit_market_order(&mut self, mut order: Order) -> u64 {
        debug_assert_eq!(order.order_type, OrderType::Market);
        let other_side = match order.side {
            Side::Bid => &mut self.asks, // Bid side is own side, Ask side is other side
            Side::Ask => &mut self.bids, // Ask side is own side, Bid side is other side
        };

        let remaining_quantity = other_side.match_order(&mut order);

        remaining_quantity
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

        let remaining = book.submit_limit_order(limit(1, Side::Bid, 100, 5));

        assert_eq!(remaining, 5);
        assert_eq!(book.bids.best_price_level(), Some(100));
        assert_eq!(level_volume(&book, Side::Bid, 100), 5);
        assert!(book.asks.price_levels.is_empty());
    }

    #[test]
    fn limit_bid_matches_resting_ask_at_same_price() {
        let mut book = OrderBook::new();
        book.submit_limit_order(limit(1, Side::Ask, 100, 5));

        let remaining = book.submit_limit_order(limit(2, Side::Bid, 100, 5));

        assert_eq!(remaining, 0);
        assert!(book.asks.price_levels.is_empty());
        assert!(book.bids.price_levels.is_empty());
    }

    #[test]
    fn limit_ask_matches_resting_bid_at_same_price() {
        let mut book = OrderBook::new();
        book.submit_limit_order(limit(1, Side::Bid, 100, 5));

        let remaining = book.submit_limit_order(limit(2, Side::Ask, 100, 5));

        assert_eq!(remaining, 0);
        assert!(book.asks.price_levels.is_empty());
        assert!(book.bids.price_levels.is_empty());
    }

    #[test]
    fn limit_bid_below_best_ask_rests() {
        let mut book = OrderBook::new();
        book.submit_limit_order(limit(1, Side::Ask, 100, 5));

        let remaining = book.submit_limit_order(limit(2, Side::Bid, 99, 5));

        assert_eq!(remaining, 5);
        assert_eq!(level_volume(&book, Side::Ask, 100), 5);
        assert_eq!(level_volume(&book, Side::Bid, 99), 5);
    }

    #[test]
    fn limit_ask_above_best_bid_rests() {
        let mut book = OrderBook::new();
        book.submit_limit_order(limit(1, Side::Bid, 100, 5));

        let remaining = book.submit_limit_order(limit(2, Side::Ask, 101, 5));

        assert_eq!(remaining, 5);
        assert_eq!(level_volume(&book, Side::Bid, 100), 5);
        assert_eq!(level_volume(&book, Side::Ask, 101), 5);
    }

    #[test]
    fn limit_order_partial_fill_rests_remainder() {
        let mut book = OrderBook::new();
        book.submit_limit_order(limit(1, Side::Ask, 100, 3));

        let remaining = book.submit_limit_order(limit(2, Side::Bid, 105, 10));

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

        let remaining = book.submit_limit_order(limit(3, Side::Bid, 101, 4));

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

        let remaining = book.submit_limit_order(limit(3, Side::Bid, 100, 3));

        assert_eq!(remaining, 0);
        assert_eq!(order_ids_at(&book, Side::Ask, 100), vec![2]);
        assert_eq!(level_volume(&book, Side::Ask, 100), 2);
    }

    #[test]
    fn market_order_ignores_price_and_sweeps_levels() {
        let mut book = OrderBook::new();
        book.submit_limit_order(limit(1, Side::Ask, 100, 2));
        book.submit_limit_order(limit(2, Side::Ask, 101, 3));

        let remaining = book.submit_market_order(market(3, Side::Bid, 4));

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

        let remaining = book.submit_market_order(market(2, Side::Bid, 5));

        assert_eq!(remaining, 3);
        assert!(book.asks.price_levels.is_empty());
        assert!(book.bids.price_levels.is_empty());
    }

    #[test]
    fn market_order_on_empty_book_is_unfilled() {
        let mut book = OrderBook::new();

        let remaining = book.submit_market_order(market(1, Side::Bid, 5));

        assert_eq!(remaining, 5);
        assert!(book.bids.price_levels.is_empty());
        assert!(book.asks.price_levels.is_empty());
    }

    #[test]
    fn market_sell_hits_highest_bid_first() {
        let mut book = OrderBook::new();
        book.submit_limit_order(limit(1, Side::Bid, 99, 2));
        book.submit_limit_order(limit(2, Side::Bid, 100, 2));

        let remaining = book.submit_market_order(market(3, Side::Ask, 3));

        assert_eq!(remaining, 0);
        assert!(!book.bids.price_levels.contains_key(&100));
        assert_eq!(book.bids.best_price_level(), Some(99));
        assert_eq!(level_volume(&book, Side::Bid, 99), 1);
    }

    #[test]
    fn cancel_removes_order_and_empty_level() {
        let mut book = OrderBook::new();
        book.submit_limit_order(limit(1, Side::Bid, 100, 5));

        assert!(book.cancel_limit_order(1));
        assert!(book.bids.price_levels.is_empty());
        assert_eq!(book.bids.best_price_level(), None);
    }

    #[test]
    fn cancel_keeps_other_orders_at_same_level() {
        let mut book = OrderBook::new();
        book.submit_limit_order(limit(1, Side::Ask, 100, 2));
        book.submit_limit_order(limit(2, Side::Ask, 100, 3));

        assert!(book.cancel_limit_order(1));
        assert_eq!(order_ids_at(&book, Side::Ask, 100), vec![2]);
        assert_eq!(level_volume(&book, Side::Ask, 100), 3);
    }

    #[test]
    fn cancel_unknown_order_returns_false() {
        let mut book = OrderBook::new();

        assert!(!book.cancel_limit_order(42));
    }

    #[test]
    fn price_level_partial_fill_keeps_order_at_front() {
        let mut level = PriceLevel::new(100);
        level.add_order(limit(1, Side::Bid, 100, 2));
        level.add_order(limit(2, Side::Bid, 100, 3));

        let mut incoming = limit(3, Side::Ask, 100, 3);
        let remaining = level.match_order(&mut incoming);

        assert_eq!(remaining, 0);
        assert_eq!(incoming.quantity, 0);
        assert_eq!(level.orders.len(), 1);
        assert_eq!(level.orders[0].id, 2);
        assert_eq!(level.orders[0].quantity, 2);
        assert_eq!(level.total_volume(), 2);
    }
}
