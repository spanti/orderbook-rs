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

    /// Next price to visit in an overlay of orders added by rests.
    ///
    /// The overlay shares the same price-priority direction as the base book,
    /// so [`BookView::plan`] can merge the two walks lazily.
    fn next_added_price(&self, levels: &BTreeMap<u64, VecDeque<Order>>) -> Option<u64> {
        let next = match self.side {
            Side::Bid => levels.range((self.start, self.end)).next_back(),
            Side::Ask => levels.range((self.start, self.end)).next(),
        };
        next.map(|(price, _)| *price)
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

    /// Price levels in ascending price order, each with its orders in FIFO
    /// order. Used when exporting a restorable engine snapshot.
    fn export_levels(&self) -> Vec<(u64, Vec<Order>)> {
        self.price_levels
            .iter()
            .map(|(&price, level)| (price, level.orders.iter().cloned().collect()))
            .collect()
    }

    /// Rebuild a side from exported levels without running matching. Price
    /// level totals and the order lookup index are derived from the orders.
    fn from_levels(side: Side, levels: Vec<(u64, Vec<Order>)>) -> Self {
        let mut book_side = Self::new(side);
        for (price, orders) in levels {
            if orders.is_empty() {
                continue;
            }
            let mut level = PriceLevel::new(price);
            for order in orders {
                debug_assert_eq!(
                    order.side, side,
                    "order {} was exported under the wrong book side",
                    order.id
                );
                book_side.order_map.insert(order.id, (side, price));
                level.add_order(order);
            }
            book_side.price_levels.insert(price, level);
        }
        book_side
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

    /// Reduce a resting order by `quantity`, removing it when it reaches zero.
    ///
    /// Returns `false` when this side does not own the order. The order keeps
    /// its FIFO position while partially filled.
    fn reduce_order(&mut self, order_id: OrderBookId, quantity: u64) -> bool {
        let Some(&(_, price)) = self.order_map.get(&order_id) else {
            return false;
        };
        let Some(level) = self.price_levels.get_mut(&price) else {
            return false;
        };
        let Some(position) = level.orders.iter().position(|order| order.id == order_id) else {
            return false;
        };

        debug_assert!(
            quantity <= level.orders[position].quantity,
            "fill {quantity} exceeds resting quantity {} for order {order_id}",
            level.orders[position].quantity
        );
        level.orders[position].quantity -= quantity;
        level.total_quantity -= quantity;

        if level.orders[position].quantity == 0 {
            level.orders.remove(position);
            self.order_map.remove(&order_id);
            self.remove_level_if_empty(price);
        }
        true
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

    /// Export both sides as `(price, FIFO orders)` pairs in ascending price
    /// order. Used to capture a restorable snapshot of this book.
    pub(crate) fn export_levels(&self) -> (Vec<(u64, Vec<Order>)>, Vec<(u64, Vec<Order>)>) {
        (self.bids.export_levels(), self.asks.export_levels())
    }

    /// Rebuild a book from exported levels without running any matching.
    ///
    /// Price-level totals and the order lookup index are recomputed from the
    /// stored orders, so the restored book matches identically.
    pub(crate) fn from_levels(bids: Vec<(u64, Vec<Order>)>, asks: Vec<(u64, Vec<Order>)>) -> Self {
        Self {
            bids: BookSide::from_levels(Side::Bid, bids),
            asks: BookSide::from_levels(Side::Ask, asks),
        }
    }

    /// Insert an order directly at its price level, without matching.
    ///
    /// This is the `OrderRested` apply primitive: replay must fold an already
    /// decided rest, never re-run matching. FIFO priority follows insertion
    /// order within the level, exactly as [`OrderBook::submit`] would place it.
    pub fn rest_order(&mut self, order: Order) {
        let own_side = match order.side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        own_side.insert_order(order);
    }

    /// Reduce a resting order by the traded quantity, removing it at zero.
    ///
    /// This is the `TradeExecuted` apply primitive. Replay applies an already
    /// matched fill and must never call [`OrderBook::submit`] again.
    pub fn reduce_order(&mut self, order_id: OrderBookId, quantity: u64) {
        let reduced = self.bids.reduce_order(order_id, quantity)
            || self.asks.reduce_order(order_id, quantity);
        debug_assert!(
            reduced,
            "trade references maker order {order_id}, which is not resting"
        );
    }

    /// Whether `order_id` is currently resting on either side.
    pub fn contains_order(&self, order_id: OrderBookId) -> bool {
        self.bids.order_map.contains_key(&order_id) || self.asks.order_map.contains_key(&order_id)
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

/// A borrowed, copy-on-write decision view of one symbol's [`OrderBook`].
///
/// `decide` walks the base price levels and an overlay of the fills and rests
/// it has already planned. Nothing is cloned: the base book stays immutable and
/// the overlay records only the touched orders and levels, so a plan is
/// O(touched levels/orders) instead of O(resting orders).
///
/// The overlay reproduces [`OrderBook::submit`] exactly:
///
/// * base orders at a price keep their FIFO position; a nonzero override is a
///   partial fill and a zero override removes a fully filled/canceled order;
/// * orders added by an earlier rest are appended after any base orders at the
///   same price, so later plans in the same stop cascade can match them;
/// * a partially filled maker keeps its position, a fully filled maker
///   disappears, and the taker remainder is appended to its own price level.
#[derive(Debug)]
pub(crate) struct BookView<'a> {
    book: Option<&'a OrderBook>,
    overlay: BookOverlay,
}

impl<'a> BookView<'a> {
    pub(crate) fn new(book: Option<&'a OrderBook>) -> Self {
        Self {
            book,
            overlay: BookOverlay::default(),
        }
    }

    /// Non-mutating equivalent of [`OrderBook::submit`].
    ///
    /// Matches the order against the base book plus earlier planned fills and
    /// rests, then records its own fills and remainder in the overlay. It never
    /// mutates the borrowed book, so `decide` stays pure.
    pub(crate) fn plan(&mut self, mut order: Order) -> SubmitResult {
        let resting_side = order.side.opposite();
        let book = self.book;
        let overlay = &mut self.overlay;

        let mut fills = Vec::new();
        let mut base_walk = MatchWalk::new(resting_side, &order);
        let mut added_walk = MatchWalk::new(resting_side, &order);

        while order.quantity > 0 {
            let next_base =
                book.and_then(|book| base_walk.next_price(side_levels(book, resting_side)));
            let next_added = added_walk.next_added_price(overlay.added_levels(resting_side));

            let Some(price) = pick_price(resting_side, next_base, next_added) else {
                break;
            };

            // A price held by both sources is matched in one step: base orders
            // first, then orders added by earlier rests.
            let base_at_price = next_base == Some(price);
            let added_at_price = next_added == Some(price);
            if base_at_price {
                base_walk.advance_past(price);
            }
            if added_at_price {
                added_walk.advance_past(price);
            }

            if base_at_price {
                let level = &side_levels(
                    book.expect("a base price level implies a base book"),
                    resting_side,
                )[&price];
                for resting in &level.orders {
                    let quantity = overlay.current_quantity(resting);
                    if quantity == 0 {
                        continue;
                    }
                    let traded = quantity.min(order.quantity);
                    fills.push(Fill {
                        maker_order_id: resting.id,
                        taker_order_id: order.id,
                        price: resting.price,
                        quantity: traded,
                        taker_side: order.side,
                    });
                    order.quantity -= traded;

                    if traded < quantity {
                        // The taker ran out; the maker keeps its position.
                        overlay.quantities.insert(resting.id, quantity - traded);
                        break;
                    }
                    overlay.quantities.insert(resting.id, 0);
                    if order.quantity == 0 {
                        break;
                    }
                }
            }

            if added_at_price && order.quantity > 0 {
                let level = overlay
                    .added_levels_mut(resting_side)
                    .get_mut(&price)
                    .expect("added price level disappeared");
                while order.quantity > 0 {
                    let Some(maker) = level.front() else {
                        break;
                    };
                    let maker_quantity = maker.quantity;
                    if maker_quantity == 0 {
                        level.pop_front();
                        continue;
                    }
                    let maker_id = maker.id;
                    let maker_price = maker.price;
                    let traded = maker_quantity.min(order.quantity);
                    fills.push(Fill {
                        maker_order_id: maker_id,
                        taker_order_id: order.id,
                        price: maker_price,
                        quantity: traded,
                        taker_side: order.side,
                    });
                    order.quantity -= traded;

                    if traded == maker_quantity {
                        level.pop_front();
                    } else {
                        // The taker ran out; the maker keeps its position.
                        level
                            .front_mut()
                            .expect("maker vanished while partially filled")
                            .quantity -= traded;
                        break;
                    }
                }
            }
        }

        let remaining_quantity = order.quantity;
        let rested = remaining_quantity > 0 && order.rests();

        if rested {
            overlay
                .added_levels_mut(order.side)
                .entry(order.price)
                .or_default()
                .push_back(Order {
                    quantity: remaining_quantity,
                    ..order
                });
        }

        SubmitResult {
            remaining_quantity,
            fills,
            rested,
        }
    }

    /// Non-mutating equivalent of [`OrderBook::cancel_limit_order`].
    ///
    /// Returns the order with its current (overlay-aware) remaining quantity,
    /// or `None` when the order was already filled or canceled. The removal is
    /// recorded in the overlay and affects subsequent plans.
    pub(crate) fn cancel_limit_order(&mut self, order_id: OrderBookId) -> Option<Order> {
        if let Some(order) = self.overlay.remove_added(order_id) {
            return Some(order);
        }

        let book = self.book?;
        for side in [Side::Bid, Side::Ask] {
            let book_side = match side {
                Side::Bid => &book.bids,
                Side::Ask => &book.asks,
            };
            let Some(&(_, price)) = book_side.order_map.get(&order_id) else {
                continue;
            };
            let Some(level) = book_side.price_levels.get(&price) else {
                continue;
            };
            let Some(order) = level.orders.iter().find(|order| order.id == order_id) else {
                continue;
            };
            let quantity = self.overlay.current_quantity(order);
            if quantity == 0 {
                return None;
            }
            self.overlay.quantities.insert(order_id, 0);
            return Some(Order {
                quantity,
                ..order.clone()
            });
        }
        None
    }
}

/// The borrowed book's price levels on `side`.
fn side_levels(book: &OrderBook, side: Side) -> &BTreeMap<u64, PriceLevel> {
    match side {
        Side::Bid => &book.bids.price_levels,
        Side::Ask => &book.asks.price_levels,
    }
}

/// The better of the next base and added prices, if either exists.
fn pick_price(side: Side, base: Option<u64>, added: Option<u64>) -> Option<u64> {
    match (base, added) {
        (Some(base), Some(added)) => Some(match side {
            Side::Bid => base.max(added),
            Side::Ask => base.min(added),
        }),
        (Some(price), None) | (None, Some(price)) => Some(price),
        (None, None) => None,
    }
}

/// Copy-on-write deltas produced by plans against one borrowed [`OrderBook`].
#[derive(Debug, Default)]
struct BookOverlay {
    /// Remaining quantity for an order whose quantity differs from the base
    /// book. `0` means the order was fully filled or canceled.
    quantities: HashMap<OrderBookId, u64>,
    /// Orders added by rests in this decision, per price, in FIFO order. Their
    /// live quantity is stored on the order itself.
    added_bids: BTreeMap<u64, VecDeque<Order>>,
    added_asks: BTreeMap<u64, VecDeque<Order>>,
}

impl BookOverlay {
    fn current_quantity(&self, order: &Order) -> u64 {
        self.quantities
            .get(&order.id)
            .copied()
            .unwrap_or(order.quantity)
    }

    fn added_levels(&self, side: Side) -> &BTreeMap<u64, VecDeque<Order>> {
        match side {
            Side::Bid => &self.added_bids,
            Side::Ask => &self.added_asks,
        }
    }

    fn added_levels_mut(&mut self, side: Side) -> &mut BTreeMap<u64, VecDeque<Order>> {
        match side {
            Side::Bid => &mut self.added_bids,
            Side::Ask => &mut self.added_asks,
        }
    }

    /// Remove an order that a prior rest added during this decision.
    fn remove_added(&mut self, order_id: OrderBookId) -> Option<Order> {
        for levels in [&mut self.added_bids, &mut self.added_asks] {
            for level in levels.values_mut() {
                if let Some(position) = level.iter().position(|order| order.id == order_id) {
                    return level.remove(position);
                }
            }
        }
        None
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

    fn fok_limit(id: OrderBookId, side: Side, price: u64, quantity: u64) -> Order {
        Order {
            time_in_force: TimeInForce::FOK,
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

    /// `BookView::plan` must agree with `OrderBook::submit` on the result and
    /// must leave the borrowed book untouched.
    fn assert_plan_matches_submit(book: &OrderBook, order: Order) {
        let before = book.clone();
        let mut view = BookView::new(Some(book));
        let planned = view.plan(order.clone());

        let mut reference = book.clone();
        let submitted = reference.submit(order);

        assert_eq!(
            planned.remaining_quantity, submitted.remaining_quantity,
            "remaining quantity diverged"
        );
        assert_eq!(planned.rested, submitted.rested, "rest verdict diverged");
        assert_eq!(
            fill_summary(&planned.fills),
            fill_summary(&submitted.fills),
            "fills diverged"
        );
        assert_eq!(*book, before, "a plan must not mutate the borrowed book");
    }

    #[test]
    fn book_view_plan_matches_submit_for_limit_gtc_resting_and_empty_book() {
        let empty = OrderBook::new();
        assert_plan_matches_submit(&empty, limit(1, Side::Bid, 100, 5));

        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 101, 1));
        assert_plan_matches_submit(&book, limit(2, Side::Bid, 100, 5));

        // A post-only order that does not cross is just a resting limit order;
        // `post_only` is event metadata and does not reach `submit`.
        assert_plan_matches_submit(&book, limit(3, Side::Bid, 100, 2));
    }

    #[test]
    fn book_view_plan_matches_submit_for_partial_fill_and_rest() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 3));

        assert_plan_matches_submit(&book, limit(2, Side::Bid, 105, 10));
    }

    #[test]
    fn book_view_plan_matches_submit_for_multi_level_sweeps() {
        let asks = ask_ladder();
        assert_plan_matches_submit(&asks, limit(10, Side::Bid, 100, 10));

        let bids = bid_ladder();
        assert_plan_matches_submit(&bids, limit(20, Side::Ask, 100, 10));
    }

    #[test]
    fn book_view_plan_matches_submit_for_discarded_residuals() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 2));

        // Market/IOC remainder never rests.
        assert_plan_matches_submit(&book, market(2, Side::Bid, 5));
        assert_plan_matches_submit(&book, ioc_limit(3, Side::Bid, 100, 5));
    }

    #[test]
    fn book_view_plan_matches_submit_for_a_fok_sized_fill() {
        let mut book = OrderBook::new();
        book.submit(limit(1, Side::Ask, 100, 2));
        book.submit(limit(2, Side::Ask, 101, 3));

        // Policy has already guaranteed the liquidity; `submit` fills it.
        assert_plan_matches_submit(&book, fok_limit(3, Side::Bid, 101, 5));
    }

    #[test]
    fn book_view_plan_sequence_matches_submit_sequence() {
        let mut base = OrderBook::new();
        base.submit(limit(1, Side::Ask, 100, 2));

        let mut reference = base.clone();
        let mut view = BookView::new(Some(&base));

        // A rest, then a taker that matches the rested order, then a sweep.
        for order in [
            limit(2, Side::Bid, 100, 5),
            limit(3, Side::Ask, 100, 4),
            market(4, Side::Bid, 10),
        ] {
            let planned = view.plan(order.clone());
            let submitted = reference.submit(order);

            assert_eq!(planned.remaining_quantity, submitted.remaining_quantity);
            assert_eq!(planned.rested, submitted.rested);
            assert_eq!(fill_summary(&planned.fills), fill_summary(&submitted.fills));
        }

        assert_eq!(base.best_ask(), Some(100));
        assert_eq!(base.depth(Side::Ask, 0), Some((100, 2)));
    }

    #[test]
    fn book_view_matches_base_then_added_orders_at_the_same_price() {
        let mut base = OrderBook::new();
        base.submit(limit(1, Side::Ask, 100, 2));

        let mut reference = base.clone();
        let mut view = BookView::new(Some(&base));

        // Rest 4 asks at 100 in the view; the reference appends them after
        // order 1, which is already at that price.
        let planned = view.plan(limit(2, Side::Ask, 100, 4));
        let submitted = reference.submit(limit(2, Side::Ask, 100, 4));
        assert_eq!(fill_summary(&planned.fills), fill_summary(&submitted.fills));
        assert_eq!(planned.rested, submitted.rested);

        // A 6-lot bid must consume base order 1 first, then the overlay rest.
        let planned = view.plan(limit(3, Side::Bid, 100, 6));
        let submitted = reference.submit(limit(3, Side::Bid, 100, 6));
        assert_eq!(fill_summary(&planned.fills), fill_summary(&submitted.fills));
        assert_eq!(
            fill_summary(&planned.fills),
            vec![(1, 100, 2), (2, 100, 4)],
            "base FIFO precedes overlay rests at the same price"
        );
        assert_eq!(planned.remaining_quantity, 0);
    }

    #[test]
    fn book_view_matches_multiple_added_orders_in_fifo_order() {
        let base = OrderBook::new();
        let mut reference = base.clone();
        let mut view = BookView::new(Some(&base));

        for order in [limit(1, Side::Ask, 100, 2), limit(2, Side::Ask, 100, 3)] {
            let planned = view.plan(order.clone());
            let submitted = reference.submit(order);
            assert_eq!(fill_summary(&planned.fills), fill_summary(&submitted.fills));
        }

        let planned = view.plan(limit(3, Side::Bid, 100, 6));
        let submitted = reference.submit(limit(3, Side::Bid, 100, 6));
        assert_eq!(fill_summary(&planned.fills), fill_summary(&submitted.fills));
        assert_eq!(
            fill_summary(&planned.fills),
            vec![(1, 100, 2), (2, 100, 3)],
            "added orders are matched in insertion order"
        );
        assert_eq!(planned.remaining_quantity, 1);
        assert!(planned.rested);

        // The bid remainder rests in the overlay; a later ask matches it.
        let planned = view.plan(limit(4, Side::Ask, 100, 1));
        let submitted = reference.submit(limit(4, Side::Ask, 100, 1));
        assert_eq!(fill_summary(&planned.fills), fill_summary(&submitted.fills));
        assert_eq!(fill_summary(&planned.fills), vec![(3, 100, 1)]);
    }

    #[test]
    fn book_view_keeps_a_partially_filled_maker_at_the_front() {
        let mut base = OrderBook::new();
        base.submit(limit(1, Side::Ask, 100, 5));
        base.submit(limit(2, Side::Ask, 100, 5));

        let mut reference = base.clone();
        let mut view = BookView::new(Some(&base));

        let first = view.plan(limit(3, Side::Bid, 100, 3));
        let first_reference = reference.submit(limit(3, Side::Bid, 100, 3));
        assert_eq!(
            fill_summary(&first.fills),
            fill_summary(&first_reference.fills)
        );

        let second = view.plan(limit(4, Side::Bid, 100, 4));
        let second_reference = reference.submit(limit(4, Side::Bid, 100, 4));
        assert_eq!(
            fill_summary(&second.fills),
            fill_summary(&second_reference.fills)
        );
        assert_eq!(
            fill_summary(&second.fills),
            vec![(1, 100, 2), (2, 100, 2)],
            "the partially filled maker keeps its FIFO position"
        );
    }

    #[test]
    fn book_view_cancel_matches_book_cancel_and_affects_later_plans() {
        let mut base = OrderBook::new();
        base.submit(limit(1, Side::Ask, 100, 5));
        base.submit(limit(2, Side::Ask, 100, 3));

        let mut reference = base.clone();
        let mut view = BookView::new(Some(&base));

        assert_eq!(
            view.cancel_limit_order(1).expect("order 1 is resting"),
            reference.cancel_limit_order(1).expect("order 1 is resting")
        );
        assert!(view.cancel_limit_order(1).is_none());
        assert!(reference.cancel_limit_order(1).is_none());
        assert!(view.cancel_limit_order(42).is_none());

        let planned = view.plan(limit(3, Side::Bid, 100, 4));
        let submitted = reference.submit(limit(3, Side::Bid, 100, 4));
        assert_eq!(planned.remaining_quantity, submitted.remaining_quantity);
        assert_eq!(planned.rested, submitted.rested);
        assert_eq!(fill_summary(&planned.fills), fill_summary(&submitted.fills));

        // The base book still contains the canceled order.
        assert_eq!(base.depth(Side::Ask, 0), Some((100, 8)));
    }

    #[test]
    fn book_view_plan_does_not_mutate_the_base_book() {
        let mut base = OrderBook::new();
        base.submit(limit(1, Side::Ask, 100, 5));
        let before = base.clone();

        let mut view = BookView::new(Some(&base));
        let _ = view.plan(limit(2, Side::Bid, 100, 10));
        let _ = view.plan(limit(3, Side::Bid, 99, 1));

        assert_eq!(base, before);
    }
}
