//! Deterministic synthetic order flow and open-order bookkeeping.

use std::collections::HashMap;

use crate::event::DomainEvent;
use crate::types::{OrderBookId, Side};

use super::config::BenchConfig;
use super::rng::SplitMix64;

pub const SYMBOL_PREFIX: &str = "SYM";
const MID_WALK_PROBABILITY: f64 = 0.05;
const CROSS_OFFSET_TICKS: u64 = 9;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandKind {
    Limit,
    Market,
    Cancel,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Action {
    pub symbol_index: usize,
    pub kind: CommandKind,
    pub side: Side,
    pub price: u64,
    pub quantity: u64,
    pub timestamp: u64,
    pub target_order_id: Option<u64>,
}

impl Action {
    fn submit(
        symbol_index: usize,
        kind: CommandKind,
        side: Side,
        price: u64,
        quantity: u64,
        timestamp: u64,
    ) -> Self {
        Self {
            symbol_index,
            kind,
            side,
            price,
            quantity,
            timestamp,
            target_order_id: None,
        }
    }

    fn cancel(symbol_index: usize, order_id: u64, timestamp: u64) -> Self {
        Self {
            symbol_index,
            kind: CommandKind::Cancel,
            side: Side::Bid,
            price: 0,
            quantity: 0,
            timestamp,
            target_order_id: Some(order_id),
        }
    }
}

pub struct Workload {
    rng: SplitMix64,
    symbol_count: usize,
    next_symbol: usize,
    mids: Vec<u64>,
    initial_mids: Vec<u64>,
    price_band: u64,
    order_quantity: u64,
    max_open_orders: u64,
    market_ratio: f64,
    cancel_ratio: f64,
    cross_ratio: f64,
    timestamp: u64,
    open_orders: OpenOrders,
    rejections: u64,
    peak_open_orders: u64,
}

impl Workload {
    pub fn new(config: &BenchConfig) -> Self {
        let mids = vec![config.mid; config.symbols];
        Self {
            rng: SplitMix64::new(config.seed),
            symbol_count: config.symbols,
            next_symbol: 0,
            initial_mids: mids.clone(),
            mids,
            price_band: config.price_band,
            order_quantity: config.order_quantity,
            max_open_orders: config.max_open_orders,
            market_ratio: config.market_ratio,
            cancel_ratio: config.cancel_ratio,
            cross_ratio: config.cross_ratio,
            timestamp: 0,
            open_orders: OpenOrders::default(),
            rejections: 0,
            peak_open_orders: 0,
        }
    }

    /// Chooses the next command. `book` returns the current best bid and best
    /// ask for a symbol, and is only called while constructing a limit order.
    pub fn next_action<F>(&mut self, book: F) -> Action
    where
        F: Fn(usize) -> (Option<u64>, Option<u64>),
    {
        let symbol_index = self.next_symbol;
        self.next_symbol = (self.next_symbol + 1) % self.symbol_count;
        self.walk_mid(symbol_index);
        self.timestamp = self.timestamp.saturating_add(1);
        let timestamp = self.timestamp;

        if self.open_orders.len() as u64 >= self.max_open_orders
            && let Some((order_id, target_symbol)) = self.open_orders.random_entry(&mut self.rng)
        {
            return Action::cancel(target_symbol, order_id, timestamp);
        }

        let roll = self.rng.next_f64();
        if roll < self.cancel_ratio {
            if let Some((order_id, target_symbol)) = self.open_orders.random_entry(&mut self.rng) {
                return Action::cancel(target_symbol, order_id, timestamp);
            }
            return self.limit_action(symbol_index, &book, timestamp, false);
        }
        if roll < self.cancel_ratio + self.market_ratio {
            return self.market_action(symbol_index, timestamp);
        }
        self.limit_action(symbol_index, &book, timestamp, true)
    }

    /// Reconcile the cancelable-order pool with one submit's event stream.
    ///
    /// Outcomes are derived from events, not a separate report: a rejection is
    /// a lone `OrderRejected`, a fill is a `TradeExecuted`, and whether the new
    /// order is still working is answered by `is_open`, which the harness
    /// points at the engine's ledger after the command completes. Callers must
    /// run this outside any timed region because it queries the engine.
    pub fn on_submit_events<F>(&mut self, action: &Action, events: &[DomainEvent], is_open: F)
    where
        F: Fn(OrderBookId) -> bool,
    {
        debug_assert_ne!(action.kind, CommandKind::Cancel);

        let Some(DomainEvent::OrderAccepted { order, .. }) = events.first() else {
            // A submit is either accepted or rejected; a rejection changes no
            // state and consumes no order id.
            self.rejections = self.rejections.saturating_add(1);
            self.update_peak();
            return;
        };
        let taker_id = order.id;

        for event in events {
            if let DomainEvent::TradeExecuted { trade } = event
                && !is_open(trade.maker_order_id)
            {
                self.open_orders.remove(trade.maker_order_id);
            }
        }
        if is_open(taker_id) {
            self.open_orders.insert(taker_id, action.symbol_index);
        }

        self.update_peak();
    }

    /// Reconcile the cancelable-order pool with one cancel's event stream.
    ///
    /// A cancel that produced no `OrderCanceled`/`StopCanceled` was rejected;
    /// the harness selected a stale order, so it is counted like a rejection.
    pub fn on_cancel_events(&mut self, action: &Action, events: &[DomainEvent]) {
        debug_assert_eq!(action.kind, CommandKind::Cancel);

        let applied = events.iter().any(|event| {
            matches!(
                event,
                DomainEvent::OrderCanceled { .. } | DomainEvent::StopCanceled { .. }
            )
        });
        if !applied {
            self.rejections = self.rejections.saturating_add(1);
        }
        if let Some(order_id) = action.target_order_id {
            self.open_orders.remove(order_id);
        }
        self.update_peak();
    }

    pub fn open_len(&self) -> u64 {
        self.open_orders.len() as u64
    }

    pub fn peak_open_orders(&self) -> u64 {
        self.peak_open_orders
    }

    pub fn rejections(&self) -> u64 {
        self.rejections
    }

    /// Resets the counters that describe the measured phase. Call once after
    /// warmup so reported stats do not include warmup commands.
    pub fn start_measurement(&mut self) {
        self.peak_open_orders = self.open_orders.len() as u64;
        self.rejections = 0;
    }

    fn walk_mid(&mut self, symbol_index: usize) {
        if !self.rng.chance(MID_WALK_PROBABILITY) {
            return;
        }

        let half = self.price_band / 2;
        let initial = self.initial_mids[symbol_index];
        let lower = initial.saturating_sub(half).max(1);
        let upper = initial.saturating_add(half);
        let current = self.mids[symbol_index];
        let next = if self.rng.chance(0.5) {
            current.saturating_add(1)
        } else {
            current.saturating_sub(1).max(1)
        };
        self.mids[symbol_index] = next.clamp(lower, upper);
    }

    fn limit_action<F>(
        &mut self,
        symbol_index: usize,
        book: &F,
        timestamp: u64,
        allow_crossing: bool,
    ) -> Action
    where
        F: Fn(usize) -> (Option<u64>, Option<u64>),
    {
        let side = self.random_side();
        let crossing = allow_crossing && self.rng.chance(self.cross_ratio);
        let mid = self.mids[symbol_index];
        let (best_bid, best_ask) = book(symbol_index);

        let price = if crossing {
            match side {
                Side::Bid => match best_ask {
                    Some(best) => {
                        best.saturating_add(self.rng.range_inclusive(0, CROSS_OFFSET_TICKS))
                    }
                    None => mid.saturating_add(self.rng.range_inclusive(1, self.price_band)),
                },
                Side::Ask => match best_bid {
                    Some(best) => best
                        .saturating_sub(self.rng.range_inclusive(0, CROSS_OFFSET_TICKS))
                        .max(1),
                    None => mid
                        .saturating_sub(self.rng.range_inclusive(1, self.price_band))
                        .max(1),
                },
            }
        } else {
            match side {
                Side::Bid => mid
                    .saturating_sub(self.rng.range_inclusive(1, self.price_band))
                    .max(1),
                Side::Ask => mid.saturating_add(self.rng.range_inclusive(1, self.price_band)),
            }
        };

        Action::submit(
            symbol_index,
            CommandKind::Limit,
            side,
            price,
            self.order_quantity,
            timestamp,
        )
    }

    fn market_action(&mut self, symbol_index: usize, timestamp: u64) -> Action {
        let side = self.random_side();
        let price = self.mids[symbol_index];
        Action::submit(
            symbol_index,
            CommandKind::Market,
            side,
            price,
            self.order_quantity,
            timestamp,
        )
    }

    fn random_side(&mut self) -> Side {
        if self.rng.chance(0.5) {
            Side::Bid
        } else {
            Side::Ask
        }
    }

    fn update_peak(&mut self) {
        let open = self.open_orders.len() as u64;
        if open > self.peak_open_orders {
            self.peak_open_orders = open;
        }
    }
}

/// A cancelable order id and the symbol it belongs to.
#[derive(Debug)]
struct OpenOrder {
    symbol_index: usize,
}

/// Index of order ids the workload may still cancel.
///
/// It stores no quantities: the engine's lifecycle records remain the only
/// authority on whether an order is still open.
#[derive(Debug, Default)]
struct OpenOrders {
    entries: HashMap<u64, OpenOrder>,
    ids: Vec<u64>,
    positions: HashMap<u64, usize>,
}

impl OpenOrders {
    fn len(&self) -> usize {
        self.ids.len()
    }

    fn insert(&mut self, order_id: u64, symbol_index: usize) {
        debug_assert!(!self.entries.contains_key(&order_id));
        if self.entries.contains_key(&order_id) {
            return;
        }
        self.entries.insert(order_id, OpenOrder { symbol_index });
        self.positions.insert(order_id, self.ids.len());
        self.ids.push(order_id);
    }

    fn remove(&mut self, order_id: u64) {
        let Some(position) = self.positions.remove(&order_id) else {
            return;
        };
        self.entries.remove(&order_id);
        let last = self
            .ids
            .pop()
            .expect("position implies a non-empty id list");
        if position < self.ids.len() {
            self.ids[position] = last;
            self.positions.insert(last, position);
        }
    }

    fn random_entry(&self, rng: &mut SplitMix64) -> Option<(u64, usize)> {
        if self.ids.is_empty() {
            return None;
        }
        let index = (rng.next_u64() % self.ids.len() as u64) as usize;
        let order_id = self.ids[index];
        self.entries
            .get(&order_id)
            .map(|order| (order_id, order.symbol_index))
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;
    use crate::engine::MatchingEngine;
    use crate::event::DomainEvent;
    use crate::types::{EngineCommand, EngineConfig, OrderRequest, OrderType, TimeInForce};

    fn actions(seed: u64) -> Vec<Action> {
        let mut config = BenchConfig::default();
        config.seed = seed;
        config.symbols = 2;
        let mut workload = Workload::new(&config);
        (0..200)
            .map(|_| workload.next_action(|_| (None, None)))
            .collect()
    }

    fn limit_action(action: &Action) -> OrderRequest {
        OrderRequest {
            side: action.side,
            price: action.price,
            quantity: action.quantity,
            timestamp: action.timestamp,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::GTC,
            post_only: false,
        }
    }

    fn execute(engine: &mut MatchingEngine, action: &Action) -> Vec<DomainEvent> {
        let symbol = format!("{SYMBOL_PREFIX}{}", action.symbol_index);
        let command = match action.kind {
            CommandKind::Limit | CommandKind::Market => {
                let mut request = limit_action(action);
                if action.kind == CommandKind::Market {
                    request.order_type = OrderType::Market;
                    request.price = 0;
                    request.time_in_force = TimeInForce::IOC;
                }
                EngineCommand::SubmitOrder { symbol, request }
            }
            CommandKind::Cancel => EngineCommand::CancelOrder {
                symbol,
                order_id: action.target_order_id.expect("cancel target"),
            },
        };
        engine.execute(command)
    }

    fn submit(
        engine: &mut MatchingEngine,
        workload: &mut Workload,
        action: &Action,
    ) -> Vec<DomainEvent> {
        let events = execute(engine, action);
        workload.on_submit_events(action, &events, |id| engine.is_open(id));
        events
    }

    fn cancel(engine: &mut MatchingEngine, workload: &mut Workload, action: &Action) {
        let events = execute(engine, action);
        workload.on_cancel_events(action, &events);
    }

    #[test]
    fn same_seed_produces_the_same_actions() {
        assert_eq!(actions(11), actions(11));
    }

    #[test]
    fn different_seeds_produce_different_actions() {
        assert_ne!(actions(11), actions(12));
    }

    #[test]
    fn cancel_falls_back_to_limit_when_the_pool_is_empty() {
        let mut config = BenchConfig::default();
        config.limit_ratio = 0.0;
        config.market_ratio = 0.0;
        config.cancel_ratio = 1.0;
        config.cross_ratio = 0.0;
        config.symbols = 1;

        let mut workload = Workload::new(&config);
        for _ in 0..10 {
            let action = workload.next_action(|_| (None, None));
            assert_eq!(action.kind, CommandKind::Limit);
        }
    }

    #[test]
    fn cancel_fallback_stays_non_crossing_even_when_cross_ratio_is_one() {
        let mut config = BenchConfig::default();
        config.limit_ratio = 0.0;
        config.market_ratio = 0.0;
        config.cancel_ratio = 1.0;
        config.cross_ratio = 1.0;
        config.symbols = 1;

        let mut workload = Workload::new(&config);
        for _ in 0..50 {
            let action = workload.next_action(|_| (Some(1), Some(1_000_000)));
            assert_eq!(action.kind, CommandKind::Limit);
            match action.side {
                Side::Bid => assert!(
                    action.price < 1_000_000,
                    "fallback bid crossed ask: {}",
                    action.price
                ),
                Side::Ask => assert!(
                    action.price > 1,
                    "fallback ask crossed bid: {}",
                    action.price
                ),
            }
        }
    }

    #[test]
    fn crossing_prices_cross_the_opposite_best() {
        let mut config = BenchConfig::default();
        config.limit_ratio = 1.0;
        config.market_ratio = 0.0;
        config.cancel_ratio = 0.0;
        config.cross_ratio = 1.0;
        config.symbols = 1;

        let mut workload = Workload::new(&config);
        for _ in 0..50 {
            let action = workload.next_action(|_| (Some(50), Some(100)));
            assert_eq!(action.kind, CommandKind::Limit);
            match action.side {
                Side::Bid => assert!(
                    action.price >= 100,
                    "bid {} did not cross ask 100",
                    action.price
                ),
                Side::Ask => assert!(
                    (1..=50).contains(&action.price),
                    "ask {} did not cross bid 50",
                    action.price
                ),
            }
        }
    }

    #[test]
    fn maker_leaves_the_pool_only_after_its_final_fill() {
        let mut config = BenchConfig::default();
        config.symbols = 1;
        let mut workload = Workload::new(&config);
        let mut engine = MatchingEngine::new(EngineConfig::default());

        // A maker rests 10 at 100 and enters the pool as a cancel candidate.
        let maker_action = Action::submit(0, CommandKind::Limit, Side::Bid, 100, 10, 1);
        submit(&mut engine, &mut workload, &maker_action);
        assert_eq!(workload.open_len(), 1);

        // A taker fills 4 of the maker and is itself fully filled. The maker is
        // still open, so it must stay cancelable even though its quantity fell.
        let partial_action = Action::submit(0, CommandKind::Limit, Side::Ask, 100, 4, 2);
        submit(&mut engine, &mut workload, &partial_action);
        assert!(engine.is_open(1));
        assert_eq!(workload.open_len(), 1);

        // A second taker exhausts the maker and rests its own remainder. The
        // pool swaps one for the other without doing quantity arithmetic.
        let full_action = Action::submit(0, CommandKind::Limit, Side::Ask, 100, 15, 3);
        let events = submit(&mut engine, &mut workload, &full_action);
        assert!(!engine.is_open(1));
        assert!(engine.is_open(3));
        assert_eq!(workload.open_len(), 1);
        assert!(!workload.open_orders.entries.contains_key(&1));
        assert!(workload.open_orders.entries.contains_key(&3));
        assert!(
            events.iter().any(
                |event| matches!(event, DomainEvent::OrderRested { order, .. } if order.id == 3)
            )
        );
    }

    #[test]
    fn taker_is_inserted_only_while_the_engine_keeps_it_open() {
        let mut config = BenchConfig::default();
        config.symbols = 1;
        let mut workload = Workload::new(&config);
        let mut engine = MatchingEngine::new(EngineConfig::default());

        // A resting bid stays open.
        let resting = Action::submit(0, CommandKind::Limit, Side::Bid, 100, 10, 1);
        submit(&mut engine, &mut workload, &resting);
        assert_eq!(workload.open_len(), 1);

        // A market sell partially fills the maker: the taker is fully filled,
        // so it never enters the cancelable pool while the maker stays open.
        let filled = Action::submit(0, CommandKind::Market, Side::Ask, 100, 5, 2);
        submit(&mut engine, &mut workload, &filled);
        assert_eq!(workload.open_len(), 1);
        assert!(!workload.open_orders.entries.contains_key(&2));
        assert!(engine.is_open(1));
    }

    #[test]
    fn cancel_removes_the_target() {
        let mut config = BenchConfig::default();
        config.symbols = 1;
        let mut workload = Workload::new(&config);
        let mut engine = MatchingEngine::new(EngineConfig::default());

        for (timestamp, price) in [(1, 100), (2, 99)] {
            let action = Action::submit(0, CommandKind::Limit, Side::Bid, price, 10, timestamp);
            submit(&mut engine, &mut workload, &action);
        }
        assert_eq!(workload.open_len(), 2);

        let cancel_action = Action::cancel(0, 1, 3);
        cancel(&mut engine, &mut workload, &cancel_action);

        assert_eq!(workload.open_len(), 1);
        assert!(!workload.open_orders.entries.contains_key(&1));
        assert!(workload.open_orders.entries.contains_key(&2));
    }

    #[test]
    fn rejections_are_counted_without_touching_the_pool() {
        let mut config = BenchConfig::default();
        config.symbols = 1;
        let mut workload = Workload::new(&config);
        let mut engine = MatchingEngine::new(EngineConfig::default());

        let action = Action::submit(0, CommandKind::Limit, Side::Bid, 100, 0, 1);
        let events = submit(&mut engine, &mut workload, &action);
        assert!(matches!(
            events.as_slice(),
            [DomainEvent::OrderRejected { .. }]
        ));
        assert_eq!(workload.rejections(), 1);
        assert_eq!(workload.open_len(), 0);
    }

    #[test]
    fn failed_cancels_are_counted_as_harness_errors() {
        let mut config = BenchConfig::default();
        config.symbols = 1;
        let mut workload = Workload::new(&config);
        let mut engine = MatchingEngine::new(EngineConfig::default());

        let submit_action = Action::submit(0, CommandKind::Limit, Side::Bid, 100, 10, 1);
        submit(&mut engine, &mut workload, &submit_action);
        assert_eq!(workload.open_len(), 1);

        let cancel_action = Action::cancel(0, 1, 2);
        cancel(&mut engine, &mut workload, &cancel_action);
        assert_eq!(workload.rejections(), 0);

        // Canceling an unknown id is a rejected event.
        let stale = Action::cancel(0, 999, 3);
        cancel(&mut engine, &mut workload, &stale);
        assert_eq!(workload.rejections(), 1);
    }

    #[test]
    fn start_measurement_resets_counters_to_the_current_pool() {
        let mut config = BenchConfig::default();
        config.symbols = 1;
        let mut workload = Workload::new(&config);
        let mut engine = MatchingEngine::new(EngineConfig::default());

        for (timestamp, price) in [(1, 100), (2, 99), (3, 98)] {
            let action = Action::submit(0, CommandKind::Limit, Side::Bid, price, 10, timestamp);
            submit(&mut engine, &mut workload, &action);
        }
        assert_eq!(workload.peak_open_orders(), 3);

        let cancel_action = Action::cancel(0, 1, 4);
        cancel(&mut engine, &mut workload, &cancel_action);
        assert_eq!(workload.peak_open_orders(), 3);

        workload.start_measurement();
        assert_eq!(workload.open_len(), 2);
        assert_eq!(workload.peak_open_orders(), 2);
        assert_eq!(workload.rejections(), 0);
    }

    #[test]
    fn peak_tracks_the_largest_pool_size() {
        let mut config = BenchConfig::default();
        config.symbols = 1;
        let mut workload = Workload::new(&config);
        let mut engine = MatchingEngine::new(EngineConfig::default());

        for (timestamp, price) in [(1, 100), (2, 99), (3, 98)] {
            let action = Action::submit(0, CommandKind::Limit, Side::Bid, price, 10, timestamp);
            submit(&mut engine, &mut workload, &action);
        }
        assert_eq!(workload.peak_open_orders(), 3);

        let cancel_action = Action::cancel(0, 1, 4);
        cancel(&mut engine, &mut workload, &cancel_action);
        assert_eq!(workload.open_len(), 2);
        assert_eq!(workload.peak_open_orders(), 3);
    }
}
