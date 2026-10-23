//! Deterministic synthetic order flow and open-order bookkeeping.

use std::collections::HashMap;

use crate::types::{EngineError, ExecutionReport, OrderStatus, Side};

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
    open: OpenOrders,
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
            open: OpenOrders::default(),
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

        if self.open.len() as u64 >= self.max_open_orders
            && let Some((order_id, target_symbol)) = self.open.random_entry(&mut self.rng)
        {
            return Action::cancel(target_symbol, order_id, timestamp);
        }

        let roll = self.rng.next_f64();
        if roll < self.cancel_ratio {
            if let Some((order_id, target_symbol)) = self.open.random_entry(&mut self.rng) {
                return Action::cancel(target_symbol, order_id, timestamp);
            }
            return self.limit_action(symbol_index, &book, timestamp);
        }
        if roll < self.cancel_ratio + self.market_ratio {
            return self.market_action(symbol_index, timestamp);
        }
        self.limit_action(symbol_index, &book, timestamp)
    }

    pub fn on_submit_result(
        &mut self,
        action: &Action,
        result: &Result<ExecutionReport, EngineError>,
    ) {
        debug_assert_ne!(action.kind, CommandKind::Cancel);

        match result {
            Ok(report) => {
                for fill in &report.fills {
                    self.open.on_fill(fill.maker_order_id, fill.quantity);
                }
                if matches!(
                    report.status,
                    OrderStatus::New | OrderStatus::PartiallyFilled
                ) {
                    self.open.insert(
                        report.order_id,
                        action.symbol_index,
                        report.remaining_quantity,
                    );
                }
            }
            Err(_) => {
                self.rejections = self.rejections.saturating_add(1);
            }
        }

        self.update_peak();
    }

    pub fn on_cancel_result(&mut self, action: &Action, _result: Option<&ExecutionReport>) {
        debug_assert_eq!(action.kind, CommandKind::Cancel);
        if let Some(order_id) = action.target_order_id {
            self.open.remove(order_id);
        }
        self.update_peak();
    }

    pub fn open_len(&self) -> u64 {
        self.open.len() as u64
    }

    pub fn peak_open_orders(&self) -> u64 {
        self.peak_open_orders
    }

    pub fn rejections(&self) -> u64 {
        self.rejections
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

    fn limit_action<F>(&mut self, symbol_index: usize, book: &F, timestamp: u64) -> Action
    where
        F: Fn(usize) -> (Option<u64>, Option<u64>),
    {
        let side = self.random_side();
        let crossing = self.rng.chance(self.cross_ratio);
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
        let open = self.open.len() as u64;
        if open > self.peak_open_orders {
            self.peak_open_orders = open;
        }
    }
}

#[derive(Debug)]
struct OpenOrder {
    symbol_index: usize,
    remaining: u64,
}

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

    fn insert(&mut self, order_id: u64, symbol_index: usize, remaining: u64) {
        debug_assert!(!self.entries.contains_key(&order_id));
        if self.entries.contains_key(&order_id) {
            return;
        }
        self.entries.insert(
            order_id,
            OpenOrder {
                symbol_index,
                remaining,
            },
        );
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

    fn on_fill(&mut self, order_id: u64, quantity: u64) {
        let remaining = {
            let Some(order) = self.entries.get_mut(&order_id) else {
                return;
            };
            order.remaining = order.remaining.saturating_sub(quantity);
            order.remaining
        };
        if remaining == 0 {
            self.remove(order_id);
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
    use crate::types::Trade;

    fn actions(seed: u64) -> Vec<Action> {
        let mut config = BenchConfig::default();
        config.seed = seed;
        config.symbols = 2;
        let mut workload = Workload::new(&config);
        (0..200)
            .map(|_| workload.next_action(|_| (None, None)))
            .collect()
    }

    fn trade(
        id: u64,
        maker_order_id: u64,
        taker_order_id: u64,
        price: u64,
        quantity: u64,
    ) -> Trade {
        Trade {
            id,
            symbol: format!("{SYMBOL_PREFIX}0"),
            maker_order_id,
            taker_order_id,
            price,
            quantity,
            taker_side: Side::Ask,
            timestamp: id,
        }
    }

    fn report(
        order_id: u64,
        status: OrderStatus,
        original_quantity: u64,
        filled_quantity: u64,
        remaining_quantity: u64,
        fills: Vec<Trade>,
    ) -> ExecutionReport {
        ExecutionReport {
            order_id,
            symbol: format!("{SYMBOL_PREFIX}0"),
            status,
            original_quantity,
            filled_quantity,
            remaining_quantity,
            fills,
        }
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
    fn partial_fills_keep_makers_until_fully_filled() {
        let mut config = BenchConfig::default();
        config.symbols = 1;
        let mut workload = Workload::new(&config);

        let maker_action = Action::submit(0, CommandKind::Limit, Side::Bid, 100, 10, 1);
        let maker_report = report(1, OrderStatus::New, 10, 0, 10, Vec::new());
        workload.on_submit_result(&maker_action, &Ok(maker_report));
        assert_eq!(workload.open_len(), 1);

        let taker_action = Action::submit(0, CommandKind::Limit, Side::Ask, 100, 10, 2);
        let taker_report = report(
            2,
            OrderStatus::PartiallyFilled,
            10,
            4,
            6,
            vec![trade(1, 1, 2, 100, 4)],
        );
        workload.on_submit_result(&taker_action, &Ok(taker_report));
        assert_eq!(workload.open_len(), 2);

        let taker_report = report(
            3,
            OrderStatus::PartiallyFilled,
            10,
            6,
            4,
            vec![trade(2, 1, 3, 100, 6)],
        );
        workload.on_submit_result(&taker_action, &Ok(taker_report));
        assert_eq!(workload.open_len(), 2);

        let cancel = Action::cancel(0, 2, 4);
        workload.on_cancel_result(&cancel, None);
        assert_eq!(workload.open_len(), 1);
    }

    #[test]
    fn rejections_are_counted_without_touching_the_pool() {
        let mut config = BenchConfig::default();
        config.symbols = 1;
        let mut workload = Workload::new(&config);

        let action = Action::submit(0, CommandKind::Limit, Side::Bid, 100, 10, 1);
        workload.on_submit_result(&action, &Err(EngineError::QuantityOutOfRange));
        assert_eq!(workload.rejections(), 1);
        assert_eq!(workload.open_len(), 0);
    }

    #[test]
    fn peak_tracks_the_largest_pool_size() {
        let mut config = BenchConfig::default();
        config.symbols = 1;
        let mut workload = Workload::new(&config);

        for order_id in 1..=3 {
            let action = Action::submit(0, CommandKind::Limit, Side::Bid, 100, 10, order_id);
            let report = report(order_id, OrderStatus::New, 10, 0, 10, Vec::new());
            workload.on_submit_result(&action, &Ok(report));
        }
        assert_eq!(workload.peak_open_orders(), 3);

        let cancel = Action::cancel(0, 1, 4);
        workload.on_cancel_result(&cancel, None);
        assert_eq!(workload.open_len(), 2);
        assert_eq!(workload.peak_open_orders(), 3);
    }
}
