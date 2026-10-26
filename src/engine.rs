use colored::Colorize;

use std::collections::HashMap;

use crate::event::{DomainEvent, OrderCancelReason};
use crate::ledger::OrderLedger;
use crate::orderbook::OrderBook;
use crate::snapshot::{
    BookState, EngineSnapshot, LastTradeState, LedgerEntry, PriceLevelState, StopBookState,
};
use crate::stop_book::StopBook;
use crate::types::{
    BookSnapshot, CommandOutcome, EngineCommand, EngineConfig, EngineError, EngineEvent,
    ExecutionReport, Fill, ModifyRequest, Order, OrderBookId, OrderRecord, OrderRequest,
    OrderStatus, OrderType, Side, StopKind, StopOrder, StopOrderRequest, TimeInForce, Trade,
};

#[derive(Debug, Clone, Copy)]
struct LastTrade {
    price: u64,
    timestamp: u64,
}

fn price_level_states(levels: Vec<(u64, Vec<Order>)>) -> Vec<PriceLevelState> {
    levels
        .into_iter()
        .map(|(price, orders)| PriceLevelState { price, orders })
        .collect()
}

/// Where a taker order's id comes from: a fresh submission allocates one, a
/// triggered stop resumes the id its pending record already owns.
#[derive(Debug, Clone, Copy)]
enum TakerOrigin {
    Fresh,
    Resume(OrderBookId),
}

// I want to implement a matching engine for the order book system
// for educational purposes......
// MatchingEngine has a hashmap <Symbol, OrderBook> ?
pub struct MatchingEngine {
    order_books: HashMap<String, OrderBook>,
    trades: Vec<Trade>,
    next_trade_id: u64,
    config: EngineConfig,
    ledger: OrderLedger,
    next_order_id: u64,
    events: Vec<EngineEvent>,
    next_sequence_id: u64,
    /// Last event sequence folded through [`MatchingEngine::apply`].
    ///
    /// The old command path keeps this in lockstep with `next_sequence_id` so
    /// `BookSnapshot.sequence` is identical on both paths.
    last_applied_sequence: u64,
    stop_books: HashMap<String, StopBook>,
    last_trades: HashMap<String, LastTrade>,
}

impl Default for MatchingEngine {
    fn default() -> Self {
        Self {
            order_books: HashMap::new(),
            trades: Vec::new(),
            next_trade_id: 1,
            config: EngineConfig::default(),
            ledger: OrderLedger::default(),
            next_order_id: 1,
            events: Vec::new(),
            stop_books: HashMap::new(),
            last_trades: HashMap::new(),
            next_sequence_id: 1,
            last_applied_sequence: 0,
        }
    }
}

/// Read-only decision state for the event-sourced path.
///
/// The tracer only decides non-crossing limit submits, so it needs nothing more
/// than a borrow of the engine plus the touched symbol and the next order id.
/// It deliberately does **not** clone the engine or any book: the scratch is a
/// small stack value, which keeps `decide` off the whole-engine-clone path.
pub struct DecisionScratch<'a> {
    engine: &'a MatchingEngine,
    symbol: &'a str,
    order_id: OrderBookId,
}

impl<'a> DecisionScratch<'a> {
    pub fn new(engine: &'a MatchingEngine, symbol: &'a str) -> Self {
        Self {
            engine,
            symbol,
            // Allocation stays local to the decision: no counter is mutated
            // until an `OrderAccepted` event is applied.
            order_id: engine.next_order_id,
        }
    }

    /// Decide a fresh submit: policy, matching, then fill/rest/cancel events.
    ///
    /// Matching runs against a clone of **only** the touched symbol's book.
    /// The engine's book, ledger, trades, last price and counters are never
    /// mutated; order and trade ids are allocated in local variables and only
    /// become state when the events are applied.
    pub fn decide_submit(&self, request: &OrderRequest) -> Result<Vec<DomainEvent>, EngineError> {
        // Validation order matches the command-sourced path: validate, then
        // post-only crossing, then FOK liquidity.
        self.engine.check_taker_policy(self.symbol, request)?;

        let order_id = self.order_id;
        let accepted = request.to_order(order_id);

        // The book this decision runs against: a private copy. `submit` both
        // matches and decides whether the remainder rests.
        let mut scratch_book = match self.engine.order_books.get(self.symbol) {
            Some(book) => book.clone(),
            None => OrderBook::new(),
        };
        let result = scratch_book.submit(accepted.clone());

        // Ticket 03 owns stop activation. If this command's resulting last
        // price would trigger a pending stop, reject the whole decision rather
        // than emit a stream that omits `StopTriggered`.
        let prospective_last_price = result.fills.last().map(|fill| fill.price).or_else(|| {
            self.engine
                .last_trades
                .get(self.symbol)
                .map(|last| last.price)
        });
        if prospective_last_price.is_some_and(|last_price| {
            self.engine
                .stop_books
                .get(self.symbol)
                .and_then(|stops| stops.next_triggered(last_price))
                .is_some()
        }) {
            return Err(EngineError::UnsupportedCommand);
        }

        let symbol = self.symbol.to_string();
        let mut events = Vec::with_capacity(1 + result.fills.len() + 1);
        events.push(DomainEvent::OrderAccepted {
            symbol: symbol.clone(),
            order: accepted.clone(),
            post_only: request.post_only,
        });

        // Trade ids are allocated locally; `apply(TradeExecuted)` advances the
        // real counter from the events.
        let mut next_trade_id = self.engine.next_trade_id;
        for fill in result.fills {
            let trade = Trade {
                id: next_trade_id,
                symbol: symbol.clone(),
                maker_order_id: fill.maker_order_id,
                taker_order_id: fill.taker_order_id,
                price: fill.price,
                quantity: fill.quantity,
                taker_side: fill.taker_side,
                timestamp: request.timestamp,
            };
            next_trade_id = next_trade_id.saturating_add(1);
            events.push(DomainEvent::TradeExecuted { trade });
        }

        if result.remaining_quantity > 0 {
            if result.rested {
                events.push(DomainEvent::OrderRested {
                    symbol,
                    order: Order {
                        quantity: result.remaining_quantity,
                        ..accepted
                    },
                });
            } else {
                events.push(DomainEvent::OrderCanceled {
                    symbol,
                    order_id,
                    canceled_quantity: result.remaining_quantity,
                    reason: OrderCancelReason::UnfilledResidual,
                });
            }
        }

        Ok(events)
    }

    /// Decide a user cancel of an open, resting order.
    ///
    /// Unknown, already-closed, wrong-symbol and not-resting orders fail with
    /// [`EngineError::UnknownOrder`]. Pending-stop cancels (`StopCanceled`) and
    /// modify belong to later tickets and fail with
    /// [`EngineError::UnsupportedCommand`].
    pub fn decide_cancel(&self, order_id: OrderBookId) -> Result<Vec<DomainEvent>, EngineError> {
        let record = self
            .engine
            .ledger
            .get(order_id)
            .ok_or(EngineError::UnknownOrder)?;

        if record.symbol != self.symbol {
            return Err(EngineError::UnknownOrder);
        }
        if record.status == OrderStatus::PendingTrigger {
            return Err(EngineError::UnsupportedCommand);
        }
        if !record.is_open() {
            return Err(EngineError::UnknownOrder);
        }

        let resting = self
            .engine
            .order_books
            .get(self.symbol)
            .is_some_and(|book| book.contains_order(order_id));
        if !resting {
            return Err(EngineError::UnknownOrder);
        }

        Ok(vec![DomainEvent::OrderCanceled {
            symbol: self.symbol.to_string(),
            order_id,
            canceled_quantity: record.remaining_quantity,
            reason: OrderCancelReason::UserCancel,
        }])
    }
}

impl MatchingEngine {
    pub fn new(config: EngineConfig) -> Self {
        Self {
            order_books: HashMap::new(),
            trades: Vec::new(),
            next_trade_id: 1,
            config,
            ledger: OrderLedger::default(),
            next_order_id: 1,
            events: Vec::new(),
            stop_books: HashMap::new(),
            last_trades: HashMap::new(),
            next_sequence_id: 1,
            last_applied_sequence: 0,
        }
    }

    /// The last **event** sequence handed out, derived from the sequence
    /// counter rather than the event history so it survives a restore that
    /// drops events.
    ///
    /// This is not the durable command sequence: recovery is bounded by
    /// [`crate::durable::DurableMatchingEngine::applied_seq`]. One command can
    /// emit several events, so an event sequence is not a recovery boundary.
    pub fn last_sequence(&self) -> u64 {
        self.next_sequence_id
            .saturating_sub(1)
            .max(self.last_applied_sequence)
    }

    /// Last event sequence folded through [`Self::apply`].
    ///
    /// This is the value reported by [`BookSnapshot::sequence`] on the
    /// event-sourced path.
    pub fn last_applied_sequence(&self) -> u64 {
        self.last_applied_sequence
    }

    /// Capture the complete decision state as a deterministic snapshot.
    ///
    /// Events are derived outputs and are not included.
    /// Books (including empty ones), ledger records, stop books and last trades
    /// are emitted in a canonical order so equal state has equal bytes.
    pub fn capture_snapshot(&self) -> EngineSnapshot {
        let mut book_symbols: Vec<&String> = self.order_books.keys().collect();
        book_symbols.sort();
        let books = book_symbols
            .into_iter()
            .map(|symbol| {
                let (bids, asks) = self.order_books[symbol].export_levels();
                BookState {
                    symbol: symbol.clone(),
                    bids: price_level_states(bids),
                    asks: price_level_states(asks),
                }
            })
            .collect();

        let ledger = self
            .ledger
            .export_records()
            .into_iter()
            .map(|(order_id, record)| LedgerEntry { order_id, record })
            .collect();

        let mut stop_symbols: Vec<&String> = self.stop_books.keys().collect();
        stop_symbols.sort();
        let stops = stop_symbols
            .into_iter()
            .map(|symbol| StopBookState {
                symbol: symbol.clone(),
                stops: self.stop_books[symbol].export_stops(),
            })
            .collect();

        let mut last_trades: Vec<LastTradeState> = self
            .last_trades
            .iter()
            .map(|(symbol, last)| LastTradeState {
                symbol: symbol.clone(),
                price: last.price,
                timestamp: last.timestamp,
            })
            .collect();
        last_trades.sort_by(|left, right| left.symbol.cmp(&right.symbol));

        EngineSnapshot {
            config: self.config,
            next_order_id: self.next_order_id,
            next_trade_id: self.next_trade_id,
            next_event_sequence: self.next_sequence_id,
            books,
            ledger,
            stops,
            last_trades,
            trades: self.trades.clone(),
        }
    }

    /// Rebuild an engine from a snapshot without running any matching.
    ///
    /// Books recompute their price-level totals and order lookup indexes from
    /// the stored orders; the ledger and both stop indexes are reinserted
    /// directly. Events start empty.
    pub fn restore_snapshot(snapshot: EngineSnapshot) -> Self {
        let order_books = snapshot
            .books
            .into_iter()
            .map(|state| {
                let bids = state
                    .bids
                    .into_iter()
                    .map(|level| (level.price, level.orders))
                    .collect();
                let asks = state
                    .asks
                    .into_iter()
                    .map(|level| (level.price, level.orders))
                    .collect();
                (state.symbol, OrderBook::from_levels(bids, asks))
            })
            .collect();

        let ledger = OrderLedger::from_records(
            snapshot
                .ledger
                .into_iter()
                .map(|entry| (entry.order_id, entry.record))
                .collect(),
        );

        let stop_books = snapshot
            .stops
            .into_iter()
            .map(|state| (state.symbol, StopBook::from_stops(state.stops)))
            .collect();

        let last_trades = snapshot
            .last_trades
            .into_iter()
            .map(|last| {
                (
                    last.symbol,
                    LastTrade {
                        price: last.price,
                        timestamp: last.timestamp,
                    },
                )
            })
            .collect();

        Self {
            order_books,
            trades: snapshot.trades,
            next_trade_id: snapshot.next_trade_id,
            config: snapshot.config,
            ledger,
            next_order_id: snapshot.next_order_id,
            events: Vec::new(),
            next_sequence_id: snapshot.next_event_sequence,
            last_applied_sequence: snapshot.next_event_sequence.saturating_sub(1),
            stop_books,
            last_trades,
        }
    }

    pub fn snapshot(&self, symbol: &str, depth: usize) -> Option<BookSnapshot> {
        let book = self.order_books.get(symbol)?;
        let (bids, asks) = book.snapshot(depth);
        Some(BookSnapshot {
            symbol: symbol.to_string(),
            sequence: self.last_sequence(),
            bids,
            asks,
        })
    }

    /// Apply one command through the engine's only write entry point.
    ///
    /// Durability and replay belong to the durable layer, which appends the
    /// command before calling this method.
    pub fn execute(&mut self, command: EngineCommand) -> CommandOutcome {
        self.execute_ref(&command)
    }

    /// Apply a command by reference.
    ///
    /// The runtime uses this to apply without cloning, then moves the same
    /// command into its journal record.
    pub fn execute_ref(&mut self, command: &EngineCommand) -> CommandOutcome {
        self.apply_command(command)
    }

    /// Pure decision step of the event-sourced path: command in, events out.
    ///
    /// Reads engine state and allocates order/trade ids in local variables
    /// only; it never mutates the engine. Ticket 02 supports normal submits
    /// (limit, market, IOC, FOK) and user cancels of open orders. Modify, stop
    /// commands and a command that would trigger a pending stop are rejected
    /// explicitly for later tickets.
    pub fn decide(&self, command: &EngineCommand) -> Result<Vec<DomainEvent>, EngineError> {
        match command {
            EngineCommand::SubmitOrder { symbol, request } => {
                DecisionScratch::new(self, symbol).decide_submit(request)
            }
            EngineCommand::CancelOrder { symbol, order_id } => {
                DecisionScratch::new(self, symbol).decide_cancel(*order_id)
            }
            EngineCommand::ModifyOrder { .. } | EngineCommand::SubmitStopOrder { .. } => {
                Err(EngineError::UnsupportedCommand)
            }
        }
    }

    /// Fold one domain event into engine state.
    ///
    /// This is the event-sourced path's only state write entry. `sequence` is
    /// the global event sequence assigned by the durable layer; it is recorded
    /// for [`BookSnapshot::sequence`] and never used to re-run matching.
    pub fn apply(&mut self, event: &DomainEvent, sequence: u64) {
        match event {
            DomainEvent::OrderAccepted { symbol, order, .. } => {
                // The ledger record is created here; the book stays untouched.
                self.ledger
                    .insert_new(order.id, symbol.clone(), order.quantity);
                self.next_order_id = self.next_order_id.max(order.id.saturating_add(1));
            }
            DomainEvent::OrderRested { symbol, order } => {
                // Direct insert, never `OrderBook::submit`: replay must not
                // re-run matching.
                self.order_books
                    .entry(symbol.clone())
                    .or_insert_with(OrderBook::new)
                    .rest_order(order.clone());
            }
            DomainEvent::TradeExecuted { trade } => {
                // Reduce the maker's resting quantity, then move both ledger
                // records and the trade tape. No matching runs here.
                self.order_books
                    .get_mut(&trade.symbol)
                    .expect("trade references a symbol with no book")
                    .reduce_order(trade.maker_order_id, trade.quantity);
                self.ledger.apply_fill(trade.maker_order_id, trade.quantity);
                self.ledger.apply_fill(trade.taker_order_id, trade.quantity);
                self.trades.push(trade.clone());
                self.last_trades.insert(
                    trade.symbol.clone(),
                    LastTrade {
                        price: trade.price,
                        timestamp: trade.timestamp,
                    },
                );
                self.next_trade_id = self.next_trade_id.max(trade.id.saturating_add(1));
            }
            DomainEvent::OrderCanceled {
                symbol,
                order_id,
                canceled_quantity,
                ..
            } => {
                // The book holds the order only for a user cancel; an
                // IOC/market residual was never inserted. Absence is fine.
                if let Some(book) = self.order_books.get_mut(symbol) {
                    book.cancel_limit_order(*order_id);
                }
                self.ledger
                    .mark_canceled(*order_id, *canceled_quantity)
                    .expect("canceled order has no ledger record");
            }
        }

        self.last_applied_sequence = self.last_applied_sequence.max(sequence);
        self.next_sequence_id = self.next_sequence_id.max(sequence.saturating_add(1));
    }

    /// Core command path used by `execute`. Never performs IO.
    fn apply_command(&mut self, command: &EngineCommand) -> CommandOutcome {
        let symbol = match command {
            EngineCommand::SubmitOrder { symbol, .. } => symbol,
            EngineCommand::SubmitStopOrder { symbol, .. } => symbol,
            EngineCommand::CancelOrder { symbol, .. } => symbol,
            EngineCommand::ModifyOrder { symbol, .. } => symbol,
        };

        let outcome = match command {
            EngineCommand::SubmitStopOrder { symbol, request } => {
                self.apply_submit_stop(symbol, request)
            }
            EngineCommand::SubmitOrder { symbol, request } => self.apply_submit(symbol, request),
            EngineCommand::CancelOrder { symbol, order_id } => {
                self.apply_cancel_order(symbol, *order_id)
            }
            EngineCommand::ModifyOrder {
                symbol,
                order_id,
                request,
            } => self.apply_modify_order(symbol, *order_id, request),
        };

        self.drain_triggered_stops(symbol);
        outcome
    }

    fn drain_triggered_stops(&mut self, symbol: &str) {
        loop {
            let Some(last) = self.last_trades.get(symbol).copied() else {
                return;
            };
            let Some(order_id) = self.next_triggered_stop(symbol, last.price) else {
                return;
            };
            self.trigger_stop(symbol, order_id, last.price, last.timestamp);
        }
    }

    fn next_triggered_stop(&self, symbol: &str, last_price: u64) -> Option<OrderBookId> {
        self.stop_books.get(symbol)?.next_triggered(last_price)
    }

    fn trigger_stop(
        &mut self,
        symbol: &str,
        order_id: OrderBookId,
        last_price: u64,
        last_timestamp: u64,
    ) {
        let stop = self
            .stop_books
            .get_mut(symbol)
            .and_then(|book| book.remove(order_id))
            .expect("stop index and order map diverged");

        // StopTriggered event
        let sequence_id = self.allocate_sequence_id();
        self.events.push(EngineEvent::StopTriggered {
            symbol: symbol.to_string(),
            sequence_id,
            order_id,
            stop_price: stop.stop_price,
            last_price,
        });

        let request = OrderRequest {
            quantity: stop.quantity,
            price: match stop.order_type {
                StopKind::Market => 0,
                StopKind::Limit { price } => price,
            },
            side: stop.side,
            timestamp: last_timestamp,
            post_only: false,
            order_type: match stop.order_type {
                StopKind::Market => OrderType::Market,
                StopKind::Limit { .. } => OrderType::Limit,
            },
            time_in_force: match stop.order_type {
                StopKind::Market => TimeInForce::IOC,
                StopKind::Limit { .. } => TimeInForce::GTC,
            },
        };

        // The stop was validated when it was submitted, and the engine builds a
        // valid market+IOC or limit+GTC request here, so a policy failure would
        // be an internal invariant violation.
        let _report = self
            .intake(symbol, &request, TakerOrigin::Resume(order_id))
            .expect("triggered stop failed taker policy after submission validation");
    }

    fn can_fill_completely(&self, symbol: &str, request: &OrderRequest) -> bool {
        let Some(book) = self.order_books.get(symbol) else {
            return false;
        };
        let probe = request.to_order(0); // 仅用于预检查
        book.can_fill_completely(&probe)
    }

    fn apply_submit(&mut self, symbol: &str, request: &OrderRequest) -> CommandOutcome {
        match self.intake(symbol, request, TakerOrigin::Fresh) {
            Ok(report) => CommandOutcome::Accepted(report),
            Err(err) => {
                self.emit_order_rejected(err);
                CommandOutcome::Rejected(err)
            }
        }
    }

    /// The one entrance for taker orders. Policy checks run once, before any
    /// id is allocated; fresh submissions then allocate an id, while a
    /// triggered stop resumes the id its pending record already owns.
    fn intake(
        &mut self,
        symbol: &str,
        request: &OrderRequest,
        origin: TakerOrigin,
    ) -> Result<ExecutionReport, EngineError> {
        self.check_taker_policy(symbol, request)?;

        let order_id = match origin {
            TakerOrigin::Fresh => {
                let order_id = self.next_order_id;
                self.next_order_id += 1;
                order_id
            }
            TakerOrigin::Resume(order_id) => order_id,
        };

        Ok(self.execute_taker(symbol, request, order_id, origin))
    }

    /// Every rejection reason a taker can hit before it reaches the book, in
    /// the order the outcome vocabulary has always reported them.
    fn check_taker_policy(&self, symbol: &str, request: &OrderRequest) -> Result<(), EngineError> {
        self.validate(request)?;

        if request.post_only && self.would_cross(symbol, request) {
            return Err(EngineError::PostOnlyWouldCross);
        }

        if request.time_in_force == TimeInForce::FOK && !self.can_fill_completely(symbol, request) {
            return Err(EngineError::InsufficientLiquidity);
        }

        Ok(())
    }

    fn execute_taker(
        &mut self,
        symbol: &str,
        request: &OrderRequest,
        order_id: OrderBookId,
        origin: TakerOrigin,
    ) -> ExecutionReport {
        let order = request.to_order(order_id);
        let original_quantity = request.quantity;
        let timestamp = request.timestamp;
        let symbol_owned = symbol.to_string();

        let order_book = self
            .order_books
            .entry(symbol.to_string())
            .or_insert_with(OrderBook::new);

        let result = order_book.submit(order);

        let fills = self.record_fills(symbol, timestamp, result.fills);

        let report = match origin {
            TakerOrigin::Fresh => self.ledger.insert_taker(
                order_id,
                symbol_owned,
                original_quantity,
                result.remaining_quantity,
                result.rested,
            ),
            TakerOrigin::Resume(stop_id) => {
                debug_assert_eq!(stop_id, order_id);
                let pending = self
                    .ledger
                    .get(order_id)
                    .expect("triggered stop has no ledger record");
                debug_assert_eq!(pending.status, OrderStatus::PendingTrigger);
                debug_assert_eq!(pending.original_quantity, original_quantity);

                self.ledger
                    .resume_stop(order_id, result.remaining_quantity, result.rested)
                    .expect("triggered stop ledger record disappeared")
            }
        }
        .to_report(order_id, fills);

        self.emit_accepted(report.clone());
        report
            .fills
            .iter()
            .for_each(|fill| self.emit_trade_executed(fill.clone()));
        report
    }

    fn would_cross(&self, symbol: &str, request: &OrderRequest) -> bool {
        let Some(book) = self.order_books.get(symbol) else {
            return false;
        };
        match request.side {
            Side::Bid => book.best_ask().is_some_and(|ask| request.price >= ask),
            Side::Ask => book.best_bid().is_some_and(|bid| request.price <= bid),
        }
    }

    fn apply_cancel_order(&mut self, symbol: &str, order_id: OrderBookId) -> CommandOutcome {
        let pending_stop = self
            .ledger
            .get(order_id)
            .is_some_and(|r| r.symbol == symbol && r.status == OrderStatus::PendingTrigger);

        if pending_stop {
            let stop = self
                .stop_books
                .get_mut(symbol)
                .and_then(|book| book.remove(order_id))
                .expect("pending stop missing from stop store");
            let report = self
                .ledger
                .mark_canceled(order_id, stop.quantity)
                .expect("pending stop ledger record disappeared")
                .to_report(order_id, Vec::new());
            self.emit_order_canceled(report.clone());
            return CommandOutcome::Canceled(report);
        }

        match self.try_cancel_order(symbol, order_id) {
            Some(report) => CommandOutcome::Canceled(report),
            None => CommandOutcome::Rejected(EngineError::UnknownOrder),
        }
    }

    /// 仅返回“待触发”的止损单；触发/撤销后返回 None。
    pub fn stop_order(&self, order_id: OrderBookId) -> Option<&StopOrder> {
        let record = self.ledger.get(order_id)?;
        self.stop_books.get(&record.symbol)?.get(order_id)
    }

    /// 按侧返回：买侧触发价升序，卖侧触发价降序；同价按 order_id 升序。
    pub fn stop_orders(&self, symbol: &str, side: Side) -> Vec<&StopOrder> {
        let Some(book) = self.stop_books.get(symbol) else {
            return Vec::new();
        };
        book.iter(side).collect()
    }

    fn validate_quantity(&self, quantity: u64) -> Result<(), EngineError> {
        if quantity < self.config.min_order_quantity || quantity > self.config.max_order_quantity {
            return Err(EngineError::QuantityOutOfRange);
        }
        Ok(())
    }

    fn validate_price_on_tick(&self, price: u64) -> Result<(), EngineError> {
        if !price.is_multiple_of(self.config.tick_size) {
            return Err(EngineError::PriceNotOnTick);
        }
        Ok(())
    }

    fn validate_limit(&self, price: u64, quantity: u64) -> Result<(), EngineError> {
        self.validate_quantity(quantity)?;
        self.validate_price_on_tick(price)
    }

    fn validate_stop(&self, symbol: &str, request: &StopOrderRequest) -> Result<(), EngineError> {
        self.validate_quantity(request.quantity)?;
        self.validate_price_on_tick(request.stop_price)?;
        if let StopKind::Limit { price } = request.order_type {
            self.validate_price_on_tick(price)?;
        }

        if let Some(last) = self.last_trades.get(symbol) {
            let would_trigger = match request.side {
                Side::Bid => request.stop_price <= last.price, // Q3 严格：必须 > last
                Side::Ask => request.stop_price >= last.price, // 必须 < last
            };
            if would_trigger {
                return Err(EngineError::StopPriceWouldTriggerImmediately);
            }
        }

        Ok(())
    }

    fn apply_submit_stop(&mut self, symbol: &str, request: &StopOrderRequest) -> CommandOutcome {
        if let Err(err) = self.validate_stop(symbol, request) {
            self.emit_order_rejected(err);
            return CommandOutcome::Rejected(err);
        }

        let order_id = self.next_order_id;
        self.next_order_id += 1;

        let stop = StopOrder {
            id: order_id,
            side: request.side,
            stop_price: request.stop_price,
            order_type: request.order_type,
            quantity: request.quantity,
            timestamp: request.timestamp,
        };

        self.stop_books
            .entry(symbol.to_string())
            .or_default()
            .insert(stop);

        let report = self
            .ledger
            .insert_pending_stop(order_id, symbol.to_string(), request.quantity)
            .to_report(order_id, Vec::new());

        self.emit_accepted(report.clone());
        CommandOutcome::Accepted(report)
    }

    fn apply_modify_order(
        &mut self,
        symbol: &str,
        order_id: OrderBookId,
        request: &ModifyRequest,
    ) -> CommandOutcome {
        // 1. order must be limit + gtc, and need check validity of price and quantity
        if let Err(err) = self.validate_limit(request.price, request.quantity) {
            self.emit_order_rejected(err);
            return CommandOutcome::Rejected(err);
        }

        if self.ledger.get(order_id).is_some_and(|record| {
            record.symbol == symbol && record.status == OrderStatus::PendingTrigger
        }) {
            let err = EngineError::StopOrderNotModifiable;
            self.emit_order_rejected(err);
            return CommandOutcome::Rejected(err);
        }

        // 2. order must exist and be open in the ledger for the given symbol
        let is_open = self
            .ledger
            .get(order_id)
            .is_some_and(|record| record.symbol == symbol && record.is_open());

        if !is_open {
            let err = EngineError::UnknownOrder;
            self.emit_order_rejected(err);
            return CommandOutcome::Rejected(err);
        }

        // 3. delete the open order from the order book and mark it as canceled in the ledger
        let Some((old_order, _canceled_report)) = self.remove_open_order(symbol, order_id) else {
            let err = EngineError::UnknownOrder;
            self.emit_order_rejected(err);
            return CommandOutcome::Rejected(err);
        };

        // 4. create a new order with the modified parameters and add it to the order book
        let submit_request = OrderRequest {
            side: old_order.side,
            price: request.price,
            quantity: request.quantity,
            timestamp: request.timestamp,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::GTC,
            post_only: false,
        };

        self.apply_submit(symbol, &submit_request)
    }

    fn remove_open_order(
        &mut self,
        symbol: &str,
        order_id: OrderBookId,
    ) -> Option<(Order, ExecutionReport)> {
        let record = self.ledger.get(order_id)?;
        if record.symbol != symbol || !record.is_open() {
            return None;
        }

        let order = self
            .order_books
            .get_mut(symbol)?
            .cancel_limit_order(order_id)?;
        let canceled_quantity = order.quantity;
        let report = self
            .ledger
            .mark_canceled(order_id, canceled_quantity)?
            .to_report(order_id, Vec::new());

        self.emit_order_canceled(report.clone());
        Some((order, report))
    }

    /// Cancel an open order. Every failure mode (unknown id, wrong symbol,
    /// already filled or canceled) returns `None` and emits no event.
    fn try_cancel_order(&mut self, symbol: &str, order_id: OrderBookId) -> Option<ExecutionReport> {
        self.remove_open_order(symbol, order_id)
            .map(|(_, report)| report)
    }

    /// Lifecycle record for `order_id`, if the engine has accepted it.
    pub fn order_record(&self, order_id: OrderBookId) -> Option<&OrderRecord> {
        self.ledger.get(order_id)
    }

    /// Whether `order_id` is currently open (`New` or `PartiallyFilled`).
    pub fn is_open(&self, order_id: OrderBookId) -> bool {
        self.ledger.is_open(order_id)
    }

    /// Every lifecycle record, keyed by order id.
    pub fn order_records(&self) -> impl Iterator<Item = (OrderBookId, &OrderRecord)> {
        self.ledger.iter()
    }

    /// Number of records retained by the ledger.
    pub fn order_count(&self) -> usize {
        self.ledger.len()
    }

    pub fn trades(&self) -> &Vec<Trade> {
        &self.trades
    }

    pub fn book(&self, symbol: &str) -> Option<&OrderBook> {
        self.order_books.get(symbol)
    }

    pub fn print_trades(&self) {
        for trade in &self.trades {
            let side = match trade.taker_side {
                Side::Bid => "BUY".green(),
                Side::Ask => "SELL".red(),
            };
            println!(
                "#{} {} {} {} @ {}",
                trade.id, trade.symbol, side, trade.quantity, trade.price
            );
        }
    }

    fn validate(&self, request: &OrderRequest) -> Result<(), EngineError> {
        self.validate_quantity(request.quantity)?;
        if request.order_type == OrderType::Limit {
            self.validate_price_on_tick(request.price)?;
        }
        // 市价单本质上是 IOC，GTC 市价单没有意义
        if request.order_type == OrderType::Market && request.time_in_force == TimeInForce::GTC {
            return Err(EngineError::InvalidTimeInForce);
        }
        // post-only 只能是限价 GTC
        if request.post_only && request.order_type == OrderType::Market {
            return Err(EngineError::InvalidTimeInForce);
        }
        if request.post_only && request.time_in_force != TimeInForce::GTC {
            return Err(EngineError::InvalidTimeInForce);
        }
        Ok(())
    }

    fn record_fills(&mut self, symbol: &str, timestamp: u64, fills: Vec<Fill>) -> Vec<Trade> {
        let mut record_trades = Vec::with_capacity(fills.len());
        fills.iter().for_each(|fill| {
            self.ledger.apply_fill(fill.maker_order_id, fill.quantity);

            let trade = Trade {
                id: self.next_trade_id,
                symbol: symbol.to_string(),
                timestamp,
                price: fill.price,
                quantity: fill.quantity,
                taker_side: fill.taker_side,
                maker_order_id: fill.maker_order_id,
                taker_order_id: fill.taker_order_id,
            };
            self.next_trade_id += 1;

            self.trades.push(trade.clone());
            self.last_trades.insert(
                symbol.to_string(),
                LastTrade {
                    price: trade.price,
                    timestamp: trade.timestamp,
                },
            );
            record_trades.push(trade);
        });
        record_trades
    }

    pub fn last_trade_price(&self, symbol: &str) -> Option<u64> {
        self.last_trades.get(symbol).map(|last| last.price)
    }

    pub fn events(&self) -> &[EngineEvent] {
        &self.events
    }

    /// Drop retained output events.
    ///
    /// Decision state, trades and the event sequence allocator are unaffected;
    /// a long-running runtime calls this after each command so output history
    /// does not grow without bound.
    pub fn clear_events(&mut self) {
        self.events.clear();
    }

    fn allocate_sequence_id(&mut self) -> u64 {
        let sequence_id = self.next_sequence_id;
        self.next_sequence_id += 1;
        self.last_applied_sequence = sequence_id;
        sequence_id
    }

    fn emit_accepted(&mut self, report: ExecutionReport) {
        let sequence_id = self.allocate_sequence_id();
        self.events.push(EngineEvent::OrderAccepted {
            sequence_id,
            report,
        });
    }

    fn emit_trade_executed(&mut self, trade: Trade) {
        let sequence_id = self.allocate_sequence_id();
        self.events
            .push(EngineEvent::TradeExecuted { sequence_id, trade });
    }

    fn emit_order_canceled(&mut self, report: ExecutionReport) {
        let sequence_id = self.allocate_sequence_id();
        self.events.push(EngineEvent::OrderCanceled {
            sequence_id,
            report,
        });
    }

    fn emit_order_rejected(&mut self, reason: EngineError) {
        let sequence_id = self.allocate_sequence_id();
        self.events.push(EngineEvent::OrderRejected {
            sequence_id,
            reason,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durable::DurableMatchingEngine;
    use crate::durable::memory::{InMemoryCommandLog, InMemorySnapshotStore};
    use crate::types::OrderStatus;

    fn limit(timestamp: u64, side: Side, price: u64, quantity: u64) -> OrderRequest {
        OrderRequest {
            side,
            price,
            quantity,
            timestamp,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::GTC,
            post_only: false,
        }
    }

    fn market(timestamp: u64, side: Side, quantity: u64) -> OrderRequest {
        OrderRequest {
            side,
            price: 0,
            quantity,
            timestamp,
            order_type: OrderType::Market,
            time_in_force: TimeInForce::IOC,
            post_only: false,
        }
    }

    fn ioc_limit(timestamp: u64, side: Side, price: u64, quantity: u64) -> OrderRequest {
        OrderRequest {
            time_in_force: TimeInForce::IOC,
            ..limit(timestamp, side, price, quantity)
        }
    }

    fn fok_limit(timestamp: u64, side: Side, price: u64, quantity: u64) -> OrderRequest {
        OrderRequest {
            time_in_force: TimeInForce::FOK,
            ..limit(timestamp, side, price, quantity)
        }
    }

    fn post_only_limit(timestamp: u64, side: Side, price: u64, quantity: u64) -> OrderRequest {
        OrderRequest {
            post_only: true,
            ..limit(timestamp, side, price, quantity)
        }
    }

    fn fok_market(timestamp: u64, side: Side, quantity: u64) -> OrderRequest {
        OrderRequest {
            time_in_force: TimeInForce::FOK,
            ..market(timestamp, side, quantity)
        }
    }

    fn submit_command(symbol: &str, request: OrderRequest) -> EngineCommand {
        EngineCommand::SubmitOrder {
            symbol: symbol.to_string(),
            request,
        }
    }

    fn cancel_command(symbol: &str, order_id: u64) -> EngineCommand {
        EngineCommand::CancelOrder {
            symbol: symbol.to_string(),
            order_id,
        }
    }

    fn modify_command(
        symbol: &str,
        order_id: u64,
        price: u64,
        quantity: u64,
        timestamp: u64,
    ) -> EngineCommand {
        EngineCommand::ModifyOrder {
            symbol: symbol.to_string(),
            order_id,
            request: ModifyRequest {
                price,
                quantity,
                timestamp,
            },
        }
    }

    /// Submit through the public command seam, keeping the old test shape of
    /// `Result<ExecutionReport, EngineError>`.
    fn submit(
        engine: &mut MatchingEngine,
        symbol: &str,
        request: OrderRequest,
    ) -> Result<ExecutionReport, EngineError> {
        match engine.execute(submit_command(symbol, request)) {
            CommandOutcome::Accepted(report) => Ok(report),
            CommandOutcome::Rejected(error) => Err(error),
            CommandOutcome::Canceled(_) => panic!("submit produced a cancel outcome"),
        }
    }

    /// Cancel through the public command seam, keeping the old test shape of
    /// `Option<ExecutionReport>`.
    fn cancel(engine: &mut MatchingEngine, symbol: &str, order_id: u64) -> Option<ExecutionReport> {
        match engine.execute(cancel_command(symbol, order_id)) {
            CommandOutcome::Canceled(report) => Some(report),
            CommandOutcome::Rejected(_) => None,
            CommandOutcome::Accepted(_) => panic!("cancel produced an accepted outcome"),
        }
    }

    /// Modify through the public command seam. A successful modify returns the
    /// report of the replacement order; the canceled half remains in the events.
    fn modify(
        engine: &mut MatchingEngine,
        symbol: &str,
        order_id: u64,
        price: u64,
        quantity: u64,
        timestamp: u64,
    ) -> Result<ExecutionReport, EngineError> {
        match engine.execute(modify_command(symbol, order_id, price, quantity, timestamp)) {
            CommandOutcome::Accepted(report) => Ok(report),
            CommandOutcome::Rejected(error) => Err(error),
            CommandOutcome::Canceled(_) => panic!("modify produced a cancel outcome"),
        }
    }

    /// Snapshot the ledger through the public query so replay comparisons are
    /// independent of hash-map iteration order.
    fn records(engine: &MatchingEngine) -> HashMap<OrderBookId, OrderRecord> {
        engine
            .order_records()
            .map(|(order_id, record)| (order_id, record.clone()))
            .collect()
    }

    /// Execute every command, ignoring outcomes; individual tests assert the
    /// outcomes they care about.
    fn execute_all(engine: &mut MatchingEngine, commands: &[EngineCommand]) {
        for command in commands {
            let _ = engine.execute(command.clone());
        }
    }

    /// Replay a locally collected command stream through the durable
    /// append-then-apply path. Events are deliberately not recovered, so
    /// callers compare decision state rather than event history.
    fn recover(commands: &[EngineCommand]) -> DurableMatchingEngine {
        let log = InMemoryCommandLog::new();
        let snapshots = InMemorySnapshotStore::new();
        DurableMatchingEngine::from_commands(
            EngineConfig::default(),
            commands,
            Box::new(log.handle()),
            Box::new(snapshots.handle()),
            0,
        )
        .expect("replaying a valid command stream succeeds")
    }

    /// Assert that durable replay reproduces the pure engine's decision state,
    /// then hand the recovered engine back for scenario-specific comparisons.
    fn assert_recovered(
        commands: &[EngineCommand],
        engine: &MatchingEngine,
    ) -> DurableMatchingEngine {
        // Events belong to live execution only; recovery does not restore them.
        let live_events = engine.events();
        assert!(!live_events.is_empty(), "live execution emits events");
        assert_eq!(
            live_events.last().unwrap().sequence_id(),
            engine.last_sequence(),
            "live event history matches the engine's last sequence"
        );

        let expected = engine.capture_snapshot();
        let recovered = recover(commands);
        let actual = recovered.engine().capture_snapshot();

        assert_eq!(actual, expected, "durable replay reproduces decision state");
        assert_eq!(recovered.applied_seq(), commands.len() as u64);
        assert_eq!(recovered.engine().trades(), engine.trades());
        assert_eq!(records(recovered.engine()), records(engine));
        assert_eq!(recovered.engine().order_count(), engine.order_count());
        // Counters that the old tests read as private fields.
        assert_eq!(actual.next_order_id, expected.next_order_id);
        assert_eq!(actual.next_trade_id, expected.next_trade_id);
        assert_eq!(actual.next_event_sequence, expected.next_event_sequence);

        recovered
    }

    #[test]
    fn crossing_orders_produce_a_trade() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 5)).unwrap();
        submit(&mut engine, "BTC-USDT", limit(2, Side::Bid, 100, 5)).unwrap();

        assert_eq!(engine.trades().len(), 1);
        let trade = &engine.trades()[0];
        assert_eq!(trade.id, 1);
        assert_eq!(trade.symbol, "BTC-USDT");
        assert_eq!(trade.maker_order_id, 1);
        assert_eq!(trade.taker_order_id, 2);
        assert_eq!(trade.price, 100);
        assert_eq!(trade.quantity, 5);
        assert_eq!(trade.taker_side, Side::Bid);
        assert_eq!(trade.timestamp, 2); // taker 的 timestamp
    }

    #[test]
    fn trade_ids_increment_across_price_levels() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 2)).unwrap();
        submit(&mut engine, "BTC-USDT", limit(2, Side::Ask, 101, 3)).unwrap();
        submit(&mut engine, "BTC-USDT", limit(3, Side::Bid, 101, 4)).unwrap();

        let trades = engine.trades();
        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].id, 1);
        assert_eq!(trades[0].maker_order_id, 1);
        assert_eq!(trades[0].price, 100);
        assert_eq!(trades[0].quantity, 2);
        assert_eq!(trades[1].id, 2);
        assert_eq!(trades[1].maker_order_id, 2);
        assert_eq!(trades[1].price, 101);
        assert_eq!(trades[1].quantity, 2);
        assert!(
            trades
                .iter()
                .all(|t| t.taker_order_id == 3 && t.taker_side == Side::Bid)
        );
    }

    #[test]
    fn market_order_records_fill_and_does_not_rest() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 2)).unwrap();

        submit(&mut engine, "BTC-USDT", market(2, Side::Bid, 5)).unwrap();

        assert_eq!(engine.trades().len(), 1);
        let trade = &engine.trades()[0];
        assert_eq!(trade.maker_order_id, 1);
        assert_eq!(trade.taker_order_id, 2);
        assert_eq!(trade.price, 100);
        assert_eq!(trade.quantity, 2);
        assert_eq!(trade.taker_side, Side::Bid);
    }

    #[test]
    fn books_are_isolated_per_symbol() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 5)).unwrap();
        submit(&mut engine, "ETH-USDT", limit(2, Side::Bid, 100, 5)).unwrap();

        assert!(engine.trades().is_empty());
        assert!(engine.book("BTC-USDT").is_some());
        assert!(engine.book("ETH-USDT").is_some());
        assert!(engine.book("SOL-USDT").is_none());
    }

    #[test]
    fn config_rejects_invalid_orders() {
        let config = EngineConfig {
            min_order_quantity: 1,
            max_order_quantity: 10,
            tick_size: 5,
        };
        let mut engine = MatchingEngine::new(config);

        assert_eq!(
            submit(&mut engine, "X", limit(1, Side::Bid, 100, 0)).unwrap_err(),
            EngineError::QuantityOutOfRange
        );
        assert_eq!(
            submit(&mut engine, "X", limit(2, Side::Bid, 100, 11)).unwrap_err(),
            EngineError::QuantityOutOfRange
        );
        assert_eq!(
            submit(&mut engine, "X", limit(3, Side::Bid, 101, 5)).unwrap_err(),
            EngineError::PriceNotOnTick
        );
        assert!(engine.trades().is_empty());
        assert!(engine.book("X").is_none()); // 校验失败不应创建簿
    }

    #[test]
    fn cancel_is_scoped_to_symbol() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Bid, 100, 5)).unwrap();

        let report = cancel(&mut engine, "BTC-USDT", 1).unwrap();
        assert_eq!(report.order_id, 1);
        assert_eq!(report.symbol, "BTC-USDT");
        assert_eq!(report.status, OrderStatus::Canceled);
        assert_eq!(report.original_quantity, 5);
        assert_eq!(report.filled_quantity, 0);
        assert_eq!(report.remaining_quantity, 5);
        assert!(report.fills.is_empty());

        assert_eq!(
            engine.order_record(1).unwrap().status,
            OrderStatus::Canceled
        );
        assert!(cancel(&mut engine, "BTC-USDT", 1).is_none());
        assert!(cancel(&mut engine, "ETH-USDT", 1).is_none());
    }

    #[test]
    fn resting_limit_order_reports_new() {
        let mut engine = MatchingEngine::new(EngineConfig::default());

        let report = submit(&mut engine, "BTC-USDT", limit(1, Side::Bid, 100, 5)).unwrap();

        assert_eq!(report.order_id, 1);
        assert_eq!(report.symbol, "BTC-USDT");
        assert_eq!(report.status, OrderStatus::New);
        assert_eq!(report.original_quantity, 5);
        assert_eq!(report.filled_quantity, 0);
        assert_eq!(report.remaining_quantity, 5);
        assert!(report.fills.is_empty());
        assert!(engine.trades().is_empty());
    }

    #[test]
    fn fully_filled_limit_order_reports_filled() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 5)).unwrap();

        let report = submit(&mut engine, "BTC-USDT", limit(2, Side::Bid, 100, 5)).unwrap();

        assert_eq!(report.status, OrderStatus::Filled);
        assert_eq!(report.original_quantity, 5);
        assert_eq!(report.filled_quantity, 5);
        assert_eq!(report.remaining_quantity, 0);
        assert_eq!(report.fills.len(), 1);
        assert_eq!(report.fills[0].id, 1);
        assert_eq!(report.fills[0].maker_order_id, 1);
        assert_eq!(report.fills[0].taker_order_id, 2);
        assert_eq!(report.fills[0].price, 100);
        assert_eq!(report.fills[0].quantity, 5);
    }

    #[test]
    fn partially_filled_limit_order_reports_partially_filled() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 3)).unwrap();

        let report = submit(&mut engine, "BTC-USDT", limit(2, Side::Bid, 105, 10)).unwrap();

        assert_eq!(report.status, OrderStatus::PartiallyFilled);
        assert_eq!(report.original_quantity, 10);
        assert_eq!(report.filled_quantity, 3);
        assert_eq!(report.remaining_quantity, 7);
        assert_eq!(report.fills.len(), 1);
        assert_eq!(report.fills[0].price, 100);
        assert_eq!(report.fills[0].quantity, 3);
    }

    #[test]
    fn filled_market_order_reports_filled() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 2)).unwrap();

        let report = submit(&mut engine, "BTC-USDT", market(2, Side::Bid, 2)).unwrap();

        assert_eq!(report.status, OrderStatus::Filled);
        assert_eq!(report.filled_quantity, 2);
        assert_eq!(report.remaining_quantity, 0);
        assert_eq!(report.fills.len(), 1);
    }

    #[test]
    fn partially_filled_market_order_reports_canceled() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 2)).unwrap();

        let report = submit(&mut engine, "BTC-USDT", market(2, Side::Bid, 5)).unwrap();

        assert_eq!(report.status, OrderStatus::Canceled);
        assert_eq!(report.original_quantity, 5);
        assert_eq!(report.filled_quantity, 2);
        assert_eq!(report.remaining_quantity, 3);
        assert_eq!(report.fills.len(), 1);
        assert_eq!(report.fills[0].quantity, 2);
    }

    #[test]
    fn market_order_without_liquidity_reports_canceled() {
        let mut engine = MatchingEngine::new(EngineConfig::default());

        let report = submit(&mut engine, "BTC-USDT", market(1, Side::Bid, 5)).unwrap();

        assert_eq!(report.status, OrderStatus::Canceled);
        assert_eq!(report.original_quantity, 5);
        assert_eq!(report.filled_quantity, 0);
        assert_eq!(report.remaining_quantity, 5);
        assert!(report.fills.is_empty());
        assert!(engine.trades().is_empty());
    }

    #[test]
    fn cancel_after_partial_fill_reports_remaining_quantity() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 2)).unwrap();
        submit(&mut engine, "BTC-USDT", limit(2, Side::Bid, 105, 5)).unwrap();

        let report = cancel(&mut engine, "BTC-USDT", 2).unwrap();

        assert_eq!(report.order_id, 2);
        assert_eq!(report.symbol, "BTC-USDT");
        assert_eq!(report.status, OrderStatus::Canceled);
        assert_eq!(report.original_quantity, 5);
        assert_eq!(report.filled_quantity, 2);
        assert_eq!(report.remaining_quantity, 3);
        assert!(report.fills.is_empty());

        let record = engine.order_record(2).unwrap();
        assert_eq!(record.status, OrderStatus::Canceled);
        assert_eq!(record.filled_quantity, 2);
        assert_eq!(record.remaining_quantity, 3);
    }

    #[test]
    fn order_ids_are_assigned_by_engine_across_symbols() {
        let mut engine = MatchingEngine::new(EngineConfig::default());

        let first = submit(&mut engine, "BTC-USDT", limit(10, Side::Bid, 100, 1)).unwrap();
        let second = submit(&mut engine, "ETH-USDT", limit(20, Side::Bid, 100, 1)).unwrap();

        assert_eq!(first.order_id, 1);
        assert_eq!(second.order_id, 2);
    }

    #[test]
    fn rejected_order_does_not_consume_order_id() {
        let mut engine = MatchingEngine::new(EngineConfig::default());

        assert!(submit(&mut engine, "BTC-USDT", limit(1, Side::Bid, 100, 0)).is_err());

        let report = submit(&mut engine, "BTC-USDT", limit(2, Side::Bid, 100, 1)).unwrap();
        assert_eq!(report.order_id, 1);
    }

    #[test]
    fn order_records_track_fills_for_maker_and_taker() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 3)).unwrap();

        // taker 成交 3，剩余 2 挂在 100
        submit(&mut engine, "BTC-USDT", limit(2, Side::Bid, 100, 5)).unwrap();

        let maker = engine.order_record(1).unwrap();
        assert_eq!(maker.status, OrderStatus::Filled);
        assert_eq!(maker.original_quantity, 3);
        assert_eq!(maker.filled_quantity, 3);
        assert_eq!(maker.remaining_quantity, 0);

        let taker = engine.order_record(2).unwrap();
        assert_eq!(taker.status, OrderStatus::PartiallyFilled);
        assert_eq!(taker.original_quantity, 5);
        assert_eq!(taker.filled_quantity, 3);
        assert_eq!(taker.remaining_quantity, 2);
    }

    #[test]
    fn maker_record_updates_across_multiple_fills() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 5)).unwrap();

        submit(&mut engine, "BTC-USDT", limit(2, Side::Bid, 100, 2)).unwrap();
        let maker = engine.order_record(1).unwrap();
        assert_eq!(maker.status, OrderStatus::PartiallyFilled);
        assert_eq!(maker.filled_quantity, 2);
        assert_eq!(maker.remaining_quantity, 3);

        submit(&mut engine, "BTC-USDT", limit(3, Side::Bid, 100, 3)).unwrap();
        let maker = engine.order_record(1).unwrap();
        assert_eq!(maker.status, OrderStatus::Filled);
        assert_eq!(maker.filled_quantity, 5);
        assert_eq!(maker.remaining_quantity, 0);
    }

    #[test]
    fn order_record_is_created_for_market_orders() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 2)).unwrap();

        let report = submit(&mut engine, "BTC-USDT", market(2, Side::Bid, 5)).unwrap();

        let record = engine.order_record(report.order_id).unwrap();
        assert_eq!(record.symbol, "BTC-USDT");
        assert_eq!(record.status, OrderStatus::Canceled);
        assert_eq!(record.original_quantity, 5);
        assert_eq!(record.filled_quantity, 2);
        assert_eq!(record.remaining_quantity, 3);
    }

    #[test]
    fn cancel_filled_or_already_canceled_order_returns_none() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 2)).unwrap();
        submit(&mut engine, "BTC-USDT", limit(2, Side::Bid, 100, 2)).unwrap();

        // 双方都已完全成交
        assert!(cancel(&mut engine, "BTC-USDT", 1).is_none());
        assert!(cancel(&mut engine, "BTC-USDT", 2).is_none());

        submit(&mut engine, "BTC-USDT", limit(3, Side::Bid, 99, 1)).unwrap();
        assert!(cancel(&mut engine, "BTC-USDT", 3).is_some());
        assert!(cancel(&mut engine, "BTC-USDT", 3).is_none());
    }

    #[test]
    fn book_accessor_exposes_market_data() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Bid, 100, 2)).unwrap();
        submit(&mut engine, "BTC-USDT", limit(2, Side::Bid, 99, 3)).unwrap();
        submit(&mut engine, "BTC-USDT", limit(3, Side::Ask, 101, 4)).unwrap();

        let book = engine.book("BTC-USDT").unwrap();
        assert_eq!(book.best_bid(), Some(100));
        assert_eq!(book.best_ask(), Some(101));
        assert_eq!(book.depth(Side::Bid, 0), Some((100, 2)));
        assert_eq!(book.depth(Side::Bid, 1), Some((99, 3)));
        assert_eq!(book.depth(Side::Ask, 0), Some((101, 4)));
        assert_eq!(book.depth(Side::Bid, 2), None);
    }

    fn event_sequence(event: &EngineEvent) -> u64 {
        match event {
            EngineEvent::OrderAccepted { sequence_id, .. }
            | EngineEvent::TradeExecuted { sequence_id, .. }
            | EngineEvent::OrderCanceled { sequence_id, .. }
            | EngineEvent::OrderRejected { sequence_id, .. }
            | EngineEvent::StopTriggered { sequence_id, .. } => *sequence_id,
        }
    }

    #[test]
    fn events_are_emitted_in_sequence() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 5)).unwrap();
        submit(&mut engine, "BTC-USDT", limit(2, Side::Bid, 100, 5)).unwrap();

        let events = engine.events();
        assert_eq!(events.len(), 3);

        match &events[0] {
            EngineEvent::OrderAccepted {
                sequence_id,
                report,
            } => {
                assert_eq!(*sequence_id, 1);
                assert_eq!(report.order_id, 1);
                assert_eq!(report.status, OrderStatus::New);
                assert!(report.fills.is_empty());
            }
            _ => panic!("expected OrderAccepted for resting ask"),
        }

        match &events[1] {
            EngineEvent::OrderAccepted {
                sequence_id,
                report,
            } => {
                assert_eq!(*sequence_id, 2);
                assert_eq!(report.order_id, 2);
                assert_eq!(report.status, OrderStatus::Filled);
                assert_eq!(report.fills.len(), 1);
            }
            _ => panic!("expected OrderAccepted for crossing bid"),
        }

        match &events[2] {
            EngineEvent::TradeExecuted { sequence_id, trade } => {
                assert_eq!(*sequence_id, 3);
                assert_eq!(trade.id, 1);
                assert_eq!(trade.maker_order_id, 1);
                assert_eq!(trade.taker_order_id, 2);
                assert_eq!(trade.price, 100);
                assert_eq!(trade.quantity, 5);
            }
            _ => panic!("expected TradeExecuted"),
        }

        assert!(
            events
                .windows(2)
                .all(|window| event_sequence(&window[0]) + 1 == event_sequence(&window[1]))
        );
    }

    #[test]
    fn rejected_order_emits_event_without_consuming_order_id() {
        let mut engine = MatchingEngine::new(EngineConfig::default());

        let error = submit(&mut engine, "BTC-USDT", limit(1, Side::Bid, 100, 0)).unwrap_err();
        assert_eq!(error, EngineError::QuantityOutOfRange);

        assert_eq!(engine.events().len(), 1);
        match &engine.events()[0] {
            EngineEvent::OrderRejected {
                sequence_id,
                reason,
            } => {
                assert_eq!(*sequence_id, 1);
                assert_eq!(*reason, EngineError::QuantityOutOfRange);
            }
            _ => panic!("expected OrderRejected"),
        }

        let report = submit(&mut engine, "BTC-USDT", limit(2, Side::Bid, 100, 1)).unwrap();
        assert_eq!(report.order_id, 1);

        match engine.events().last().unwrap() {
            EngineEvent::OrderAccepted { sequence_id, .. } => assert_eq!(*sequence_id, 2),
            _ => panic!("expected OrderAccepted"),
        }
    }

    #[test]
    fn cancel_emits_order_canceled_event() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Bid, 100, 5)).unwrap();

        let report = cancel(&mut engine, "BTC-USDT", 1).unwrap();

        match engine.events().last().unwrap() {
            EngineEvent::OrderCanceled {
                sequence_id,
                report: event_report,
            } => {
                assert_eq!(*sequence_id, 2);
                assert_eq!(event_report.order_id, report.order_id);
                assert_eq!(event_report.status, OrderStatus::Canceled);
                assert_eq!(event_report.original_quantity, 5);
                assert_eq!(event_report.filled_quantity, 0);
                assert_eq!(event_report.remaining_quantity, 5);
                assert!(event_report.fills.is_empty());
            }
            _ => panic!("expected OrderCanceled"),
        }

        // 失败的撤单目前不产生事件
        let event_count = engine.events().len();
        assert!(cancel(&mut engine, "BTC-USDT", 1).is_none());
        assert_eq!(engine.events().len(), event_count);
    }

    #[test]
    fn trade_executed_events_match_trade_tape() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 2)).unwrap();
        submit(&mut engine, "BTC-USDT", limit(2, Side::Ask, 101, 3)).unwrap();
        submit(&mut engine, "BTC-USDT", limit(3, Side::Bid, 101, 4)).unwrap();

        let event_trades: Vec<&Trade> = engine
            .events()
            .iter()
            .filter_map(|event| match event {
                EngineEvent::TradeExecuted { trade, .. } => Some(trade),
                _ => None,
            })
            .collect();

        assert_eq!(event_trades.len(), engine.trades().len());
        for (event_trade, tape_trade) in event_trades.iter().zip(engine.trades().iter()) {
            assert_eq!(event_trade.id, tape_trade.id);
            assert_eq!(event_trade.price, tape_trade.price);
            assert_eq!(event_trade.quantity, tape_trade.quantity);
            assert_eq!(event_trade.maker_order_id, tape_trade.maker_order_id);
            assert_eq!(event_trade.taker_order_id, tape_trade.taker_order_id);
            assert_eq!(event_trade.taker_side, tape_trade.taker_side);
            assert_eq!(event_trade.timestamp, tape_trade.timestamp);
        }
    }

    #[test]
    fn market_order_emits_accepted_and_trade_events() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 2)).unwrap();

        let report = submit(&mut engine, "BTC-USDT", market(2, Side::Bid, 5)).unwrap();

        // ask 的 Accepted + market 的 Accepted + 1 笔成交
        assert_eq!(engine.events().len(), 3);

        match &engine.events()[1] {
            EngineEvent::OrderAccepted {
                sequence_id,
                report: accepted,
            } => {
                assert_eq!(*sequence_id, 2);
                assert_eq!(accepted.order_id, report.order_id);
                assert_eq!(accepted.status, OrderStatus::Canceled);
            }
            _ => panic!("expected OrderAccepted for market order"),
        }

        match &engine.events()[2] {
            EngineEvent::TradeExecuted { sequence_id, trade } => {
                assert_eq!(*sequence_id, 3);
                assert_eq!(trade.taker_order_id, report.order_id);
                assert_eq!(trade.quantity, 2);
            }
            _ => panic!("expected TradeExecuted"),
        }
    }

    #[test]
    fn commands_are_recorded_in_order() {
        let commands = vec![
            submit_command("BTC-USDT", limit(1, Side::Ask, 100, 5)),
            submit_command("BTC-USDT", market(2, Side::Bid, 2)),
            cancel_command("BTC-USDT", 1),
        ];

        let log = InMemoryCommandLog::new();
        let snapshots = InMemorySnapshotStore::new();
        let mut durable = DurableMatchingEngine::create(
            EngineConfig::default(),
            Box::new(log.handle()),
            Box::new(snapshots.handle()),
            0,
        )
        .unwrap();
        for command in &commands {
            durable.execute(command.clone()).unwrap();
        }

        let history = durable.history_from(1).unwrap();
        assert_eq!(history.len(), 3);
        for (record, expected) in history.iter().zip(&commands) {
            assert_eq!(&record.command, expected);
        }

        match &history[0].command {
            EngineCommand::SubmitOrder { symbol, request } => {
                assert_eq!(symbol, "BTC-USDT");
                assert_eq!(request.order_type, OrderType::Limit);
                assert_eq!(request.side, Side::Ask);
                assert_eq!(request.price, 100);
                assert_eq!(request.quantity, 5);
            }
            _ => panic!("expected SubmitOrder"),
        }

        match &history[1].command {
            EngineCommand::SubmitOrder { symbol, request } => {
                assert_eq!(symbol, "BTC-USDT");
                assert_eq!(request.order_type, OrderType::Market);
                assert_eq!(request.side, Side::Bid);
                assert_eq!(request.quantity, 2);
            }
            _ => panic!("expected SubmitOrder"),
        }

        match &history[2].command {
            EngineCommand::CancelOrder { symbol, order_id } => {
                assert_eq!(symbol, "BTC-USDT");
                assert_eq!(*order_id, 1);
            }
            _ => panic!("expected CancelOrder"),
        }
    }

    #[test]
    fn submit_command_returns_accepted_outcome_for_limit_and_market() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        let mut commands: Vec<EngineCommand> = Vec::new();

        let command = submit_command("BTC-USDT", limit(1, Side::Ask, 100, 5));
        commands.push(command.clone());
        match engine.execute(command) {
            CommandOutcome::Accepted(report) => {
                assert_eq!(report.order_id, 1);
                assert_eq!(report.status, OrderStatus::New);
                assert_eq!(report.remaining_quantity, 5);
                assert!(report.fills.is_empty());
            }
            other => panic!("expected Accepted, got {other:?}"),
        }

        let command = submit_command("BTC-USDT", market(2, Side::Bid, 5));
        commands.push(command.clone());
        match engine.execute(command) {
            CommandOutcome::Accepted(report) => {
                assert_eq!(report.order_id, 2);
                assert_eq!(report.status, OrderStatus::Filled);
                assert_eq!(report.filled_quantity, 5);
                assert_eq!(report.fills.len(), 1);
            }
            other => panic!("expected Accepted, got {other:?}"),
        }

        assert_eq!(engine.trades().len(), 1);
        assert_eq!(commands.len(), 2);
    }

    #[test]
    fn cancel_command_returns_canceled_and_failures_map_to_rejected() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        let mut commands: Vec<EngineCommand> = Vec::new();

        let command = submit_command("BTC-USDT", limit(1, Side::Bid, 100, 5));
        commands.push(command.clone());
        let submitted = engine.execute(command);
        assert!(matches!(submitted, CommandOutcome::Accepted(_)));

        // 错误 symbol 与未知订单一样映射为 Rejected，且不产生事件
        let events_before = engine.events().len();
        let command = cancel_command("ETH-USDT", 1);
        commands.push(command.clone());
        let wrong_symbol = engine.execute(command);
        assert_eq!(
            wrong_symbol,
            CommandOutcome::Rejected(EngineError::UnknownOrder)
        );
        assert_eq!(engine.events().len(), events_before);

        let command = cancel_command("BTC-USDT", 1);
        commands.push(command.clone());
        match engine.execute(command) {
            CommandOutcome::Canceled(report) => {
                assert_eq!(report.order_id, 1);
                assert_eq!(report.status, OrderStatus::Canceled);
                assert_eq!(report.remaining_quantity, 5);
            }
            other => panic!("expected Canceled, got {other:?}"),
        }

        // 已撤销的订单再次撤销同样被拒绝
        let command = cancel_command("BTC-USDT", 1);
        commands.push(command.clone());
        let already_canceled = engine.execute(command);
        assert_eq!(
            already_canceled,
            CommandOutcome::Rejected(EngineError::UnknownOrder)
        );

        // 四次命令全部写入持久化命令流：submit + 三次 cancel
        assert_eq!(commands.len(), 4);

        // 回放失败的撤单不产生事件，也不改变状态
        let recovered = assert_recovered(&commands, &engine);
        assert_eq!(recovered.engine().book("BTC-USDT"), engine.book("BTC-USDT"));
        let history = recovered.history_from(1).unwrap();
        assert_eq!(history.len(), 4);
        for (record, expected) in history.iter().zip(&commands) {
            assert_eq!(&record.command, expected);
        }
    }

    #[test]
    fn replay_reproduces_state_and_counters_after_execute() {
        let commands = vec![
            submit_command("BTC-USDT", limit(1, Side::Ask, 100, 5)),
            submit_command("BTC-USDT", limit(2, Side::Bid, 100, 3)),
            cancel_command("BTC-USDT", 1),
        ];
        let mut engine = MatchingEngine::new(EngineConfig::default());
        execute_all(&mut engine, &commands);

        let recovered = assert_recovered(&commands, &engine);
        assert_eq!(recovered.engine().book("BTC-USDT"), engine.book("BTC-USDT"));
    }

    #[test]
    fn rejected_order_is_recorded_and_replayed() {
        let mut engine = MatchingEngine::new(EngineConfig::default());

        let command = submit_command("BTC-USDT", limit(1, Side::Bid, 100, 0));
        let commands = vec![command.clone()];
        let outcome = engine.execute(command);
        assert_eq!(
            outcome,
            CommandOutcome::Rejected(EngineError::QuantityOutOfRange)
        );

        // 拒绝的请求也要进持久化命令流，回放才能复现同一个拒绝
        assert_eq!(commands.len(), 1);
        assert_eq!(engine.events().len(), 1);

        let recovered = assert_recovered(&commands, &engine);
        let history = recovered.history_from(1).unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].command, commands[0]);
        assert_eq!(recovered.engine().order_count(), 0);
        let snapshot = recovered.engine().capture_snapshot();
        assert_eq!(snapshot.next_order_id, 1);
        assert_eq!(snapshot.next_event_sequence, 2);
    }

    #[test]
    fn replay_reproduces_identical_state() {
        let mut engine = MatchingEngine::new(EngineConfig::default());

        let commands = vec![
            // 部分成交
            submit_command("BTC-USDT", limit(1, Side::Ask, 100, 5)),
            submit_command("BTC-USDT", limit(2, Side::Bid, 100, 2)),
            // 撤销剩余挂单
            cancel_command("BTC-USDT", 1),
            // 挂单后由市价单部分吃掉
            submit_command("BTC-USDT", limit(3, Side::Bid, 99, 4)),
            submit_command("BTC-USDT", market(4, Side::Ask, 1)),
            // 非法订单：产生拒绝事件，不消耗订单 id
            submit_command("BTC-USDT", limit(5, Side::Bid, 98, 0)),
            // 另一个 symbol
            submit_command("ETH-USDT", limit(6, Side::Ask, 200, 1)),
        ];
        execute_all(&mut engine, &commands[..5]);
        assert_eq!(
            engine.execute(commands[5].clone()),
            CommandOutcome::Rejected(EngineError::QuantityOutOfRange)
        );
        assert!(matches!(
            engine.execute(commands[6].clone()),
            CommandOutcome::Accepted(_)
        ));

        let recovered = assert_recovered(&commands, &engine);
        assert_eq!(recovered.engine().book("BTC-USDT"), engine.book("BTC-USDT"));
        assert_eq!(recovered.engine().book("ETH-USDT"), engine.book("ETH-USDT"));
        assert_eq!(
            recovered.engine().snapshot("BTC-USDT", 10),
            engine.snapshot("BTC-USDT", 10)
        );
        assert_eq!(
            recovered.engine().snapshot("ETH-USDT", 10),
            engine.snapshot("ETH-USDT", 10)
        );
    }

    #[test]
    fn snapshot_carries_last_event_sequence() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Bid, 100, 5)).unwrap();

        let snapshot = engine.snapshot("BTC-USDT", 10).unwrap();
        assert_eq!(snapshot.symbol, "BTC-USDT");
        assert_eq!(snapshot.sequence, 1);
        assert_eq!(snapshot.bids, vec![(100, 5)]);
        assert!(snapshot.asks.is_empty());

        // 成交后最后一条事件是 TradeExecuted
        submit(&mut engine, "BTC-USDT", limit(2, Side::Ask, 100, 5)).unwrap();

        let snapshot = engine.snapshot("BTC-USDT", 10).unwrap();
        assert_eq!(snapshot.sequence, 3);
        assert!(snapshot.bids.is_empty());
        assert!(snapshot.asks.is_empty());
    }

    #[test]
    fn snapshot_unknown_symbol_returns_none() {
        let engine = MatchingEngine::new(EngineConfig::default());

        assert!(engine.snapshot("BTC-USDT", 10).is_none());
    }

    #[test]
    fn snapshot_depth_limits_levels() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Bid, 99, 1)).unwrap();
        submit(&mut engine, "BTC-USDT", limit(2, Side::Bid, 101, 2)).unwrap();
        submit(&mut engine, "BTC-USDT", limit(3, Side::Bid, 100, 3)).unwrap();

        let snapshot = engine.snapshot("BTC-USDT", 2).unwrap();
        assert_eq!(snapshot.bids, vec![(101, 2), (100, 3)]);
        assert!(snapshot.asks.is_empty());
        assert_eq!(snapshot.sequence, 3);
    }

    #[test]
    fn snapshot_reflects_canceled_volume() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 5)).unwrap();
        submit(&mut engine, "BTC-USDT", limit(2, Side::Ask, 100, 3)).unwrap();
        assert_eq!(
            engine.snapshot("BTC-USDT", 10).unwrap().asks,
            vec![(100, 8)]
        );

        cancel(&mut engine, "BTC-USDT", 1).unwrap();
        assert_eq!(
            engine.snapshot("BTC-USDT", 10).unwrap().asks,
            vec![(100, 3)]
        );
    }

    #[test]
    fn ioc_limit_fully_filled_reports_filled() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 5)).unwrap();

        let report = submit(&mut engine, "BTC-USDT", ioc_limit(2, Side::Bid, 100, 5)).unwrap();

        assert_eq!(report.status, OrderStatus::Filled);
        assert_eq!(report.filled_quantity, 5);
        assert_eq!(report.remaining_quantity, 0);
        assert_eq!(report.fills.len(), 1);

        let book = engine.book("BTC-USDT").unwrap();
        assert_eq!(book.best_bid(), None);
        assert_eq!(book.best_ask(), None);
    }

    #[test]
    fn ioc_limit_partial_fill_reports_canceled_and_does_not_rest() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 2)).unwrap();

        let report = submit(&mut engine, "BTC-USDT", ioc_limit(2, Side::Bid, 100, 5)).unwrap();

        assert_eq!(report.status, OrderStatus::Canceled);
        assert_eq!(report.original_quantity, 5);
        assert_eq!(report.filled_quantity, 2);
        assert_eq!(report.remaining_quantity, 3);
        assert_eq!(report.fills.len(), 1);

        let book = engine.book("BTC-USDT").unwrap();
        assert_eq!(book.best_bid(), None);
        assert_eq!(book.best_ask(), None);

        let record = engine.order_record(report.order_id).unwrap();
        assert_eq!(record.status, OrderStatus::Canceled);
        assert_eq!(record.filled_quantity, 2);
        assert_eq!(record.remaining_quantity, 3);
    }

    #[test]
    fn ioc_limit_with_no_liquidity_is_canceled() {
        let mut engine = MatchingEngine::new(EngineConfig::default());

        let report = submit(&mut engine, "BTC-USDT", ioc_limit(1, Side::Bid, 100, 5)).unwrap();

        assert_eq!(report.status, OrderStatus::Canceled);
        assert_eq!(report.filled_quantity, 0);
        assert_eq!(report.remaining_quantity, 5);
        assert!(report.fills.is_empty());
        assert!(engine.trades().is_empty());

        assert_eq!(engine.book("BTC-USDT").unwrap().best_bid(), None);
    }

    #[test]
    fn fok_limit_fills_across_levels() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 2)).unwrap();
        submit(&mut engine, "BTC-USDT", limit(2, Side::Ask, 101, 3)).unwrap();

        let report = submit(&mut engine, "BTC-USDT", fok_limit(3, Side::Bid, 101, 5)).unwrap();

        assert_eq!(report.status, OrderStatus::Filled);
        assert_eq!(report.filled_quantity, 5);
        assert_eq!(report.remaining_quantity, 0);
        assert_eq!(report.fills.len(), 2);
        assert_eq!(report.fills[0].price, 100);
        assert_eq!(report.fills[0].quantity, 2);
        assert_eq!(report.fills[1].price, 101);
        assert_eq!(report.fills[1].quantity, 3);
    }

    #[test]
    fn fok_limit_rejects_when_liquidity_insufficient() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 2)).unwrap();

        let next_id_before = engine.next_order_id;
        let events_before = engine.events().len();

        let error = submit(&mut engine, "BTC-USDT", fok_limit(2, Side::Bid, 100, 5)).unwrap_err();

        assert_eq!(error, EngineError::InsufficientLiquidity);
        assert_eq!(engine.next_order_id, next_id_before);
        assert_eq!(engine.order_count(), 1);
        assert!(engine.trades().is_empty());
        assert_eq!(
            engine.book("BTC-USDT").unwrap().depth(Side::Ask, 0),
            Some((100, 2))
        );
        assert_eq!(engine.events().len(), events_before + 1);
        match engine.events().last().unwrap() {
            EngineEvent::OrderRejected { reason, .. } => {
                assert_eq!(*reason, EngineError::InsufficientLiquidity)
            }
            _ => panic!("expected OrderRejected"),
        }
    }

    #[test]
    fn fok_limit_respects_limit_price() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 5)).unwrap();

        let error = submit(&mut engine, "BTC-USDT", fok_limit(2, Side::Bid, 99, 5)).unwrap_err();

        assert_eq!(error, EngineError::InsufficientLiquidity);
        assert!(engine.trades().is_empty());
        assert_eq!(engine.book("BTC-USDT").unwrap().best_ask(), Some(100));
    }

    #[test]
    fn fok_market_fills_when_liquidity_sufficient() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 2)).unwrap();
        submit(&mut engine, "BTC-USDT", limit(2, Side::Ask, 101, 3)).unwrap();

        let report = submit(&mut engine, "BTC-USDT", fok_market(3, Side::Bid, 4)).unwrap();

        assert_eq!(report.status, OrderStatus::Filled);
        assert_eq!(report.filled_quantity, 4);
        assert_eq!(report.remaining_quantity, 0);
        assert_eq!(report.fills.len(), 2);
    }

    #[test]
    fn fok_market_rejects_when_liquidity_insufficient() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 2)).unwrap();

        let error = submit(&mut engine, "BTC-USDT", fok_market(2, Side::Bid, 5)).unwrap_err();

        assert_eq!(error, EngineError::InsufficientLiquidity);
        assert_eq!(engine.next_order_id, 2);
        assert!(engine.trades().is_empty());
        assert_eq!(engine.book("BTC-USDT").unwrap().best_ask(), Some(100));
    }

    #[test]
    fn post_only_rejects_crossing_bid() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 5)).unwrap();

        let error = submit(
            &mut engine,
            "BTC-USDT",
            post_only_limit(2, Side::Bid, 100, 5),
        )
        .unwrap_err();

        assert_eq!(error, EngineError::PostOnlyWouldCross);
        assert_eq!(engine.next_order_id, 2);
        assert!(engine.trades().is_empty());

        let book = engine.book("BTC-USDT").unwrap();
        assert_eq!(book.best_bid(), None);
        assert_eq!(book.best_ask(), Some(100));
    }

    #[test]
    fn post_only_rejects_crossing_ask() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Bid, 100, 5)).unwrap();

        let error = submit(
            &mut engine,
            "BTC-USDT",
            post_only_limit(2, Side::Ask, 100, 5),
        )
        .unwrap_err();

        assert_eq!(error, EngineError::PostOnlyWouldCross);
        assert!(engine.trades().is_empty());

        let book = engine.book("BTC-USDT").unwrap();
        assert_eq!(book.best_bid(), Some(100));
        assert_eq!(book.best_ask(), None);
    }

    #[test]
    fn post_only_rests_when_not_crossing() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 101, 5)).unwrap();

        let report = submit(
            &mut engine,
            "BTC-USDT",
            post_only_limit(2, Side::Bid, 100, 5),
        )
        .unwrap();

        assert_eq!(report.status, OrderStatus::New);
        assert_eq!(report.filled_quantity, 0);
        assert_eq!(report.remaining_quantity, 5);
        assert!(report.fills.is_empty());

        let book = engine.book("BTC-USDT").unwrap();
        assert_eq!(book.best_bid(), Some(100));
        assert_eq!(book.best_ask(), Some(101));
    }

    #[test]
    fn invalid_time_in_force_combinations_are_rejected() {
        let mut engine = MatchingEngine::new(EngineConfig::default());

        let market_gtc = OrderRequest {
            time_in_force: TimeInForce::GTC,
            ..market(1, Side::Bid, 5)
        };
        assert_eq!(
            submit(&mut engine, "BTC-USDT", market_gtc).unwrap_err(),
            EngineError::InvalidTimeInForce
        );

        let post_only_ioc = OrderRequest {
            time_in_force: TimeInForce::IOC,
            ..post_only_limit(2, Side::Bid, 100, 5)
        };
        assert_eq!(
            submit(&mut engine, "BTC-USDT", post_only_ioc).unwrap_err(),
            EngineError::InvalidTimeInForce
        );

        let post_only_fok = OrderRequest {
            time_in_force: TimeInForce::FOK,
            ..post_only_limit(3, Side::Bid, 100, 5)
        };
        assert_eq!(
            submit(&mut engine, "BTC-USDT", post_only_fok).unwrap_err(),
            EngineError::InvalidTimeInForce
        );

        assert!(engine.trades().is_empty());
        assert!(engine.book("BTC-USDT").is_none());
    }

    #[test]
    fn post_only_market_order_is_rejected_before_crossing_check() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        let mut commands: Vec<EngineCommand> = Vec::new();

        let command = submit_command("BTC-USDT", limit(1, Side::Ask, 100, 5));
        commands.push(command.clone());
        assert!(matches!(
            engine.execute(command),
            CommandOutcome::Accepted(_)
        ));

        let command = submit_command(
            "BTC-USDT",
            OrderRequest {
                post_only: true,
                ..market(2, Side::Bid, 5)
            },
        );
        commands.push(command.clone());
        assert_eq!(
            engine.execute(command),
            CommandOutcome::Rejected(EngineError::InvalidTimeInForce)
        );

        // 校验先于 would_cross：即使簿上有可交叉的卖单，也应报 InvalidTimeInForce
        assert!(engine.trades().is_empty());
        assert_eq!(engine.book("BTC-USDT").unwrap().best_ask(), Some(100));
        assert_eq!(commands.len(), 2);
        assert_eq!(engine.events().len(), 2);
        match engine.events().last().unwrap() {
            EngineEvent::OrderRejected { reason, .. } => {
                assert_eq!(*reason, EngineError::InvalidTimeInForce)
            }
            _ => panic!("expected OrderRejected"),
        }
    }

    #[test]
    fn replay_reproduces_tif_commands() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        let mut commands: Vec<EngineCommand> = Vec::new();

        commands.push(submit_command("BTC-USDT", limit(1, Side::Ask, 100, 5)));
        assert!(matches!(
            engine.execute(commands[0].clone()),
            CommandOutcome::Accepted(_)
        ));
        commands.push(submit_command(
            "BTC-USDT",
            ioc_limit(2, Side::Bid, 100, 10),
        ));
        assert!(matches!(
            engine.execute(commands[1].clone()),
            CommandOutcome::Accepted(_)
        ));
        commands.push(submit_command(
            "BTC-USDT",
            fok_limit(3, Side::Bid, 101, 10),
        ));
        assert!(matches!(
            engine.execute(commands[2].clone()),
            CommandOutcome::Rejected(_)
        ));
        commands.push(submit_command(
            "BTC-USDT",
            post_only_limit(4, Side::Bid, 99, 1),
        ));
        let post_only = match engine.execute(commands[3].clone()) {
            CommandOutcome::Accepted(report) => report,
            other => panic!("post-only submit failed: {other:?}"),
        };
        commands.push(cancel_command("BTC-USDT", post_only.order_id));
        assert!(matches!(
            engine.execute(commands[4].clone()),
            CommandOutcome::Canceled(_)
        ));

        let recovered = assert_recovered(&commands, &engine);
        assert_eq!(recovered.engine().book("BTC-USDT"), engine.book("BTC-USDT"));
        assert_eq!(
            recovered.engine().snapshot("BTC-USDT", 10),
            engine.snapshot("BTC-USDT", 10)
        );
    }

    #[test]
    fn ledger_invariant_holds_across_mixed_lifecycles() {
        let mut engine = MatchingEngine::new(EngineConfig::default());

        // Resting ask, partially filled by a taker that rests its own remainder.
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 5)).unwrap();
        submit(&mut engine, "BTC-USDT", limit(2, Side::Bid, 100, 2)).unwrap();
        // The same ask is fully filled by a later taker that also rests.
        submit(&mut engine, "BTC-USDT", limit(3, Side::Bid, 100, 4)).unwrap();
        cancel(&mut engine, "BTC-USDT", 3).unwrap();
        // Market taker leaves an unfilled remainder that is canceled.
        submit(&mut engine, "BTC-USDT", limit(4, Side::Ask, 101, 1)).unwrap();
        submit(&mut engine, "BTC-USDT", market(5, Side::Bid, 5)).unwrap();
        // IOC taker leaves an unfilled remainder that is canceled.
        submit(&mut engine, "BTC-USDT", limit(6, Side::Ask, 102, 2)).unwrap();
        submit(&mut engine, "BTC-USDT", ioc_limit(7, Side::Bid, 102, 5)).unwrap();
        // Plain resting order canceled without a fill.
        submit(&mut engine, "BTC-USDT", limit(8, Side::Bid, 99, 1)).unwrap();
        cancel(&mut engine, "BTC-USDT", 8).unwrap();

        assert_eq!(engine.order_count(), 8);
        for (order_id, record) in engine.order_records() {
            assert_eq!(
                record.filled_quantity + record.remaining_quantity,
                record.original_quantity,
                "order {order_id} violated filled + remaining == original"
            );
        }
    }

    #[test]
    fn order_record_returns_none_for_unknown_id() {
        let mut engine = MatchingEngine::new(EngineConfig::default());

        assert!(engine.order_record(42).is_none());
        assert!(!engine.is_open(42));

        let report = submit(&mut engine, "BTC-USDT", limit(1, Side::Bid, 100, 5)).unwrap();
        assert!(engine.order_record(report.order_id).is_some());
        assert!(engine.order_record(report.order_id + 1).is_none());
    }

    #[test]
    fn is_open_tracks_new_partial_filled_and_canceled() {
        let mut engine = MatchingEngine::new(EngineConfig::default());

        let resting = submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 5)).unwrap();
        assert_eq!(resting.status, OrderStatus::New);
        assert!(engine.is_open(resting.order_id));

        // Maker partially filled, taker fully filled.
        submit(&mut engine, "BTC-USDT", limit(2, Side::Bid, 100, 3)).unwrap();
        assert!(engine.is_open(1));
        assert!(!engine.is_open(2));

        // A taker fills the maker's remainder and rests its own remainder.
        let partial = submit(&mut engine, "BTC-USDT", limit(3, Side::Bid, 100, 10)).unwrap();
        assert_eq!(partial.status, OrderStatus::PartiallyFilled);
        assert!(!engine.is_open(1));
        assert!(engine.is_open(partial.order_id));

        // Market leftovers are canceled, never open.
        let market = submit(&mut engine, "BTC-USDT", market(4, Side::Bid, 1)).unwrap();
        assert_eq!(market.status, OrderStatus::Canceled);
        assert!(!engine.is_open(market.order_id));

        let canceled = cancel(&mut engine, "BTC-USDT", partial.order_id).unwrap();
        assert_eq!(canceled.status, OrderStatus::Canceled);
        assert!(!engine.is_open(partial.order_id));
    }

    #[test]
    fn maker_and_taker_statuses_are_observable_through_order_record() {
        let mut engine = MatchingEngine::new(EngineConfig::default());

        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 5)).unwrap();
        // Taker fills 3 of the maker and is itself fully filled.
        submit(&mut engine, "BTC-USDT", limit(2, Side::Bid, 100, 3)).unwrap();

        let maker = engine.order_record(1).unwrap();
        assert_eq!(maker.status, OrderStatus::PartiallyFilled);
        assert_eq!(maker.original_quantity, 5);
        assert_eq!(maker.filled_quantity, 3);
        assert_eq!(maker.remaining_quantity, 2);

        let taker = engine.order_record(2).unwrap();
        assert_eq!(taker.status, OrderStatus::Filled);
        assert_eq!(taker.filled_quantity, 3);
        assert_eq!(taker.remaining_quantity, 0);

        // Next taker fills the maker's remainder and rests its own remainder.
        submit(&mut engine, "BTC-USDT", limit(3, Side::Bid, 100, 4)).unwrap();

        let maker = engine.order_record(1).unwrap();
        assert_eq!(maker.status, OrderStatus::Filled);
        assert_eq!(maker.filled_quantity, 5);
        assert_eq!(maker.remaining_quantity, 0);

        let taker = engine.order_record(3).unwrap();
        assert_eq!(taker.status, OrderStatus::PartiallyFilled);
        assert_eq!(taker.original_quantity, 4);
        assert_eq!(taker.filled_quantity, 2);
        assert_eq!(taker.remaining_quantity, 2);

        // Cancel projects the canceled status and the book's remaining quantity.
        cancel(&mut engine, "BTC-USDT", 3).unwrap();
        let canceled = engine.order_record(3).unwrap();
        assert_eq!(canceled.status, OrderStatus::Canceled);
        assert_eq!(canceled.filled_quantity, 2);
        assert_eq!(canceled.remaining_quantity, 2);
        assert!(!engine.is_open(3));
    }

    #[test]
    fn modify_price_moves_order_to_new_level() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        let first = submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 5)).unwrap();
        submit(&mut engine, "BTC-USDT", limit(2, Side::Ask, 101, 2)).unwrap();

        let modified = modify(&mut engine, "BTC-USDT", first.order_id, 102, 5, 3).unwrap();

        assert_eq!(modified.order_id, 3);
        assert_eq!(modified.status, OrderStatus::New);
        assert_eq!(modified.original_quantity, 5);
        assert_eq!(modified.remaining_quantity, 5);
        assert!(modified.fills.is_empty());

        // The old id is canceled; the replacement is open at the new price.
        let old = engine.order_record(first.order_id).unwrap();
        assert_eq!(old.status, OrderStatus::Canceled);
        assert_eq!(old.remaining_quantity, 5);
        assert!(!engine.is_open(first.order_id));
        assert!(engine.is_open(modified.order_id));

        let book = engine.book("BTC-USDT").unwrap();
        assert_eq!(book.depth(Side::Ask, 0), Some((101, 2)));
        assert_eq!(book.depth(Side::Ask, 1), Some((102, 5)));
        assert_eq!(book.depth(Side::Ask, 2), None);
    }

    #[test]
    fn modify_quantity_only_loses_queue_priority() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        let first = submit(&mut engine, "BTC-USDT", limit(1, Side::Bid, 100, 2)).unwrap();
        submit(&mut engine, "BTC-USDT", limit(2, Side::Bid, 100, 3)).unwrap();

        // Same price and quantity, but the replacement is a new order at the back.
        let modified = modify(&mut engine, "BTC-USDT", first.order_id, 100, 2, 3).unwrap();
        assert_eq!(modified.order_id, 3);

        // Sell into the level in three slices. The first two must hit order 2,
        // proving the replacement sits behind it; the last hits the replacement.
        submit(&mut engine, "BTC-USDT", limit(4, Side::Ask, 100, 1)).unwrap();
        submit(&mut engine, "BTC-USDT", limit(5, Side::Ask, 100, 2)).unwrap();
        submit(&mut engine, "BTC-USDT", limit(6, Side::Ask, 100, 2)).unwrap();

        let fills: Vec<(OrderBookId, u64)> = engine
            .trades()
            .iter()
            .map(|trade| (trade.maker_order_id, trade.quantity))
            .collect();
        assert_eq!(fills, vec![(2, 1), (2, 2), (3, 2)]);
        assert_eq!(
            engine.order_record(1).unwrap().status,
            OrderStatus::Canceled
        );
        assert_eq!(engine.order_record(2).unwrap().status, OrderStatus::Filled);
        assert_eq!(engine.order_record(3).unwrap().status, OrderStatus::Filled);
    }

    #[test]
    fn modify_to_crossing_price_trades_and_rests_remainder() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 2)).unwrap();
        submit(&mut engine, "BTC-USDT", limit(2, Side::Bid, 99, 5)).unwrap();

        // The replacement bid crosses the resting ask, fills 2, and rests 3.
        let modified = modify(&mut engine, "BTC-USDT", 2, 100, 5, 3).unwrap();

        assert_eq!(modified.order_id, 3);
        assert_eq!(modified.status, OrderStatus::PartiallyFilled);
        assert_eq!(modified.filled_quantity, 2);
        assert_eq!(modified.remaining_quantity, 3);
        assert_eq!(modified.fills.len(), 1);
        assert_eq!(modified.fills[0].maker_order_id, 1);
        assert_eq!(modified.fills[0].taker_order_id, 3);
        assert_eq!(modified.fills[0].quantity, 2);

        assert_eq!(engine.trades().len(), 1);
        let trade = &engine.trades()[0];
        assert_eq!(trade.maker_order_id, 1);
        assert_eq!(trade.taker_order_id, 3);
        assert_eq!(trade.taker_side, Side::Bid);
        assert_eq!(trade.timestamp, 3);

        assert_eq!(engine.order_record(1).unwrap().status, OrderStatus::Filled);
        assert_eq!(
            engine.order_record(2).unwrap().status,
            OrderStatus::Canceled
        );
        assert!(engine.is_open(3));

        let book = engine.book("BTC-USDT").unwrap();
        assert_eq!(book.best_ask(), None);
        assert_eq!(book.best_bid(), Some(100));
        assert_eq!(book.depth(Side::Bid, 0), Some((100, 3)));
    }

    #[test]
    fn modify_to_crossing_price_can_fully_fill() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 5)).unwrap();
        submit(&mut engine, "BTC-USDT", limit(2, Side::Bid, 99, 3)).unwrap();

        let modified = modify(&mut engine, "BTC-USDT", 2, 100, 3, 3).unwrap();

        assert_eq!(modified.order_id, 3);
        assert_eq!(modified.status, OrderStatus::Filled);
        assert_eq!(modified.filled_quantity, 3);
        assert_eq!(modified.remaining_quantity, 0);
        assert_eq!(modified.fills.len(), 1);

        let maker = engine.order_record(1).unwrap();
        assert_eq!(maker.status, OrderStatus::PartiallyFilled);
        assert_eq!(maker.filled_quantity, 3);
        assert_eq!(maker.remaining_quantity, 2);
        assert_eq!(
            engine.order_record(2).unwrap().status,
            OrderStatus::Canceled
        );
        assert!(!engine.is_open(2));
        assert!(!engine.is_open(3));

        let book = engine.book("BTC-USDT").unwrap();
        assert_eq!(book.best_bid(), None);
        assert_eq!(book.best_ask(), Some(100));
        assert_eq!(book.depth(Side::Ask, 0), Some((100, 2)));
    }

    #[test]
    fn modify_partially_filled_order_keeps_history_on_old_id() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 5)).unwrap();
        submit(&mut engine, "BTC-USDT", limit(2, Side::Bid, 100, 2)).unwrap();

        // The old id keeps its partial-fill history; the replacement starts fresh.
        let modified = modify(&mut engine, "BTC-USDT", 1, 101, 6, 3).unwrap();

        assert_eq!(modified.order_id, 3);
        assert_eq!(modified.status, OrderStatus::New);
        assert_eq!(modified.original_quantity, 6);
        assert_eq!(modified.filled_quantity, 0);
        assert_eq!(modified.remaining_quantity, 6);

        let old = engine.order_record(1).unwrap();
        assert_eq!(old.status, OrderStatus::Canceled);
        assert_eq!(old.original_quantity, 5);
        assert_eq!(old.filled_quantity, 2);
        assert_eq!(old.remaining_quantity, 3);

        let book = engine.book("BTC-USDT").unwrap();
        assert_eq!(book.best_ask(), Some(101));
        assert_eq!(book.depth(Side::Ask, 0), Some((101, 6)));
        assert_eq!(book.best_bid(), None);
    }

    #[test]
    fn invalid_modify_leaves_original_order_untouched() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        let original = submit(&mut engine, "BTC-USDT", limit(1, Side::Bid, 100, 5)).unwrap();
        let events_before = engine.events().len();

        let error = modify(&mut engine, "BTC-USDT", original.order_id, 100, 0, 2).unwrap_err();

        assert_eq!(error, EngineError::QuantityOutOfRange);
        assert_eq!(engine.events().len(), events_before + 1);
        match engine.events().last().unwrap() {
            EngineEvent::OrderRejected { reason, .. } => {
                assert_eq!(*reason, EngineError::QuantityOutOfRange)
            }
            other => panic!("expected OrderRejected, got {other:?}"),
        }

        assert!(engine.is_open(original.order_id));
        let record = engine.order_record(original.order_id).unwrap();
        assert_eq!(record.status, OrderStatus::New);
        assert_eq!(record.remaining_quantity, 5);
        assert_eq!(engine.next_order_id, 2);

        let book = engine.book("BTC-USDT").unwrap();
        assert_eq!(book.best_bid(), Some(100));
        assert_eq!(book.depth(Side::Bid, 0), Some((100, 5)));

        // An off-tick modify fails the same way, before the old order is touched.
        let mut tick_engine = MatchingEngine::new(EngineConfig {
            min_order_quantity: 1,
            max_order_quantity: u64::MAX,
            tick_size: 5,
        });
        let tick_order = submit(&mut tick_engine, "BTC-USDT", limit(1, Side::Bid, 100, 5)).unwrap();
        let error =
            modify(&mut tick_engine, "BTC-USDT", tick_order.order_id, 101, 5, 2).unwrap_err();

        assert_eq!(error, EngineError::PriceNotOnTick);
        assert!(tick_engine.is_open(tick_order.order_id));
        let book = tick_engine.book("BTC-USDT").unwrap();
        assert_eq!(book.best_bid(), Some(100));
        assert_eq!(book.depth(Side::Bid, 0), Some((100, 5)));
    }

    #[test]
    fn modify_unknown_wrong_symbol_or_closed_order_is_rejected() {
        let mut engine = MatchingEngine::new(EngineConfig::default());

        let events_before = engine.events().len();
        assert_eq!(
            modify(&mut engine, "BTC-USDT", 42, 100, 5, 1).unwrap_err(),
            EngineError::UnknownOrder
        );
        assert_eq!(engine.events().len(), events_before + 1);

        // Fully filled orders are closed.
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 2)).unwrap();
        submit(&mut engine, "BTC-USDT", limit(2, Side::Bid, 100, 2)).unwrap();
        let events_before = engine.events().len();
        assert_eq!(
            modify(&mut engine, "BTC-USDT", 1, 101, 1, 3).unwrap_err(),
            EngineError::UnknownOrder
        );
        assert_eq!(engine.events().len(), events_before + 1);

        // A wrong symbol leaves an otherwise open order untouched.
        let open = submit(&mut engine, "BTC-USDT", limit(4, Side::Bid, 99, 1)).unwrap();
        assert_eq!(
            modify(&mut engine, "ETH-USDT", open.order_id, 98, 1, 5).unwrap_err(),
            EngineError::UnknownOrder
        );
        assert!(engine.is_open(open.order_id));
        assert_eq!(engine.book("BTC-USDT").unwrap().best_bid(), Some(99));
        assert!(engine.book("ETH-USDT").is_none());
    }

    #[test]
    fn modify_emits_cancel_then_accept_and_is_logged_once() {
        let log = InMemoryCommandLog::new();
        let snapshots = InMemorySnapshotStore::new();
        let mut durable = DurableMatchingEngine::create(
            EngineConfig::default(),
            Box::new(log.handle()),
            Box::new(snapshots.handle()),
            0,
        )
        .unwrap();
        durable
            .execute(submit_command("BTC-USDT", limit(1, Side::Ask, 100, 5)))
            .unwrap();

        let events_before = durable.engine().events().len();
        let modified = match durable
            .execute(modify_command("BTC-USDT", 1, 101, 5, 2))
            .unwrap()
        {
            CommandOutcome::Accepted(report) => report,
            other => panic!("modify failed: {other:?}"),
        };

        assert_eq!(durable.engine().events().len(), events_before + 2);
        match &durable.engine().events()[events_before] {
            EngineEvent::OrderCanceled {
                sequence_id,
                report,
            } => {
                assert_eq!(*sequence_id, 2);
                assert_eq!(report.order_id, 1);
                assert_eq!(report.status, OrderStatus::Canceled);
                assert_eq!(report.remaining_quantity, 5);
            }
            other => panic!("expected OrderCanceled, got {other:?}"),
        }
        match &durable.engine().events()[events_before + 1] {
            EngineEvent::OrderAccepted {
                sequence_id,
                report,
            } => {
                assert_eq!(*sequence_id, 3);
                assert_eq!(report.order_id, modified.order_id);
                assert_eq!(report.status, OrderStatus::New);
            }
            other => panic!("expected OrderAccepted, got {other:?}"),
        }

        // One client command is logged; the internal cancel and submit are not.
        let history = durable.history_from(1).unwrap();
        assert_eq!(history.len(), 2);
        match &history[1].command {
            EngineCommand::ModifyOrder {
                symbol,
                order_id,
                request,
            } => {
                assert_eq!(symbol, "BTC-USDT");
                assert_eq!(*order_id, 1);
                assert_eq!(
                    *request,
                    ModifyRequest {
                        price: 101,
                        quantity: 5,
                        timestamp: 2,
                    }
                );
            }
            other => panic!("expected ModifyOrder, got {other:?}"),
        }
    }

    #[test]
    fn replay_reproduces_modify_commands() {
        let commands = vec![
            submit_command("BTC-USDT", limit(1, Side::Ask, 100, 5)),
            submit_command("BTC-USDT", limit(2, Side::Bid, 99, 2)),
            modify_command("BTC-USDT", 2, 100, 2, 3),
            submit_command("BTC-USDT", limit(4, Side::Bid, 98, 1)),
        ];
        let mut engine = MatchingEngine::new(EngineConfig::default());
        execute_all(&mut engine, &commands);

        let recovered = assert_recovered(&commands, &engine);
        assert_eq!(recovered.engine().book("BTC-USDT"), engine.book("BTC-USDT"));
    }

    #[test]
    fn execute_ref_matches_execute_state_and_outcome() {
        let commands = vec![
            submit_command("BTC-USDT", limit(1, Side::Ask, 100, 5)),
            submit_command("BTC-USDT", limit(2, Side::Bid, 100, 3)),
            cancel_command("BTC-USDT", 1),
            submit_command("BTC-USDT", limit(3, Side::Bid, 100, 0)),
        ];

        let mut by_value = MatchingEngine::new(EngineConfig::default());
        let mut by_ref = MatchingEngine::new(EngineConfig::default());

        for command in &commands {
            let value_outcome = by_value.execute(command.clone());
            let ref_outcome = by_ref.execute_ref(command);
            assert_eq!(ref_outcome, value_outcome);
        }

        assert_eq!(by_ref.capture_snapshot(), by_value.capture_snapshot());
        assert_eq!(by_ref.events(), by_value.events());
        assert_eq!(by_ref.last_sequence(), by_value.last_sequence());
    }

    #[test]
    fn clear_events_keeps_state_and_the_event_sequence_counter() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 5)).unwrap();
        submit(&mut engine, "BTC-USDT", limit(2, Side::Bid, 100, 2)).unwrap();

        let state_before = engine.capture_snapshot();
        let sequence_before = engine.last_sequence();
        assert!(!engine.events().is_empty());

        engine.clear_events();

        assert!(engine.events().is_empty());
        assert_eq!(engine.last_sequence(), sequence_before);
        assert_eq!(engine.capture_snapshot(), state_before);

        // Clearing output history does not reset the sequence allocator.
        submit(&mut engine, "BTC-USDT", limit(3, Side::Bid, 99, 1)).unwrap();
        assert_eq!(engine.last_sequence(), sequence_before + 1);
    }
}
