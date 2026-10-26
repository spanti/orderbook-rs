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
    BookSnapshot, EngineCommand, EngineConfig, EngineError, Fill, ModifyRequest, Order,
    OrderBookId, OrderRecord, OrderRequest, OrderStatus, OrderType, Side, StopKind, StopOrder,
    StopOrderRequest, SubmitResult, TimeInForce, Trade,
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
    /// Last event sequence folded through [`MatchingEngine::apply`].
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
            stop_books: HashMap::new(),
            last_trades: HashMap::new(),
            last_applied_sequence: 0,
        }
    }
}

/// Read-only decision state for the event-sourced path.
///
/// A scratch borrows the engine plus the touched symbol and the next order id.
/// Matching runs on clones of **only** that symbol's book and stop book; the
/// ledger, trades, last price and counters are read-only. It deliberately does
/// **not** clone the whole engine, keeping `decide` off the whole-engine-clone
/// path.
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

    fn scratch_book(&self) -> OrderBook {
        match self.engine.order_books.get(self.symbol) {
            Some(book) => book.clone(),
            None => OrderBook::new(),
        }
    }

    fn scratch_stops(&self) -> StopBook {
        match self.engine.stop_books.get(self.symbol) {
            Some(stops) => stops.clone(),
            None => StopBook::default(),
        }
    }

    /// `(price, timestamp)` of this symbol's last trade, if there is one.
    fn last_trade(&self) -> Option<(u64, u64)> {
        self.engine
            .last_trades
            .get(self.symbol)
            .map(|last| (last.price, last.timestamp))
    }

    /// Push a `TradeExecuted` per fill, allocating trade ids locally.
    ///
    /// Returns the last fill's `(price, timestamp)`, or `None` when nothing
    /// filled.
    fn push_fills(
        symbol: &str,
        timestamp: u64,
        fills: Vec<Fill>,
        next_trade_id: &mut u64,
        events: &mut Vec<DomainEvent>,
    ) -> Option<(u64, u64)> {
        let mut last = None;
        for fill in fills {
            let trade = Trade {
                id: *next_trade_id,
                symbol: symbol.to_string(),
                maker_order_id: fill.maker_order_id,
                taker_order_id: fill.taker_order_id,
                price: fill.price,
                quantity: fill.quantity,
                taker_side: fill.taker_side,
                timestamp,
            };
            *next_trade_id = next_trade_id.saturating_add(1);
            last = Some((fill.price, timestamp));
            events.push(DomainEvent::TradeExecuted { trade });
        }
        last
    }

    /// Emit a `StopTriggered` cascade after the command's own events.
    ///
    /// Each triggered stop is converted to a market+IOC or limit+GTC order and
    /// matched against the scratch book. Events follow the catalog order:
    /// `StopTriggered`, `TradeExecuted`*, then `OrderRested` or
    /// `OrderCanceled{UnfilledResidual}`. The loop continues with the resulting
    /// last price until no pending stop triggers.
    #[allow(clippy::too_many_arguments)]
    fn run_stop_cascade(
        &self,
        book: &mut OrderBook,
        stops: &mut StopBook,
        symbol: &str,
        mut last_trade: Option<(u64, u64)>,
        next_trade_id: &mut u64,
        events: &mut Vec<DomainEvent>,
    ) {
        while let Some((last_price, last_timestamp)) = last_trade {
            let Some(stop_id) = stops.next_triggered(last_price) else {
                break;
            };
            let stop = stops
                .remove(stop_id)
                .expect("triggered stop missing from the stop book");

            let converted = match stop.order_type {
                StopKind::Market => Order {
                    id: stop.id,
                    side: stop.side,
                    price: 0,
                    quantity: stop.quantity,
                    timestamp: last_timestamp,
                    order_type: OrderType::Market,
                    time_in_force: TimeInForce::IOC,
                },
                StopKind::Limit { price } => Order {
                    id: stop.id,
                    side: stop.side,
                    price,
                    quantity: stop.quantity,
                    timestamp: last_timestamp,
                    order_type: OrderType::Limit,
                    time_in_force: TimeInForce::GTC,
                },
            };

            events.push(DomainEvent::StopTriggered {
                symbol: symbol.to_string(),
                order: converted.clone(),
                last_price,
            });

            let SubmitResult {
                remaining_quantity,
                fills,
                rested,
            } = book.submit(converted.clone());

            let cascade_last =
                Self::push_fills(symbol, last_timestamp, fills, next_trade_id, events);

            if remaining_quantity > 0 {
                if rested {
                    events.push(DomainEvent::OrderRested {
                        symbol: symbol.to_string(),
                        order: Order {
                            quantity: remaining_quantity,
                            ..converted
                        },
                    });
                } else {
                    events.push(DomainEvent::OrderCanceled {
                        symbol: symbol.to_string(),
                        order_id: stop.id,
                        canceled_quantity: remaining_quantity,
                        reason: OrderCancelReason::UnfilledResidual,
                    });
                }
            }

            // If the converted order did not trade, the last price is unchanged
            // and the next eligible stop (if any) triggers at the same price.
            if let Some(new_last) = cascade_last {
                last_trade = Some(new_last);
            }
        }
    }

    /// Push `OrderAccepted`, the matching `TradeExecuted` events, and the
    /// trailing rest/residual event for one taker order.
    ///
    /// Returns the last fill's `(price, timestamp)`, or `None` when nothing
    /// filled. The caller supplies the scratch book so modify can reuse the
    /// same routine after removing the old order.
    #[allow(clippy::too_many_arguments)]
    fn push_taker_match(
        symbol: &str,
        order: Order,
        post_only: bool,
        timestamp: u64,
        book: &mut OrderBook,
        next_trade_id: &mut u64,
        events: &mut Vec<DomainEvent>,
    ) -> Option<(u64, u64)> {
        events.push(DomainEvent::OrderAccepted {
            symbol: symbol.to_string(),
            order: order.clone(),
            post_only,
        });

        let SubmitResult {
            remaining_quantity,
            fills,
            rested,
        } = book.submit(order.clone());

        let last_trade = Self::push_fills(symbol, timestamp, fills, next_trade_id, events);

        if remaining_quantity > 0 {
            if rested {
                events.push(DomainEvent::OrderRested {
                    symbol: symbol.to_string(),
                    order: Order {
                        quantity: remaining_quantity,
                        ..order
                    },
                });
            } else {
                events.push(DomainEvent::OrderCanceled {
                    symbol: symbol.to_string(),
                    order_id: order.id,
                    canceled_quantity: remaining_quantity,
                    reason: OrderCancelReason::UnfilledResidual,
                });
            }
        }

        last_trade
    }

    /// Decide a fresh submit: policy, matching, then fill/rest/cancel events.
    ///
    /// Matching runs against a clone of **only** the touched symbol's book and
    /// stop book. The engine's book, ledger, trades, last price and counters
    /// are never mutated; order and trade ids are allocated in local variables
    /// and only become state when the events are applied. Any pending stops
    /// the resulting last price triggers become part of the same event stream.
    pub fn decide_submit(&self, request: &OrderRequest) -> Result<Vec<DomainEvent>, EngineError> {
        // Validation order matches the command-sourced path: validate, then
        // post-only crossing, then FOK liquidity.
        self.engine.check_taker_policy(self.symbol, request)?;

        let accepted = request.to_order(self.order_id);
        let symbol = self.symbol.to_string();

        // The book this decision runs against: a private copy. `submit` both
        // matches and decides whether the remainder rests.
        let mut scratch_book = self.scratch_book();
        let mut events = Vec::new();

        // Trade ids are allocated locally; `apply(TradeExecuted)` advances the
        // real counter from the events.
        let mut next_trade_id = self.engine.next_trade_id;
        let command_last_trade = Self::push_taker_match(
            &symbol,
            accepted,
            request.post_only,
            request.timestamp,
            &mut scratch_book,
            &mut next_trade_id,
            &mut events,
        );

        // A trade can reach a pending stop's trigger price. Run the cascade on
        // the same scratch book/stop book so the decision is self-contained.
        let last_trade = command_last_trade.or_else(|| self.last_trade());
        let mut scratch_stops = self.scratch_stops();
        self.run_stop_cascade(
            &mut scratch_book,
            &mut scratch_stops,
            &symbol,
            last_trade,
            &mut next_trade_id,
            &mut events,
        );

        Ok(events)
    }

    /// Decide a modify: cancel the old id, accept and match a replacement.
    ///
    /// Semantics match the command-sourced path exactly: validation runs
    /// first, a pending stop is not modifiable, and an unknown or closed order
    /// is rejected. The replacement is a limit/GTC order on the old side with
    /// the requested price, quantity and timestamp. It always gets a new id
    /// and runs against the same scratch book, so the old priority is lost and
    /// the new order matches as a taker. The stop cascade follows the
    /// replacement's match result.
    pub fn decide_modify(
        &self,
        order_id: OrderBookId,
        request: &ModifyRequest,
    ) -> Result<Vec<DomainEvent>, EngineError> {
        // 1. The replacement must be a valid limit order, checked before any
        //    order lookup so validation errors win over unknown/closed ids.
        self.engine
            .validate_limit(request.price, request.quantity)?;

        // 2. A pending stop cannot be modified; an unknown, wrong-symbol or
        //    closed order is unknown.
        let Some(record) = self.engine.ledger.get(order_id) else {
            return Err(EngineError::UnknownOrder);
        };
        if record.symbol != self.symbol {
            return Err(EngineError::UnknownOrder);
        }
        if record.status == OrderStatus::PendingTrigger {
            return Err(EngineError::StopOrderNotModifiable);
        }
        if !record.is_open() {
            return Err(EngineError::UnknownOrder);
        }

        // 3. Cancel the old order on the scratch book. The ledger is not
        //    mutated here: `apply(OrderCanceled)` does that.
        let mut scratch_book = self.scratch_book();
        let Some(old_order) = scratch_book.cancel_limit_order(order_id) else {
            return Err(EngineError::UnknownOrder);
        };

        let symbol = self.symbol.to_string();
        let mut events = Vec::new();
        events.push(DomainEvent::OrderCanceled {
            symbol: symbol.clone(),
            order_id,
            canceled_quantity: old_order.quantity,
            reason: OrderCancelReason::Modified,
        });

        // 4. The replacement keeps the old side and uses the requested
        //    price/quantity/timestamp. It is a plain limit/GTC order.
        let replacement = OrderRequest {
            side: old_order.side,
            price: request.price,
            quantity: request.quantity,
            timestamp: request.timestamp,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::GTC,
            post_only: false,
        }
        .to_order(self.order_id);

        let mut next_trade_id = self.engine.next_trade_id;
        let command_last_trade = Self::push_taker_match(
            &symbol,
            replacement,
            false,
            request.timestamp,
            &mut scratch_book,
            &mut next_trade_id,
            &mut events,
        );

        // 5. The replacement's trades can trigger pending stops.
        let last_trade = command_last_trade.or_else(|| self.last_trade());
        let mut scratch_stops = self.scratch_stops();
        self.run_stop_cascade(
            &mut scratch_book,
            &mut scratch_stops,
            &symbol,
            last_trade,
            &mut next_trade_id,
            &mut events,
        );

        Ok(events)
    }

    /// Decide a stop submission: validate locally, then emit `StopPlaced`.
    ///
    /// The id is allocated in a local variable only; it becomes state when the
    /// event is applied. Direction and tick validation match the command path.
    pub fn decide_submit_stop(
        &self,
        request: &StopOrderRequest,
    ) -> Result<Vec<DomainEvent>, EngineError> {
        self.engine.validate_stop(self.symbol, request)?;
        let stop = StopOrder {
            id: self.order_id,
            side: request.side,
            stop_price: request.stop_price,
            order_type: request.order_type,
            quantity: request.quantity,
            timestamp: request.timestamp,
        };
        Ok(vec![DomainEvent::StopPlaced {
            symbol: self.symbol.to_string(),
            stop,
        }])
    }

    /// Decide a user cancel of an open order or a pending stop.
    ///
    /// An open, resting order emits `OrderCanceled{UserCancel}`; a pending stop
    /// emits `StopCanceled`. Unknown, already-closed and wrong-symbol orders
    /// fail with [`EngineError::UnknownOrder`], which `decide` turns into an
    /// `OrderRejected` event.
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
            // A pending stop is canceled with `StopCanceled`, not
            // `OrderCanceled`; it must still be in this symbol's stop book.
            let present = self
                .engine
                .stop_books
                .get(self.symbol)
                .is_some_and(|stops| stops.get(order_id).is_some());
            if !present {
                return Err(EngineError::UnknownOrder);
            }
            return Ok(vec![DomainEvent::StopCanceled {
                symbol: self.symbol.to_string(),
                order_id,
            }]);
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
            stop_books: HashMap::new(),
            last_trades: HashMap::new(),
            last_applied_sequence: 0,
        }
    }

    /// Last event sequence folded through [`Self::apply`].
    ///
    /// This is the value reported by [`BookSnapshot::sequence`]. Recovery sets
    /// it to the snapshot's applied event sequence before folding the tail.
    pub fn last_applied_sequence(&self) -> u64 {
        self.last_applied_sequence
    }

    /// Set the applied event boundary after restoring a snapshot.
    ///
    /// A snapshot does not store the engine's derived event counter; the
    /// durable layer owns the boundary and passes it in.
    pub(crate) fn set_last_applied_sequence(&mut self, sequence: u64) {
        self.last_applied_sequence = sequence;
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
            last_applied_sequence: 0,
            stop_books,
            last_trades,
        }
    }

    pub fn snapshot(&self, symbol: &str, depth: usize) -> Option<BookSnapshot> {
        let book = self.order_books.get(symbol)?;
        let (bids, asks) = book.snapshot(depth);
        Some(BookSnapshot {
            symbol: symbol.to_string(),
            sequence: self.last_applied_sequence,
            bids,
            asks,
        })
    }

    /// Convenience composition of [`Self::decide`] and [`Self::apply`].
    ///
    /// Returns the command's events, which are the only command result. This
    /// path allocates the next contiguous event sequences locally; the durable
    /// layer assigns the authoritative sequences from its log cursor.
    pub fn execute(&mut self, command: EngineCommand) -> Vec<DomainEvent> {
        let events = self.decide(&command);
        for event in &events {
            let sequence = self.last_applied_sequence.saturating_add(1);
            self.apply(event, sequence);
        }
        events
    }

    /// Pure decision step of the event-sourced path: command in, events out.
    ///
    /// Reads engine state and allocates order/trade ids in local variables
    /// only; it never mutates the engine. Every command produces at least one
    /// event: business rejections produce exactly one
    /// [`DomainEvent::OrderRejected`] carrying the full command, never an
    /// `Err`. Internal invariant violations still panic.
    pub fn decide(&self, command: &EngineCommand) -> Vec<DomainEvent> {
        let decision = match command {
            EngineCommand::SubmitOrder { symbol, request } => {
                DecisionScratch::new(self, symbol).decide_submit(request)
            }
            EngineCommand::SubmitStopOrder { symbol, request } => {
                DecisionScratch::new(self, symbol).decide_submit_stop(request)
            }
            EngineCommand::CancelOrder { symbol, order_id } => {
                DecisionScratch::new(self, symbol).decide_cancel(*order_id)
            }
            EngineCommand::ModifyOrder {
                symbol,
                order_id,
                request,
            } => DecisionScratch::new(self, symbol).decide_modify(*order_id, request),
        };

        match decision {
            Ok(events) => events,
            Err(reason) => vec![Self::rejected_event(command, reason)],
        }
    }

    /// Build the audit event for a business rejection.
    fn rejected_event(command: &EngineCommand, reason: EngineError) -> DomainEvent {
        let symbol = match command {
            EngineCommand::SubmitOrder { symbol, .. }
            | EngineCommand::SubmitStopOrder { symbol, .. }
            | EngineCommand::CancelOrder { symbol, .. }
            | EngineCommand::ModifyOrder { symbol, .. } => symbol,
        };
        DomainEvent::OrderRejected {
            symbol: symbol.clone(),
            reason,
            command: command.clone(),
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
            DomainEvent::StopPlaced { symbol, stop } => {
                // Register the pending stop and its ledger record. The order
                // book is untouched until the stop triggers and trades/rests.
                self.stop_books
                    .entry(symbol.clone())
                    .or_default()
                    .insert(stop.clone());
                self.ledger
                    .insert_pending_stop(stop.id, symbol.clone(), stop.quantity);
                self.next_order_id = self.next_order_id.max(stop.id.saturating_add(1));
            }
            DomainEvent::StopTriggered { symbol, order, .. } => {
                // Remove the pending stop and activate its existing ledger
                // record. Replay never re-runs matching; the following events
                // carry the match result.
                let _removed = self
                    .stop_books
                    .get_mut(symbol)
                    .and_then(|stops| stops.remove(order.id));
                debug_assert!(_removed.is_some(), "triggered stop is not pending");
                self.ledger
                    .activate_stop(order.id)
                    .expect("triggered stop has no ledger record");
            }
            DomainEvent::StopCanceled { symbol, order_id } => {
                let _removed = self
                    .stop_books
                    .get_mut(symbol)
                    .and_then(|stops| stops.remove(*order_id));
                debug_assert!(_removed.is_some(), "canceled stop is not pending");
                let canceled_quantity = self
                    .ledger
                    .get(*order_id)
                    .expect("canceled stop has no ledger record")
                    .remaining_quantity;
                self.ledger
                    .mark_canceled(*order_id, canceled_quantity)
                    .expect("canceled stop has no ledger record");
            }
            DomainEvent::OrderRejected { .. } => {
                // Audit-only: the only event with no state effect. Replay and
                // live execution both fold it into nothing.
            }
        }

        self.last_applied_sequence = self.last_applied_sequence.max(sequence);
    }

    fn can_fill_completely(&self, symbol: &str, request: &OrderRequest) -> bool {
        let Some(book) = self.order_books.get(symbol) else {
            return false;
        };
        let probe = request.to_order(0); // 仅用于预检查
        book.can_fill_completely(&probe)
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

    fn would_cross(&self, symbol: &str, request: &OrderRequest) -> bool {
        let Some(book) = self.order_books.get(symbol) else {
            return false;
        };
        match request.side {
            Side::Bid => book.best_ask().is_some_and(|ask| request.price >= ask),
            Side::Ask => book.best_bid().is_some_and(|bid| request.price <= bid),
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

    pub fn last_trade_price(&self, symbol: &str) -> Option<u64> {
        self.last_trades.get(symbol).map(|last| last.price)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::OrderCancelReason;
    use crate::types::{EngineCommand, EngineConfig, EngineError, OrderStatus};

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

    fn run(engine: &mut MatchingEngine, command: EngineCommand) -> Vec<DomainEvent> {
        engine.execute(command)
    }

    fn submit(
        engine: &mut MatchingEngine,
        symbol: &str,
        request: OrderRequest,
    ) -> Vec<DomainEvent> {
        run(engine, submit_command(symbol, request))
    }

    fn accepted_order(events: &[DomainEvent]) -> &Order {
        match events.first() {
            Some(DomainEvent::OrderAccepted { order, .. }) => order,
            other => panic!("expected the stream to start with OrderAccepted, got {other:?}"),
        }
    }

    fn rejection(events: &[DomainEvent]) -> Option<EngineError> {
        match events {
            [DomainEvent::OrderRejected { reason, .. }] => Some(*reason),
            _ => None,
        }
    }

    fn executed_trades(events: &[DomainEvent]) -> Vec<&Trade> {
        events
            .iter()
            .filter_map(|event| match event {
                DomainEvent::TradeExecuted { trade } => Some(trade),
                _ => None,
            })
            .collect()
    }

    fn first_trade(events: &[DomainEvent]) -> &Trade {
        executed_trades(events)
            .into_iter()
            .next()
            .unwrap_or_else(|| panic!("expected a TradeExecuted event, got {events:?}"))
    }

    #[test]
    fn crossing_orders_emit_a_trade_and_move_both_ledger_records() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 5));
        let events = submit(&mut engine, "BTC-USDT", limit(2, Side::Bid, 100, 5));

        assert_eq!(
            events,
            vec![
                DomainEvent::OrderAccepted {
                    symbol: "BTC-USDT".to_string(),
                    order: limit(2, Side::Bid, 100, 5).to_order(2),
                    post_only: false,
                },
                DomainEvent::TradeExecuted {
                    trade: Trade {
                        id: 1,
                        symbol: "BTC-USDT".to_string(),
                        maker_order_id: 1,
                        taker_order_id: 2,
                        price: 100,
                        quantity: 5,
                        taker_side: Side::Bid,
                        timestamp: 2,
                    },
                },
            ],
            "a fully filled taker has no trailing rest or cancel"
        );

        assert_eq!(engine.trades().len(), 1);
        assert_eq!(engine.order_record(1).unwrap().status, OrderStatus::Filled);
        assert_eq!(engine.order_record(2).unwrap().status, OrderStatus::Filled);
        assert_eq!(engine.last_trade_price("BTC-USDT"), Some(100));
    }

    #[test]
    fn trade_ids_increment_across_price_levels() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 2));
        submit(&mut engine, "BTC-USDT", limit(2, Side::Ask, 101, 3));
        let events = submit(&mut engine, "BTC-USDT", limit(3, Side::Bid, 101, 4));

        let trades = executed_trades(&events);
        assert_eq!(trades.len(), 2);
        assert_eq!(
            (
                trades[0].id,
                trades[0].maker_order_id,
                trades[0].price,
                trades[0].quantity
            ),
            (1, 1, 100, 2)
        );
        assert_eq!(
            (
                trades[1].id,
                trades[1].maker_order_id,
                trades[1].price,
                trades[1].quantity
            ),
            (2, 2, 101, 2)
        );
        assert!(
            trades
                .iter()
                .all(|trade| trade.taker_order_id == 3 && trade.taker_side == Side::Bid)
        );
        assert_eq!(
            engine.trades(),
            &trades.into_iter().cloned().collect::<Vec<_>>()
        );
    }

    #[test]
    fn market_order_fills_then_discards_its_residual() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 2));

        let events = submit(&mut engine, "BTC-USDT", market(2, Side::Bid, 5));

        assert_eq!(events.len(), 3);
        assert_eq!(first_trade(&events).quantity, 2);
        assert_eq!(
            events.last(),
            Some(&DomainEvent::OrderCanceled {
                symbol: "BTC-USDT".to_string(),
                order_id: 2,
                canceled_quantity: 3,
                reason: OrderCancelReason::UnfilledResidual,
            })
        );
        let record = engine.order_record(2).unwrap();
        assert_eq!(record.status, OrderStatus::Canceled);
        assert_eq!(record.filled_quantity, 2);
        assert_eq!(record.remaining_quantity, 3);
        assert_eq!(engine.book("BTC-USDT").unwrap().best_bid(), None);
    }

    #[test]
    fn books_are_isolated_per_symbol() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 5));
        submit(&mut engine, "ETH-USDT", limit(2, Side::Bid, 100, 5));

        assert!(engine.trades().is_empty());
        assert!(engine.book("BTC-USDT").is_some());
        assert!(engine.book("ETH-USDT").is_some());
        assert!(engine.book("SOL-USDT").is_none());
    }

    #[test]
    fn config_rejects_quantity_and_tick_violations_without_mutating_state() {
        let config = EngineConfig {
            min_order_quantity: 1,
            max_order_quantity: 10,
            tick_size: 5,
        };
        let engine = MatchingEngine::new(config);

        for (request, reason) in [
            (limit(1, Side::Bid, 100, 0), EngineError::QuantityOutOfRange),
            (
                limit(2, Side::Bid, 100, 11),
                EngineError::QuantityOutOfRange,
            ),
            (limit(3, Side::Bid, 101, 5), EngineError::PriceNotOnTick),
        ] {
            let command = submit_command("X", request);
            assert_eq!(rejection(&engine.decide(&command)), Some(reason));
        }
        assert!(engine.trades().is_empty());
        assert!(
            engine.book("X").is_none(),
            "validation failures create no book"
        );
        assert_eq!(engine.order_count(), 0);
    }

    #[test]
    fn resting_limit_order_emits_accepted_then_rested() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        let events = submit(&mut engine, "BTC-USDT", limit(1, Side::Bid, 100, 5));

        assert_eq!(
            events,
            vec![
                DomainEvent::OrderAccepted {
                    symbol: "BTC-USDT".to_string(),
                    order: limit(1, Side::Bid, 100, 5).to_order(1),
                    post_only: false,
                },
                DomainEvent::OrderRested {
                    symbol: "BTC-USDT".to_string(),
                    order: limit(1, Side::Bid, 100, 5).to_order(1),
                },
            ]
        );
        assert_eq!(engine.order_record(1).unwrap().status, OrderStatus::New);
        assert_eq!(
            engine.book("BTC-USDT").unwrap().depth(Side::Bid, 0),
            Some((100, 5))
        );
    }

    #[test]
    fn partially_filled_limit_order_rests_its_remainder() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 3));
        let events = submit(&mut engine, "BTC-USDT", limit(2, Side::Bid, 105, 10));

        assert_eq!(executed_trades(&events).len(), 1);
        match events.last() {
            Some(DomainEvent::OrderRested { order, .. }) => {
                assert_eq!(order.id, 2);
                assert_eq!(order.quantity, 7, "the unfilled remainder rests");
            }
            other => panic!("expected OrderRested, got {other:?}"),
        }
        let record = engine.order_record(2).unwrap();
        assert_eq!(record.status, OrderStatus::PartiallyFilled);
        assert_eq!(record.filled_quantity, 3);
        assert_eq!(record.remaining_quantity, 7);
    }

    #[test]
    fn cancel_is_scoped_to_symbol_and_unknown_orders_are_rejected() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Bid, 100, 5));

        let events = run(&mut engine, cancel_command("BTC-USDT", 1));
        assert_eq!(
            events,
            vec![DomainEvent::OrderCanceled {
                symbol: "BTC-USDT".to_string(),
                order_id: 1,
                canceled_quantity: 5,
                reason: OrderCancelReason::UserCancel,
            }]
        );
        assert_eq!(
            engine.order_record(1).unwrap().status,
            OrderStatus::Canceled
        );
        assert_eq!(engine.book("BTC-USDT").unwrap().best_bid(), None);

        for command in [
            cancel_command("BTC-USDT", 1),
            cancel_command("ETH-USDT", 1),
            cancel_command("BTC-USDT", 999),
        ] {
            assert_eq!(
                rejection(&engine.decide(&command)),
                Some(EngineError::UnknownOrder)
            );
        }
    }

    #[test]
    fn cancel_after_partial_fill_carries_the_book_remainder() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 2));
        submit(&mut engine, "BTC-USDT", limit(2, Side::Bid, 105, 5));

        let events = run(&mut engine, cancel_command("BTC-USDT", 2));
        assert_eq!(
            events,
            vec![DomainEvent::OrderCanceled {
                symbol: "BTC-USDT".to_string(),
                order_id: 2,
                canceled_quantity: 3,
                reason: OrderCancelReason::UserCancel,
            }]
        );
        let record = engine.order_record(2).unwrap();
        assert_eq!(record.filled_quantity, 2);
        assert_eq!(record.remaining_quantity, 3);
    }

    #[test]
    fn order_ids_are_assigned_by_the_engine_across_symbols() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        let first = submit(&mut engine, "BTC-USDT", limit(10, Side::Bid, 100, 1));
        let second = submit(&mut engine, "ETH-USDT", limit(20, Side::Bid, 100, 1));

        assert_eq!(accepted_order(&first).id, 1);
        assert_eq!(accepted_order(&second).id, 2);
    }

    #[test]
    fn rejected_order_does_not_consume_an_order_id() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        let rejected = engine.decide(&submit_command("BTC-USDT", limit(1, Side::Bid, 100, 0)));
        assert_eq!(rejection(&rejected), Some(EngineError::QuantityOutOfRange));

        let events = submit(&mut engine, "BTC-USDT", limit(2, Side::Bid, 100, 1));
        assert_eq!(accepted_order(&events).id, 1);
        assert_eq!(engine.order_count(), 1);
    }

    #[test]
    fn order_records_track_fills_for_maker_and_taker_across_multiple_fills() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 5));

        submit(&mut engine, "BTC-USDT", limit(2, Side::Bid, 100, 2));
        let maker = engine.order_record(1).unwrap();
        assert_eq!(maker.status, OrderStatus::PartiallyFilled);
        assert_eq!((maker.filled_quantity, maker.remaining_quantity), (2, 3));

        submit(&mut engine, "BTC-USDT", limit(3, Side::Bid, 100, 3));
        let maker = engine.order_record(1).unwrap();
        assert_eq!(maker.status, OrderStatus::Filled);
        assert_eq!((maker.filled_quantity, maker.remaining_quantity), (5, 0));
        let taker = engine.order_record(3).unwrap();
        assert_eq!(taker.status, OrderStatus::Filled);
        assert!(!engine.is_open(1));
    }

    #[test]
    fn market_order_record_is_canceled_when_the_residual_is_discarded() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 2));

        let events = submit(&mut engine, "BTC-USDT", market(2, Side::Bid, 5));
        let order_id = accepted_order(&events).id;
        let record = engine.order_record(order_id).unwrap();
        assert_eq!(record.symbol, "BTC-USDT");
        assert_eq!(record.status, OrderStatus::Canceled);
        assert_eq!(record.original_quantity, 5);
        assert_eq!(record.filled_quantity, 2);
        assert_eq!(record.remaining_quantity, 3);
    }

    #[test]
    fn book_accessor_exposes_market_data() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Bid, 100, 2));
        submit(&mut engine, "BTC-USDT", limit(2, Side::Bid, 99, 3));
        submit(&mut engine, "BTC-USDT", limit(3, Side::Ask, 101, 4));

        let book = engine.book("BTC-USDT").unwrap();
        assert_eq!(book.best_bid(), Some(100));
        assert_eq!(book.best_ask(), Some(101));
        assert_eq!(book.depth(Side::Bid, 0), Some((100, 2)));
        assert_eq!(book.depth(Side::Bid, 1), Some((99, 3)));
        assert_eq!(book.depth(Side::Ask, 0), Some((101, 4)));
        assert_eq!(book.depth(Side::Bid, 2), None);
    }

    #[test]
    fn trade_executed_events_match_the_trade_tape() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 2));
        submit(&mut engine, "BTC-USDT", limit(2, Side::Ask, 101, 3));
        let events = submit(&mut engine, "BTC-USDT", limit(3, Side::Bid, 101, 4));

        let event_trades: Vec<Trade> = executed_trades(&events).into_iter().cloned().collect();
        assert_eq!(engine.trades(), &event_trades);
    }

    #[test]
    fn book_snapshot_sequence_is_the_last_applied_event_sequence() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        let events = submit(&mut engine, "BTC-USDT", limit(1, Side::Bid, 100, 2));
        assert_eq!(engine.last_applied_sequence(), events.len() as u64);

        let snapshot = engine.snapshot("BTC-USDT", 10).unwrap();
        assert_eq!(snapshot.sequence, events.len() as u64);
        assert_eq!(snapshot.bids, vec![(100, 2)]);
        assert!(engine.snapshot("ETH-USDT", 10).is_none());
    }

    #[test]
    fn ioc_limit_partial_fill_discards_only_the_residual() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 2));

        let events = submit(&mut engine, "BTC-USDT", ioc_limit(2, Side::Bid, 100, 5));
        assert_eq!(events.len(), 3);
        assert_eq!(
            events.last(),
            Some(&DomainEvent::OrderCanceled {
                symbol: "BTC-USDT".to_string(),
                order_id: 2,
                canceled_quantity: 3,
                reason: OrderCancelReason::UnfilledResidual,
            })
        );
        assert_eq!(engine.book("BTC-USDT").unwrap().best_bid(), None);
    }

    #[test]
    fn ioc_limit_with_no_liquidity_is_accepted_then_canceled() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        let events = submit(&mut engine, "BTC-USDT", ioc_limit(1, Side::Bid, 100, 5));

        assert_eq!(
            events,
            vec![
                DomainEvent::OrderAccepted {
                    symbol: "BTC-USDT".to_string(),
                    order: ioc_limit(1, Side::Bid, 100, 5).to_order(1),
                    post_only: false,
                },
                DomainEvent::OrderCanceled {
                    symbol: "BTC-USDT".to_string(),
                    order_id: 1,
                    canceled_quantity: 5,
                    reason: OrderCancelReason::UnfilledResidual,
                },
            ]
        );
        assert!(
            engine.book("BTC-USDT").is_none(),
            "nothing was ever inserted"
        );
    }

    #[test]
    fn fok_limit_fills_across_levels_and_threads_the_limit_price() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 2));
        submit(&mut engine, "BTC-USDT", limit(2, Side::Ask, 101, 3));

        let events = submit(&mut engine, "BTC-USDT", fok_limit(3, Side::Bid, 101, 5));
        assert_eq!(executed_trades(&events).len(), 2);
        assert_eq!(engine.order_record(3).unwrap().status, OrderStatus::Filled);

        // The limit price must be respected: 3 at 101 are not enough for 4.
        let command = submit_command("BTC-USDT", fok_limit(4, Side::Bid, 100, 4));
        assert_eq!(
            rejection(&engine.decide(&command)),
            Some(EngineError::InsufficientLiquidity)
        );
    }

    #[test]
    fn fok_rejects_when_liquidity_or_the_limit_price_is_insufficient() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 101, 5));

        for request in [
            fok_limit(2, Side::Bid, 101, 6),
            fok_limit(3, Side::Bid, 100, 5),
        ] {
            let command = submit_command("BTC-USDT", request);
            assert_eq!(
                rejection(&engine.decide(&command)),
                Some(EngineError::InsufficientLiquidity)
            );
        }
        assert!(engine.trades().is_empty());
    }

    #[test]
    fn post_only_rejects_crossing_bids_and_asks() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 1));
        submit(&mut engine, "BTC-USDT", limit(2, Side::Bid, 99, 1));

        for request in [
            post_only_limit(3, Side::Bid, 100, 1),
            post_only_limit(4, Side::Ask, 99, 1),
        ] {
            let command = submit_command("BTC-USDT", request);
            assert_eq!(
                rejection(&engine.decide(&command)),
                Some(EngineError::PostOnlyWouldCross)
            );
        }
        assert!(engine.trades().is_empty());
    }

    #[test]
    fn post_only_rests_when_it_does_not_cross() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 101, 1));

        let events = submit(
            &mut engine,
            "BTC-USDT",
            post_only_limit(2, Side::Bid, 100, 1),
        );
        assert_eq!(
            events,
            vec![
                DomainEvent::OrderAccepted {
                    symbol: "BTC-USDT".to_string(),
                    order: post_only_limit(2, Side::Bid, 100, 1).to_order(2),
                    post_only: true,
                },
                DomainEvent::OrderRested {
                    symbol: "BTC-USDT".to_string(),
                    order: post_only_limit(2, Side::Bid, 100, 1).to_order(2),
                },
            ]
        );
    }

    #[test]
    fn invalid_time_in_force_combinations_are_rejected() {
        let engine = MatchingEngine::new(EngineConfig::default());

        let mut market_gtc = market(1, Side::Bid, 1);
        market_gtc.time_in_force = TimeInForce::GTC;
        let market_post_only = OrderRequest {
            post_only: true,
            ..market(2, Side::Bid, 1)
        };
        let post_only_ioc = OrderRequest {
            post_only: true,
            ..ioc_limit(3, Side::Bid, 100, 1)
        };

        for request in [market_gtc, market_post_only, post_only_ioc] {
            let command = submit_command("BTC-USDT", request);
            assert_eq!(
                rejection(&engine.decide(&command)),
                Some(EngineError::InvalidTimeInForce)
            );
        }
    }

    #[test]
    fn post_only_market_order_is_rejected_before_the_crossing_check() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 1));

        let command = submit_command(
            "BTC-USDT",
            OrderRequest {
                post_only: true,
                ..market(2, Side::Bid, 1)
            },
        );
        assert_eq!(
            rejection(&engine.decide(&command)),
            Some(EngineError::InvalidTimeInForce)
        );
    }

    #[test]
    fn price_time_priority_selects_makers_in_fifo_order() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 2));
        submit(&mut engine, "BTC-USDT", limit(2, Side::Ask, 100, 3));

        let events = submit(&mut engine, "BTC-USDT", limit(3, Side::Bid, 100, 4));
        let makers: Vec<u64> = executed_trades(&events)
            .into_iter()
            .map(|trade| trade.maker_order_id)
            .collect();
        assert_eq!(
            makers,
            vec![1, 2],
            "the earlier order at a level fills first"
        );
    }

    #[test]
    fn modify_cancels_the_old_id_and_rests_the_replacement() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Bid, 100, 5));

        let events = run(&mut engine, modify_command("BTC-USDT", 1, 99, 3, 2));
        assert_eq!(
            events,
            vec![
                DomainEvent::OrderCanceled {
                    symbol: "BTC-USDT".to_string(),
                    order_id: 1,
                    canceled_quantity: 5,
                    reason: OrderCancelReason::Modified,
                },
                DomainEvent::OrderAccepted {
                    symbol: "BTC-USDT".to_string(),
                    order: limit(2, Side::Bid, 99, 3).to_order(2),
                    post_only: false,
                },
                DomainEvent::OrderRested {
                    symbol: "BTC-USDT".to_string(),
                    order: limit(2, Side::Bid, 99, 3).to_order(2),
                },
            ]
        );
        assert_eq!(
            engine.order_record(1).unwrap().status,
            OrderStatus::Canceled
        );
        assert!(engine.is_open(2));
    }

    #[test]
    fn modify_to_a_crossing_price_trades_and_rests_the_remainder() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        submit(&mut engine, "BTC-USDT", limit(1, Side::Ask, 100, 2));
        submit(&mut engine, "BTC-USDT", limit(2, Side::Bid, 99, 5));

        let events = run(&mut engine, modify_command("BTC-USDT", 2, 100, 5, 3));
        assert_eq!(executed_trades(&events).len(), 1);
        match events.last() {
            Some(DomainEvent::OrderRested { order, .. }) => {
                assert_eq!(order.id, 3);
                assert_eq!(order.quantity, 3);
            }
            other => panic!("expected OrderRested, got {other:?}"),
        }
        let record = engine.order_record(3).unwrap();
        assert_eq!(record.status, OrderStatus::PartiallyFilled);
        assert_eq!(record.remaining_quantity, 3);
    }

    #[test]
    fn invalid_modify_leaves_the_original_order_untouched() {
        let mut engine = MatchingEngine::new(EngineConfig {
            min_order_quantity: 1,
            max_order_quantity: u64::MAX,
            tick_size: 5,
        });
        submit(&mut engine, "BTC-USDT", limit(1, Side::Bid, 100, 5));

        for (command, reason) in [
            (
                modify_command("BTC-USDT", 1, 101, 5, 2),
                EngineError::PriceNotOnTick,
            ),
            (
                modify_command("BTC-USDT", 1, 100, 0, 2),
                EngineError::QuantityOutOfRange,
            ),
            (
                modify_command("BTC-USDT", 999, 100, 1, 2),
                EngineError::UnknownOrder,
            ),
            (
                modify_command("ETH-USDT", 1, 100, 1, 2),
                EngineError::UnknownOrder,
            ),
        ] {
            let before = engine.capture_snapshot();
            assert_eq!(rejection(&engine.decide(&command)), Some(reason));
            assert_eq!(engine.capture_snapshot(), before);
        }
    }

    #[test]
    fn executing_is_deciding_then_applying_for_every_command_variant() {
        let commands = vec![
            submit_command("BTC-USDT", limit(1, Side::Ask, 100, 5)),
            submit_command("BTC-USDT", limit(2, Side::Bid, 100, 3)),
            submit_command("BTC-USDT", limit(3, Side::Bid, 99, 2)),
            cancel_command("BTC-USDT", 3),
            modify_command("BTC-USDT", 2, 101, 2, 4),
            submit_command("BTC-USDT", limit(5, Side::Bid, 98, 0)),
        ];

        let mut executed = MatchingEngine::new(EngineConfig::default());
        let mut folded = MatchingEngine::new(EngineConfig::default());
        for command in &commands {
            let events = executed.execute(command.clone());
            let decided = folded.decide(command);
            assert_eq!(events, decided);
            for event in &decided {
                let sequence = folded.last_applied_sequence() + 1;
                folded.apply(event, sequence);
            }
        }

        assert_eq!(executed.capture_snapshot(), folded.capture_snapshot());
        assert_eq!(
            executed.last_applied_sequence(),
            folded.last_applied_sequence()
        );
    }

    #[test]
    fn stop_submission_creates_a_pending_record_without_a_book() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        let events = submit_stop_command(&mut engine, stop_market(Side::Bid, 90, 1, 1));

        assert!(matches!(
            events.as_slice(),
            [DomainEvent::StopPlaced { .. }]
        ));
        assert_eq!(
            engine.order_record(1).unwrap().status,
            OrderStatus::PendingTrigger
        );
        assert!(engine.is_open(1));
        assert!(engine.stop_order(1).is_some());
        assert!(engine.book("BTC-USDT").is_none());
    }

    #[test]
    fn stop_policy_rejects_immediate_triggers_and_off_tick_prices() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        seed_last_price(&mut engine, 100);
        for request in [
            stop_market(Side::Bid, 100, 1, 3),
            stop_market(Side::Bid, 99, 1, 3),
            stop_market(Side::Ask, 100, 1, 3),
            stop_market(Side::Ask, 101, 1, 3),
        ] {
            let command = EngineCommand::SubmitStopOrder {
                symbol: "BTC-USDT".to_string(),
                request,
            };
            assert_eq!(
                rejection(&engine.decide(&command)),
                Some(EngineError::StopPriceWouldTriggerImmediately)
            );
        }

        let off_tick_engine = MatchingEngine::new(EngineConfig {
            min_order_quantity: 1,
            max_order_quantity: u64::MAX,
            tick_size: 5,
        });
        let off_tick = EngineCommand::SubmitStopOrder {
            symbol: "BTC-USDT".to_string(),
            request: stop_market(Side::Ask, 96, 1, 1),
        };
        assert_eq!(
            rejection(&off_tick_engine.decide(&off_tick)),
            Some(EngineError::PriceNotOnTick)
        );
        let stop_limit_off_tick = EngineCommand::SubmitStopOrder {
            symbol: "BTC-USDT".to_string(),
            request: StopOrderRequest {
                side: Side::Ask,
                stop_price: 95,
                order_type: StopKind::Limit { price: 96 },
                quantity: 1,
                timestamp: 2,
            },
        };
        assert_eq!(
            rejection(&off_tick_engine.decide(&stop_limit_off_tick)),
            Some(EngineError::PriceNotOnTick)
        );
    }

    #[test]
    fn pending_stop_is_not_modifiable_but_can_be_canceled() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        seed_last_price(&mut engine, 100);
        let stop = pending_stop(&mut engine, stop_market(Side::Ask, 95, 1, 3));

        let modify = modify_command("BTC-USDT", stop, 94, 1, 4);
        assert_eq!(
            rejection(&engine.decide(&modify)),
            Some(EngineError::StopOrderNotModifiable)
        );
        assert!(engine.stop_order(stop).is_some());

        let events = run(&mut engine, cancel_command("BTC-USDT", stop));
        assert_eq!(
            events,
            vec![DomainEvent::StopCanceled {
                symbol: "BTC-USDT".to_string(),
                order_id: stop,
            }]
        );
        assert!(engine.stop_order(stop).is_none());
        assert!(!engine.is_open(stop));
    }

    #[test]
    fn stop_queries_are_isolated_per_symbol_and_ordered_by_price_then_fifo() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        for (symbol, request) in [
            ("AAA-USDT", stop_market(Side::Bid, 105, 1, 1)),
            ("AAA-USDT", stop_market(Side::Bid, 101, 1, 2)),
            ("AAA-USDT", stop_market(Side::Bid, 101, 1, 3)),
            ("AAA-USDT", stop_market(Side::Ask, 95, 1, 4)),
            ("AAA-USDT", stop_market(Side::Ask, 99, 1, 5)),
            ("AAA-USDT", stop_market(Side::Ask, 99, 1, 6)),
            ("BBB-USDT", stop_market(Side::Bid, 90, 1, 7)),
        ] {
            run(
                &mut engine,
                EngineCommand::SubmitStopOrder {
                    symbol: symbol.to_string(),
                    request,
                },
            );
        }

        let bid_ids: Vec<u64> = engine
            .stop_orders("AAA-USDT", Side::Bid)
            .iter()
            .map(|stop| stop.id)
            .collect();
        let ask_ids: Vec<u64> = engine
            .stop_orders("AAA-USDT", Side::Ask)
            .iter()
            .map(|stop| stop.id)
            .collect();
        assert_eq!(bid_ids, vec![2, 3, 1], "bids ascend by trigger price");
        assert_eq!(ask_ids, vec![5, 6, 4], "asks descend by trigger price");
        assert_eq!(engine.stop_orders("BBB-USDT", Side::Bid).len(), 1);
        assert!(engine.stop_orders("CCC-USDT", Side::Bid).is_empty());
    }

    #[test]
    fn last_trade_price_tracks_the_latest_fill_per_symbol() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        seed_last_price(&mut engine, 100);

        submit(&mut engine, "BTC-USDT", limit(3, Side::Ask, 101, 1));
        submit(&mut engine, "BTC-USDT", limit(4, Side::Bid, 101, 1));

        assert_eq!(engine.last_trade_price("BTC-USDT"), Some(101));
        assert_eq!(engine.last_trade_price("ETH-USDT"), None);
    }

    #[test]
    fn triggered_stop_reuses_its_id_and_becomes_a_working_order() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        seed_last_price(&mut engine, 100);
        let stop = pending_stop(&mut engine, stop_limit(Side::Ask, 95, 94, 2, 3));
        submit(&mut engine, "BTC-USDT", limit(4, Side::Bid, 95, 1));
        submit(&mut engine, "BTC-USDT", limit(5, Side::Bid, 93, 5));

        let events = submit(&mut engine, "BTC-USDT", limit(6, Side::Ask, 95, 1));
        assert!(
            events
                .iter()
                .any(|event| matches!(event, DomainEvent::StopTriggered { .. }))
        );
        assert!(engine.stop_order(stop).is_none());
        assert_eq!(engine.order_record(stop).unwrap().status, OrderStatus::New);
        assert_eq!(engine.book("BTC-USDT").unwrap().best_ask(), Some(94));
    }

    fn stop_market(side: Side, stop_price: u64, quantity: u64, timestamp: u64) -> StopOrderRequest {
        StopOrderRequest {
            side,
            stop_price,
            order_type: StopKind::Market,
            quantity,
            timestamp,
        }
    }

    fn stop_limit(
        side: Side,
        stop_price: u64,
        price: u64,
        quantity: u64,
        timestamp: u64,
    ) -> StopOrderRequest {
        StopOrderRequest {
            side,
            stop_price,
            order_type: StopKind::Limit { price },
            quantity,
            timestamp,
        }
    }

    fn submit_stop_command(
        engine: &mut MatchingEngine,
        request: StopOrderRequest,
    ) -> Vec<DomainEvent> {
        run(
            engine,
            EngineCommand::SubmitStopOrder {
                symbol: "BTC-USDT".to_string(),
                request,
            },
        )
    }

    fn pending_stop(engine: &mut MatchingEngine, request: StopOrderRequest) -> u64 {
        let events = submit_stop_command(engine, request);
        match events.first() {
            Some(DomainEvent::StopPlaced { stop, .. }) => stop.id,
            other => panic!("expected StopPlaced, got {other:?}"),
        }
    }

    fn seed_last_price(engine: &mut MatchingEngine, price: u64) {
        submit(engine, "BTC-USDT", limit(1, Side::Ask, price, 1));
        submit(engine, "BTC-USDT", limit(2, Side::Bid, price, 1));
        assert_eq!(engine.last_trade_price("BTC-USDT"), Some(price));
    }
}
