//! Event-sourced stops (ticket 03).
//!
//! These tests exercise the event-sourced path only through public seams: the
//! pure `decide`, the sole state-write entry `apply`, and the append-before-
//! apply `DurableMatchingEngine`. Expected event streams and state are literals
//! derived from the ticket and the event catalog, never recomputed by the code
//! under test.

use std::collections::HashMap;

use orderbook_rs::durable::{DurableMatchingEngine, InMemoryEventLog, InMemorySnapshotStore};
use orderbook_rs::engine::MatchingEngine;
use orderbook_rs::event::{DomainEvent, OrderCancelReason};
use orderbook_rs::snapshot::EngineSnapshot;
use orderbook_rs::types::{
    EngineCommand, EngineConfig, EngineError, Order, OrderBookId, OrderRecord, OrderRequest,
    OrderStatus, OrderType, Side, StopKind, StopOrder, StopOrderRequest, TimeInForce, Trade,
};

const BTC: &str = "BTC-USDT";

fn limit(side: Side, price: u64, quantity: u64, timestamp: u64) -> OrderRequest {
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

fn submit(symbol: &str, request: OrderRequest) -> EngineCommand {
    EngineCommand::SubmitOrder {
        symbol: symbol.to_string(),
        request,
    }
}

fn cancel(symbol: &str, order_id: OrderBookId) -> EngineCommand {
    EngineCommand::CancelOrder {
        symbol: symbol.to_string(),
        order_id,
    }
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

fn submit_stop(symbol: &str, request: StopOrderRequest) -> EngineCommand {
    EngineCommand::SubmitStopOrder {
        symbol: symbol.to_string(),
        request,
    }
}

fn stop_order(id: OrderBookId, request: &StopOrderRequest) -> StopOrder {
    StopOrder {
        id,
        side: request.side,
        stop_price: request.stop_price,
        order_type: request.order_type,
        quantity: request.quantity,
        timestamp: request.timestamp,
    }
}

fn converted(
    id: OrderBookId,
    side: Side,
    price: u64,
    quantity: u64,
    timestamp: u64,
    order_type: OrderType,
    time_in_force: TimeInForce,
) -> Order {
    Order {
        id,
        side,
        price,
        quantity,
        timestamp,
        order_type,
        time_in_force,
    }
}

fn trade(
    id: u64,
    maker: OrderBookId,
    taker: OrderBookId,
    price: u64,
    quantity: u64,
    taker_side: Side,
    timestamp: u64,
) -> Trade {
    Trade {
        id,
        symbol: BTC.to_string(),
        maker_order_id: maker,
        taker_order_id: taker,
        price,
        quantity,
        taker_side,
        timestamp,
    }
}

fn accepted(order_id: OrderBookId, request: &OrderRequest) -> DomainEvent {
    DomainEvent::OrderAccepted {
        symbol: BTC.to_string(),
        order: request.to_order(order_id),
        post_only: request.post_only,
    }
}

fn stop_placed(id: OrderBookId, request: &StopOrderRequest) -> DomainEvent {
    DomainEvent::StopPlaced {
        symbol: BTC.to_string(),
        stop: stop_order(id, request),
    }
}

fn stop_triggered(order: Order, last_price: u64) -> DomainEvent {
    DomainEvent::StopTriggered {
        symbol: BTC.to_string(),
        order,
        last_price,
    }
}

fn rejected(command: &EngineCommand, reason: EngineError) -> DomainEvent {
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

fn apply_all(engine: &mut MatchingEngine, events: &[DomainEvent]) {
    for event in events {
        let sequence = engine.last_applied_sequence() + 1;
        engine.apply(event, sequence);
    }
}

fn run(engine: &mut MatchingEngine, command: &EngineCommand) -> Vec<DomainEvent> {
    let events = engine.decide(command);
    apply_all(engine, &events);
    events
}

/// Rest an order through the event-sourced path and return its id.
fn rest(engine: &mut MatchingEngine, request: OrderRequest) -> OrderBookId {
    let events = run(engine, &submit(BTC, request));
    match events.first() {
        Some(DomainEvent::OrderAccepted { order, .. }) => order.id,
        other => panic!("resting submit must start with OrderAccepted, got {other:?}"),
    }
}

/// Place a pending stop through the event-sourced path and return its id.
fn place_stop(engine: &mut MatchingEngine, request: StopOrderRequest) -> OrderBookId {
    let events = run(engine, &submit_stop(BTC, request));
    match events.first() {
        Some(DomainEvent::StopPlaced { stop, .. }) => stop.id,
        other => panic!("stop submit must start with StopPlaced, got {other:?}"),
    }
}

/// Seed a last trade at `price` and leave the book empty.
fn seed_last_price(engine: &mut MatchingEngine, price: u64) {
    rest(engine, limit(Side::Ask, price, 1, 1));
    rest(engine, limit(Side::Bid, price, 1, 2));
    assert_eq!(engine.last_trade_price(BTC), Some(price));
}

fn records(engine: &MatchingEngine) -> Vec<(OrderBookId, OrderRecord)> {
    let mut records: Vec<(OrderBookId, OrderRecord)> = engine
        .order_records()
        .map(|(order_id, record)| (order_id, record.clone()))
        .collect();
    records.sort_by_key(|(order_id, _)| *order_id);
    records
}

fn ledger_map(snapshot: &EngineSnapshot) -> HashMap<OrderBookId, OrderRecord> {
    snapshot
        .ledger
        .iter()
        .map(|entry| (entry.order_id, entry.record.clone()))
        .collect()
}

/// Every prefix of the stream must leave a structurally valid engine state.
fn assert_prefix_invariants(engine: &MatchingEngine) {
    let snapshot = engine.capture_snapshot();
    let ledger = ledger_map(&snapshot);

    for entry in &snapshot.ledger {
        assert_eq!(
            entry.record.filled_quantity + entry.record.remaining_quantity,
            entry.record.original_quantity,
            "order {} violated filled + remaining == original",
            entry.order_id
        );
    }

    // Book orders are backed by an open ledger record with the same quantity.
    let mut seen = HashMap::new();
    for book in &snapshot.books {
        for level in book.bids.iter().chain(book.asks.iter()) {
            for order in &level.orders {
                assert!(
                    seen.insert(order.id, ()).is_none(),
                    "order {} appears twice in the books",
                    order.id
                );
                let record = ledger
                    .get(&order.id)
                    .unwrap_or_else(|| panic!("book order {} has no ledger record", order.id));
                assert_eq!(record.symbol, book.symbol);
                assert!(
                    record.is_open(),
                    "book order {} has non-open ledger status {:?}",
                    order.id,
                    record.status
                );
                assert_eq!(
                    record.remaining_quantity, order.quantity,
                    "book order {} quantity disagrees with the ledger",
                    order.id
                );
            }
        }
    }

    // Pending stops and PendingTrigger ledger records are in one-to-one
    // correspondence, and every pending stop still holds its full quantity.
    let mut pending_records: Vec<OrderBookId> = snapshot
        .ledger
        .iter()
        .filter(|entry| entry.record.status == OrderStatus::PendingTrigger)
        .map(|entry| entry.order_id)
        .collect();
    pending_records.sort_unstable();
    let mut pending_stops: Vec<OrderBookId> = snapshot
        .stops
        .iter()
        .flat_map(|state| state.stops.iter())
        .map(|stop| stop.id)
        .collect();
    pending_stops.sort_unstable();
    assert_eq!(
        pending_records, pending_stops,
        "pending stops and PendingTrigger ledger records must match"
    );
    for state in &snapshot.stops {
        for stop in &state.stops {
            let record = ledger
                .get(&stop.id)
                .unwrap_or_else(|| panic!("stop {} has no ledger record", stop.id));
            assert_eq!(record.status, OrderStatus::PendingTrigger);
            assert_eq!(record.remaining_quantity, stop.quantity);
        }
    }

    // Trade tape is contiguous.
    for (index, trade) in snapshot.trades.iter().enumerate() {
        assert_eq!(trade.id, index as u64 + 1, "trade ids must be contiguous");
    }

    // Last price is the price of the symbol's most recent trade.
    let expected = snapshot
        .trades
        .iter()
        .rev()
        .find(|trade| trade.symbol == BTC)
        .map(|trade| trade.price);
    assert_eq!(engine.last_trade_price(BTC), expected);
}

// --------------------------------------------------------------------------
// Stop submission
// --------------------------------------------------------------------------

#[test]
fn submitting_a_stop_emits_stop_placed_and_apply_registers_a_pending_record() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    let request = stop_market(Side::Bid, 90, 3, 1);
    let command = submit_stop(BTC, request);

    let events = engine.decide(&command);

    assert_eq!(
        events,
        vec![stop_placed(1, &request)],
        "one StopPlaced event"
    );

    // Purity: deciding twice allocates the same local id and mutates nothing.
    assert_eq!(engine.decide(&command), events);
    assert_eq!(engine.order_count(), 0);
    assert!(engine.stop_order(1).is_none());
    assert_eq!(engine.last_applied_sequence(), 0);

    apply_all(&mut engine, &events);

    let record = engine.order_record(1).expect("stop has a ledger record");
    assert_eq!(record.status, OrderStatus::PendingTrigger);
    assert_eq!(record.original_quantity, 3);
    assert_eq!(record.filled_quantity, 0);
    assert_eq!(record.remaining_quantity, 3);
    assert_eq!(engine.stop_order(1), Some(&stop_order(1, &request)));
    assert!(engine.is_open(1));
    assert!(
        engine.book(BTC).is_none(),
        "a pending stop never creates a book"
    );
    assert_eq!(engine.snapshot(BTC, 10), None);
}

#[test]
fn a_rejected_stop_emits_one_order_rejected_and_allocates_no_id() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    seed_last_price(&mut engine, 100);

    // A buy stop at or below the last price would trigger immediately.
    let command = submit_stop(BTC, stop_market(Side::Bid, 100, 1, 3));
    assert_eq!(
        engine.decide(&command),
        vec![rejected(
            &command,
            EngineError::StopPriceWouldTriggerImmediately
        )]
    );
    assert_eq!(engine.order_count(), 2, "only the two seed trades");

    let log = InMemoryEventLog::new();
    let mut durable = DurableMatchingEngine::create(
        EngineConfig::default(),
        Box::new(log.handle()),
        Box::new(InMemorySnapshotStore::new()),
    )
    .expect("fresh event log");
    // The durable engine starts empty, so use an invalid stop there.
    let command = submit_stop(BTC, stop_market(Side::Bid, 90, 0, 1));
    let events = durable
        .execute(command.clone())
        .expect("a business rejection is an event-producing execute");
    assert_eq!(
        events,
        vec![rejected(&command, EngineError::QuantityOutOfRange)]
    );
    assert!(!durable.is_failed());
    assert_eq!(durable.applied_seq(), 1);
    assert_eq!(durable.history_from(1).unwrap().len(), 1);
    assert_eq!(durable.engine().order_count(), 0);
    assert_eq!(durable.engine().stop_order(1), None, "no id was allocated");
}

// --------------------------------------------------------------------------
// Triggering: converted order, trades, rest, residual discard
// --------------------------------------------------------------------------

#[test]
fn market_stop_triggers_after_the_command_and_reuses_its_order_id() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    seed_last_price(&mut engine, 100);
    let stop = place_stop(&mut engine, stop_market(Side::Ask, 95, 2, 3));

    rest(&mut engine, limit(Side::Bid, 95, 5, 4));

    let request = limit(Side::Ask, 95, 1, 5);
    let events = engine.decide(&submit(BTC, request));

    assert_eq!(events.len(), 4, "accepted + trade + trigger + trade");
    assert_eq!(events[0], accepted(5, &request));
    assert_eq!(
        events[1],
        DomainEvent::TradeExecuted {
            trade: trade(2, 4, 5, 95, 1, Side::Ask, 5),
        }
    );
    assert_eq!(
        events[2],
        stop_triggered(
            converted(
                stop,
                Side::Ask,
                0,
                2,
                5,
                OrderType::Market,
                TimeInForce::IOC
            ),
            95
        ),
        "StopTriggered carries the converted order with the last-trade timestamp"
    );
    assert_eq!(
        events[3],
        DomainEvent::TradeExecuted {
            trade: trade(3, 4, stop, 95, 2, Side::Ask, 5),
        }
    );

    apply_all(&mut engine, &events);

    assert!(engine.stop_order(stop).is_none());
    let record = engine.order_record(stop).unwrap();
    assert_eq!(record.status, OrderStatus::Filled);
    assert_eq!(record.filled_quantity, 2);
    assert_eq!(record.remaining_quantity, 0);
    assert_eq!(engine.last_trade_price(BTC), Some(95));
    assert_eq!(engine.trades().len(), 3, "seed, command and cascade trades");
}

#[test]
fn limit_stop_that_does_not_cross_rests_as_a_gtc_limit() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    seed_last_price(&mut engine, 100);
    let stop = place_stop(&mut engine, stop_limit(Side::Ask, 95, 94, 2, 3));

    rest(&mut engine, limit(Side::Bid, 95, 1, 4));
    rest(&mut engine, limit(Side::Bid, 93, 5, 5));

    let request = limit(Side::Ask, 95, 1, 6);
    let events = engine.decide(&submit(BTC, request));

    assert_eq!(events.len(), 4, "accepted + trade + trigger + rest");
    assert_eq!(
        events[2],
        stop_triggered(
            converted(
                stop,
                Side::Ask,
                94,
                2,
                6,
                OrderType::Limit,
                TimeInForce::GTC
            ),
            95
        )
    );
    assert_eq!(
        events[3],
        DomainEvent::OrderRested {
            symbol: BTC.to_string(),
            order: converted(
                stop,
                Side::Ask,
                94,
                2,
                6,
                OrderType::Limit,
                TimeInForce::GTC
            ),
        },
        "OrderRested carries the remaining quantity"
    );

    apply_all(&mut engine, &events);

    assert!(engine.stop_order(stop).is_none());
    let record = engine.order_record(stop).unwrap();
    assert_eq!(record.status, OrderStatus::New);
    assert_eq!(record.filled_quantity, 0);
    assert_eq!(record.remaining_quantity, 2);
    let book = engine.book(BTC).unwrap();
    assert_eq!(book.best_ask(), Some(94));
    assert_eq!(book.depth(Side::Ask, 0), Some((94, 2)));
}

#[test]
fn limit_stop_that_crosses_fills_completely() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    seed_last_price(&mut engine, 100);
    let stop = place_stop(&mut engine, stop_limit(Side::Ask, 95, 90, 1, 3));

    rest(&mut engine, limit(Side::Bid, 95, 1, 4));
    rest(&mut engine, limit(Side::Bid, 93, 5, 5));

    let events = run(&mut engine, &submit(BTC, limit(Side::Ask, 95, 1, 6)));

    assert_eq!(
        events.last().unwrap(),
        &DomainEvent::TradeExecuted {
            trade: trade(3, 5, stop, 93, 1, Side::Ask, 6),
        },
        "a full fill has no trailing rest or cancel"
    );
    let record = engine.order_record(stop).unwrap();
    assert_eq!(record.status, OrderStatus::Filled);
    assert_eq!(record.filled_quantity, 1);
    assert_eq!(engine.last_trade_price(BTC), Some(93));
}

#[test]
fn market_stop_residual_with_no_liquidity_is_discarded() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    seed_last_price(&mut engine, 100);
    let stop = place_stop(&mut engine, stop_market(Side::Ask, 95, 2, 3));

    // The triggering command consumes the only bid, so the converted market
    // order has nothing left to hit.
    rest(&mut engine, limit(Side::Bid, 95, 1, 4));

    let events = run(&mut engine, &submit(BTC, limit(Side::Ask, 95, 1, 5)));

    assert_eq!(
        events.last().unwrap(),
        &DomainEvent::OrderCanceled {
            symbol: BTC.to_string(),
            order_id: stop,
            canceled_quantity: 2,
            reason: OrderCancelReason::UnfilledResidual,
        }
    );

    let record = engine.order_record(stop).unwrap();
    assert_eq!(record.status, OrderStatus::Canceled);
    assert_eq!(record.filled_quantity, 0);
    assert_eq!(record.remaining_quantity, 2);
    assert_eq!(engine.book(BTC).unwrap().best_bid(), None);
}

#[test]
fn cascading_triggers_emit_in_trigger_order() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    seed_last_price(&mut engine, 100);
    let first = place_stop(&mut engine, stop_market(Side::Ask, 95, 1, 3));
    let second = place_stop(&mut engine, stop_market(Side::Ask, 93, 1, 4));

    rest(&mut engine, limit(Side::Bid, 95, 1, 5));
    rest(&mut engine, limit(Side::Bid, 93, 5, 6));

    let events = run(&mut engine, &submit(BTC, limit(Side::Ask, 95, 1, 7)));

    assert_eq!(
        events,
        vec![
            accepted(7, &limit(Side::Ask, 95, 1, 7)),
            DomainEvent::TradeExecuted {
                trade: trade(2, 5, 7, 95, 1, Side::Ask, 7),
            },
            stop_triggered(
                converted(
                    first,
                    Side::Ask,
                    0,
                    1,
                    7,
                    OrderType::Market,
                    TimeInForce::IOC
                ),
                95
            ),
            DomainEvent::TradeExecuted {
                trade: trade(3, 6, first, 93, 1, Side::Ask, 7),
            },
            stop_triggered(
                converted(
                    second,
                    Side::Ask,
                    0,
                    1,
                    7,
                    OrderType::Market,
                    TimeInForce::IOC
                ),
                93
            ),
            DomainEvent::TradeExecuted {
                trade: trade(4, 6, second, 93, 1, Side::Ask, 7),
            },
        ],
        "the cascade follows trigger order and shares one command"
    );

    assert_eq!(engine.last_trade_price(BTC), Some(93));
    assert!(engine.stop_orders(BTC, Side::Ask).is_empty());
    assert_eq!(
        engine.order_record(first).unwrap().status,
        OrderStatus::Filled
    );
    assert_eq!(
        engine.order_record(second).unwrap().status,
        OrderStatus::Filled
    );
}

#[test]
fn triggered_limit_stop_rests_behind_existing_orders_at_the_level() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    seed_last_price(&mut engine, 100);
    let stop = place_stop(&mut engine, stop_limit(Side::Bid, 105, 104, 2, 3));

    let existing = rest(&mut engine, limit(Side::Bid, 104, 5, 4));
    rest(&mut engine, limit(Side::Ask, 105, 1, 5));

    // A trade at 105 triggers the buy stop; the converted bid at 104 rests.
    run(&mut engine, &submit(BTC, limit(Side::Bid, 105, 1, 6)));

    let snapshot = engine.capture_snapshot();
    let book = snapshot
        .books
        .iter()
        .find(|book| book.symbol == BTC)
        .expect("BTC book exists");
    let level = book
        .bids
        .iter()
        .find(|level| level.price == 104)
        .expect("level 104 exists");
    assert_eq!(
        level
            .orders
            .iter()
            .map(|order| order.id)
            .collect::<Vec<_>>(),
        vec![existing, stop],
        "the activated stop joins the back of the level"
    );

    // A sell into the level fills the older order first.
    let events = run(&mut engine, &submit(BTC, limit(Side::Ask, 104, 1, 7)));
    assert_eq!(
        events.iter().find_map(|event| match event {
            DomainEvent::TradeExecuted { trade } => Some(trade.maker_order_id),
            _ => None,
        }),
        Some(existing)
    );
}

#[test]
fn decide_is_pure_for_stop_submit_and_triggering_submits() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    seed_last_price(&mut engine, 100);
    place_stop(&mut engine, stop_market(Side::Ask, 95, 1, 3));
    rest(&mut engine, limit(Side::Bid, 95, 5, 4));

    let before = engine.capture_snapshot();
    let sequence_before = engine.last_applied_sequence();

    let stop_command = submit_stop(BTC, stop_limit(Side::Bid, 110, 109, 2, 5));
    let triggering = submit(BTC, limit(Side::Ask, 95, 1, 6));

    let first_stop = engine.decide(&stop_command);
    let second_stop = engine.decide(&stop_command);
    assert_eq!(first_stop, second_stop);

    let first_trigger = engine.decide(&triggering);
    let second_trigger = engine.decide(&triggering);
    assert_eq!(first_trigger, second_trigger);

    assert_eq!(
        engine.capture_snapshot(),
        before,
        "neither stop submission nor a cascade decision mutates state"
    );
    assert_eq!(engine.last_applied_sequence(), sequence_before);
    assert_eq!(engine.last_trade_price(BTC), Some(100));
}

// --------------------------------------------------------------------------
// Pending-stop cancel
// --------------------------------------------------------------------------

#[test]
fn canceling_a_pending_stop_emits_stop_canceled_and_clears_stop_and_ledger() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    seed_last_price(&mut engine, 100);
    let stop = place_stop(&mut engine, stop_market(Side::Bid, 105, 4, 3));

    let events = engine.decide(&cancel(BTC, stop));
    assert_eq!(
        events,
        vec![DomainEvent::StopCanceled {
            symbol: BTC.to_string(),
            order_id: stop,
        }]
    );

    apply_all(&mut engine, &events);

    assert!(engine.stop_order(stop).is_none());
    assert!(engine.stop_orders(BTC, Side::Bid).is_empty());
    let record = engine.order_record(stop).unwrap();
    assert_eq!(record.status, OrderStatus::Canceled);
    assert_eq!(record.original_quantity, 4);
    assert_eq!(record.filled_quantity, 0);
    assert_eq!(record.remaining_quantity, 4);
    assert!(!engine.is_open(stop));
    let book = engine.book(BTC).expect("the seed created an empty book");
    assert_eq!(book.best_bid(), None);
    assert_eq!(book.best_ask(), None);

    // Canceling again is a clear unknown-order rejection event.
    let command = cancel(BTC, stop);
    assert_eq!(
        engine.decide(&command),
        vec![rejected(&command, EngineError::UnknownOrder)]
    );
}

#[test]
fn pending_stop_cancel_is_scoped_to_the_symbol() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    let stop = place_stop(&mut engine, stop_market(Side::Bid, 90, 1, 1));

    let wrong_symbol = cancel("ETH-USDT", stop);
    assert_eq!(
        engine.decide(&wrong_symbol),
        vec![rejected(&wrong_symbol, EngineError::UnknownOrder)]
    );
    assert!(matches!(
        engine.decide(&cancel(BTC, stop)).as_slice(),
        [DomainEvent::StopCanceled { .. }]
    ));
}

// --------------------------------------------------------------------------
// Replay equality, prefix validity, and counters
// --------------------------------------------------------------------------

/// A command stream covering a pending stop, a triggered limit stop that rests,
/// cascading market stops, a canceled pending stop, and a canceled triggered
/// order.
fn lifecycle_commands() -> Vec<EngineCommand> {
    vec![
        // Seed a last trade at 100 and leave the book empty.
        submit(BTC, limit(Side::Ask, 100, 1, 1)),
        submit(BTC, limit(Side::Bid, 100, 1, 2)),
        // A buy stop that never triggers.
        submit_stop(BTC, stop_market(Side::Bid, 110, 1, 3)),
        // A buy limit stop that triggers at 105 and rests at 104.
        submit_stop(BTC, stop_limit(Side::Bid, 105, 104, 2, 4)),
        submit(BTC, limit(Side::Bid, 104, 5, 5)),
        submit(BTC, limit(Side::Ask, 105, 1, 6)),
        // Trade at 105 triggers the limit stop.
        submit(BTC, limit(Side::Bid, 105, 1, 7)),
        // Sell stops that will cascade once the price falls.
        submit_stop(BTC, stop_market(Side::Ask, 95, 1, 8)),
        submit_stop(BTC, stop_market(Side::Ask, 93, 1, 9)),
        // A stop canceled before it can trigger.
        submit_stop(BTC, stop_market(Side::Ask, 99, 1, 10)),
        cancel(BTC, 10),
        // Liquidity for the downward cascade.
        submit(BTC, limit(Side::Bid, 95, 1, 11)),
        submit(BTC, limit(Side::Bid, 93, 5, 12)),
        // Trigger both sell stops in one command.
        submit(BTC, limit(Side::Ask, 95, 1, 13)),
        // Cancel the triggered limit order that rested at 104.
        cancel(BTC, 4),
    ]
}

#[test]
fn every_prefix_is_valid_and_replay_equals_live_execution() {
    let commands = lifecycle_commands();

    let mut live = MatchingEngine::new(EngineConfig::default());
    let mut live_events = Vec::new();
    for command in &commands {
        let events = live.decide(command);
        for event in &events {
            let sequence = live.last_applied_sequence() + 1;
            live.apply(event, sequence);
            assert_prefix_invariants(&live);
        }
        live_events.extend(events);
    }

    // The execute convenience must match the explicit decide/apply fold.
    let mut convenience = MatchingEngine::new(EngineConfig::default());
    for command in &commands {
        let _ = convenience.execute(command.clone());
    }
    let live_snapshot = live.capture_snapshot();
    assert_eq!(
        live_snapshot,
        convenience.capture_snapshot(),
        "decide/apply and the execute convenience must reach the same state"
    );

    // Replay the same events from an empty engine through the durable seam.
    let log = InMemoryEventLog::new();
    {
        let mut durable = DurableMatchingEngine::create(
            EngineConfig::default(),
            Box::new(log.handle()),
            Box::new(InMemorySnapshotStore::new()),
        )
        .expect("fresh event log");
        for command in &commands {
            durable.execute(command.clone()).expect("supported command");
        }
    }
    let reopened = DurableMatchingEngine::open(
        Box::new(log.handle()),
        Box::new(InMemorySnapshotStore::new()),
    )
    .expect("replay");

    assert_eq!(
        reopened.engine().capture_snapshot(),
        live_snapshot,
        "replay reproduces books, stops, ledger, trades, last prices and counters"
    );
    assert_eq!(reopened.engine().trades(), live.trades());
    assert_eq!(records(reopened.engine()), records(&live));
    assert_eq!(
        reopened.engine().last_trade_price(BTC),
        live.last_trade_price(BTC)
    );
    assert_eq!(
        reopened.engine().stop_orders(BTC, Side::Bid),
        live.stop_orders(BTC, Side::Bid)
    );
    assert_eq!(
        reopened.engine().stop_orders(BTC, Side::Ask),
        live.stop_orders(BTC, Side::Ask)
    );

    // The durable history is exactly the live event stream, in order.
    let history = reopened.history_from(1).unwrap();
    let history_events: Vec<DomainEvent> = history.iter().map(|r| r.event.clone()).collect();
    assert_eq!(history_events, live_events);
    assert_eq!(reopened.applied_seq(), live_events.len() as u64);
    assert_eq!(
        reopened.engine().last_applied_sequence(),
        live_events.len() as u64
    );

    // Each command's events share one command id, and ids are continuous.
    let mut first_by_command: Vec<u64> = Vec::new();
    for record in &history {
        if first_by_command.last() != Some(&record.command_id) {
            first_by_command.push(record.command_id);
        }
    }
    assert_eq!(
        first_by_command,
        (1..=commands.len() as u64).collect::<Vec<_>>()
    );

    // Counters continue after replay: the next stop gets the next order id.
    let mut reopened = reopened;
    let next_command = submit_stop(BTC, stop_market(Side::Bid, 120, 1, 20));
    let next_events = reopened.engine().decide(&next_command);
    match next_events.first() {
        Some(DomainEvent::StopPlaced { stop, .. }) => {
            assert_eq!(stop.id, live_snapshot.next_order_id)
        }
        other => panic!("expected StopPlaced, got {other:?}"),
    }
    reopened.execute(next_command).expect("append after replay");
    assert_eq!(
        reopened.engine().stop_orders(BTC, Side::Bid).len(),
        2,
        "the never-triggered stop and the new one are both pending"
    );
}
