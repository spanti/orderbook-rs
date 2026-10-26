//! Event-sourced modify and rejection (ticket 04).
//!
//! These tests exercise the event-sourced path only through public seams: the
//! pure `decide`, the sole state-write entry `apply`, and the append-before-
//! apply `DurableMatchingEngine`. Expected event streams are literals from the
//! ticket and the event catalog, never recomputed by the code under test.

use std::collections::HashMap;

use orderbook_rs::durable::{DurableMatchingEngine, InMemoryEventLog, InMemorySnapshotStore};
use orderbook_rs::engine::MatchingEngine;
use orderbook_rs::event::{DomainEvent, OrderCancelReason};
use orderbook_rs::snapshot::EngineSnapshot;
use orderbook_rs::types::{
    EngineCommand, EngineConfig, EngineError, ModifyRequest, Order, OrderBookId, OrderRecord,
    OrderRequest, OrderStatus, OrderType, Side, StopKind, StopOrderRequest, TimeInForce, Trade,
};

const BTC: &str = "BTC-USDT";
const ETH: &str = "ETH-USDT";

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

fn modify(
    symbol: &str,
    order_id: OrderBookId,
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

fn stop_market(side: Side, stop_price: u64, quantity: u64, timestamp: u64) -> StopOrderRequest {
    StopOrderRequest {
        side,
        stop_price,
        order_type: StopKind::Market,
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

fn accepted(symbol: &str, order_id: OrderBookId, request: &OrderRequest) -> DomainEvent {
    DomainEvent::OrderAccepted {
        symbol: symbol.to_string(),
        order: request.to_order(order_id),
        post_only: request.post_only,
    }
}

fn canceled_modified(symbol: &str, order_id: OrderBookId, canceled_quantity: u64) -> DomainEvent {
    DomainEvent::OrderCanceled {
        symbol: symbol.to_string(),
        order_id,
        canceled_quantity,
        reason: OrderCancelReason::Modified,
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

fn apply_all(engine: &mut MatchingEngine, events: &[DomainEvent]) {
    for event in events {
        let sequence = engine.last_applied_sequence() + 1;
        engine.apply(event, sequence);
    }
}

/// Decide and apply one command through the pure engine seam.
fn run(engine: &mut MatchingEngine, command: &EngineCommand) -> Vec<DomainEvent> {
    let events = engine.decide(command);
    apply_all(engine, &events);
    events
}

/// Seed a resting order through the pure decide/apply path.
fn rest(engine: &mut MatchingEngine, symbol: &str, request: OrderRequest) -> OrderBookId {
    let events = run(engine, &submit(symbol, request));
    match events.first() {
        Some(DomainEvent::OrderAccepted { order, .. }) => order.id,
        other => panic!("resting submit must start with OrderAccepted, got {other:?}"),
    }
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

    for (index, trade) in snapshot.trades.iter().enumerate() {
        assert_eq!(trade.id, index as u64 + 1, "trade ids must be contiguous");
    }

    for symbol in [BTC, ETH] {
        let expected = snapshot
            .trades
            .iter()
            .rev()
            .find(|trade| trade.symbol == symbol)
            .map(|trade| trade.price);
        assert_eq!(engine.last_trade_price(symbol), expected);
    }
}

// --------------------------------------------------------------------------
// Rejection: one OrderRejected per rejected command, no state change
// --------------------------------------------------------------------------

#[test]
fn a_rejected_submit_emits_one_order_rejected_with_the_full_command() {
    let engine = MatchingEngine::new(EngineConfig::default());
    let command = submit(BTC, limit(Side::Bid, 100, 0, 1));

    let events = engine.decide(&command);

    assert_eq!(
        events,
        vec![rejected(&command, EngineError::QuantityOutOfRange)],
        "a business rejection is an event, not an Err"
    );
    assert_eq!(
        engine.order_count(),
        0,
        "rejection creates no ledger record"
    );
    assert!(engine.book(BTC).is_none(), "rejection creates no book");

    // A rejection consumes no order id: the next accepted order is id 1.
    let events = engine.decide(&submit(BTC, limit(Side::Bid, 99, 1, 2)));
    match events.first() {
        Some(DomainEvent::OrderAccepted { order, .. }) => assert_eq!(order.id, 1),
        other => panic!("expected OrderAccepted, got {other:?}"),
    }
}

#[test]
fn a_rejected_stop_submit_emits_one_order_rejected_and_allocates_no_id() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    // Seed a last trade at 100 so a buy stop at 100 would trigger immediately.
    rest(&mut engine, BTC, limit(Side::Ask, 100, 1, 1));
    rest(&mut engine, BTC, limit(Side::Bid, 100, 1, 2));

    let command = submit_stop(BTC, stop_market(Side::Bid, 100, 1, 3));
    let events = engine.decide(&command);

    assert_eq!(
        events,
        vec![rejected(
            &command,
            EngineError::StopPriceWouldTriggerImmediately
        )]
    );
    assert_eq!(engine.order_count(), 2, "only the two seeds are recorded");

    // The next accepted command still gets order id 3.
    let events = engine.decide(&submit(BTC, limit(Side::Bid, 98, 1, 4)));
    match events.first() {
        Some(DomainEvent::OrderAccepted { order, .. }) => assert_eq!(order.id, 3),
        other => panic!("expected OrderAccepted, got {other:?}"),
    }
}

#[test]
fn a_rejected_cancel_emits_one_order_rejected_and_allocates_no_id() {
    let engine = MatchingEngine::new(EngineConfig::default());
    let command = cancel(BTC, 42);

    let events = engine.decide(&command);

    assert_eq!(events, vec![rejected(&command, EngineError::UnknownOrder)]);
    assert_eq!(engine.order_count(), 0);

    let events = engine.decide(&submit(BTC, limit(Side::Bid, 99, 1, 1)));
    match events.first() {
        Some(DomainEvent::OrderAccepted { order, .. }) => assert_eq!(order.id, 1),
        other => panic!("expected OrderAccepted, got {other:?}"),
    }
}

#[test]
fn a_rejected_modify_emits_one_order_rejected_for_every_business_reason() {
    let config = EngineConfig {
        min_order_quantity: 1,
        max_order_quantity: 10,
        tick_size: 5,
    };
    let mut engine = MatchingEngine::new(config);
    let id = rest(&mut engine, BTC, limit(Side::Bid, 100, 5, 1));

    for (command, reason) in [
        // Validation runs before any order lookup.
        (modify(BTC, id, 101, 5, 2), EngineError::PriceNotOnTick),
        (modify(BTC, id, 100, 11, 2), EngineError::QuantityOutOfRange),
        // Unknown order.
        (modify(BTC, 99, 100, 5, 2), EngineError::UnknownOrder),
        // Wrong symbol.
        (modify(ETH, id, 100, 5, 2), EngineError::UnknownOrder),
    ] {
        assert_eq!(
            engine.decide(&command),
            vec![rejected(&command, reason)],
            "command {command:?}"
        );
    }

    // An already-canceled order is rejected as unknown.
    run(&mut engine, &cancel(BTC, id));
    let command = modify(BTC, id, 100, 5, 3);
    assert_eq!(
        engine.decide(&command),
        vec![rejected(&command, EngineError::UnknownOrder)]
    );

    // A pending stop is not modifiable.
    let stop = match run(
        &mut engine,
        &submit_stop(BTC, stop_market(Side::Bid, 120, 1, 4)),
    )
    .first()
    {
        Some(DomainEvent::StopPlaced { stop, .. }) => stop.id,
        other => panic!("expected StopPlaced, got {other:?}"),
    };
    let command = modify(BTC, stop, 110, 1, 5);
    assert_eq!(
        engine.decide(&command),
        vec![rejected(&command, EngineError::StopOrderNotModifiable)]
    );

    // None of the rejections consumed an order id.
    let events = engine.decide(&submit(BTC, limit(Side::Bid, 95, 1, 6)));
    match events.first() {
        Some(DomainEvent::OrderAccepted { order, .. }) => assert_eq!(order.id, stop + 1),
        other => panic!("expected OrderAccepted, got {other:?}"),
    }
}

#[test]
fn rejected_commands_consume_a_sequence_and_command_id_but_replay_as_a_no_op() {
    let log = InMemoryEventLog::new();
    let mut engine = DurableMatchingEngine::create(
        EngineConfig::default(),
        Box::new(log.handle()),
        Box::new(InMemorySnapshotStore::new()),
    )
    .expect("fresh event log");

    // command 1: accepted + rested (events 1, 2)
    engine
        .execute(submit(BTC, limit(Side::Bid, 99, 5, 1)))
        .unwrap();
    // command 2: rejected cancel (event 3)
    let rejection = engine
        .execute(cancel(BTC, 42))
        .expect("a business rejection is a successful event-producing execute");
    assert_eq!(
        rejection,
        vec![rejected(&cancel(BTC, 42), EngineError::UnknownOrder)]
    );
    // command 3: accepted + rested (events 4, 5), order id 2
    let events = engine
        .execute(submit(BTC, limit(Side::Bid, 98, 1, 2)))
        .unwrap();
    match events.first() {
        Some(DomainEvent::OrderAccepted { order, .. }) => {
            assert_eq!(order.id, 2, "the rejection allocated no order id")
        }
        other => panic!("expected OrderAccepted, got {other:?}"),
    }

    let history = engine.history_from(1).unwrap();
    assert_eq!(
        history
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5]
    );
    assert_eq!(
        history
            .iter()
            .map(|record| record.command_id)
            .collect::<Vec<_>>(),
        vec![1, 1, 2, 3, 3]
    );
    assert!(matches!(
        history[2].event,
        DomainEvent::OrderRejected { .. }
    ));

    let before_replay = engine.engine().capture_snapshot();
    let applied = engine.applied_seq();
    let reopened = DurableMatchingEngine::open(
        Box::new(log.handle()),
        Box::new(InMemorySnapshotStore::new()),
    )
    .expect("replay");
    assert_eq!(
        reopened.engine().capture_snapshot(),
        before_replay,
        "replaying OrderRejected is a no-op"
    );
    assert_eq!(reopened.applied_seq(), applied);
}

// --------------------------------------------------------------------------
// Modify: cancel old, accept a new id, match, then rest or discard
// --------------------------------------------------------------------------

#[test]
fn modifying_a_resting_order_cancels_the_old_id_and_rests_a_new_one() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    let old = rest(&mut engine, BTC, limit(Side::Bid, 100, 5, 1));

    let command = modify(BTC, old, 98, 3, 2);
    let events = engine.decide(&command);

    let replacement = limit(Side::Bid, 98, 3, 2);
    assert_eq!(
        events,
        vec![
            canceled_modified(BTC, old, 5),
            accepted(BTC, 2, &replacement),
            DomainEvent::OrderRested {
                symbol: BTC.to_string(),
                order: replacement.to_order(2),
            },
        ],
        "modify is cancel-old + accept-new + rest"
    );

    apply_all(&mut engine, &events);

    let old_record = engine.order_record(old).expect("old order is recorded");
    assert_eq!(old_record.status, OrderStatus::Canceled);
    assert_eq!(old_record.remaining_quantity, 5);
    let new_record = engine.order_record(2).expect("new order is recorded");
    assert_eq!(new_record.status, OrderStatus::New);
    assert_eq!(new_record.original_quantity, 3);
    assert!(engine.is_open(2));
    assert!(!engine.is_open(old));
    let snapshot = engine.snapshot(BTC, 10).unwrap();
    assert_eq!(snapshot.bids, vec![(98, 3)]);
    assert_eq!(engine.capture_snapshot().next_order_id, 3);
}

#[test]
fn a_modify_that_crosses_emits_cancel_accept_and_the_trade() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    let maker = rest(&mut engine, BTC, limit(Side::Ask, 100, 5, 1));
    let old = rest(&mut engine, BTC, limit(Side::Bid, 99, 5, 2));

    let command = modify(BTC, old, 100, 5, 3);
    let events = engine.decide(&command);

    let replacement = limit(Side::Bid, 100, 5, 3);
    assert_eq!(
        events,
        vec![
            canceled_modified(BTC, old, 5),
            accepted(BTC, 3, &replacement),
            DomainEvent::TradeExecuted {
                trade: trade(1, maker, 3, 100, 5, Side::Bid, 3),
            },
        ],
        "a full fill has no trailing rest or cancel"
    );

    apply_all(&mut engine, &events);

    assert_eq!(engine.trades().len(), 1);
    assert_eq!(
        engine.order_record(maker).unwrap().status,
        OrderStatus::Filled
    );
    assert_eq!(
        engine.order_record(old).unwrap().status,
        OrderStatus::Canceled
    );
    assert_eq!(engine.order_record(3).unwrap().status, OrderStatus::Filled);
    assert!(engine.book(BTC).unwrap().best_bid().is_none());
    assert!(engine.book(BTC).unwrap().best_ask().is_none());
}

#[test]
fn modifying_a_partially_filled_order_cancels_the_book_remainder() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    let maker = rest(&mut engine, BTC, limit(Side::Ask, 100, 5, 1));
    // Partially fill the maker: 2 traded, 3 remain at 100.
    run(&mut engine, &submit(BTC, limit(Side::Bid, 100, 2, 2)));

    let replacement = limit(Side::Ask, 101, 3, 3);
    let command = modify(BTC, maker, 101, 3, 3);
    let events = engine.decide(&command);

    assert_eq!(
        events,
        vec![
            canceled_modified(BTC, maker, 3),
            accepted(BTC, 3, &replacement),
            DomainEvent::OrderRested {
                symbol: BTC.to_string(),
                order: replacement.to_order(3),
            },
        ],
        "the Modified cancel carries the remaining book quantity, not the original"
    );

    apply_all(&mut engine, &events);

    let old_record = engine.order_record(maker).unwrap();
    assert_eq!(old_record.status, OrderStatus::Canceled);
    assert_eq!(old_record.filled_quantity, 2);
    assert_eq!(old_record.remaining_quantity, 3);
    assert_eq!(engine.snapshot(BTC, 10).unwrap().asks, vec![(101, 3)]);
}

#[test]
fn a_partially_filled_modify_rests_its_remainder() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    let maker = rest(&mut engine, BTC, limit(Side::Ask, 100, 2, 1));
    let old = rest(&mut engine, BTC, limit(Side::Bid, 99, 5, 2));

    let command = modify(BTC, old, 100, 5, 3);
    let events = engine.decide(&command);

    let replacement = limit(Side::Bid, 100, 5, 3);
    let mut rested = replacement.to_order(3);
    rested.quantity = 3;
    assert_eq!(
        events,
        vec![
            canceled_modified(BTC, old, 5),
            accepted(BTC, 3, &replacement),
            DomainEvent::TradeExecuted {
                trade: trade(1, maker, 3, 100, 2, Side::Bid, 3),
            },
            DomainEvent::OrderRested {
                symbol: BTC.to_string(),
                order: rested,
            },
        ]
    );

    apply_all(&mut engine, &events);

    let record = engine.order_record(3).unwrap();
    assert_eq!(record.status, OrderStatus::PartiallyFilled);
    assert_eq!(record.filled_quantity, 2);
    assert_eq!(record.remaining_quantity, 3);
    assert_eq!(engine.snapshot(BTC, 10).unwrap().bids, vec![(100, 3)]);
}

#[test]
fn a_modify_runs_the_stop_cascade_after_the_replacement_submit() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    // Establish last price 100.
    rest(&mut engine, BTC, limit(Side::Ask, 100, 1, 1));
    rest(&mut engine, BTC, limit(Side::Bid, 100, 1, 2));
    // A sell stop at 95 triggers once the price trades there.
    let stop = match run(
        &mut engine,
        &submit_stop(BTC, stop_market(Side::Ask, 95, 1, 3)),
    )
    .first()
    {
        Some(DomainEvent::StopPlaced { stop, .. }) => stop.id,
        other => panic!("expected StopPlaced, got {other:?}"),
    };
    // Liquidity for both the replacement and the triggered stop.
    let bid = rest(&mut engine, BTC, limit(Side::Bid, 95, 5, 4));
    let old = rest(&mut engine, BTC, limit(Side::Ask, 101, 1, 5));

    let command = modify(BTC, old, 95, 1, 6);
    let events = engine.decide(&command);

    let replacement = limit(Side::Ask, 95, 1, 6);
    assert_eq!(
        events,
        vec![
            canceled_modified(BTC, old, 1),
            accepted(BTC, 6, &replacement),
            DomainEvent::TradeExecuted {
                trade: trade(2, bid, 6, 95, 1, Side::Ask, 6),
            },
            DomainEvent::StopTriggered {
                symbol: BTC.to_string(),
                order: Order {
                    id: stop,
                    side: Side::Ask,
                    price: 0,
                    quantity: 1,
                    timestamp: 6,
                    order_type: OrderType::Market,
                    time_in_force: TimeInForce::IOC,
                },
                last_price: 95,
            },
            DomainEvent::TradeExecuted {
                trade: trade(3, bid, stop, 95, 1, Side::Ask, 6),
            },
        ],
        "the replacement's trade triggers the pending stop in the same stream"
    );

    apply_all(&mut engine, &events);

    assert!(engine.stop_order(stop).is_none(), "the stop triggered");
    assert_eq!(
        engine.order_record(stop).unwrap().status,
        OrderStatus::Filled
    );
    assert_eq!(engine.snapshot(BTC, 10).unwrap().bids, vec![(95, 3)]);
    assert_eq!(engine.last_trade_price(BTC), Some(95));
}

// --------------------------------------------------------------------------
// Replay equality, prefix validity, and decide/apply versus execute equality
// --------------------------------------------------------------------------

fn modify_and_rejection_commands() -> Vec<EngineCommand> {
    vec![
        // Seed asks and bids; the crossing pair establishes a last price.
        submit(BTC, limit(Side::Ask, 100, 2, 1)),
        submit(BTC, limit(Side::Bid, 99, 5, 2)),
        // A rejected submit consumes a sequence, no order id.
        submit(BTC, limit(Side::Bid, 98, 0, 3)),
        // A modify that crosses and fully fills.
        modify(BTC, 2, 100, 5, 4),
        // A resting modify that creates id 3.
        submit(BTC, limit(Side::Bid, 97, 4, 5)),
        modify(BTC, 3, 96, 4, 6),
        // Rejected modify (unknown id) and rejected cancel.
        modify(BTC, 40, 96, 1, 7),
        cancel(BTC, 41),
        // A pending stop, a rejected immediate-trigger stop, and a valid cancel.
        submit_stop(BTC, stop_market(Side::Bid, 120, 1, 8)),
        submit_stop(BTC, stop_market(Side::Bid, 100, 1, 9)),
        cancel(BTC, 4),
        // A stop liquidation that cascades from a submit.
        submit_stop(BTC, stop_market(Side::Ask, 99, 1, 10)),
        submit(BTC, limit(Side::Bid, 99, 1, 11)),
    ]
}

#[test]
fn every_prefix_is_valid_and_replay_equals_live_execution() {
    let commands = modify_and_rejection_commands();

    let mut live = MatchingEngine::new(EngineConfig::default());
    let mut live_events = Vec::new();
    for command in &commands {
        let events = live.decide(command);
        assert!(
            !events.is_empty(),
            "every command produces at least one event"
        );
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
            durable
                .execute(command.clone())
                .expect("event-producing execute");
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

    let history = reopened.history_from(1).unwrap();
    let history_events: Vec<DomainEvent> = history.iter().map(|r| r.event.clone()).collect();
    assert_eq!(history_events, live_events);
    assert_eq!(reopened.applied_seq(), live_events.len() as u64);

    // Every command still owns exactly one contiguous command id.
    let mut command_ids: Vec<u64> = Vec::new();
    for record in &history {
        if command_ids.last() != Some(&record.command_id) {
            command_ids.push(record.command_id);
        }
    }
    assert_eq!(command_ids, (1..=commands.len() as u64).collect::<Vec<_>>());

    // All four command variants have an event stream; rejections are journaled.
    assert!(
        history_events
            .iter()
            .any(|event| matches!(event, DomainEvent::OrderRejected { .. }))
    );
    assert!(history_events.iter().any(|event| matches!(
        event,
        DomainEvent::OrderCanceled {
            reason: OrderCancelReason::Modified,
            ..
        }
    )));
    assert!(
        history_events
            .iter()
            .any(|event| matches!(event, DomainEvent::StopPlaced { .. }))
    );
}
