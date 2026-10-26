//! Event-sourced resting-submit tracer (ticket 01).
//!
//! These tests exercise the new path only through public seams: the pure
//! `decide`, the sole state-write entry `apply`, and the append-before-apply
//! `EventSourcedEngine`. The existing command-sourced path keeps its own tests.

use orderbook_rs::durable::{
    DurableError, EVENT_FORMAT_VERSION, EventLog, EventRecord, EventSourcedEngine,
    InMemoryEventLog, InMemoryEventSnapshotStore, LogHeader,
};
use orderbook_rs::engine::MatchingEngine;
use orderbook_rs::event::DomainEvent;
use orderbook_rs::types::{
    EngineCommand, EngineConfig, EngineError, ModifyRequest, OrderBookId, OrderRequest,
    OrderStatus, OrderType, Side, StopKind, StopOrderRequest, TimeInForce,
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

fn order_id(events: &[DomainEvent]) -> OrderBookId {
    match events.first().expect("decision emits at least one event") {
        DomainEvent::OrderAccepted { order, .. } => order.id,
        DomainEvent::OrderRested { order, .. } => order.id,
        DomainEvent::OrderCanceled { order_id, .. } => *order_id,
        DomainEvent::StopPlaced { stop, .. } => stop.id,
        DomainEvent::StopTriggered { order, .. } => order.id,
        DomainEvent::StopCanceled { order_id, .. } => *order_id,
        DomainEvent::TradeExecuted { .. } => {
            panic!("a decision starts with OrderAccepted or StopPlaced, not TradeExecuted")
        }
        DomainEvent::OrderRejected { .. } => {
            panic!("a rejected decision has no order id")
        }
    }
}

#[test]
fn decide_returns_accepted_then_rested_for_a_non_crossing_limit_submit() {
    let engine = MatchingEngine::new(EngineConfig::default());
    let command = submit(BTC, limit(Side::Bid, 99, 5, 1));

    let events = engine.decide(&command);

    assert_eq!(events.len(), 2);
    match &events[0] {
        DomainEvent::OrderAccepted {
            symbol,
            order,
            post_only,
        } => {
            assert_eq!(symbol, BTC);
            assert_eq!(order.id, 1);
            assert_eq!(order.side, Side::Bid);
            assert_eq!(order.price, 99);
            assert_eq!(order.quantity, 5);
            assert!(!post_only);
        }
        other => panic!("expected OrderAccepted, got {other:?}"),
    }
    match &events[1] {
        DomainEvent::OrderRested { symbol, order } => {
            assert_eq!(symbol, BTC);
            assert_eq!(order.id, 1);
            assert_eq!(
                order.quantity, 5,
                "nothing filled, so the whole order rests"
            );
        }
        other => panic!("expected OrderRested, got {other:?}"),
    }

    // `decide` is pure: no book, ledger, counter or sequence mutation.
    assert_eq!(engine.last_sequence(), 0);
    assert_eq!(engine.order_count(), 0);
    assert!(engine.book(BTC).is_none());
}

#[test]
fn order_accepted_carries_the_post_only_flag_of_a_non_crossing_submit() {
    let engine = MatchingEngine::new(EngineConfig::default());
    let mut request = limit(Side::Bid, 99, 2, 1);
    request.post_only = true;

    let events = engine.decide(&submit(BTC, request));

    match &events[0] {
        DomainEvent::OrderAccepted { post_only, .. } => assert!(*post_only),
        other => panic!("expected OrderAccepted, got {other:?}"),
    }
}

#[test]
fn applying_either_prefix_of_the_decision_leaves_a_valid_engine_state() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    let command = submit(BTC, limit(Side::Bid, 99, 5, 1));
    let events = engine.decide(&command);

    // Prefix 1: OrderAccepted creates the ledger record but does not touch the book.
    engine.apply(&events[0], 1);
    assert_eq!(engine.order_count(), 1);
    assert!(
        engine.book(BTC).is_none(),
        "accepted must not insert into the book"
    );
    let record = engine
        .order_record(1)
        .expect("accepted order has a ledger record");
    assert_eq!(record.status, OrderStatus::New);
    assert_eq!(record.original_quantity, 5);
    assert_eq!(record.filled_quantity, 0);
    assert_eq!(record.remaining_quantity, 5);

    // Prefix 2: OrderRested inserts the order and advances the applied sequence.
    engine.apply(&events[1], 2);
    assert_eq!(engine.last_sequence(), 2);
    let snapshot = engine.snapshot(BTC, 10).expect("book exists after rest");
    assert_eq!(
        snapshot.sequence, 2,
        "BookSnapshot.sequence is the applied event sequence"
    );
    assert_eq!(snapshot.bids, vec![(99, 5)]);
    assert!(snapshot.asks.is_empty());
    assert!(engine.is_open(1));
}

#[test]
fn decide_allocates_the_order_id_without_mutating_counters() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    let first = submit(BTC, limit(Side::Bid, 99, 1, 1));

    let first_decision = engine.decide(&first);
    let repeated_decision = engine.decide(&first);
    assert_eq!(order_id(&first_decision), 1);
    assert_eq!(
        order_id(&repeated_decision),
        1,
        "deciding twice must not consume an id"
    );

    for (index, event) in first_decision.iter().enumerate() {
        engine.apply(event, index as u64 + 1);
    }

    let second = submit(BTC, limit(Side::Bid, 98, 1, 2));
    let second_decision = engine.decide(&second);
    assert_eq!(
        order_id(&second_decision),
        2,
        "apply advanced the order counter"
    );
}

#[test]
fn event_records_carry_format_version_contiguous_sequence_and_one_command_id() {
    let log = InMemoryEventLog::new();
    let mut engine = EventSourcedEngine::create(
        EngineConfig::default(),
        Box::new(log.handle()),
        Box::new(InMemoryEventSnapshotStore::new()),
    )
        .expect("fresh event log");

    engine
        .execute(submit(BTC, limit(Side::Bid, 99, 5, 1)))
        .expect("resting submit is supported");
    engine
        .execute(submit(BTC, limit(Side::Bid, 98, 3, 2)))
        .expect("resting submit is supported");

    let history = engine.history_from(1).expect("history is readable");
    assert_eq!(history.len(), 4);
    let sequences: Vec<u64> = history.iter().map(|record| record.sequence).collect();
    assert_eq!(sequences, vec![1, 2, 3, 4], "event sequence is contiguous");
    assert!(
        history
            .iter()
            .all(|record| record.format_version == EVENT_FORMAT_VERSION)
    );
    assert_eq!(
        EVENT_FORMAT_VERSION, 2,
        "the event format is distinct from command v1"
    );

    // Each command's two events share one command id, and ids are continuous.
    assert_eq!(history[0].command_id, history[1].command_id);
    assert_eq!(history[2].command_id, history[3].command_id);
    assert_ne!(history[0].command_id, history[2].command_id);
    assert_eq!(history[1].command_id + 1, history[2].command_id);
    assert_eq!(engine.applied_seq(), 4);
    assert_eq!(engine.engine().last_applied_sequence(), 4);
}

#[test]
fn reopening_from_the_in_memory_event_log_reproduces_decision_state() {
    let log = InMemoryEventLog::new();
    let expected = {
        let mut engine =
            EventSourcedEngine::create(
        EngineConfig::default(),
        Box::new(log.handle()),
        Box::new(InMemoryEventSnapshotStore::new()),
    )
                .expect("fresh event log");
        // Same price level on BTC proves FIFO priority survives; ETH proves
        // per-symbol isolation survives.
        engine
            .execute(submit(BTC, limit(Side::Bid, 99, 5, 1)))
            .unwrap();
        engine
            .execute(submit(BTC, limit(Side::Bid, 99, 3, 2)))
            .unwrap();
        engine
            .execute(submit("ETH-USDT", limit(Side::Ask, 200, 4, 3)))
            .unwrap();
        engine.engine().capture_snapshot()
    };

    let mut reopened = EventSourcedEngine::open(
        Box::new(log.handle()),
        Box::new(InMemoryEventSnapshotStore::new()),
    )
        .expect("recovery folds the event log alone");

    assert_eq!(reopened.engine().capture_snapshot(), expected);
    assert_eq!(reopened.applied_seq(), 6);
    assert_eq!(reopened.engine().last_applied_sequence(), 6);
    assert!(reopened.engine().trades().is_empty());
    assert_eq!(reopened.engine().last_trade_price(BTC), None);
    assert_eq!(reopened.engine().order_count(), 3);

    // FIFO priority: ids 1 then 2 at the same level, in insertion order.
    let snapshot = reopened.engine().capture_snapshot();
    let btc = snapshot
        .books
        .iter()
        .find(|book| book.symbol == BTC)
        .expect("BTC book exists");
    let level = btc
        .bids
        .iter()
        .find(|level| level.price == 99)
        .expect("level 99 exists");
    assert_eq!(
        level
            .orders
            .iter()
            .map(|order| order.id)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );

    // Counters continue: the next decision allocates id 4.
    let command = submit(BTC, limit(Side::Bid, 98, 1, 4));
    let next = reopened.engine().decide(&command);
    assert_eq!(order_id(&next), 4);

    // Sequence and command id continue across the restart too, not just ids.
    reopened.execute(command).expect("append after recovery");
    let history = reopened.history_from(7).expect("tail after recovery");
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].sequence, 7);
    assert_eq!(history[1].sequence, 8);
    assert_eq!(history[0].command_id, 4);
    assert_eq!(reopened.applied_seq(), 8);
    assert_eq!(
        reopened.engine().snapshot(BTC, 10).unwrap().sequence,
        8,
        "BookSnapshot.sequence follows the applied event sequence"
    );
}

/// An event log that accepts its header but refuses every batch append.
struct FailingEventLog {
    inner: InMemoryEventLog,
}

impl FailingEventLog {
    fn new(inner: InMemoryEventLog) -> Self {
        Self { inner }
    }
}

impl EventLog for FailingEventLog {
    fn read_header(&self) -> Result<Option<LogHeader>, DurableError> {
        self.inner.read_header()
    }

    fn write_header(&mut self, header: LogHeader) -> Result<(), DurableError> {
        self.inner.write_header(header)
    }

    fn append(&mut self, record: EventRecord) -> Result<(), DurableError> {
        self.inner.append(record)
    }

    fn append_batch(&mut self, _records: Vec<EventRecord>) -> Result<(), DurableError> {
        Err(DurableError::AppendFailed("injected append failure".into()))
    }

    fn read_from(&self, from: u64) -> Result<Vec<EventRecord>, DurableError> {
        self.inner.read_from(from)
    }

    fn first_sequence(&self) -> Option<u64> {
        self.inner.first_sequence()
    }

    fn last_sequence(&self) -> Option<u64> {
        self.inner.last_sequence()
    }

    fn last_command_id(&self) -> Option<u64> {
        self.inner.last_command_id()
    }
}

#[test]
fn a_failed_append_applies_nothing_and_fails_the_instance_closed() {
    let log = InMemoryEventLog::new();
    let mut engine = EventSourcedEngine::create(
        EngineConfig::default(),
        Box::new(FailingEventLog::new(log.handle())),
        Box::new(InMemoryEventSnapshotStore::new()),
    )
    .expect("header write succeeds");

    let error = engine
        .execute(submit(BTC, limit(Side::Bid, 99, 5, 1)))
        .expect_err("the injected append must fail");
    assert!(matches!(error, DurableError::AppendFailed(_)));

    // Append-before-apply: nothing reached the engine.
    assert_eq!(engine.engine().last_sequence(), 0);
    assert_eq!(engine.engine().order_count(), 0);
    assert!(engine.engine().book(BTC).is_none());
    assert_eq!(engine.applied_seq(), 0);
    assert!(engine.history_from(1).unwrap().is_empty());
    assert!(engine.is_failed());

    // A failed instance accepts no further commands.
    let error = engine
        .execute(submit(BTC, limit(Side::Bid, 98, 1, 2)))
        .expect_err("a failed instance is closed");
    assert!(matches!(error, DurableError::EngineFailed));
    assert_eq!(engine.engine().last_sequence(), 0);
}

#[test]
fn rejected_commands_are_journaled_as_events_and_change_no_state() {
    let log = InMemoryEventLog::new();
    let mut engine = EventSourcedEngine::create(
        EngineConfig::default(),
        Box::new(log.handle()),
        Box::new(InMemoryEventSnapshotStore::new()),
    )
        .expect("fresh event log");

    let modify = EngineCommand::ModifyOrder {
        symbol: BTC.to_string(),
        order_id: 1,
        request: ModifyRequest {
            price: 100,
            quantity: 1,
            timestamp: 1,
        },
    };
    let events = engine
        .execute(modify.clone())
        .expect("a business rejection is an event-producing execute");
    assert_eq!(events, vec![rejected(&modify, EngineError::UnknownOrder)]);
    assert!(
        !engine.is_failed(),
        "a client error does not fail the instance"
    );
    assert_eq!(engine.applied_seq(), 1);
    assert_eq!(engine.history_from(1).unwrap().len(), 1);

    // An unknown cancel is rejected the same way: one journaled event.
    let cancel = EngineCommand::CancelOrder {
        symbol: BTC.to_string(),
        order_id: 1,
    };
    let events = engine
        .execute(cancel.clone())
        .expect("a business rejection is an event-producing execute");
    assert_eq!(events, vec![rejected(&cancel, EngineError::UnknownOrder)]);
    assert_eq!(engine.applied_seq(), 2);
    assert_eq!(engine.history_from(1).unwrap().len(), 2);
    assert_eq!(engine.engine().order_count(), 0);

    // Ticket 02 makes crossing submits supported: accepted + one trade.
    engine
        .execute(submit(BTC, limit(Side::Ask, 100, 5, 1)))
        .unwrap();
    engine
        .execute(submit(BTC, limit(Side::Bid, 100, 1, 2)))
        .unwrap();
    assert_eq!(
        engine.history_from(1).unwrap().len(),
        6,
        "two rejections, then accepted+rested and accepted+trade"
    );
    assert_eq!(engine.applied_seq(), 6);

    // Ticket 03 makes stop submissions supported: exactly one StopPlaced.
    engine
        .execute(EngineCommand::SubmitStopOrder {
            symbol: BTC.to_string(),
            request: StopOrderRequest {
                side: Side::Bid,
                stop_price: 110,
                order_type: StopKind::Market,
                quantity: 1,
                timestamp: 3,
            },
        })
        .expect("stop submission is supported from ticket 03");
    assert_eq!(engine.history_from(7).unwrap().len(), 1);
    assert_eq!(engine.applied_seq(), 7);
}
