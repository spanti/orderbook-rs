//! End-to-end contracts of the matching runtime: apply-first event
//! durability, the applied-vs-durable crash window, exact event replay,
//! concurrent producers, backpressure, failure semantics, metrics, and
//! snapshot gating.
//!
//! These tests sit on the public `EngineRuntime` / `EngineHandle` seam. The one
//! exception is the matching-thread panic test, which uses the hidden
//! `start_with_matching_panic_for_test` constructor because a matching-thread
//! panic is not reachable through valid commands.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use orderbook_rs::durable::{
    DurableError, EventLog, EventRecord, FORMAT_VERSION, InMemoryEventLog, InMemorySnapshotStore,
    LogHeader, SnapshotStore, StoredSnapshot,
};
use orderbook_rs::engine::MatchingEngine;
use orderbook_rs::event::DomainEvent;
use orderbook_rs::runtime::{
    CommandReceipt, EngineHandle, EngineRuntime, RuntimeConfig, RuntimeError, RuntimeMetrics,
    SubmitError, Ticket, Worker, enqueued_at_for_test, start_with_matching_panic_for_test,
};
use orderbook_rs::types::{
    EngineCommand, EngineConfig, ModifyRequest, OrderRequest, OrderType, Side, StopKind,
    StopOrderRequest, TimeInForce,
};

const WAIT: Duration = Duration::from_secs(10);
const SHORT: Duration = Duration::from_millis(150);
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

/// A single-producer workload that reaches accepted, filled, rested, modified,
/// canceled, stop-placed, stop-canceled, and rejected commands.
fn mixed_commands() -> Vec<EngineCommand> {
    vec![
        EngineCommand::SubmitOrder {
            symbol: BTC.to_string(),
            request: limit(Side::Ask, 100, 5, 1),
        },
        EngineCommand::SubmitOrder {
            symbol: BTC.to_string(),
            request: limit(Side::Bid, 100, 7, 2),
        },
        EngineCommand::SubmitOrder {
            symbol: BTC.to_string(),
            request: limit(Side::Bid, 99, 2, 3),
        },
        EngineCommand::ModifyOrder {
            symbol: BTC.to_string(),
            order_id: 2,
            request: ModifyRequest {
                price: 99,
                quantity: 3,
                timestamp: 4,
            },
        },
        EngineCommand::CancelOrder {
            symbol: BTC.to_string(),
            order_id: 3,
        },
        EngineCommand::SubmitStopOrder {
            symbol: BTC.to_string(),
            request: StopOrderRequest {
                side: Side::Bid,
                stop_price: 101,
                order_type: StopKind::Market,
                quantity: 3,
                timestamp: 5,
            },
        },
        EngineCommand::CancelOrder {
            symbol: BTC.to_string(),
            order_id: 5,
        },
        EngineCommand::CancelOrder {
            symbol: BTC.to_string(),
            order_id: 999,
        },
        EngineCommand::SubmitOrder {
            symbol: ETH.to_string(),
            request: limit(Side::Ask, 201, 1, 6),
        },
    ]
}

fn submit_order(timestamp: u64) -> EngineCommand {
    EngineCommand::SubmitOrder {
        symbol: "BTC-USDT".to_string(),
        request: OrderRequest {
            side: Side::Bid,
            price: 100,
            quantity: 1,
            timestamp,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::GTC,
            post_only: false,
        },
    }
}

fn cancel_order(order_id: u64) -> EngineCommand {
    EngineCommand::CancelOrder {
        symbol: "BTC-USDT".to_string(),
        order_id,
    }
}

/// Decide and apply `commands` directly, producing the receipts the runtime
/// must match. Event sequences and `command_id`s are assigned exactly as the
/// matching thread assigns them.
fn direct_receipts(commands: &[EngineCommand]) -> (MatchingEngine, Vec<CommandReceipt>) {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    let mut next_sequence = 1u64;
    let receipts = commands
        .iter()
        .map(|command| {
            let events = engine.decide(command);
            let first_event_sequence = next_sequence;
            for (offset, event) in events.iter().enumerate() {
                engine.apply(event, first_event_sequence + offset as u64);
            }
            let last_event_sequence = first_event_sequence + events.len() as u64 - 1;
            next_sequence = last_event_sequence + 1;
            CommandReceipt {
                events,
                first_event_sequence,
                last_event_sequence,
            }
        })
        .collect();
    (engine, receipts)
}

fn wait_until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !condition() {
        assert!(Instant::now() < deadline, "condition was not met in time");
        thread::sleep(Duration::from_millis(1));
    }
}

/// A gate that blocks the journal thread until the test opens it.
#[derive(Clone, Default)]
struct Gate {
    open: Arc<Mutex<bool>>,
    ready: Arc<Condvar>,
}

impl Gate {
    fn wait(&self) {
        let mut open = self.open.lock().unwrap_or_else(|p| p.into_inner());
        while !*open {
            open = self.ready.wait(open).unwrap_or_else(|p| p.into_inner());
        }
    }

    fn open(&self) {
        *self.open.lock().unwrap_or_else(|p| p.into_inner()) = true;
        self.ready.notify_all();
    }
}

/// An in-memory log whose `append_batch` blocks until the gate opens.
#[derive(Clone)]
struct BlockingLog {
    inner: InMemoryEventLog,
    gate: Gate,
    entered: Arc<AtomicBool>,
}

impl BlockingLog {
    fn new(inner: InMemoryEventLog, gate: Gate) -> Self {
        Self {
            inner,
            gate,
            entered: Arc::new(AtomicBool::new(false)),
        }
    }

    fn entered(&self) -> bool {
        self.entered.load(Ordering::Acquire)
    }
}

impl EventLog for BlockingLog {
    fn read_header(&self) -> Result<Option<LogHeader>, DurableError> {
        self.inner.read_header()
    }

    fn write_header(&mut self, header: LogHeader) -> Result<(), DurableError> {
        self.inner.write_header(header)
    }

    fn append(&mut self, record: EventRecord) -> Result<(), DurableError> {
        self.inner.append(record)
    }

    fn append_batch(&mut self, records: Vec<EventRecord>) -> Result<(), DurableError> {
        self.entered.store(true, Ordering::Release);
        self.gate.wait();
        self.inner.append_batch(records)
    }

    fn read_from(&self, from: u64) -> Result<Vec<EventRecord>, DurableError> {
        self.inner.read_from(from)
    }

    fn first_sequence(&self) -> Option<u64> {
        self.inner.first_sequence()
    }

    fn last_stored_sequence(&self) -> Option<u64> {
        self.inner.last_stored_sequence()
    }

    fn last_command_id(&self) -> Option<u64> {
        self.inner.last_command_id()
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum JournalFault {
    AppendError,
    Panic,
}

/// A log that blocks its first append until the gate opens, then either returns
/// a storage error or panics on the journal thread.
#[derive(Clone)]
struct FaultingLog {
    gate: Gate,
    entered: Arc<AtomicBool>,
    fault: JournalFault,
}

impl FaultingLog {
    fn new(fault: JournalFault, gate: Gate) -> Self {
        Self {
            gate,
            entered: Arc::new(AtomicBool::new(false)),
            fault,
        }
    }

    fn entered(&self) -> bool {
        self.entered.load(Ordering::Acquire)
    }

    fn fail<T>(&self) -> Result<T, DurableError> {
        match self.fault {
            JournalFault::AppendError => Err(DurableError::AppendFailed("disk full".to_string())),
            JournalFault::Panic => panic!("journal log panic"),
        }
    }
}

impl EventLog for FaultingLog {
    fn read_header(&self) -> Result<Option<LogHeader>, DurableError> {
        Ok(None)
    }

    fn write_header(&mut self, _header: LogHeader) -> Result<(), DurableError> {
        Ok(())
    }

    fn append(&mut self, _record: EventRecord) -> Result<(), DurableError> {
        self.fail()
    }

    fn append_batch(&mut self, _records: Vec<EventRecord>) -> Result<(), DurableError> {
        self.entered.store(true, Ordering::Release);
        self.gate.wait();
        self.fail()
    }

    fn read_from(&self, _from: u64) -> Result<Vec<EventRecord>, DurableError> {
        Ok(Vec::new())
    }

    fn first_sequence(&self) -> Option<u64> {
        None
    }

    fn last_stored_sequence(&self) -> Option<u64> {
        None
    }

    fn last_command_id(&self) -> Option<u64> {
        None
    }
}

/// A snapshot store that records `(applied_seq, log_last_seq)` for every save.
///
/// The log tail is the durable evidence at save time; a snapshot must never
/// cover an event sequence the log does not already hold.
#[derive(Clone)]
struct GatingSnapshotStore {
    log: InMemoryEventLog,
    inner: InMemorySnapshotStore,
    seen: Arc<Mutex<Vec<(u64, u64)>>>,
}

impl GatingSnapshotStore {
    fn new(log: InMemoryEventLog) -> Self {
        Self {
            log,
            inner: InMemorySnapshotStore::new(),
            seen: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn handle(&self) -> Self {
        self.clone()
    }

    fn seen(&self) -> Vec<(u64, u64)> {
        self.seen
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

impl SnapshotStore for GatingSnapshotStore {
    fn save(&mut self, snapshot: StoredSnapshot) -> Result<(), DurableError> {
        let log_last = self.log.last_stored_sequence().unwrap_or(0);
        self.seen
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push((snapshot.applied_seq, log_last));
        self.inner.save(snapshot)
    }

    fn load_latest(&self) -> Result<Option<StoredSnapshot>, DurableError> {
        self.inner.load_latest()
    }

    fn load_previous(&self) -> Result<Option<StoredSnapshot>, DurableError> {
        self.inner.load_previous()
    }
}

/// Copy only the durable state of `source`: its header and records.
///
/// Used to simulate a process crash at a point where an applied command has
/// not reached the log yet.
fn clone_log(source: &InMemoryEventLog) -> InMemoryEventLog {
    let header = source
        .read_header()
        .expect("readable source header")
        .expect("source has a header");
    let records = source.read_from(1).expect("readable source records");
    let copy = InMemoryEventLog::new();
    let mut writer = copy.handle();
    writer.write_header(header).expect("copy the header");
    for record in records {
        writer.append(record).expect("copy every record");
    }
    copy
}

#[test]
fn single_producer_runtime_matches_direct_execution() {
    let commands = mixed_commands();
    let (direct, expected_receipts) = direct_receipts(&commands);
    let total_events = expected_receipts
        .last()
        .expect("the workload is non-empty")
        .last_event_sequence;

    let log = InMemoryEventLog::new();
    let snapshots = InMemorySnapshotStore::new();
    let (handle, runtime) = EngineRuntime::create(
        RuntimeConfig::default(),
        EngineConfig::default(),
        Box::new(log.handle()),
        Box::new(snapshots.handle()),
    )
    .expect("create the runtime");

    let mut receipts = Vec::with_capacity(commands.len());
    for command in &commands {
        receipts.push(
            handle
                .submit_and_wait(command.clone())
                .expect("durable submission"),
        );
    }
    assert_eq!(
        receipts, expected_receipts,
        "runtime and direct event decisions agree"
    );

    runtime.shutdown().expect("shutdown");
    let stored = snapshots
        .load_latest()
        .expect("snapshot store readable")
        .expect("shutdown writes a final snapshot");
    assert_eq!(stored.applied_seq, total_events);
    assert_eq!(
        stored.last_command_id,
        commands.len() as u64,
        "the final snapshot records the last command id"
    );
    assert_eq!(
        stored.snapshot,
        direct.capture_snapshot(),
        "runtime and direct execution reach the same decision state"
    );

    let records = log.read_from(1).expect("read the event log");
    let expected_events: Vec<DomainEvent> = expected_receipts
        .iter()
        .flat_map(|receipt| receipt.events.iter().cloned())
        .collect();
    assert_eq!(records.len(), expected_events.len());
    for (index, (record, event)) in records.iter().zip(&expected_events).enumerate() {
        assert_eq!(record.format_version, FORMAT_VERSION);
        assert_eq!(record.sequence, index as u64 + 1);
        assert_eq!(&record.event, event);
    }
    assert_eq!(log.last_stored_sequence(), Some(total_events));
}

#[test]
fn end_to_end_commands_recover_from_the_event_log() {
    let commands = mixed_commands();
    let log = InMemoryEventLog::new();
    let snapshots = InMemorySnapshotStore::new();
    let (handle, runtime) = EngineRuntime::create(
        RuntimeConfig::default(),
        EngineConfig::default(),
        Box::new(log.handle()),
        Box::new(snapshots.handle()),
    )
    .expect("create the runtime");

    let mut receipts = Vec::new();
    for command in &commands {
        receipts.push(
            handle
                .submit_and_wait(command.clone())
                .expect("durable submission"),
        );
    }

    // Every command reached the log as a contiguous event range.
    let records = log.read_from(1).expect("read the event log");
    let mut next = 1u64;
    for receipt in &receipts {
        assert_eq!(receipt.first_event_sequence, next);
        assert_eq!(
            receipt.last_event_sequence - receipt.first_event_sequence + 1,
            receipt.events.len() as u64
        );
        next = receipt.last_event_sequence + 1;
    }
    assert_eq!(next - 1, records.len() as u64);

    // The event stream covers the whole command catalog this ticket exercises.
    let has = |predicate: fn(&DomainEvent) -> bool| records.iter().any(|r| predicate(&r.event));
    assert!(has(|event| matches!(
        event,
        DomainEvent::OrderAccepted { .. }
    )));
    assert!(has(|event| matches!(
        event,
        DomainEvent::OrderRested { .. }
    )));
    assert!(has(|event| matches!(
        event,
        DomainEvent::TradeExecuted { .. }
    )));
    assert!(has(|event| matches!(
        event,
        DomainEvent::OrderCanceled { .. }
    )));
    assert!(has(|event| matches!(event, DomainEvent::StopPlaced { .. })));
    assert!(has(|event| matches!(
        event,
        DomainEvent::StopCanceled { .. }
    )));
    assert!(has(|event| matches!(
        event,
        DomainEvent::OrderRejected { .. }
    )));

    // Drop without a final snapshot: recovery must come from the event log.
    drop(runtime);
    assert!(
        snapshots.load_latest().unwrap().is_none(),
        "Drop does not write a snapshot"
    );

    let recovered_snapshots = InMemorySnapshotStore::new();
    let (recovered, recovered_runtime) = EngineRuntime::open(
        RuntimeConfig::default(),
        Box::new(log.handle()),
        Box::new(recovered_snapshots.handle()),
    )
    .expect("recover from the event log");

    let last_stored = log.last_stored_sequence().expect("the log has records");
    let cancel = cancel_order(4);
    let after = recovered
        .submit_and_wait(cancel.clone())
        .expect("continue after recovery");
    assert_eq!(
        after.first_event_sequence,
        last_stored + 1,
        "recovery continues the global event sequence"
    );
    assert!(
        matches!(
            after.events[0],
            DomainEvent::OrderCanceled {
                order_id: 4,
                reason: orderbook_rs::event::OrderCancelReason::UserCancel,
                ..
            }
        ),
        "the recovered engine retained the resting order"
    );

    // Folding the log from empty plus the post-recovery command must match the
    // recovered runtime's final decision state.
    let mut expected = MatchingEngine::new(EngineConfig::default());
    for record in &records {
        expected.apply(&record.event, record.sequence);
    }
    let recovery_events = expected.decide(&cancel);
    for (offset, event) in recovery_events.iter().enumerate() {
        expected.apply(event, last_stored + 1 + offset as u64);
    }

    recovered_runtime
        .shutdown()
        .expect("shutdown recovered runtime");
    let stored = recovered_snapshots
        .load_latest()
        .expect("snapshot store readable")
        .expect("the recovered runtime writes a final snapshot");
    assert_eq!(stored.snapshot, expected.capture_snapshot());
}

#[test]
fn applied_ack_is_preliminary_while_the_journal_is_blocked() {
    let gate = Gate::default();
    let log = InMemoryEventLog::new();
    let blocking = BlockingLog::new(log.handle(), gate.clone());
    let probe = blocking.clone();
    let config = RuntimeConfig {
        apply_batch_max: 1,
        journal_batch_capacity: 1,
        journal_flush_interval: Duration::from_millis(1),
        ..RuntimeConfig::default()
    };

    let (handle, runtime) = EngineRuntime::create(
        config,
        EngineConfig::default(),
        Box::new(blocking),
        Box::new(InMemorySnapshotStore::new()),
    )
    .expect("create");

    let ticket = handle.try_submit(submit_order(1)).expect("submit");
    let applied = ticket.wait_applied_timeout(WAIT).expect("applied first");
    assert_eq!(applied.first_event_sequence, 1);
    assert_eq!(applied.last_event_sequence, 2);
    assert!(matches!(
        applied.events[0],
        DomainEvent::OrderAccepted { .. }
    ));

    wait_until(|| probe.entered());
    assert_eq!(
        log.last_stored_sequence(),
        None,
        "the blocked append has made no event durable"
    );
    assert_eq!(
        ticket.wait_durable_timeout(SHORT),
        Err(RuntimeError::Timeout),
        "durability is not confirmed while the journal append is blocked"
    );

    gate.open();
    let durable = ticket
        .wait_durable_timeout(WAIT)
        .expect("durable on release");
    assert_eq!(durable, applied);
    assert_eq!(log.last_stored_sequence(), Some(2));
    let records = log.read_from(1).expect("read the log");
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].sequence, 1);
    assert_eq!(records[1].sequence, 2);
    assert_eq!(records[0].command_id, 1);
    assert_eq!(records[1].command_id, 1);

    runtime.shutdown().expect("shutdown");
}

#[test]
fn an_applied_but_not_durable_command_is_lost_on_crash_and_survives_after_ack() {
    let gate = Gate::default();
    let log = InMemoryEventLog::new();
    let blocking = BlockingLog::new(log.handle(), gate.clone());
    let probe = blocking.clone();
    let config = RuntimeConfig {
        apply_batch_max: 1,
        journal_batch_capacity: 1,
        journal_flush_interval: Duration::from_millis(1),
        ..RuntimeConfig::default()
    };

    let (handle, runtime) = EngineRuntime::create(
        config,
        EngineConfig::default(),
        Box::new(blocking),
        Box::new(InMemorySnapshotStore::new()),
    )
    .expect("create");

    let ticket = handle.try_submit(submit_order(1)).expect("submit");
    assert_eq!(
        ticket
            .wait_applied_timeout(WAIT)
            .expect("applied")
            .last_event_sequence,
        2
    );
    wait_until(|| probe.entered());
    assert_eq!(
        log.last_stored_sequence(),
        None,
        "the applied command has no durable event at the crash point"
    );

    // Reopen from the log as it would exist after a crash: header, no record.
    let crashed_log = clone_log(&log);
    let (recovered, recovered_runtime) = EngineRuntime::open(
        RuntimeConfig::default(),
        Box::new(crashed_log.handle()),
        Box::new(InMemorySnapshotStore::new()),
    )
    .expect("reopen at the crash point");
    let replayed = recovered
        .submit_and_wait(cancel_order(1))
        .expect("submit after the crash replay");
    assert_eq!(
        replayed.first_event_sequence, 1,
        "the applied-but-not-durable command consumed no durable event sequence"
    );
    assert!(
        matches!(
            replayed.events[0],
            DomainEvent::OrderRejected {
                reason: orderbook_rs::types::EngineError::UnknownOrder,
                ..
            }
        ),
        "the recovered engine never saw the lost command"
    );
    recovered_runtime
        .shutdown()
        .expect("shutdown recovered runtime");

    // Let the original journal finish: the command's events are now durable.
    gate.open();
    assert_eq!(
        ticket
            .wait_durable_timeout(WAIT)
            .expect("durable after release")
            .last_event_sequence,
        2
    );
    runtime.shutdown().expect("shutdown original runtime");

    // Reopening the log after the acknowledgement recovers the command.
    let survived_log = clone_log(&log);
    let (survived, survived_runtime) = EngineRuntime::open(
        RuntimeConfig::default(),
        Box::new(survived_log.handle()),
        Box::new(InMemorySnapshotStore::new()),
    )
    .expect("reopen after the durable acknowledgement");
    let after = survived
        .submit_and_wait(cancel_order(1))
        .expect("cancel the recovered order");
    assert_eq!(after.first_event_sequence, 3);
    assert!(
        matches!(
            after.events[0],
            DomainEvent::OrderCanceled { order_id: 1, .. }
        ),
        "the durable command survives recovery, got {:?}",
        after.events
    );
    survived_runtime
        .shutdown()
        .expect("shutdown survived runtime");
}

/// A command that is unique per `(producer, index)` and exercises both sides.
fn producer_command(producer: u64, index: u64) -> EngineCommand {
    let timestamp = producer * 10_000 + index + 1;
    let side = if (producer + index).is_multiple_of(2) {
        Side::Bid
    } else {
        Side::Ask
    };
    let price = 90 + index % 20;
    EngineCommand::SubmitOrder {
        symbol: BTC.to_string(),
        request: limit(side, price, 1, timestamp),
    }
}

#[test]
fn concurrent_producers_apply_every_command_exactly_once() {
    const PRODUCERS: u64 = 4;
    const PER_PRODUCER: u64 = 50;
    const TOTAL: u64 = PRODUCERS * PER_PRODUCER;

    let log = InMemoryEventLog::new();
    let snapshots = InMemorySnapshotStore::new();
    let config = RuntimeConfig {
        queue_capacity: 64,
        ..RuntimeConfig::default()
    };
    let (handle, runtime) = EngineRuntime::create(
        config,
        EngineConfig::default(),
        Box::new(log.handle()),
        Box::new(snapshots.handle()),
    )
    .expect("create");

    let workers = (0..PRODUCERS)
        .map(|producer| {
            let handle = handle.clone();
            thread::spawn(move || {
                let mut receipts = Vec::with_capacity(PER_PRODUCER as usize);
                for index in 0..PER_PRODUCER {
                    let receipt = handle
                        .submit_and_wait(producer_command(producer, index))
                        .expect("durable submission");
                    receipts.push(receipt);
                }
                receipts
            })
        })
        .collect::<Vec<_>>();

    let mut all_receipts: Vec<CommandReceipt> = Vec::with_capacity(TOTAL as usize);
    for worker in workers {
        let receipts = worker.join().expect("producer thread");
        assert!(
            receipts
                .windows(2)
                .all(|pair| pair[0].last_event_sequence < pair[1].first_event_sequence),
            "each producer observes its own submission order"
        );
        all_receipts.extend(receipts);
    }

    // Every receipt covers exactly the events it carries, and the ranges tile
    // the global event sequence with no gap or overlap.
    let mut ranges: Vec<(u64, u64)> = all_receipts
        .iter()
        .map(|receipt| {
            assert_eq!(
                receipt.last_event_sequence - receipt.first_event_sequence + 1,
                receipt.events.len() as u64
            );
            (receipt.first_event_sequence, receipt.last_event_sequence)
        })
        .collect();
    ranges.sort_unstable();
    let mut expected_first = 1u64;
    for (first, last) in &ranges {
        assert_eq!(*first, expected_first, "event ranges are contiguous");
        expected_first = last + 1;
    }
    let total_events = expected_first - 1;

    runtime.shutdown().expect("shutdown");
    let final_snapshot = snapshots
        .load_latest()
        .expect("snapshot store readable")
        .expect("shutdown writes a final snapshot");
    assert_eq!(final_snapshot.applied_seq, total_events);

    let records = log.read_from(1).expect("read the event log");
    assert_eq!(records.len() as u64, total_events);
    for (index, record) in records.iter().enumerate() {
        assert_eq!(record.sequence, index as u64 + 1);
    }

    // Replaying the log in order must reproduce the runtime's final state. An
    // event applied twice (or skipped) would diverge here.
    let mut replay = MatchingEngine::new(EngineConfig::default());
    for record in &records {
        replay.apply(&record.event, record.sequence);
    }
    assert_eq!(
        final_snapshot.snapshot,
        replay.capture_snapshot(),
        "the event-log fold reproduces the runtime's final snapshot"
    );
}

#[test]
fn a_full_ingress_rejects_without_consuming_a_sequence() {
    let metrics = Arc::new(RuntimeMetrics::new());
    let gate = Gate::default();
    let log = InMemoryEventLog::new();
    let blocking = BlockingLog::new(log.handle(), gate.clone());
    let probe = blocking.clone();
    let config = RuntimeConfig {
        queue_capacity: 4,
        apply_batch_max: 1,
        journal_batch_capacity: 1,
        journal_flush_interval: Duration::from_secs(10),
        metrics: Some(Arc::clone(&metrics)),
        ..RuntimeConfig::default()
    };

    let (handle, runtime) = EngineRuntime::create(
        config,
        EngineConfig::default(),
        Box::new(blocking),
        Box::new(InMemorySnapshotStore::new()),
    )
    .expect("create");

    let mut accepted_tickets: Vec<Ticket> = Vec::new();
    let mut timestamp = 1u64;
    loop {
        match handle.try_submit(submit_order(timestamp)) {
            Ok(ticket) => {
                accepted_tickets.push(ticket);
                timestamp += 1;
            }
            Err(error) => {
                assert_eq!(error, SubmitError::QueueFull);
                break;
            }
        }
        assert!(timestamp < 1_000, "the ingress never filled");
    }

    wait_until(|| probe.entered());
    assert_eq!(metrics.queue_full(), 1, "one refusal was observed");

    gate.open();
    let mut accepted_events = 0u64;
    for ticket in &accepted_tickets {
        let receipt = ticket
            .wait_durable_timeout(WAIT)
            .expect("every accepted command drains");
        accepted_events += receipt.events.len() as u64;
    }
    let accepted = accepted_tickets.len() as u64;
    runtime.shutdown().expect("shutdown");

    assert_eq!(
        log.last_stored_sequence(),
        Some(accepted_events),
        "the refused command emitted no events"
    );
    let records = log.read_from(1).expect("read the log");
    assert_eq!(records.len() as u64, accepted_events);
    for (index, record) in records.iter().enumerate() {
        assert_eq!(
            record.sequence,
            index as u64 + 1,
            "accepted commands consume contiguous event sequences"
        );
    }
    assert_eq!(metrics.applied(), accepted);
    assert_eq!(metrics.durable(), accepted);
    assert_eq!(metrics.queue_full(), 1);

    // Reopening proves the refused submission consumed no event sequence.
    let (reopened, reopened_runtime) = EngineRuntime::open(
        RuntimeConfig::default(),
        Box::new(log.handle()),
        Box::new(InMemorySnapshotStore::new()),
    )
    .expect("reopen");
    let next = reopened
        .submit_and_wait(submit_order(9_999))
        .expect("submit after reopen");
    assert_eq!(next.first_event_sequence, accepted_events + 1);
    assert_eq!(next.last_event_sequence, accepted_events + 2);
    reopened_runtime
        .shutdown()
        .expect("shutdown reopened runtime");
}

fn submit_several(handle: &EngineHandle, count: u64) -> Vec<Ticket> {
    (1..=count)
        .map(|timestamp| handle.try_submit(submit_order(timestamp)).expect("submit"))
        .collect()
}

#[test]
fn journal_append_failure_fails_every_waiter_closed() {
    let metrics = Arc::new(RuntimeMetrics::new());
    let gate = Gate::default();
    let log = FaultingLog::new(JournalFault::AppendError, gate.clone());
    let probe = log.clone();
    let config = RuntimeConfig {
        apply_batch_max: 1,
        journal_batch_capacity: 1,
        journal_flush_interval: Duration::from_secs(10),
        metrics: Some(Arc::clone(&metrics)),
        ..RuntimeConfig::default()
    };

    let (handle, runtime) = EngineRuntime::create(
        config,
        EngineConfig::default(),
        Box::new(log),
        Box::new(InMemorySnapshotStore::new()),
    )
    .expect("create");

    let tickets = submit_several(&handle, 6);
    wait_until(|| probe.entered());
    gate.open();

    for ticket in &tickets {
        assert!(
            matches!(
                ticket.wait_durable_timeout(WAIT),
                Err(RuntimeError::EngineFailed(_))
            ),
            "every durable waiter fails after an append failure"
        );
    }
    for ticket in &tickets {
        match ticket.wait_applied_timeout(WAIT) {
            // Commands applied before the failure keep their preliminary receipt.
            Ok(receipt) => assert!(receipt.last_event_sequence >= 1),
            Err(RuntimeError::EngineFailed(_)) => {}
            other => panic!("unexpected applied result: {other:?}"),
        }
    }

    wait_until(|| handle.is_failed());
    assert!(matches!(
        handle.try_submit(submit_order(99)),
        Err(SubmitError::EngineFailed(_))
    ));
    assert!(metrics.engine_failed() >= 1);

    let result = runtime.shutdown();
    assert!(
        matches!(result, Err(RuntimeError::EngineFailed(_))),
        "shutdown must report the fatal append failure, got {result:?}"
    );
}

#[test]
fn journal_panic_fails_every_waiter_and_the_runtime_closed() {
    let gate = Gate::default();
    let log = FaultingLog::new(JournalFault::Panic, gate.clone());
    let probe = log.clone();
    let config = RuntimeConfig {
        apply_batch_max: 1,
        journal_batch_capacity: 1,
        journal_flush_interval: Duration::from_secs(10),
        ..RuntimeConfig::default()
    };

    let (handle, runtime) = EngineRuntime::create(
        config,
        EngineConfig::default(),
        Box::new(log),
        Box::new(InMemorySnapshotStore::new()),
    )
    .expect("create");

    let tickets = submit_several(&handle, 6);
    wait_until(|| probe.entered());
    gate.open();

    for ticket in &tickets {
        assert!(
            matches!(
                ticket.wait_durable_timeout(WAIT),
                Err(RuntimeError::EngineFailed(_))
            ),
            "a journal panic must fail every durable waiter"
        );
    }
    for ticket in &tickets {
        match ticket.wait_applied_timeout(WAIT) {
            Ok(receipt) => assert!(receipt.last_event_sequence >= 1),
            Err(RuntimeError::EngineFailed(_)) => {}
            other => panic!("unexpected applied result: {other:?}"),
        }
    }

    wait_until(|| handle.is_failed());
    assert!(matches!(
        handle.try_submit(submit_order(99)),
        Err(SubmitError::EngineFailed(_))
    ));

    let result = runtime.shutdown();
    assert!(
        matches!(result, Err(RuntimeError::WorkerPanicked(Worker::Journal))),
        "shutdown must report the journal panic, got {result:?}"
    );
}

#[test]
fn metrics_counts_and_latencies_are_internally_consistent() {
    let metrics = Arc::new(RuntimeMetrics::new());
    let config = RuntimeConfig {
        metrics: Some(Arc::clone(&metrics)),
        ..RuntimeConfig::default()
    };
    let (handle, runtime) = EngineRuntime::create(
        config,
        EngineConfig::default(),
        Box::new(InMemoryEventLog::new()),
        Box::new(InMemorySnapshotStore::new()),
    )
    .expect("create");

    const N: u64 = 64;
    for timestamp in 1..=N {
        handle
            .submit_and_wait(submit_order(timestamp))
            .expect("durable submission");
    }
    // The acknowledgement is written before its metric, so let the last one land.
    wait_until(|| metrics.applied() == N && metrics.durable() == N);

    assert_eq!(metrics.submitted(), N);
    assert_eq!(metrics.applied(), N);
    assert_eq!(metrics.durable(), N);
    assert_eq!(metrics.queue_full(), 0);
    assert_eq!(metrics.engine_failed(), 0);
    assert_eq!(metrics.stopped(), 0);
    // Every submit is an `OrderAccepted` plus an `OrderRested`.
    assert_eq!(metrics.batch_records_sum(), 2 * N);
    assert_eq!(
        metrics.event_records(),
        2 * N,
        "event volume is counted in event records, not commands"
    );
    assert_eq!(metrics.batch_records_max(), 2);
    assert!(metrics.batch_count() >= 1);
    assert!(metrics.journal_append_count() >= 1);
    let queue_capacity = 1024u64;
    assert!(metrics.queue_depth_max() >= 1);
    assert!(
        metrics.queue_depth_max() <= queue_capacity + 1,
        "queue depth {} exceeded the bounded ingress",
        metrics.queue_depth_max()
    );

    assert!(metrics.queue_wait_ns_sum() >= metrics.queue_wait_ns_max());
    assert!(metrics.apply_ns_sum() >= metrics.apply_ns_max());
    assert!(metrics.durable_wait_ns_sum() >= metrics.durable_wait_ns_max());
    assert!(metrics.journal_append_ns_sum() >= metrics.journal_append_ns_max());

    runtime.shutdown().expect("shutdown");
}

#[test]
fn metrics_disabled_reads_no_clock_and_exposes_no_sink() {
    assert_eq!(
        enqueued_at_for_test(None),
        None,
        "metrics disabled must not read the clock for an envelope"
    );
    assert!(
        enqueued_at_for_test(Some(Arc::new(RuntimeMetrics::new()))).is_some(),
        "metrics enabled stamps the envelope"
    );

    let (handle, runtime) = EngineRuntime::create(
        RuntimeConfig::default(),
        EngineConfig::default(),
        Box::new(InMemoryEventLog::new()),
        Box::new(InMemorySnapshotStore::new()),
    )
    .expect("create without metrics");
    assert!(handle.metrics().is_none());
    assert!(runtime.metrics().is_none());
    handle
        .submit_and_wait(submit_order(1))
        .expect("durable submission without metrics");
    runtime.shutdown().expect("shutdown");
}

#[test]
fn automatic_snapshots_never_run_ahead_of_durability() {
    const CADENCE: u64 = 2;
    const N: u64 = 6;

    let log = InMemoryEventLog::new();
    let store = GatingSnapshotStore::new(log.handle());
    let config = RuntimeConfig {
        snapshot_cadence: CADENCE,
        apply_batch_max: 1,
        journal_flush_interval: Duration::from_millis(1),
        ..RuntimeConfig::default()
    };
    let (handle, runtime) = EngineRuntime::create(
        config,
        EngineConfig::default(),
        Box::new(log.handle()),
        Box::new(store.handle()),
    )
    .expect("create");

    let tickets = submit_several(&handle, N);
    for ticket in &tickets {
        ticket.wait_durable_timeout(WAIT).expect("durable");
    }

    // The first cadence boundary pauses intake until durability catches up,
    // then captures the snapshot.
    wait_until(|| store.seen().iter().any(|(applied, _)| *applied == 2));

    runtime.shutdown().expect("shutdown");

    let seen = store.seen();
    assert!(!seen.is_empty());
    for (applied_seq, log_last) in &seen {
        assert!(
            *applied_seq <= *log_last,
            "snapshot at {applied_seq} was saved while the log only held {log_last}"
        );
    }
    assert_eq!(
        seen.iter().map(|(applied, _)| *applied).max(),
        Some(2 * N),
        "shutdown writes the final snapshot at the applied event head"
    );
}

#[test]
fn matching_thread_panic_unblocks_every_waiter_and_is_reported() {
    let barrier = Arc::new(Barrier::new(2));
    let config = RuntimeConfig {
        queue_capacity: 16,
        // Keep the journaled command in the matching thread so the panic, not
        // durability, is what releases the tickets.
        apply_batch_max: 64,
        journal_flush_interval: Duration::from_secs(10),
        ..RuntimeConfig::default()
    };

    let (handle, runtime) = start_with_matching_panic_for_test(
        config,
        EngineConfig::default(),
        Box::new(InMemoryEventLog::new()),
        Box::new(InMemorySnapshotStore::new()),
        Arc::clone(&barrier),
        1,
    )
    .expect("start the panic-injecting runtime");

    // Enqueue every command before the matching thread starts processing, so
    // the test deterministically has live waiters behind the panicking one.
    let tickets = (1..=4)
        .map(|timestamp| handle.try_submit(submit_order(timestamp)).expect("submit"))
        .collect::<Vec<_>>();
    barrier.wait();

    assert_eq!(
        tickets[0]
            .wait_applied_timeout(WAIT)
            .expect("the first command is applied before the panic")
            .last_event_sequence,
        2
    );
    assert_eq!(
        tickets[0].wait_durable_timeout(WAIT),
        Err(RuntimeError::EngineStopped),
        "a panic must not leave the durable waiter hanging"
    );
    for ticket in &tickets[1..] {
        assert_eq!(
            ticket.wait_applied_timeout(WAIT),
            Err(RuntimeError::EngineStopped),
            "queued applied waiters are released"
        );
        assert_eq!(
            ticket.wait_durable_timeout(WAIT),
            Err(RuntimeError::EngineStopped),
            "queued durable waiters are released"
        );
    }

    // The matching thread is gone; the ingress cannot accept work.
    wait_until(|| {
        matches!(
            handle.try_submit(submit_order(99)),
            Err(SubmitError::Shutdown)
        )
    });

    let result = runtime.shutdown();
    assert!(
        matches!(result, Err(RuntimeError::WorkerPanicked(Worker::Matching))),
        "shutdown must report the matching panic, got {result:?}"
    );
}
