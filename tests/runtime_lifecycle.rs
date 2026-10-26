//! End-to-end lifecycle tests for the public matching runtime.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use orderbook_rs::durable::{
    DurableError, EventLog, EventRecord, EventSnapshot, EventSnapshotStore, EventSourcedEngine,
    InMemoryEventLog, InMemoryEventSnapshotStore, LogHeader,
};
use orderbook_rs::engine::MatchingEngine;
use orderbook_rs::event::{DomainEvent, OrderCancelReason};
use orderbook_rs::runtime::{
    CommandReceipt, EngineHandle, EngineRuntime, RuntimeConfig, RuntimeError, RuntimeMetrics,
    SubmitError, Worker,
};
use orderbook_rs::types::{
    EngineCommand, EngineConfig, OrderRequest, OrderType, Side, TimeInForce,
};

const WAIT: Duration = Duration::from_secs(5);

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

/// Fold commands through `decide`/`apply`, the same path recovery uses.
fn fold_commands(commands: &[EngineCommand]) -> MatchingEngine {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    let mut sequence = 1u64;
    for command in commands {
        for event in engine.decide(command) {
            engine.apply(&event, sequence);
            sequence += 1;
        }
    }
    engine
}

fn runtime_config() -> RuntimeConfig {
    RuntimeConfig::default()
}

fn wait_until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !condition() {
        assert!(Instant::now() < deadline, "condition was not met in time");
        thread::sleep(Duration::from_millis(1));
    }
}

/// A gate that blocks a chosen thread until the test opens it.
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

/// An in-memory log whose `append_batch` blocks until a gate opens.
///
/// Used to hold the journal thread in an append so the matching thread's
/// backpressure and shutdown deadline are observable.
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

    fn last_sequence(&self) -> Option<u64> {
        self.inner.last_sequence()
    }

    fn last_command_id(&self) -> Option<u64> {
        self.inner.last_command_id()
    }
}

/// An event snapshot store handle for tests that do not inspect snapshots.
fn unused_snapshots() -> Box<dyn EventSnapshotStore> {
    Box::new(InMemoryEventSnapshotStore::new())
}

/// An event log that panics on the first append, simulating a journal worker
/// panic rather than an append error.
#[derive(Clone, Default)]
struct PanickingLog;

impl EventLog for PanickingLog {
    fn read_header(&self) -> Result<Option<LogHeader>, DurableError> {
        Ok(None)
    }

    fn write_header(&mut self, _header: LogHeader) -> Result<(), DurableError> {
        Ok(())
    }

    fn append(&mut self, _record: EventRecord) -> Result<(), DurableError> {
        Ok(())
    }

    fn append_batch(&mut self, _records: Vec<EventRecord>) -> Result<(), DurableError> {
        panic!("journal log panic");
    }

    fn read_from(&self, _from: u64) -> Result<Vec<EventRecord>, DurableError> {
        Ok(Vec::new())
    }

    fn first_sequence(&self) -> Option<u64> {
        None
    }

    fn last_sequence(&self) -> Option<u64> {
        None
    }

    fn last_command_id(&self) -> Option<u64> {
        None
    }
}

/// An event log whose appends fail with a storage error.
#[derive(Clone, Default)]
struct FailingLog;

impl EventLog for FailingLog {
    fn read_header(&self) -> Result<Option<LogHeader>, DurableError> {
        Ok(None)
    }

    fn write_header(&mut self, _header: LogHeader) -> Result<(), DurableError> {
        Ok(())
    }

    fn append(&mut self, _record: EventRecord) -> Result<(), DurableError> {
        Err(DurableError::AppendFailed("disk full".to_string()))
    }

    fn append_batch(&mut self, _records: Vec<EventRecord>) -> Result<(), DurableError> {
        Err(DurableError::AppendFailed("disk full".to_string()))
    }

    fn read_from(&self, _from: u64) -> Result<Vec<EventRecord>, DurableError> {
        Ok(Vec::new())
    }

    fn first_sequence(&self) -> Option<u64> {
        None
    }

    fn last_sequence(&self) -> Option<u64> {
        None
    }

    fn last_command_id(&self) -> Option<u64> {
        None
    }
}

/// A snapshot store whose saves always fail without panicking.
#[derive(Clone, Default)]
struct FailingSnapshotStore;

impl EventSnapshotStore for FailingSnapshotStore {
    fn save(&mut self, _snapshot: EventSnapshot) -> Result<(), DurableError> {
        Err(DurableError::Storage("snapshot disk full".to_string()))
    }

    fn load_latest(&self) -> Result<Option<EventSnapshot>, DurableError> {
        Ok(None)
    }

    fn load_previous(&self) -> Result<Option<EventSnapshot>, DurableError> {
        Ok(None)
    }
}

/// A snapshot store that panics on the first save.
#[derive(Clone, Default)]
struct PanickingSnapshotStore;

impl EventSnapshotStore for PanickingSnapshotStore {
    fn save(&mut self, _snapshot: EventSnapshot) -> Result<(), DurableError> {
        panic!("snapshot store panic");
    }

    fn load_latest(&self) -> Result<Option<EventSnapshot>, DurableError> {
        Ok(None)
    }

    fn load_previous(&self) -> Result<Option<EventSnapshot>, DurableError> {
        Ok(None)
    }
}

#[test]
fn create_submits_and_acknowledges_applied_and_durable() {
    let log = InMemoryEventLog::new();
    let snapshots = InMemoryEventSnapshotStore::new();

    let (handle, runtime) = EngineRuntime::create(
        runtime_config(),
        EngineConfig::default(),
        Box::new(log.handle()),
        Box::new(snapshots.handle()),
    )
    .expect("create a fresh runtime");

    let durable: CommandReceipt = handle
        .submit_and_wait(submit_order(1))
        .expect("durable receipt");
    assert_eq!(durable.first_event_sequence, 1);
    assert_eq!(durable.last_event_sequence, 2);

    let applied = handle
        .submit_and_wait_applied(submit_order(2))
        .expect("applied receipt");
    assert_eq!(applied.first_event_sequence, 3);
    assert_eq!(applied.last_event_sequence, 4);
    assert!(!handle.is_failed());

    runtime.shutdown().expect("shutdown");
}

#[test]
fn shutdown_drains_writes_a_final_snapshot_and_reopens() {
    let log = InMemoryEventLog::new();
    let snapshots = InMemoryEventSnapshotStore::new();

    let (handle, runtime) = EngineRuntime::create(
        runtime_config(),
        EngineConfig::default(),
        Box::new(log.handle()),
        Box::new(snapshots.handle()),
    )
    .expect("create");

    let tickets = (1..=4)
        .map(|timestamp| handle.try_submit(submit_order(timestamp)).expect("submit"))
        .collect::<Vec<_>>();
    for ticket in &tickets {
        ticket.wait_applied_timeout(WAIT).expect("applied");
    }

    runtime.shutdown().expect("shutdown drains and snapshots");

    let final_snapshot = snapshots
        .load_latest()
        .expect("snapshot store readable")
        .expect("explicit shutdown writes a final snapshot");
    assert_eq!(final_snapshot.applied_seq, 8);
    assert_eq!(final_snapshot.last_command_id, 4);

    let commands = (1..=4).map(submit_order).collect::<Vec<_>>();
    assert_eq!(
        final_snapshot.snapshot,
        fold_commands(&commands).capture_snapshot(),
        "the runtime's final decision state matches event folding"
    );

    // Reopening uses the snapshot plus the event-log tail and continues the
    // global event sequence and command id.
    let (reopened, runtime) = EngineRuntime::open(
        runtime_config(),
        Box::new(log.handle()),
        Box::new(snapshots.handle()),
    )
    .expect("reopen");
    let receipt = reopened
        .submit_and_wait(submit_order(5))
        .expect("submit after reopen");
    assert_eq!(receipt.first_event_sequence, 9);
    assert_eq!(receipt.last_event_sequence, 10);

    runtime.shutdown().expect("second shutdown");
    let stored = snapshots
        .load_latest()
        .unwrap()
        .expect("second final snapshot");
    assert_eq!(stored.applied_seq, 10);
    assert_eq!(stored.last_command_id, 5);
}

#[test]
fn drop_drains_without_a_final_snapshot_and_reopens() {
    let log = InMemoryEventLog::new();
    let snapshots = InMemoryEventSnapshotStore::new();

    let (handle, runtime) = EngineRuntime::create(
        runtime_config(),
        EngineConfig::default(),
        Box::new(log.handle()),
        Box::new(snapshots.handle()),
    )
    .expect("create");

    for timestamp in 1..=3 {
        handle
            .submit_and_wait(submit_order(timestamp))
            .expect("durable before drop");
    }

    drop(runtime);

    assert!(
        snapshots.load_latest().unwrap().is_none(),
        "Drop performs the same stop without writing a snapshot"
    );

    // Every durable event is still in the log, so reopening restores them.
    let (reopened, runtime) = EngineRuntime::open(
        runtime_config(),
        Box::new(log.handle()),
        Box::new(snapshots.handle()),
    )
    .expect("reopen after drop");
    let receipt = reopened
        .submit_and_wait(cancel_order(2))
        .expect("durable after reopen");
    assert_eq!(receipt.first_event_sequence, 7);
    assert_eq!(receipt.last_event_sequence, 7);
    assert!(
        matches!(
            receipt.events[0],
            DomainEvent::OrderCanceled {
                reason: OrderCancelReason::UserCancel,
                ..
            }
        ),
        "the recovered engine retained the replayed order, got {:?}",
        receipt.events
    );

    runtime.shutdown().expect("shutdown");
}

#[test]
fn open_recovers_a_log_seeded_by_the_synchronous_facade() {
    let log = InMemoryEventLog::new();
    let snapshots = InMemoryEventSnapshotStore::new();
    {
        let mut durable = EventSourcedEngine::create(
            EngineConfig::default(),
            Box::new(log.handle()),
            Box::new(InMemoryEventSnapshotStore::new()),
        )
        .expect("seed event-sourced engine");
        for timestamp in 1..=3 {
            durable
                .execute(submit_order(timestamp))
                .expect("seed append-before-apply");
        }
    }

    let (handle, runtime) = EngineRuntime::open(
        runtime_config(),
        Box::new(log.handle()),
        Box::new(snapshots.handle()),
    )
    .expect("recover at startup");

    let receipt = handle
        .submit_and_wait(cancel_order(1))
        .expect("continue after recovery");
    assert_eq!(receipt.first_event_sequence, 7);
    assert!(matches!(
        receipt.events[0],
        DomainEvent::OrderCanceled { order_id: 1, .. }
    ));

    runtime.shutdown().expect("shutdown");
    assert_eq!(
        snapshots.load_latest().unwrap().unwrap().applied_seq,
        7,
        "the reopened runtime snapshots from the recovered applied event sequence"
    );
}

#[test]
fn a_full_ingress_rejects_with_queue_full_and_consumes_no_sequence() {
    let metrics = Arc::new(RuntimeMetrics::new());
    let gate = Gate::default();
    let log = InMemoryEventLog::new();
    let blocking = BlockingLog::new(log.handle(), gate.clone());
    let config = RuntimeConfig {
        queue_capacity: 1,
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
        unused_snapshots(),
    )
    .expect("create");

    let mut tickets = Vec::new();
    let mut next = 1u64;
    let refused = loop {
        match handle.try_submit(submit_order(next)) {
            Ok(ticket) => {
                tickets.push(ticket);
                next += 1;
            }
            Err(error) => break error,
        }
        assert!(next < 1000, "the ingress never filled");
        thread::sleep(Duration::from_millis(1));
    };

    assert_eq!(refused, SubmitError::QueueFull);
    assert!(metrics.queue_full() >= 1);
    let accepted = tickets.len() as u64;

    gate.open();
    for ticket in &tickets {
        ticket
            .wait_durable_timeout(WAIT)
            .expect("every accepted command drains");
    }
    runtime.shutdown().expect("shutdown");

    assert_eq!(
        log.last_sequence(),
        Some(2 * accepted),
        "the refused command emitted no events"
    );

    // Reopening consumes the next event sequence only after the accepted
    // commands' events.
    let (reopened, runtime) =
        EngineRuntime::open(runtime_config(), Box::new(log.handle()), unused_snapshots())
            .expect("reopen");
    let receipt = reopened
        .submit_and_wait(submit_order(9999))
        .expect("submit after reopen");
    assert_eq!(receipt.first_event_sequence, 2 * accepted + 1);
    runtime.shutdown().expect("shutdown");
}

#[test]
fn journal_panic_unblocks_waiters_and_fails_closed() {
    let config = RuntimeConfig {
        apply_batch_max: 1,
        journal_flush_interval: Duration::from_secs(10),
        ..RuntimeConfig::default()
    };
    let (handle, runtime) = EngineRuntime::create(
        config,
        EngineConfig::default(),
        Box::new(PanickingLog),
        unused_snapshots(),
    )
    .expect("create");

    let ticket = handle.try_submit(submit_order(1)).expect("submit");
    ticket
        .wait_applied_timeout(WAIT)
        .expect("apply-first acknowledges before the journal appends");

    let durable = ticket.wait_durable_timeout(WAIT);
    assert!(
        matches!(durable, Err(RuntimeError::EngineFailed(_))),
        "a journal panic must fail the durable waiter, got {durable:?}"
    );
    assert!(handle.is_failed());
    assert!(matches!(
        handle.try_submit(submit_order(2)),
        Err(SubmitError::EngineFailed(_))
    ));

    let result = runtime.shutdown();
    assert!(
        matches!(result, Err(RuntimeError::WorkerPanicked(Worker::Journal))),
        "shutdown must report the journal panic, got {result:?}"
    );
}

#[test]
fn journal_append_failure_fails_durable_waiters_and_shutdown() {
    let config = RuntimeConfig {
        apply_batch_max: 1,
        journal_flush_interval: Duration::from_secs(10),
        ..RuntimeConfig::default()
    };
    let (handle, runtime) = EngineRuntime::create(
        config,
        EngineConfig::default(),
        Box::new(FailingLog),
        unused_snapshots(),
    )
    .expect("create");

    let ticket = handle.try_submit(submit_order(1)).expect("submit");
    ticket.wait_applied_timeout(WAIT).expect("applied first");
    assert!(
        matches!(
            ticket.wait_durable_timeout(WAIT),
            Err(RuntimeError::EngineFailed(_))
        ),
        "the pending durable waiter must fail"
    );
    assert!(handle.is_failed());
    assert!(matches!(
        handle.try_submit(submit_order(2)),
        Err(SubmitError::EngineFailed(_))
    ));
    assert!(matches!(
        handle.submit_and_wait(submit_order(3)),
        Err(RuntimeError::EngineFailed(_))
    ));

    let result = runtime.shutdown();
    assert!(
        matches!(result, Err(RuntimeError::EngineFailed(_))),
        "shutdown reports the fatal journal failure, got {result:?}"
    );
}

#[test]
fn a_failing_final_snapshot_maps_to_snapshot_failed() {
    let (handle, runtime) = EngineRuntime::create(
        runtime_config(),
        EngineConfig::default(),
        Box::new(InMemoryEventLog::new()),
        Box::new(FailingSnapshotStore),
    )
    .expect("create");

    handle
        .submit_and_wait(submit_order(1))
        .expect("durable command");

    let result = runtime.shutdown();
    assert!(
        matches!(result, Err(RuntimeError::SnapshotFailed(_))),
        "a refused final snapshot must surface, got {result:?}"
    );
}

#[test]
fn a_panicking_snapshot_worker_maps_to_worker_panicked() {
    let (handle, runtime) = EngineRuntime::create(
        runtime_config(),
        EngineConfig::default(),
        Box::new(InMemoryEventLog::new()),
        Box::new(PanickingSnapshotStore),
    )
    .expect("create");

    handle
        .submit_and_wait(submit_order(1))
        .expect("durable command");

    let result = runtime.shutdown();
    assert!(
        matches!(result, Err(RuntimeError::WorkerPanicked(Worker::Snapshot))),
        "a snapshot worker panic must surface by name, got {result:?}"
    );
}

#[test]
fn shutdown_timeout_bounds_the_durability_wait() {
    let gate = Gate::default();
    let log = InMemoryEventLog::new();
    let blocking = BlockingLog::new(log.handle(), gate.clone());
    let probe = blocking.clone();
    let config = RuntimeConfig {
        apply_batch_max: 1,
        journal_batch_capacity: 1,
        journal_flush_interval: Duration::from_secs(10),
        shutdown_timeout: Duration::from_millis(50),
        ..RuntimeConfig::default()
    };

    let (handle, runtime) = EngineRuntime::create(
        config,
        EngineConfig::default(),
        Box::new(blocking),
        unused_snapshots(),
    )
    .expect("create");

    let ticket = handle.try_submit(submit_order(1)).expect("submit");
    ticket.wait_applied_timeout(WAIT).expect("applied");
    wait_until(|| probe.entered());

    let shutdown = thread::spawn(move || runtime.shutdown());
    // The matching thread must give up on the blocked journal by itself.
    thread::sleep(Duration::from_millis(200));
    gate.open();

    let result = shutdown.join().expect("shutdown thread does not panic");
    assert_eq!(
        result,
        Err(RuntimeError::Timeout),
        "the shutdown deadline expired while the journal was blocked"
    );
    assert_eq!(
        ticket.wait_durable_timeout(WAIT),
        Err(RuntimeError::EngineStopped),
        "waiters are released when shutdown stops waiting for durability"
    );
}

#[test]
fn concurrent_producers_get_unique_contiguous_event_ranges() {
    const PRODUCERS: u64 = 4;
    const PER_PRODUCER: u64 = 50;

    let log = InMemoryEventLog::new();
    let snapshots = InMemoryEventSnapshotStore::new();
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
                let mut receipts = Vec::new();
                for index in 0..PER_PRODUCER {
                    let timestamp = producer * 1_000 + index + 1;
                    let receipt = handle
                        .submit_and_wait(submit_order(timestamp))
                        .expect("durable submission");
                    receipts.push(receipt);
                }
                receipts
            })
        })
        .collect::<Vec<_>>();

    let mut all_receipts = Vec::new();
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

    let mut ranges: Vec<(u64, u64)> = all_receipts
        .iter()
        .map(|receipt| (receipt.first_event_sequence, receipt.last_event_sequence))
        .collect();
    ranges.sort_unstable();
    let mut expected_first = 1u64;
    for (first, last) in &ranges {
        assert_eq!(*first, expected_first, "event ranges are contiguous");
        expected_first = last + 1;
    }
    let total_events = expected_first - 1;

    runtime.shutdown().expect("shutdown");
    assert_eq!(
        snapshots.load_latest().unwrap().unwrap().applied_seq,
        total_events
    );
}

#[test]
fn an_invalid_affinity_index_fails_startup() {
    let Some(cores) = core_affinity::get_core_ids() else {
        return;
    };
    let config = RuntimeConfig {
        matching_core: Some(cores.len()),
        ..RuntimeConfig::default()
    };

    match EngineRuntime::create(
        config,
        EngineConfig::default(),
        Box::new(InMemoryEventLog::new()),
        unused_snapshots(),
    ) {
        Err(RuntimeError::Startup(detail)) => {
            assert!(
                detail.contains("out of range"),
                "unexpected detail: {detail}"
            );
        }
        Err(other) => panic!("expected a startup error, got {other:?}"),
        Ok(_) => panic!("an out-of-range core index must fail startup"),
    }
}

#[test]
fn a_valid_affinity_index_binds_and_runs() {
    let Some(cores) = core_affinity::get_core_ids() else {
        return;
    };
    if cores.is_empty() {
        return;
    }
    let config = RuntimeConfig {
        matching_core: Some(0),
        ..RuntimeConfig::default()
    };

    let (handle, runtime) = EngineRuntime::create(
        config,
        EngineConfig::default(),
        Box::new(InMemoryEventLog::new()),
        unused_snapshots(),
    )
    .expect("bind the matching thread to the first available core");
    handle
        .submit_and_wait(submit_order(1))
        .expect("submission on the bound thread");
    runtime.shutdown().expect("shutdown");
}

#[test]
fn engine_handle_is_clone_send_and_sync() {
    fn assert_clone<T: Clone>() {}
    fn assert_send_sync<T: Send + Sync>() {}

    assert_clone::<EngineHandle>();
    assert_send_sync::<EngineHandle>();
}

#[test]
fn submissions_are_rejected_after_shutdown() {
    let (handle, runtime) = EngineRuntime::create(
        runtime_config(),
        EngineConfig::default(),
        Box::new(InMemoryEventLog::new()),
        unused_snapshots(),
    )
    .expect("create");
    runtime.shutdown().expect("shutdown");

    assert_eq!(
        handle.submit_and_wait(submit_order(1)),
        Err(RuntimeError::Shutdown)
    );
    assert!(matches!(
        handle.try_submit(submit_order(2)),
        Err(SubmitError::Shutdown)
    ));
}
