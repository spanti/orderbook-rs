//! Snapshot cadence, snapshot-plus-tail recovery, and the snapshot store
//! contract, exercised through the public durable surface and the in-memory
//! adapters.
//!
//! A "crash" is simulated by dropping the durable engine and reopening from a
//! second handle; no test sleeps or inspects private engine state.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use orderbook_rs::durable::memory::{InMemoryCommandLog, InMemorySnapshotStore};
use orderbook_rs::durable::{
    CommandLog, CommandRecord, DurableError, DurableMatchingEngine, FORMAT_VERSION, LogHeader,
    SnapshotStore, StoredSnapshot,
};
use orderbook_rs::engine::MatchingEngine;
use orderbook_rs::types::{
    EngineCommand, EngineConfig, ModifyRequest, OrderRequest, Side, StopKind, StopOrderRequest,
    TimeInForce,
};

const BTC: &str = "BTC-USDT";
const ETH: &str = "ETH-USDT";

fn limit(timestamp: u64, side: Side, price: u64, quantity: u64) -> OrderRequest {
    OrderRequest {
        side,
        price,
        quantity,
        timestamp,
        order_type: orderbook_rs::types::OrderType::Limit,
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

fn sample_commands() -> Vec<EngineCommand> {
    vec![
        submit(BTC, limit(1, Side::Bid, 100, 5)),
        submit(BTC, limit(2, Side::Ask, 100, 3)),
        submit(ETH, limit(3, Side::Bid, 200, 4)),
        EngineCommand::ModifyOrder {
            symbol: BTC.to_string(),
            order_id: 1,
            request: ModifyRequest {
                price: 99,
                quantity: 2,
                timestamp: 4,
            },
        },
        EngineCommand::CancelOrder {
            symbol: ETH.to_string(),
            order_id: 3,
        },
        EngineCommand::SubmitStopOrder {
            symbol: BTC.to_string(),
            request: StopOrderRequest {
                side: Side::Ask,
                stop_price: 95,
                order_type: StopKind::Market,
                quantity: 1,
                timestamp: 5,
            },
        },
        submit(BTC, limit(6, Side::Bid, 100, 0)),
    ]
}

fn pure_engine_after(commands: &[EngineCommand]) -> MatchingEngine {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    for command in commands {
        let _ = engine.execute(command.clone());
    }
    engine
}

#[test]
fn automatic_snapshots_run_at_the_configured_cadence() {
    let log = InMemoryCommandLog::new();
    let snapshots = InMemorySnapshotStore::new();
    let mut durable = DurableMatchingEngine::create(
        EngineConfig::default(),
        Box::new(log.handle()),
        Box::new(snapshots.handle()),
        2,
    )
    .unwrap();

    for command in sample_commands().into_iter().take(5) {
        durable.execute(command).unwrap();
    }

    let latest = snapshots
        .load_latest()
        .unwrap()
        .expect("cadence 2 must snapshot at applied sequence 4");
    let previous = snapshots
        .load_previous()
        .unwrap()
        .expect("cadence 2 must also have snapshotted at applied sequence 2");
    assert_eq!(latest.applied_seq, 4);
    assert_eq!(previous.applied_seq, 2);
    assert_eq!(durable.applied_seq(), 5);
    assert_eq!(durable.last_snapshot_error(), None);
}

#[test]
fn cadence_zero_writes_no_automatic_snapshots() {
    let log = InMemoryCommandLog::new();
    let snapshots = InMemorySnapshotStore::new();
    let mut durable = DurableMatchingEngine::create(
        EngineConfig::default(),
        Box::new(log.handle()),
        Box::new(snapshots.handle()),
        0,
    )
    .unwrap();

    for command in sample_commands() {
        durable.execute(command).unwrap();
    }

    assert!(
        snapshots.load_latest().unwrap().is_none(),
        "cadence zero must disable automatic snapshots"
    );
    assert_eq!(durable.last_snapshot_error(), None);
}

#[test]
fn snapshot_now_checkpoints_on_demand() {
    let log = InMemoryCommandLog::new();
    let snapshots = InMemorySnapshotStore::new();
    let mut durable = DurableMatchingEngine::create(
        EngineConfig::default(),
        Box::new(log.handle()),
        Box::new(snapshots.handle()),
        0,
    )
    .unwrap();

    for command in sample_commands().into_iter().take(5) {
        durable.execute(command).unwrap();
    }
    durable.snapshot_now().expect("an explicit snapshot succeeds");

    let latest = snapshots.load_latest().unwrap().expect("snapshot was saved");
    assert_eq!(latest.format_version, FORMAT_VERSION);
    assert_eq!(latest.applied_seq, 5);
    assert_eq!(
        latest.snapshot,
        durable.engine().capture_snapshot(),
        "the stored snapshot is the applied decision state"
    );
    assert_eq!(durable.last_snapshot_error(), None);
}

/// A log decorator that records the `from` arguments passed to `read_from` and
/// how many records were returned, so a test can prove recovery never re-reads
/// the pre-snapshot prefix.
struct RecordingLog {
    inner: InMemoryCommandLog,
    reads: Arc<Mutex<Vec<u64>>>,
    records_read: Arc<AtomicUsize>,
}

impl RecordingLog {
    fn new(inner: InMemoryCommandLog) -> Self {
        Self {
            inner,
            reads: Arc::new(Mutex::new(Vec::new())),
            records_read: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl CommandLog for RecordingLog {
    fn read_header(&self) -> Result<Option<LogHeader>, DurableError> {
        self.inner.read_header()
    }

    fn write_header(&mut self, header: LogHeader) -> Result<(), DurableError> {
        self.inner.write_header(header)
    }

    fn append(&mut self, record: CommandRecord) -> Result<(), DurableError> {
        self.inner.append(record)
    }

    fn read_from(&self, from: u64) -> Result<Vec<CommandRecord>, DurableError> {
        self.reads.lock().unwrap().push(from);
        let records = self.inner.read_from(from)?;
        self.records_read
            .fetch_add(records.len(), Ordering::SeqCst);
        Ok(records)
    }

    fn first_sequence(&self) -> Option<u64> {
        self.inner.first_sequence()
    }

    fn last_sequence(&self) -> Option<u64> {
        self.inner.last_sequence()
    }
}

#[test]
fn recovery_resumes_after_the_snapshot_and_never_rereads_its_prefix() {
    // Crash window: after sequence 6 was applied and snapshotted, sequence 7
    // was appended and applied, then the process died before the next cadence.
    let log = InMemoryCommandLog::new();
    let snapshots = InMemorySnapshotStore::new();
    let mut durable = DurableMatchingEngine::create(
        EngineConfig::default(),
        Box::new(log.handle()),
        Box::new(snapshots.handle()),
        3,
    )
    .unwrap();

    let commands = sample_commands();
    for command in commands.iter().take(7) {
        durable.execute(command.clone()).unwrap();
    }
    let before_crash = durable.engine().capture_snapshot();
    drop(durable);

    let stored = snapshots.load_latest().unwrap().expect("cadence 3 stored");
    assert_eq!(stored.applied_seq, 6);

    let recording = RecordingLog::new(log.handle());
    let reads = recording.reads.clone();
    let records_read = recording.records_read.clone();
    let reopened =
        DurableMatchingEngine::open(Box::new(recording), Box::new(snapshots.handle()), 3).unwrap();

    assert_eq!(reopened.applied_seq(), 7);
    assert_eq!(
        reopened.engine().capture_snapshot(),
        before_crash,
        "snapshot plus the single tail command rebuilds exact state"
    );
    assert_eq!(
        *reads.lock().unwrap(),
        vec![7],
        "recovery reads only the tail after the snapshot"
    );
    assert_eq!(
        records_read.load(Ordering::SeqCst),
        1,
        "no pre-snapshot record is read back"
    );
}

/// A log decorator that delivers one tail record twice, simulating at-least-once
/// delivery after the snapshot.
struct DuplicateTailLog {
    inner: InMemoryCommandLog,
    duplicate: CommandRecord,
}

impl CommandLog for DuplicateTailLog {
    fn read_header(&self) -> Result<Option<LogHeader>, DurableError> {
        self.inner.read_header()
    }

    fn write_header(&mut self, header: LogHeader) -> Result<(), DurableError> {
        self.inner.write_header(header)
    }

    fn append(&mut self, record: CommandRecord) -> Result<(), DurableError> {
        self.inner.append(record)
    }

    fn read_from(&self, from: u64) -> Result<Vec<CommandRecord>, DurableError> {
        let mut records = self.inner.read_from(from)?;
        if self.duplicate.sequence >= from
            && let Some(position) = records
                .iter()
                .position(|record| record.sequence == self.duplicate.sequence)
        {
            records.insert(position + 1, self.duplicate.clone());
        }
        Ok(records)
    }

    fn first_sequence(&self) -> Option<u64> {
        self.inner.first_sequence()
    }

    fn last_sequence(&self) -> Option<u64> {
        self.inner.last_sequence()
    }
}

#[test]
fn a_duplicate_sequence_in_the_tail_is_skipped() {
    let log = InMemoryCommandLog::new();
    let snapshots = InMemorySnapshotStore::new();
    let mut durable = DurableMatchingEngine::create(
        EngineConfig::default(),
        Box::new(log.handle()),
        Box::new(snapshots.handle()),
        2,
    )
    .unwrap();

    let commands = sample_commands();
    for command in commands.iter().take(5) {
        durable.execute(command.clone()).unwrap();
    }
    let before_crash = durable.engine().capture_snapshot();
    drop(durable);

    let duplicate = log
        .read_from(5)
        .unwrap()
        .into_iter()
        .next()
        .expect("sequence 5 is the tail");
    assert_eq!(duplicate.sequence, 5);

    let reopened = DurableMatchingEngine::open(
        Box::new(DuplicateTailLog {
            inner: log.handle(),
            duplicate,
        }),
        Box::new(snapshots.handle()),
        2,
    )
    .expect("an already-applied duplicate must be skipped");

    assert_eq!(reopened.applied_seq(), 5);
    assert_eq!(
        reopened.engine().capture_snapshot(),
        before_crash,
        "double-applying the duplicate would move order and trade ids"
    );
}

/// A log decorator that hides one middle record so the tail jumps.
struct DroppedRecordLog {
    inner: InMemoryCommandLog,
    dropped: u64,
}

impl CommandLog for DroppedRecordLog {
    fn read_header(&self) -> Result<Option<LogHeader>, DurableError> {
        self.inner.read_header()
    }

    fn write_header(&mut self, header: LogHeader) -> Result<(), DurableError> {
        self.inner.write_header(header)
    }

    fn append(&mut self, record: CommandRecord) -> Result<(), DurableError> {
        self.inner.append(record)
    }

    fn read_from(&self, from: u64) -> Result<Vec<CommandRecord>, DurableError> {
        Ok(self
            .inner
            .read_from(from)?
            .into_iter()
            .filter(|record| record.sequence != self.dropped)
            .collect())
    }

    fn first_sequence(&self) -> Option<u64> {
        self.inner.first_sequence()
    }

    fn last_sequence(&self) -> Option<u64> {
        self.inner.last_sequence()
    }
}

#[test]
fn a_non_contiguous_sequence_in_the_tail_refuses_recovery() {
    let log = InMemoryCommandLog::new();
    let snapshots = InMemorySnapshotStore::new();
    let mut durable = DurableMatchingEngine::create(
        EngineConfig::default(),
        Box::new(log.handle()),
        Box::new(snapshots.handle()),
        2,
    )
    .unwrap();

    let commands = sample_commands();
    for command in commands.iter().take(3) {
        durable.execute(command.clone()).unwrap();
    }
    drop(durable);

    // Append sequence 4 through the raw log without applying it. Recovery will
    // see [4] after sequence 3 is hidden and must refuse the jump.
    log.handle()
        .append(CommandRecord {
            format_version: FORMAT_VERSION,
            sequence: 4,
            command: submit(ETH, limit(9, Side::Bid, 150, 1)),
        })
        .unwrap();

    let error = match DurableMatchingEngine::open(
        Box::new(DroppedRecordLog {
            inner: log.handle(),
            dropped: 3,
        }),
        Box::new(snapshots.handle()),
        2,
    ) {
        Ok(_) => panic!("recovery must refuse a non-contiguous tail"),
        Err(error) => error,
    };

    assert_eq!(
        error,
        DurableError::NonContiguousSequence {
            expected: 3,
            found: 4
        }
    );
}

#[test]
fn a_command_applied_without_its_own_snapshot_recovers_from_the_previous_snapshot() {
    // Crash window: sequences 1..5 were applied and durable, but the process
    // died after sequence 5 and before the cadence-6 snapshot. The latest
    // snapshot covers 4; sequence 5 must be replayed from the log.
    let log = InMemoryCommandLog::new();
    let snapshots = InMemorySnapshotStore::new();
    let mut durable = DurableMatchingEngine::create(
        EngineConfig::default(),
        Box::new(log.handle()),
        Box::new(snapshots.handle()),
        2,
    )
    .unwrap();

    let commands = sample_commands();
    for command in commands.iter().take(5) {
        durable.execute(command.clone()).unwrap();
    }
    let before_crash = durable.engine().capture_snapshot();
    drop(durable);

    assert_eq!(snapshots.load_latest().unwrap().unwrap().applied_seq, 4);

    let mut reopened =
        DurableMatchingEngine::open(Box::new(log.handle()), Box::new(snapshots.handle()), 2)
            .unwrap();

    assert_eq!(reopened.applied_seq(), 5);
    assert_eq!(reopened.engine().capture_snapshot(), before_crash);

    // Recovery continues numbering after the log's tail.
    reopened
        .execute(submit(ETH, limit(7, Side::Ask, 300, 2)))
        .unwrap();
    assert_eq!(reopened.history_from(6).unwrap()[0].sequence, 6);
}

/// A snapshot store decorator whose saves fail while the flag is set.
struct FailingSnapshotStore {
    inner: InMemorySnapshotStore,
    fail: Arc<AtomicBool>,
    saves: Arc<AtomicUsize>,
}

impl FailingSnapshotStore {
    fn new(inner: InMemorySnapshotStore, fail: Arc<AtomicBool>) -> Self {
        Self {
            inner,
            fail,
            saves: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl SnapshotStore for FailingSnapshotStore {
    fn save(&mut self, snapshot: StoredSnapshot) -> Result<(), DurableError> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(DurableError::Storage(
                "injected snapshot failure".to_string(),
            ));
        }
        self.inner.save(snapshot)?;
        self.saves.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn load_latest(&self) -> Result<Option<StoredSnapshot>, DurableError> {
        self.inner.load_latest()
    }

    fn load_previous(&self) -> Result<Option<StoredSnapshot>, DurableError> {
        self.inner.load_previous()
    }
}

#[test]
fn a_snapshot_failure_leaves_the_command_durable_and_retries_at_the_next_boundary() {
    let log = InMemoryCommandLog::new();
    let raw_snapshots = InMemorySnapshotStore::new();
    let fail = Arc::new(AtomicBool::new(true));
    let failing = FailingSnapshotStore::new(raw_snapshots.handle(), fail.clone());
    let saves = failing.saves.clone();
    let mut durable = DurableMatchingEngine::create(
        EngineConfig::default(),
        Box::new(log.handle()),
        Box::new(failing),
        2,
    )
    .unwrap();

    let commands = sample_commands();
    for command in commands.iter().take(2) {
        durable
            .execute(command.clone())
            .expect("a snapshot failure must not fail the durable command");
    }

    assert_eq!(durable.applied_seq(), 2);
    assert_eq!(
        durable.engine().capture_snapshot(),
        pure_engine_after(&commands[..2]).capture_snapshot()
    );
    assert_eq!(
        durable.last_snapshot_error(),
        Some(&DurableError::Storage(
            "injected snapshot failure".to_string()
        ))
    );
    assert!(
        raw_snapshots.load_latest().unwrap().is_none(),
        "the failed save stored nothing"
    );
    assert_eq!(durable.history_from(1).unwrap().len(), 2);

    // The next cadence boundary retries and succeeds, clearing the error.
    fail.store(false, Ordering::SeqCst);
    durable.execute(commands[2].clone()).unwrap();
    assert!(durable.last_snapshot_error().is_some(), "no boundary yet");
    durable.execute(commands[3].clone()).unwrap();

    assert_eq!(durable.last_snapshot_error(), None);
    assert_eq!(saves.load(Ordering::SeqCst), 1);
    let latest = raw_snapshots.load_latest().unwrap().unwrap();
    assert_eq!(latest.applied_seq, 4);
    assert_eq!(latest.snapshot, durable.engine().capture_snapshot());
}

// ---------------------------------------------------------------------------
// Snapshot store contract, asserted against the reference in-memory adapter.
// ---------------------------------------------------------------------------

fn stored_at(applied_seq: u64) -> StoredSnapshot {
    StoredSnapshot {
        format_version: FORMAT_VERSION,
        applied_seq,
        snapshot: MatchingEngine::new(EngineConfig::default()).capture_snapshot(),
    }
}

#[test]
fn saving_below_the_snapshot_floor_is_rejected() {
    let mut store = InMemorySnapshotStore::new();
    store.save(stored_at(5)).unwrap();

    let error = store
        .save(stored_at(4))
        .expect_err("an older applied sequence must be refused");
    assert_eq!(
        error,
        DurableError::SnapshotBelowFloor {
            applied_seq: 4,
            floor: 5
        }
    );
    assert_eq!(store.load_latest().unwrap().unwrap().applied_seq, 5);

    store
        .save(stored_at(5))
        .expect("an equal applied sequence is allowed");
    assert_eq!(store.load_latest().unwrap().unwrap().applied_seq, 5);
}

#[test]
fn the_store_retains_the_two_newest_snapshots() {
    let mut store = InMemorySnapshotStore::new();
    assert!(store.load_latest().unwrap().is_none());
    assert!(store.load_previous().unwrap().is_none());

    store.save(stored_at(1)).unwrap();
    assert_eq!(store.load_latest().unwrap().unwrap().applied_seq, 1);
    assert!(
        store.load_previous().unwrap().is_none(),
        "one snapshot has no previous selection"
    );

    store.save(stored_at(2)).unwrap();
    assert_eq!(store.load_latest().unwrap().unwrap().applied_seq, 2);
    assert_eq!(store.load_previous().unwrap().unwrap().applied_seq, 1);

    store.save(stored_at(3)).unwrap();
    assert_eq!(store.load_latest().unwrap().unwrap().applied_seq, 3);
    assert_eq!(
        store.load_previous().unwrap().unwrap().applied_seq,
        2,
        "sequence 1 was pruned once three snapshots existed"
    );

    store.save(stored_at(4)).unwrap();
    assert_eq!(store.load_latest().unwrap().unwrap().applied_seq, 4);
    assert_eq!(store.load_previous().unwrap().unwrap().applied_seq, 3);
}

// ---------------------------------------------------------------------------
// Recovery validation: version, config, snapshot-ahead, recovery gap.
// ---------------------------------------------------------------------------

#[test]
fn an_unsupported_snapshot_version_refuses_recovery() {
    let mut log = InMemoryCommandLog::new();
    log.write_header(LogHeader {
        format_version: FORMAT_VERSION,
        config: EngineConfig::default(),
    })
    .unwrap();

    let mut store = InMemorySnapshotStore::new();
    store
        .save(StoredSnapshot {
            format_version: FORMAT_VERSION + 1,
            applied_seq: 0,
            snapshot: MatchingEngine::new(EngineConfig::default()).capture_snapshot(),
        })
        .unwrap();

    let error = match DurableMatchingEngine::open(Box::new(log), Box::new(store), 0) {
        Ok(_) => panic!("recovery must refuse an unsupported snapshot version"),
        Err(error) => error,
    };
    assert_eq!(
        error,
        DurableError::UnsupportedVersion {
            found: FORMAT_VERSION + 1,
            supported: FORMAT_VERSION
        }
    );
}

#[test]
fn a_snapshot_with_a_different_config_refuses_recovery() {
    let config = EngineConfig {
        min_order_quantity: 1,
        max_order_quantity: 1_000,
        tick_size: 5,
    };
    let mut log = InMemoryCommandLog::new();
    log.write_header(LogHeader {
        format_version: FORMAT_VERSION,
        config: EngineConfig::default(),
    })
    .unwrap();

    let mut store = InMemorySnapshotStore::new();
    store
        .save(StoredSnapshot {
            format_version: FORMAT_VERSION,
            applied_seq: 0,
            snapshot: MatchingEngine::new(config).capture_snapshot(),
        })
        .unwrap();

    let error = match DurableMatchingEngine::open(Box::new(log), Box::new(store), 0) {
        Ok(_) => panic!("recovery must refuse a config mismatch"),
        Err(error) => error,
    };
    assert_eq!(
        error,
        DurableError::ConfigMismatch {
            header: EngineConfig::default(),
            snapshot: config
        }
    );
}

#[test]
fn a_snapshot_ahead_of_the_log_refuses_recovery() {
    let log = InMemoryCommandLog::new();
    let snapshots = InMemorySnapshotStore::new();
    let mut durable = DurableMatchingEngine::create(
        EngineConfig::default(),
        Box::new(log.handle()),
        Box::new(snapshots.handle()),
        0,
    )
    .unwrap();
    for command in sample_commands().into_iter().take(3) {
        durable.execute(command).unwrap();
    }
    drop(durable);

    let mut store = snapshots.handle();
    store
        .save(stored_at(5))
        .expect("the store accepts a floor above the log");

    let error = match DurableMatchingEngine::open(Box::new(log.handle()), Box::new(store), 0) {
        Ok(_) => panic!("recovery must refuse a snapshot beyond the log"),
        Err(error) => error,
    };
    assert_eq!(
        error,
        DurableError::SnapshotAhead {
            applied_seq: 5,
            log_last_seq: 3
        }
    );
}

/// A log decorator that hides every record through `hide_through`, simulating a
/// lost prefix that leaves the log starting too late for the snapshot.
struct PrefixHiddenLog {
    inner: InMemoryCommandLog,
    hide_through: u64,
}

impl CommandLog for PrefixHiddenLog {
    fn read_header(&self) -> Result<Option<LogHeader>, DurableError> {
        self.inner.read_header()
    }

    fn write_header(&mut self, header: LogHeader) -> Result<(), DurableError> {
        self.inner.write_header(header)
    }

    fn append(&mut self, record: CommandRecord) -> Result<(), DurableError> {
        self.inner.append(record)
    }

    fn read_from(&self, from: u64) -> Result<Vec<CommandRecord>, DurableError> {
        Ok(self
            .inner
            .read_from(from)?
            .into_iter()
            .filter(|record| record.sequence > self.hide_through)
            .collect())
    }

    fn first_sequence(&self) -> Option<u64> {
        match self.inner.last_sequence() {
            Some(last) if last > self.hide_through => Some(self.hide_through + 1),
            _ => None,
        }
    }

    fn last_sequence(&self) -> Option<u64> {
        self.inner.last_sequence()
    }
}

#[test]
fn a_log_starting_more_than_one_past_the_snapshot_refuses_recovery() {
    let log = InMemoryCommandLog::new();
    let snapshots = InMemorySnapshotStore::new();
    let mut durable = DurableMatchingEngine::create(
        EngineConfig::default(),
        Box::new(log.handle()),
        Box::new(snapshots.handle()),
        0,
    )
    .unwrap();

    let commands = sample_commands();
    for command in commands.iter().take(2) {
        durable.execute(command.clone()).unwrap();
    }
    durable.snapshot_now().unwrap();
    for command in commands.iter().skip(2).take(3) {
        durable.execute(command.clone()).unwrap();
    }
    drop(durable);
    assert_eq!(snapshots.load_latest().unwrap().unwrap().applied_seq, 2);

    let error = match DurableMatchingEngine::open(
        Box::new(PrefixHiddenLog {
            inner: log.handle(),
            hide_through: 3,
        }),
        Box::new(snapshots.handle()),
        0,
    ) {
        Ok(_) => panic!("recovery must refuse a missing tail prefix"),
        Err(error) => error,
    };
    assert_eq!(
        error,
        DurableError::RecoveryGap {
            expected: 3,
            found: Some(4)
        }
    );
}

#[test]
fn a_restored_engine_starts_with_no_events_and_a_continuous_event_sequence() {
    let log = InMemoryCommandLog::new();
    let snapshots = InMemorySnapshotStore::new();
    let mut durable = DurableMatchingEngine::create(
        EngineConfig::default(),
        Box::new(log.handle()),
        Box::new(snapshots.handle()),
        0,
    )
    .unwrap();

    for command in sample_commands() {
        durable.execute(command).unwrap();
    }
    durable.snapshot_now().unwrap();

    let before = durable.engine().capture_snapshot();
    let before_last_sequence = durable.engine().last_sequence();
    assert!(
        !durable.engine().events().is_empty(),
        "live execution produced events"
    );
    drop(durable);

    let mut reopened =
        DurableMatchingEngine::open(Box::new(log.handle()), Box::new(snapshots.handle()), 0)
            .unwrap();

    assert!(
        reopened.engine().events().is_empty(),
        "events are derived outputs and are not recovered"
    );
    assert_eq!(
        reopened.engine().last_sequence(),
        before_last_sequence,
        "last_sequence derives from the restored event counter"
    );
    assert_eq!(reopened.engine().capture_snapshot(), before);

    reopened
        .execute(submit(ETH, limit(8, Side::Ask, 400, 2)))
        .unwrap();
    let after = reopened.engine().capture_snapshot();
    assert!(after.next_event_sequence > before.next_event_sequence);
    assert!(reopened.engine().last_sequence() > before_last_sequence);
}
