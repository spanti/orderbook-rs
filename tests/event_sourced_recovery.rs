//! Recovery tests for the event-sourced path: restore the latest event
//! snapshot, validate it against the log, then fold the contiguous event tail.
//!
//! The scenarios use the resting-submit event path (ticket 01) so they do not
//! depend on tickets 02-04. Failures are injected through the storage seams:
//! the log adapter controls ordering, duplicates and gaps, and the snapshot
//! store supplies the boundary.

use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};

use orderbook_rs::durable::{
    DurableError, DurableMatchingEngine, EventLog, EventRecord, FORMAT_VERSION, FileEventLog,
    FileSnapshotStore, InMemoryEventLog, InMemorySnapshotStore, LogHeader, SnapshotStore,
    StoredSnapshot,
};
use orderbook_rs::engine::MatchingEngine;
use orderbook_rs::event::DomainEvent;
use orderbook_rs::types::{
    EngineCommand, EngineConfig, Order, OrderRequest, OrderType, Side, TimeInForce,
};
use tempfile::tempdir;

const BTC: &str = "BTC-USDT";

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn header(config: EngineConfig) -> LogHeader {
    LogHeader {
        format_version: FORMAT_VERSION,
        config,
    }
}

fn limit(price: u64, timestamp: u64) -> OrderRequest {
    OrderRequest {
        side: Side::Bid,
        price,
        quantity: 1,
        timestamp,
        order_type: OrderType::Limit,
        time_in_force: TimeInForce::GTC,
        post_only: false,
    }
}

fn submit(price: u64, timestamp: u64) -> EngineCommand {
    EngineCommand::SubmitOrder {
        symbol: BTC.to_string(),
        request: limit(price, timestamp),
    }
}

fn order(id: u64, price: u64) -> Order {
    Order {
        id,
        side: Side::Bid,
        price,
        quantity: 1,
        timestamp: id,
        order_type: OrderType::Limit,
        time_in_force: TimeInForce::GTC,
    }
}

fn accepted(id: u64, price: u64) -> DomainEvent {
    DomainEvent::OrderAccepted {
        symbol: BTC.to_string(),
        order: order(id, price),
        post_only: false,
    }
}

fn rested(id: u64, price: u64) -> DomainEvent {
    DomainEvent::OrderRested {
        symbol: BTC.to_string(),
        order: order(id, price),
    }
}

/// Records for `commands` resting submits: two events and one command id each.
fn resting_records(commands: u64) -> Vec<EventRecord> {
    let mut records = Vec::new();
    for command in 1..=commands {
        let price = 100 - command;
        records.push(EventRecord {
            format_version: FORMAT_VERSION,
            sequence: command * 2 - 1,
            command_id: command,
            event: accepted(command, price),
        });
        records.push(EventRecord {
            format_version: FORMAT_VERSION,
            sequence: command * 2,
            command_id: command,
            event: rested(command, price),
        });
    }
    records
}

fn fresh_engine(
    log: &InMemoryEventLog,
    snapshots: &InMemorySnapshotStore,
) -> DurableMatchingEngine {
    DurableMatchingEngine::create(
        EngineConfig::default(),
        Box::new(log.handle()),
        Box::new(snapshots.handle()),
    )
    .expect("a fresh event log accepts the v2 header")
}

fn fold_full_log(log: &InMemoryEventLog) -> DurableMatchingEngine {
    DurableMatchingEngine::open(
        Box::new(log.handle()),
        Box::new(InMemorySnapshotStore::new()),
    )
    .expect("a contiguous log folds from empty")
}

// ---------------------------------------------------------------------------
// Read-only / fault-injecting log adapters
// ---------------------------------------------------------------------------

/// A read-only log serving an exact record stream, so a test can create gaps or
/// duplicates that the real adapters would reject on append.
struct FixedEventLog {
    header: LogHeader,
    records: Vec<EventRecord>,
}

impl EventLog for FixedEventLog {
    fn read_header(&self) -> Result<Option<LogHeader>, DurableError> {
        Ok(Some(self.header.clone()))
    }

    fn write_header(&mut self, _header: LogHeader) -> Result<(), DurableError> {
        Err(DurableError::HeaderAlreadyPresent)
    }

    fn append(&mut self, _record: EventRecord) -> Result<(), DurableError> {
        Err(DurableError::Storage("read-only test log".into()))
    }

    fn read_from(&self, from: u64) -> Result<Vec<EventRecord>, DurableError> {
        Ok(self
            .records
            .iter()
            .filter(|record| record.sequence >= from)
            .cloned()
            .collect())
    }

    fn first_sequence(&self) -> Option<u64> {
        self.records.first().map(|record| record.sequence)
    }

    fn last_stored_sequence(&self) -> Option<u64> {
        self.records.last().map(|record| record.sequence)
    }

    fn last_command_id(&self) -> Option<u64> {
        self.records.last().map(|record| record.command_id)
    }
}

/// At-least-once delivery: every `read_from(n > 1)` re-delivers `n - 1` before
/// the requested records, so recovery must skip a sequence it already applied.
struct DuplicatingEventLog {
    inner: InMemoryEventLog,
}

impl DuplicatingEventLog {
    fn new(inner: InMemoryEventLog) -> Self {
        Self { inner }
    }
}

impl EventLog for DuplicatingEventLog {
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
        self.inner.append_batch(records)
    }

    fn read_from(&self, from: u64) -> Result<Vec<EventRecord>, DurableError> {
        let mut records = self.inner.read_from(from)?;
        if from > 1 {
            let previous = self.inner.read_from(from - 1)?;
            if let Some(duplicate) = previous
                .into_iter()
                .find(|record| record.sequence == from - 1)
            {
                records.insert(0, duplicate);
            }
        }
        Ok(records)
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

/// Records the `from` argument of every `read_from` call, proving recovery
/// reads only the tail after the snapshot boundary.
struct CountingEventLog {
    inner: FileEventLog,
    reads: Arc<Mutex<Vec<u64>>>,
}

impl CountingEventLog {
    fn new(inner: FileEventLog) -> Self {
        Self {
            inner,
            reads: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl EventLog for CountingEventLog {
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
        self.inner.append_batch(records)
    }

    fn read_from(&self, from: u64) -> Result<Vec<EventRecord>, DurableError> {
        self.reads.lock().unwrap().push(from);
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

// ---------------------------------------------------------------------------
// Snapshot plus tail
// ---------------------------------------------------------------------------

#[test]
fn snapshot_plus_event_tail_recovers_exactly_the_full_log_fold() {
    let log = InMemoryEventLog::new();
    let snapshots = InMemorySnapshotStore::new();

    let expected = {
        let mut engine = fresh_engine(&log, &snapshots);
        engine.execute(submit(99, 1)).unwrap();
        engine.execute(submit(98, 2)).unwrap();
        engine.snapshot_now().expect("snapshot at sequence 4");
        // Applied but not snapshotted: the crash happens after this command.
        engine.execute(submit(97, 3)).unwrap();
        engine.engine().capture_snapshot()
    };

    assert_eq!(
        snapshots.load_latest().unwrap().unwrap().applied_seq,
        4,
        "the snapshot stops at the command boundary before the crash"
    );

    let reopened =
        DurableMatchingEngine::open(Box::new(log.handle()), Box::new(snapshots.handle()))
            .expect("snapshot plus contiguous tail recovers");
    assert_eq!(reopened.applied_seq(), 6);
    assert_eq!(reopened.engine().capture_snapshot(), expected);

    // Exact Recovery: the same state as folding the entire log from empty.
    let full = fold_full_log(&log);
    assert_eq!(
        reopened.engine().capture_snapshot(),
        full.engine().capture_snapshot()
    );
    assert_eq!(reopened.cursor().next_command_id, 4);
}

#[test]
fn recovery_reads_only_the_tail_after_the_snapshot_boundary() {
    let dir = tempdir().unwrap();
    let log_path = dir.path().join("events.log");
    let snapshot_dir = dir.path().join("snapshots");

    {
        let mut engine = DurableMatchingEngine::create(
            EngineConfig::default(),
            Box::new(FileEventLog::open(&log_path).unwrap()),
            Box::new(FileSnapshotStore::open(&snapshot_dir).unwrap()),
        )
        .unwrap();
        engine.execute(submit(99, 1)).unwrap();
        engine.execute(submit(98, 2)).unwrap();
        engine.snapshot_now().unwrap();
        engine.execute(submit(97, 3)).unwrap();
        engine.execute(submit(96, 4)).unwrap();
    }

    let log = CountingEventLog::new(FileEventLog::open(&log_path).unwrap());
    let reads = log.reads.clone();
    let reopened = DurableMatchingEngine::open(
        Box::new(log),
        Box::new(FileSnapshotStore::open(&snapshot_dir).unwrap()),
    )
    .expect("file event log plus file event snapshot recovers");

    assert_eq!(reopened.applied_seq(), 8);
    assert_eq!(
        *reads.lock().unwrap(),
        vec![5],
        "recovery must start at snapshot.applied_seq + 1"
    );
}

#[test]
fn an_applied_but_not_snapshotted_tail_is_folded_after_recovery() {
    let log = InMemoryEventLog::new();
    let snapshots = InMemorySnapshotStore::new();

    {
        let mut engine = fresh_engine(&log, &snapshots);
        engine.execute(submit(99, 1)).unwrap();
        engine.snapshot_now().unwrap();
        engine.execute(submit(98, 2)).unwrap();
    }

    let latest = snapshots.load_latest().unwrap().unwrap();
    assert_eq!(latest.applied_seq, 2);
    assert_eq!(latest.last_command_id, 1);

    let reopened =
        DurableMatchingEngine::open(Box::new(log.handle()), Box::new(snapshots.handle())).unwrap();
    assert_eq!(
        reopened.applied_seq(),
        4,
        "the unsnapshotted command's events are replayed"
    );
    assert_eq!(reopened.engine().order_count(), 2);
    assert_eq!(reopened.cursor().next_command_id, 3);
}

#[test]
fn command_ids_continue_from_the_snapshot_when_the_log_tail_is_empty() {
    let log = InMemoryEventLog::new();
    let snapshots = InMemorySnapshotStore::new();

    {
        let mut engine = fresh_engine(&log, &snapshots);
        engine.execute(submit(99, 1)).unwrap();
        engine.execute(submit(98, 2)).unwrap();
        engine.snapshot_now().unwrap();
    }

    let mut reopened =
        DurableMatchingEngine::open(Box::new(log.handle()), Box::new(snapshots.handle())).unwrap();
    assert_eq!(reopened.applied_seq(), 4);
    assert_eq!(reopened.cursor().next_command_id, 3);

    reopened.execute(submit(97, 3)).unwrap();
    let tail = reopened.history_from(5).unwrap();
    assert_eq!(tail.len(), 2);
    assert!(
        tail.iter().all(|record| record.command_id == 3),
        "command ids must not restart after a snapshot-only recovery"
    );
}

#[test]
fn command_ids_continue_from_the_maximum_of_snapshot_and_log_tail() {
    let log = InMemoryEventLog::new();
    let mut writer = log.handle();
    writer
        .write_header(header(EngineConfig::default()))
        .unwrap();
    // Sequence 3 was written by a later command than the snapshot records.
    for record in resting_records(1) {
        writer.append(record).unwrap();
    }
    writer
        .append(EventRecord {
            format_version: FORMAT_VERSION,
            sequence: 3,
            command_id: 7,
            event: accepted(2, 95),
        })
        .unwrap();
    writer
        .append(EventRecord {
            format_version: FORMAT_VERSION,
            sequence: 4,
            command_id: 7,
            event: rested(2, 95),
        })
        .unwrap();

    // The snapshot tracks sequence 2 (one accepted and rested order) so the
    // folded tail is coherent.
    let mut reference = MatchingEngine::new(EngineConfig::default());
    for record in resting_records(1) {
        reference.apply(&record.event, record.sequence);
    }
    let store = store_with_snapshot(StoredSnapshot {
        format_version: FORMAT_VERSION,
        applied_seq: 2,
        last_command_id: 1,
        snapshot: reference.capture_snapshot(),
    });

    let mut reopened =
        DurableMatchingEngine::open(Box::new(log.handle()), Box::new(store)).unwrap();
    assert_eq!(
        reopened.cursor().next_command_id,
        8,
        "recovery takes max(snapshot.last_command_id, last log command_id) + 1"
    );

    reopened.execute(submit(94, 5)).unwrap();
    let tail = reopened.history_from(5).unwrap();
    assert!(tail.iter().all(|record| record.command_id == 8));
}

// ---------------------------------------------------------------------------
// Duplicate, gap and non-contiguous sequences
// ---------------------------------------------------------------------------

#[test]
fn a_duplicate_record_at_or_below_the_applied_sequence_is_skipped() {
    let log = InMemoryEventLog::new();
    let snapshots = InMemorySnapshotStore::new();

    {
        let mut engine = fresh_engine(&log, &snapshots);
        engine.execute(submit(99, 1)).unwrap();
        engine.execute(submit(98, 2)).unwrap();
        engine.snapshot_now().unwrap();
        engine.execute(submit(97, 3)).unwrap();
    }

    let expected = fold_full_log(&log).engine().capture_snapshot();
    let reopened = DurableMatchingEngine::open(
        Box::new(DuplicatingEventLog::new(log.handle())),
        Box::new(snapshots.handle()),
    )
    .expect("a duplicate already applied is skipped, not a gap");
    assert_eq!(reopened.applied_seq(), 6);
    assert_eq!(reopened.engine().capture_snapshot(), expected);
}

#[test]
fn a_gap_between_the_snapshot_and_the_log_aborts_recovery() {
    let records = resting_records(2);
    let mut boundary = MatchingEngine::new(EngineConfig::default());
    for record in &records[..2] {
        boundary.apply(&record.event, record.sequence);
    }
    let store = store_with_snapshot(StoredSnapshot {
        format_version: FORMAT_VERSION,
        applied_seq: 2,
        last_command_id: 1,
        snapshot: boundary.capture_snapshot(),
    });
    // After the snapshot boundary the log resumes at sequence 4, one short.
    let log = FixedEventLog {
        header: header(EngineConfig::default()),
        records: records[3..].to_vec(),
    };

    let error = match DurableMatchingEngine::open(Box::new(log), Box::new(store)) {
        Ok(_) => panic!("recovery must refuse a gap after the snapshot"),
        Err(error) => error,
    };
    assert_eq!(
        error,
        DurableError::RecoveryGap {
            expected: 3,
            found: Some(4),
        }
    );
}

#[test]
fn a_log_that_starts_after_the_first_expected_sequence_refuses_recovery() {
    // No snapshot: the log must start at sequence 1. Starting at 3 is a gap.
    let log = FixedEventLog {
        header: header(EngineConfig::default()),
        records: resting_records(2)[2..].to_vec(),
    };

    let error =
        match DurableMatchingEngine::open(Box::new(log), Box::new(InMemorySnapshotStore::new())) {
            Ok(_) => panic!("recovery must refuse a log that starts too late"),
            Err(error) => error,
        };
    assert_eq!(
        error,
        DurableError::RecoveryGap {
            expected: 1,
            found: Some(3),
        }
    );
}

#[test]
fn a_non_contiguous_sequence_in_the_tail_aborts_recovery() {
    let records = resting_records(2);
    let mut tail = records.clone();
    tail.remove(2); // drop sequence 3, leaving 1, 2, 4
    let log = FixedEventLog {
        header: header(EngineConfig::default()),
        records: tail,
    };

    let error =
        match DurableMatchingEngine::open(Box::new(log), Box::new(InMemorySnapshotStore::new())) {
            Ok(_) => panic!("recovery must refuse a gap inside the tail"),
            Err(error) => error,
        };
    assert_eq!(
        error,
        DurableError::NonContiguousSequence {
            expected: 3,
            found: 4,
        }
    );
}

// ---------------------------------------------------------------------------
// Snapshot validation: ahead, version, config
// ---------------------------------------------------------------------------

fn store_with_snapshot(snapshot: StoredSnapshot) -> InMemorySnapshotStore {
    let mut store = InMemorySnapshotStore::new();
    store.save(snapshot).unwrap();
    store
}

fn empty_snapshot_at(applied_seq: u64, last_command_id: u64) -> StoredSnapshot {
    StoredSnapshot {
        format_version: FORMAT_VERSION,
        applied_seq,
        last_command_id,
        snapshot: MatchingEngine::new(EngineConfig::default()).capture_snapshot(),
    }
}

#[test]
fn a_snapshot_ahead_of_the_log_is_refused() {
    let log = FixedEventLog {
        header: header(EngineConfig::default()),
        records: resting_records(2),
    };
    let store = store_with_snapshot(empty_snapshot_at(6, 2));

    let error = match DurableMatchingEngine::open(Box::new(log), Box::new(store)) {
        Ok(_) => panic!("recovery must refuse a snapshot ahead of the log"),
        Err(error) => error,
    };
    assert_eq!(
        error,
        DurableError::SnapshotAhead {
            applied_seq: 6,
            log_last_seq: 4,
        }
    );
}

#[test]
fn an_unsupported_snapshot_version_is_refused() {
    let log = FixedEventLog {
        header: header(EngineConfig::default()),
        records: resting_records(2),
    };
    let mut snapshot = empty_snapshot_at(2, 1);
    snapshot.format_version = 1;
    let store = store_with_snapshot(snapshot);

    assert_eq!(
        match DurableMatchingEngine::open(Box::new(log), Box::new(store)) {
            Ok(_) => panic!("recovery must refuse a v1 snapshot"),
            Err(error) => error,
        },
        DurableError::UnsupportedVersion {
            found: 1,
            supported: FORMAT_VERSION,
        }
    );
}

#[test]
fn a_snapshot_with_a_different_config_is_refused() {
    let log = FixedEventLog {
        header: header(EngineConfig::default()),
        records: Vec::new(),
    };
    let other_config = EngineConfig {
        min_order_quantity: 1,
        max_order_quantity: 1_000,
        tick_size: 5,
    };
    let mut snapshot = empty_snapshot_at(0, 0);
    snapshot.snapshot = MatchingEngine::new(other_config).capture_snapshot();
    let store = store_with_snapshot(snapshot);

    assert_eq!(
        match DurableMatchingEngine::open(Box::new(log), Box::new(store)) {
            Ok(_) => panic!("recovery must refuse a config mismatch"),
            Err(error) => error,
        },
        DurableError::ConfigMismatch {
            header: EngineConfig::default(),
            snapshot: other_config,
        }
    );
}

// ---------------------------------------------------------------------------
// File-backed end to end: v1 refusal and corrupt-newest fallback
// ---------------------------------------------------------------------------

fn snapshot_file(dir: &Path, applied_seq: u64) -> std::path::PathBuf {
    dir.join(format!("snapshot-{applied_seq:020}.json"))
}

fn write_snapshot_line(path: &Path, value: &impl serde::Serialize) {
    let json = serde_json::to_vec(value).unwrap();
    let mut line = format!("{:08x} ", crc32fast::hash(&json)).into_bytes();
    line.extend_from_slice(&json);
    line.push(b'\n');
    fs::write(path, line).unwrap();
}

#[test]
fn a_v1_file_snapshot_is_refused_by_recovery() {
    let dir = tempdir().unwrap();
    let log_path = dir.path().join("events.log");
    let snapshot_dir = dir.path().join("snapshots");
    {
        let mut engine = DurableMatchingEngine::create(
            EngineConfig::default(),
            Box::new(FileEventLog::open(&log_path).unwrap()),
            Box::new(FileSnapshotStore::open(&snapshot_dir).unwrap()),
        )
        .unwrap();
        engine.execute(submit(99, 1)).unwrap();
    }

    // A checksum-valid command-sourced snapshot from the v1 format. The store
    // surfaces it; recovery must refuse its version, not skip it.
    let v1 = StoredSnapshot {
        format_version: 1,
        applied_seq: 2,
        last_command_id: 0,
        snapshot: MatchingEngine::new(EngineConfig::default()).capture_snapshot(),
    };
    write_snapshot_line(&snapshot_file(&snapshot_dir, 2), &v1);

    let error = DurableMatchingEngine::open(
        Box::new(FileEventLog::open(&log_path).unwrap()),
        Box::new(FileSnapshotStore::open(&snapshot_dir).unwrap()),
    )
    .unwrap_err();
    assert_eq!(
        error,
        DurableError::UnsupportedVersion {
            found: 1,
            supported: FORMAT_VERSION,
        }
    );
}

#[test]
fn recovery_falls_back_to_the_previous_snapshot_when_the_newest_is_corrupt() {
    let dir = tempdir().unwrap();
    let log_path = dir.path().join("events.log");
    let snapshot_dir = dir.path().join("snapshots");

    let expected = {
        let mut engine = DurableMatchingEngine::create(
            EngineConfig::default(),
            Box::new(FileEventLog::open(&log_path).unwrap()),
            Box::new(FileSnapshotStore::open(&snapshot_dir).unwrap()),
        )
        .unwrap();
        engine.execute(submit(99, 1)).unwrap();
        engine.snapshot_now().unwrap(); // event sequence 2
        engine.execute(submit(98, 2)).unwrap();
        engine.snapshot_now().unwrap(); // event sequence 4
        engine.execute(submit(97, 3)).unwrap(); // applied, not snapshotted
        engine.engine().capture_snapshot()
    };

    // Corrupt the newest snapshot file; the previous one plus the longer tail
    // must recover the same state.
    let newest = snapshot_file(&snapshot_dir, 4);
    let mut bytes = fs::read(&newest).unwrap();
    let index = bytes.len() - 2; // last JSON byte, before the newline
    bytes[index] ^= 0xff;
    fs::write(&newest, bytes).unwrap();

    let reopened = DurableMatchingEngine::open(
        Box::new(FileEventLog::open(&log_path).unwrap()),
        Box::new(FileSnapshotStore::open(&snapshot_dir).unwrap()),
    )
    .expect("the previous complete snapshot is used");
    assert_eq!(reopened.applied_seq(), 6);
    assert_eq!(reopened.engine().capture_snapshot(), expected);
}
