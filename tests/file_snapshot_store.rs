//! Atomic writes, retention, corruption fallback and the snapshot floor for
//! [`FileSnapshotStore`], plus end-to-end recovery with a file command log.
//!
//! Corrupt snapshots are produced by editing the bytes on disk; a "crash" is a
//! drop and reopen, never a sleep or a wall-clock assumption. Every test uses
//! its own temporary directory.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use orderbook_rs::durable::{
    CommandLog, CommandRecord, DurableError, DurableMatchingEngine, FORMAT_VERSION, FileCommandLog,
    FileSnapshotStore, LogHeader, SnapshotStore, StoredSnapshot,
};
use orderbook_rs::engine::MatchingEngine;
use orderbook_rs::types::{
    CommandOutcome, EngineCommand, EngineConfig, ModifyRequest, OrderRequest, OrderType, Side,
    StopKind, StopOrderRequest, TimeInForce,
};
use tempfile::tempdir;

const BTC: &str = "BTC-USDT";
const ETH: &str = "ETH-USDT";

// ---------------------------------------------------------------------------
// Fixtures and file helpers
// ---------------------------------------------------------------------------

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

fn submit(symbol: &str, request: OrderRequest) -> EngineCommand {
    EngineCommand::SubmitOrder {
        symbol: symbol.to_string(),
        request,
    }
}

/// A mixed stream: a trade, a modify, a cancel, a pending stop, a rejected
/// submission, and a final resting order.
fn recovery_commands() -> Vec<EngineCommand> {
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
        // Quantity zero: appended durably, then reported as rejected.
        submit(BTC, limit(6, Side::Bid, 100, 0)),
        submit(ETH, limit(7, Side::Ask, 300, 2)),
    ]
}

fn snapshot_at(applied_seq: u64) -> StoredSnapshot {
    StoredSnapshot {
        format_version: FORMAT_VERSION,
        applied_seq,
        snapshot: MatchingEngine::new(EngineConfig::default()).capture_snapshot(),
    }
}

fn encode_line(json: &[u8]) -> Vec<u8> {
    let crc = crc32fast::hash(json);
    let mut line = Vec::with_capacity(9 + json.len() + 1);
    line.extend_from_slice(format!("{crc:08x}").as_bytes());
    line.push(b' ');
    line.extend_from_slice(json);
    line.push(b'\n');
    line
}

fn file_name_for(applied_seq: u64) -> String {
    format!("snapshot-{applied_seq:020}.json")
}

fn snapshot_path(dir: &Path, applied_seq: u64) -> PathBuf {
    dir.join(file_name_for(applied_seq))
}

/// Every snapshot file's applied sequence, ascending. Temporary files are
/// deliberately excluded by requiring the exact `snapshot-NNN.json` name.
fn snapshot_sequences(dir: &Path) -> Vec<u64> {
    let mut sequences: Vec<u64> = fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap())
        .filter_map(|entry| {
            let name = entry.file_name().into_string().ok()?;
            let digits = name.strip_prefix("snapshot-")?.strip_suffix(".json")?;
            if digits.len() != 20 || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
                return None;
            }
            digits.parse().ok()
        })
        .collect();
    sequences.sort_unstable();
    sequences
}

/// Place a valid snapshot file without going through the store's floor/prune
/// logic, simulating a file left by an earlier run.
fn write_snapshot_file(dir: &Path, snapshot: &StoredSnapshot) {
    let json = serde_json::to_vec(snapshot).unwrap();
    fs::write(snapshot_path(dir, snapshot.applied_seq), encode_line(&json)).unwrap();
}

fn corrupt_bytes(path: &Path) {
    let mut bytes = fs::read(path).unwrap();
    let index = bytes.len() - 2; // last JSON byte, before the newline
    bytes[index] ^= 0xff;
    fs::write(path, bytes).unwrap();
}

fn truncate_file(path: &Path) {
    let bytes = fs::read(path).unwrap();
    fs::write(path, &bytes[..bytes.len() / 2]).unwrap();
}

// ---------------------------------------------------------------------------
// Store contract: round trip, retention, floor
// ---------------------------------------------------------------------------

#[test]
fn open_creates_the_directory_and_starts_empty() {
    let dir = tempdir().unwrap();
    let store_dir = dir.path().join("nested").join("snapshots");
    assert!(!store_dir.exists());

    let store = FileSnapshotStore::open(&store_dir).unwrap();

    assert!(
        store_dir.is_dir(),
        "open must create the snapshot directory"
    );
    assert_eq!(store.dir(), store_dir.as_path());
    assert!(store.load_latest().unwrap().is_none());
    assert!(store.load_previous().unwrap().is_none());
}

#[test]
fn save_load_and_prune_keep_the_two_newest_snapshots() {
    let dir = tempdir().unwrap();
    let store_dir = dir.path().join("snapshots");
    let mut store = FileSnapshotStore::open(&store_dir).unwrap();

    store.save(snapshot_at(1)).unwrap();
    assert_eq!(store.load_latest().unwrap().unwrap().applied_seq, 1);
    assert!(
        store.load_previous().unwrap().is_none(),
        "one snapshot has no previous selection"
    );

    store.save(snapshot_at(2)).unwrap();
    assert_eq!(store.load_latest().unwrap().unwrap().applied_seq, 2);
    assert_eq!(store.load_previous().unwrap().unwrap().applied_seq, 1);

    store.save(snapshot_at(3)).unwrap();
    assert_eq!(
        snapshot_sequences(&store_dir),
        vec![2, 3],
        "the oldest snapshot was pruned after a successful save"
    );
    assert_eq!(store.load_latest().unwrap().unwrap().applied_seq, 3);
    assert_eq!(store.load_previous().unwrap().unwrap().applied_seq, 2);

    store.save(snapshot_at(4)).unwrap();
    assert_eq!(snapshot_sequences(&store_dir), vec![3, 4]);
    assert_eq!(store.load_latest().unwrap().unwrap().applied_seq, 4);
    assert_eq!(store.load_previous().unwrap().unwrap().applied_seq, 3);
}

#[test]
fn a_refused_save_prunes_nothing_and_a_successful_one_prunes_older_files() {
    let dir = tempdir().unwrap();
    let store_dir = dir.path().join("snapshots");
    let mut store = FileSnapshotStore::open(&store_dir).unwrap();
    store.save(snapshot_at(5)).unwrap();
    store.save(snapshot_at(6)).unwrap();
    assert_eq!(snapshot_sequences(&store_dir), vec![5, 6]);

    // A stale third file placed out of band. A refused save must not remove it,
    // because pruning runs only after a successful save.
    write_snapshot_file(&store_dir, &snapshot_at(4));
    assert_eq!(snapshot_sequences(&store_dir), vec![4, 5, 6]);

    let error = store.save(snapshot_at(3)).unwrap_err();
    assert_eq!(
        error,
        DurableError::SnapshotBelowFloor {
            applied_seq: 3,
            floor: 6
        }
    );
    assert_eq!(
        snapshot_sequences(&store_dir),
        vec![4, 5, 6],
        "a rejected save leaves every file intact"
    );
    assert_eq!(store.load_latest().unwrap().unwrap().applied_seq, 6);

    // A successful save prunes everything but itself and the new previous one.
    store.save(snapshot_at(7)).unwrap();
    assert_eq!(snapshot_sequences(&store_dir), vec![6, 7]);
}

#[test]
fn saving_an_equal_applied_sequence_is_allowed() {
    let dir = tempdir().unwrap();
    let store_dir = dir.path().join("snapshots");
    let mut store = FileSnapshotStore::open(&store_dir).unwrap();
    store.save(snapshot_at(5)).unwrap();

    store
        .save(snapshot_at(5))
        .expect("an equal applied sequence is allowed");

    assert_eq!(snapshot_sequences(&store_dir), vec![5]);
    assert_eq!(store.load_latest().unwrap().unwrap().applied_seq, 5);
    assert!(store.load_previous().unwrap().is_none());
}

// ---------------------------------------------------------------------------
// Corruption fallback and versioning
// ---------------------------------------------------------------------------

#[test]
fn a_crc_corrupt_newest_snapshot_falls_back_to_the_previous_one() {
    let dir = tempdir().unwrap();
    let store_dir = dir.path().join("snapshots");
    let mut store = FileSnapshotStore::open(&store_dir).unwrap();
    store.save(snapshot_at(3)).unwrap();
    store.save(snapshot_at(4)).unwrap();

    corrupt_bytes(&snapshot_path(&store_dir, 4));

    assert_eq!(
        store.load_latest().unwrap().unwrap().applied_seq,
        3,
        "a corrupt newest file is skipped"
    );
    assert!(
        store.load_previous().unwrap().is_none(),
        "only one complete snapshot remains"
    );
}

#[test]
fn a_truncated_newest_snapshot_falls_back_to_the_previous_one() {
    let dir = tempdir().unwrap();
    let store_dir = dir.path().join("snapshots");
    let mut store = FileSnapshotStore::open(&store_dir).unwrap();
    store.save(snapshot_at(3)).unwrap();
    store.save(snapshot_at(4)).unwrap();

    truncate_file(&snapshot_path(&store_dir, 4));

    assert_eq!(store.load_latest().unwrap().unwrap().applied_seq, 3);
}

#[test]
fn a_version_mismatched_but_valid_snapshot_is_returned_for_the_durable_layer() {
    let dir = tempdir().unwrap();
    let store_dir = dir.path().join("snapshots");
    let mut store = FileSnapshotStore::open(&store_dir).unwrap();
    store.save(snapshot_at(1)).unwrap();

    let mut mismatched = snapshot_at(2);
    mismatched.format_version = FORMAT_VERSION + 1;
    store
        .save(mismatched.clone())
        .expect("the store does not validate the format version");

    let latest = store.load_latest().unwrap().unwrap();
    assert_eq!(latest, mismatched);
    assert_eq!(
        latest.format_version,
        FORMAT_VERSION + 1,
        "an unknown version must be surfaced to the durable layer, not skipped"
    );
}

// ---------------------------------------------------------------------------
// End-to-end: file command log plus file snapshot store
// ---------------------------------------------------------------------------

/// A command-log decorator that records the `from` argument of every
/// `read_from` call and how many records came back, so a test can prove only
/// the post-snapshot tail is replayed.
struct CountingLog {
    inner: FileCommandLog,
    reads: std::sync::Arc<Mutex<Vec<u64>>>,
    records_read: std::sync::Arc<AtomicUsize>,
}

impl CountingLog {
    fn new(inner: FileCommandLog) -> Self {
        Self {
            inner,
            reads: std::sync::Arc::new(Mutex::new(Vec::new())),
            records_read: std::sync::Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl CommandLog for CountingLog {
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
        self.records_read.fetch_add(records.len(), Ordering::SeqCst);
        Ok(records)
    }

    fn first_sequence(&self) -> Option<u64> {
        self.inner.first_sequence()
    }

    fn last_sequence(&self) -> Option<u64> {
        self.inner.last_sequence()
    }
}

const CADENCE: u64 = 3;

/// Execute the mixed stream with cadence 3 and return the pre-crash decision
/// state. Cadence boundaries snapshot at sequences 3 and 6; the crash happens
/// after sequence 8, so the tail 7..=8 must be replayed.
fn build_file_backed_engine(
    dir: &Path,
) -> (Vec<EngineCommand>, orderbook_rs::snapshot::EngineSnapshot) {
    let log_path = dir.join("commands.log");
    let commands = recovery_commands();
    let durable = {
        let log = FileCommandLog::open(&log_path).unwrap();
        let snapshots = FileSnapshotStore::open(dir).unwrap();
        let mut durable = DurableMatchingEngine::create(
            EngineConfig::default(),
            Box::new(log),
            Box::new(snapshots),
            CADENCE,
        )
        .unwrap();

        let mut rejected = 0;
        for command in &commands {
            if let CommandOutcome::Rejected(_) = durable.execute(command.clone()).unwrap() {
                rejected += 1;
            }
        }
        assert_eq!(
            rejected, 1,
            "the zero-quantity command is rejected but durable"
        );
        durable
    };
    let before_crash = durable.engine().capture_snapshot();
    drop(durable);
    (commands, before_crash)
}

#[test]
fn file_backed_recovery_replays_only_the_tail_after_the_snapshot() {
    // Crash window: sequences 1..8 are durable and applied; snapshots exist at
    // 3 and 6, so the process died after apply and before the sequence-9
    // cadence snapshot.
    let dir = tempdir().unwrap();
    let (commands, before_crash) = build_file_backed_engine(dir.path());
    assert_eq!(commands.len(), 8);

    let latest = FileSnapshotStore::open(dir.path())
        .unwrap()
        .load_latest()
        .unwrap()
        .expect("cadence 3 must have written a snapshot");
    assert_eq!(latest.applied_seq, 6);

    let log = CountingLog::new(FileCommandLog::open(dir.path().join("commands.log")).unwrap());
    let reads = log.reads.clone();
    let records_read = log.records_read.clone();
    let reopened = DurableMatchingEngine::open(
        Box::new(log),
        Box::new(FileSnapshotStore::open(dir.path()).unwrap()),
        CADENCE,
    )
    .unwrap();

    assert_eq!(reopened.applied_seq(), 8);
    assert_eq!(
        reopened.engine().capture_snapshot(),
        before_crash,
        "snapshot plus tail must rebuild exact decision state across a restart"
    );
    assert_eq!(
        *reads.lock().unwrap(),
        vec![7],
        "recovery reads only the tail after the applied snapshot"
    );
    assert_eq!(
        records_read.load(Ordering::SeqCst),
        2,
        "only sequences 7 and 8 are replayed"
    );
}

#[test]
fn recovery_falls_back_to_the_previous_snapshot_when_the_newest_is_corrupt() {
    let dir = tempdir().unwrap();
    let (commands, before_crash) = build_file_backed_engine(dir.path());
    assert_eq!(commands.len(), 8);

    let newest = snapshot_path(dir.path(), 6);
    assert!(newest.is_file());
    corrupt_bytes(&newest);

    let log = CountingLog::new(FileCommandLog::open(dir.path().join("commands.log")).unwrap());
    let reads = log.reads.clone();
    let records_read = log.records_read.clone();
    let reopened = DurableMatchingEngine::open(
        Box::new(log),
        Box::new(FileSnapshotStore::open(dir.path()).unwrap()),
        CADENCE,
    )
    .expect("recovery must fall back to the previous complete snapshot");

    assert_eq!(reopened.applied_seq(), 8);
    assert_eq!(
        reopened.engine().capture_snapshot(),
        before_crash,
        "the previous snapshot plus the larger tail recovers the same state"
    );
    assert_eq!(
        *reads.lock().unwrap(),
        vec![4],
        "recovery re-reads from just after the previous snapshot"
    );
    assert_eq!(records_read.load(Ordering::SeqCst), 5);
}

// ---------------------------------------------------------------------------
// Durable-layer validation with file-backed stores
// ---------------------------------------------------------------------------

#[test]
fn an_unsupported_snapshot_version_in_a_file_refuses_recovery() {
    let dir = tempdir().unwrap();
    let log_path = dir.path().join("commands.log");
    {
        let log = FileCommandLog::open(&log_path).unwrap();
        let snapshots = FileSnapshotStore::open(dir.path()).unwrap();
        let mut durable = DurableMatchingEngine::create(
            EngineConfig::default(),
            Box::new(log),
            Box::new(snapshots),
            0,
        )
        .unwrap();
        durable
            .execute(submit(BTC, limit(1, Side::Bid, 100, 5)))
            .unwrap();
        durable.snapshot_now().unwrap();
    }

    // Replace the valid snapshot with a checksum-valid file of a future format.
    let mut store = FileSnapshotStore::open(dir.path()).unwrap();
    let mut stored = store.load_latest().unwrap().unwrap();
    stored.format_version = FORMAT_VERSION + 1;
    store.save(stored).unwrap();

    let log = FileCommandLog::open(&log_path).unwrap();
    let error = match DurableMatchingEngine::open(
        Box::new(log),
        Box::new(FileSnapshotStore::open(dir.path()).unwrap()),
        0,
    ) {
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
fn a_file_snapshot_with_a_different_config_refuses_recovery() {
    let dir = tempdir().unwrap();
    let log_path = dir.path().join("commands.log");
    let mut log = FileCommandLog::open(&log_path).unwrap();
    log.write_header(LogHeader {
        format_version: FORMAT_VERSION,
        config: EngineConfig::default(),
    })
    .unwrap();

    let other_config = EngineConfig {
        min_order_quantity: 1,
        max_order_quantity: 1_000,
        tick_size: 5,
    };
    let mut store = FileSnapshotStore::open(dir.path()).unwrap();
    store
        .save(StoredSnapshot {
            format_version: FORMAT_VERSION,
            applied_seq: 0,
            snapshot: MatchingEngine::new(other_config).capture_snapshot(),
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
            snapshot: other_config
        }
    );
}

#[test]
fn a_hand_written_file_log_starting_at_sequence_two_refuses_recovery() {
    let dir = tempdir().unwrap();
    let log_path = dir.path().join("commands.log");
    let header = LogHeader {
        format_version: FORMAT_VERSION,
        config: EngineConfig::default(),
    };
    let first_record = CommandRecord {
        format_version: FORMAT_VERSION,
        sequence: 2,
        command: submit(BTC, limit(1, Side::Bid, 100, 1)),
    };

    // Hand-written with valid CRCs, bypassing the append contiguity check.
    let mut bytes = encode_line(&serde_json::to_vec(&header).unwrap());
    bytes.extend(encode_line(&serde_json::to_vec(&first_record).unwrap()));
    fs::write(&log_path, bytes).unwrap();

    let log = FileCommandLog::open(&log_path).unwrap();
    assert_eq!(log.first_sequence(), Some(2));

    let error = match DurableMatchingEngine::open(
        Box::new(log),
        Box::new(FileSnapshotStore::open(dir.path()).unwrap()),
        0,
    ) {
        Ok(_) => panic!("recovery must refuse a log that starts too late"),
        Err(error) => error,
    };
    assert_eq!(
        error,
        DurableError::RecoveryGap {
            expected: 1,
            found: Some(2)
        }
    );
}
