//! Contract, retention, framing and corruption-fallback tests for the
//! event-sequence snapshot stores.
//!
//! [`StoredSnapshot`] is tagged with the last applied **event** sequence and the
//! last `command_id`, not with a command sequence. Both the in-memory and file
//! adapters keep the two newest complete snapshots so a corrupt newest file can
//! fall back to the previous one. A version-mismatched snapshot is deliberately
//! returned, not skipped: the durable layer validates it and fails loudly.

use std::fs;
use std::path::{Path, PathBuf};

use orderbook_rs::durable::{
    DurableError, FORMAT_VERSION, FileSnapshotStore, InMemorySnapshotStore, SnapshotStore,
    StoredSnapshot,
};
use orderbook_rs::engine::MatchingEngine;
use orderbook_rs::types::EngineConfig;
use tempfile::tempdir;

// ---------------------------------------------------------------------------
// Fixtures and file helpers
// ---------------------------------------------------------------------------

fn snapshot_at(applied_seq: u64, last_command_id: u64) -> StoredSnapshot {
    StoredSnapshot {
        format_version: FORMAT_VERSION,
        applied_seq,
        last_command_id,
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

fn snapshot_path(dir: &Path, applied_seq: u64) -> PathBuf {
    dir.join(format!("snapshot-{applied_seq:020}.json"))
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

fn contract_round_trip_and_retention(store: &mut dyn SnapshotStore) {
    assert!(store.load_latest().unwrap().is_none());
    assert!(store.load_previous().unwrap().is_none());

    store.save(snapshot_at(2, 1)).unwrap();
    assert_eq!(store.load_latest().unwrap().unwrap().applied_seq, 2);
    assert_eq!(store.load_latest().unwrap().unwrap().last_command_id, 1);
    assert!(store.load_previous().unwrap().is_none());

    store.save(snapshot_at(4, 2)).unwrap();
    assert_eq!(store.load_latest().unwrap().unwrap().applied_seq, 4);
    assert_eq!(store.load_previous().unwrap().unwrap().applied_seq, 2);

    store.save(snapshot_at(6, 3)).unwrap();
    assert_eq!(store.load_latest().unwrap().unwrap().applied_seq, 6);
    assert_eq!(store.load_previous().unwrap().unwrap().applied_seq, 4);
}

#[test]
fn the_in_memory_store_round_trips_and_keeps_two_snapshots() {
    contract_round_trip_and_retention(&mut InMemorySnapshotStore::new());
}

#[test]
fn the_file_store_round_trips_and_keeps_two_snapshots() {
    let dir = tempdir().unwrap();
    let store_dir = dir.path().join("snapshots");
    let mut store = FileSnapshotStore::open(&store_dir).unwrap();

    contract_round_trip_and_retention(&mut store);
    assert_eq!(
        snapshot_sequences(&store_dir),
        vec![4, 6],
        "the oldest snapshot was pruned after a successful save"
    );
    assert_eq!(store.dir(), store_dir.as_path());
}

fn contract_refuses_below_floor(store: &mut dyn SnapshotStore) {
    store.save(snapshot_at(5, 2)).unwrap();
    store.save(snapshot_at(6, 3)).unwrap();
    assert_eq!(
        store.save(snapshot_at(3, 1)),
        Err(DurableError::SnapshotBelowFloor {
            applied_seq: 3,
            floor: 6,
        })
    );
    assert_eq!(store.load_latest().unwrap().unwrap().applied_seq, 6);

    store
        .save(snapshot_at(6, 3))
        .expect("an equal applied sequence is allowed");
}

#[test]
fn both_stores_refuse_a_snapshot_below_the_floor() {
    contract_refuses_below_floor(&mut InMemorySnapshotStore::new());

    let dir = tempdir().unwrap();
    let mut store = FileSnapshotStore::open(dir.path().join("snapshots")).unwrap();
    contract_refuses_below_floor(&mut store);
}

// ---------------------------------------------------------------------------
// File-specific: framing and corrupt-newest fallback
// ---------------------------------------------------------------------------

#[test]
fn open_creates_the_directory_and_starts_empty() {
    let dir = tempdir().unwrap();
    let store_dir = dir.path().join("nested").join("snapshots");
    assert!(!store_dir.exists());

    let store = FileSnapshotStore::open(&store_dir).unwrap();

    assert!(store_dir.is_dir());
    assert!(store.load_latest().unwrap().is_none());
    assert!(store.load_previous().unwrap().is_none());
}

#[test]
fn the_file_store_uses_the_v2_checksummed_framing() {
    let dir = tempdir().unwrap();
    let store_dir = dir.path().join("snapshots");
    let mut store = FileSnapshotStore::open(&store_dir).unwrap();
    let saved = snapshot_at(3, 2);
    store.save(saved.clone()).unwrap();

    let bytes = fs::read(snapshot_path(&store_dir, 3)).unwrap();
    let text = std::str::from_utf8(&bytes).unwrap();
    assert!(text.ends_with('\n'));
    let (crc, json) = text.trim_end().split_once(' ').expect("crc then JSON");
    assert_eq!(
        crc,
        format!("{:08x}", crc32fast::hash(json.as_bytes())),
        "the checksum covers exactly the JSON bytes"
    );
    let decoded: StoredSnapshot = serde_json::from_str(json).unwrap();
    assert_eq!(decoded, saved);
}

#[test]
fn a_crc_corrupt_newest_snapshot_falls_back_to_the_previous_one() {
    let dir = tempdir().unwrap();
    let store_dir = dir.path().join("snapshots");
    let mut store = FileSnapshotStore::open(&store_dir).unwrap();
    store.save(snapshot_at(3, 2)).unwrap();
    store.save(snapshot_at(4, 2)).unwrap();

    corrupt_bytes(&snapshot_path(&store_dir, 4));

    assert_eq!(
        store.load_latest().unwrap().unwrap().applied_seq,
        3,
        "a corrupt newest file is skipped"
    );
    assert!(store.load_previous().unwrap().is_none());
}

#[test]
fn a_truncated_newest_snapshot_falls_back_to_the_previous_one() {
    let dir = tempdir().unwrap();
    let store_dir = dir.path().join("snapshots");
    let mut store = FileSnapshotStore::open(&store_dir).unwrap();
    store.save(snapshot_at(3, 2)).unwrap();
    store.save(snapshot_at(4, 2)).unwrap();

    truncate_file(&snapshot_path(&store_dir, 4));

    assert_eq!(store.load_latest().unwrap().unwrap().applied_seq, 3);
}

#[test]
fn a_version_mismatched_but_valid_snapshot_is_returned_for_the_durable_layer() {
    let dir = tempdir().unwrap();
    let store_dir = dir.path().join("snapshots");
    let mut store = FileSnapshotStore::open(&store_dir).unwrap();
    store.save(snapshot_at(1, 1)).unwrap();

    let mut mismatched = snapshot_at(2, 1);
    mismatched.format_version = FORMAT_VERSION + 1;
    store.save(mismatched.clone()).unwrap();

    let latest = store.load_latest().unwrap().unwrap();
    assert_eq!(latest, mismatched);
    assert_eq!(
        latest.format_version,
        FORMAT_VERSION + 1,
        "an unknown version must be surfaced to the durable layer, not skipped"
    );
}

#[test]
fn a_v1_stored_snapshot_decodes_so_the_durable_layer_can_refuse_it() {
    let dir = tempdir().unwrap();
    let store_dir = dir.path().join("snapshots");
    fs::create_dir_all(&store_dir).unwrap();

    // The old command-sourced snapshot type, with a v1 version and no
    // `last_command_id`. It is checksum-valid and decodable, so the store must
    // return it; version refusal belongs to recovery.
    let v1 = StoredSnapshot {
        format_version: 1,
        applied_seq: 7,
        last_command_id: 0,
        snapshot: MatchingEngine::new(EngineConfig::default()).capture_snapshot(),
    };
    let json = serde_json::to_vec(&v1).unwrap();
    fs::write(snapshot_path(&store_dir, 7), encode_line(&json)).unwrap();

    let store = FileSnapshotStore::open(&store_dir).unwrap();
    let latest = store
        .load_latest()
        .unwrap()
        .expect("a v1 snapshot is surfaced, not silently skipped");
    assert_eq!(latest.format_version, 1);
    assert_eq!(latest.applied_seq, 7);
    assert_eq!(latest.last_command_id, 0);
}

#[test]
fn a_hand_written_extra_candidate_pruned_only_after_a_successful_save() {
    let dir = tempdir().unwrap();
    let store_dir = dir.path().join("snapshots");
    let mut store = FileSnapshotStore::open(&store_dir).unwrap();
    store.save(snapshot_at(5, 2)).unwrap();
    store.save(snapshot_at(6, 3)).unwrap();
    assert_eq!(snapshot_sequences(&store_dir), vec![5, 6]);

    write_snapshot_file(&store_dir, &snapshot_at(4, 1));
    assert_eq!(snapshot_sequences(&store_dir), vec![4, 5, 6]);

    let error = store.save(snapshot_at(3, 1)).unwrap_err();
    assert_eq!(
        error,
        DurableError::SnapshotBelowFloor {
            applied_seq: 3,
            floor: 6,
        }
    );
    assert_eq!(
        snapshot_sequences(&store_dir),
        vec![4, 5, 6],
        "a rejected save leaves every file intact"
    );

    store.save(snapshot_at(7, 4)).unwrap();
    assert_eq!(snapshot_sequences(&store_dir), vec![6, 7]);
}
