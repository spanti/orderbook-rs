//! Contract and durability tests for the v2 file-backed event log.
//!
//! The shared contract (header-once, append, bounds, read-from ordering and
//! non-contiguous rejection) is exercised against both the in-memory reference
//! adapter and [`FileEventLog`] through the [`EventLog`] trait. File-specific
//! tests simulate torn and corrupt records by editing bytes on disk; a "crash"
//! is a drop and reopen, never a sleep.
//!
//! Version 1 command logs are a different format and must be refused loudly
//! rather than misread as event logs.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

use orderbook_rs::durable::{
    DurableError, EventLog, EventRecord, FORMAT_VERSION, FileEventLog, InMemoryEventLog, LogHeader,
};
use orderbook_rs::event::DomainEvent;
use orderbook_rs::types::{EngineConfig, Order, OrderType, Side, TimeInForce};
use tempfile::tempdir;

const BTC: &str = "BTC-USDT";

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn header() -> LogHeader {
    LogHeader {
        format_version: FORMAT_VERSION,
        config: EngineConfig::default(),
    }
}

fn order(id: u64) -> Order {
    Order {
        id,
        side: Side::Bid,
        price: 100,
        quantity: 1,
        timestamp: id,
        order_type: OrderType::Limit,
        time_in_force: TimeInForce::GTC,
    }
}

/// A resting submit emits `OrderAccepted` then `OrderRested`; each command owns
/// one order id and one command id, and every event owns one sequence.
fn record(sequence: u64) -> EventRecord {
    let order_id = sequence.div_ceil(2);
    let event = if sequence % 2 == 1 {
        DomainEvent::OrderAccepted {
            symbol: BTC.to_string(),
            order: order(order_id),
            post_only: false,
        }
    } else {
        DomainEvent::OrderRested {
            symbol: BTC.to_string(),
            order: order(order_id),
        }
    };
    EventRecord {
        format_version: FORMAT_VERSION,
        sequence,
        command_id: order_id,
        event,
    }
}

// ---------------------------------------------------------------------------
// Shared contract, run against both adapters
// ---------------------------------------------------------------------------

fn contract_header_is_written_once(log: &mut dyn EventLog) {
    assert_eq!(log.read_header().unwrap(), None);
    log.write_header(header()).unwrap();
    assert_eq!(log.read_header().unwrap(), Some(header()));
    assert_eq!(
        log.write_header(header()),
        Err(DurableError::HeaderAlreadyPresent)
    );
}

fn contract_append_requires_a_header(log: &mut dyn EventLog) {
    assert_eq!(log.append(record(1)), Err(DurableError::MissingHeader));
    log.write_header(header()).unwrap();
    log.append(record(1)).unwrap();
}

fn contract_append_and_bounds(log: &mut dyn EventLog) {
    log.write_header(header()).unwrap();
    assert_eq!(log.first_sequence(), None);
    assert_eq!(log.last_stored_sequence(), None);
    assert_eq!(log.last_command_id(), None);

    for sequence in 1..=4 {
        log.append(record(sequence)).unwrap();
    }

    assert_eq!(log.first_sequence(), Some(1));
    assert_eq!(log.last_stored_sequence(), Some(4));
    assert_eq!(log.last_command_id(), Some(2));
    assert_eq!(
        log.read_from(1).unwrap(),
        (1..=4).map(record).collect::<Vec<_>>()
    );
}

fn contract_read_from_is_ordered(log: &mut dyn EventLog) {
    log.write_header(header()).unwrap();

    // An empty log has no records regardless of the requested sequence.
    assert!(log.read_from(0).unwrap().is_empty());
    assert!(log.read_from(1).unwrap().is_empty());
    assert!(log.read_from(u64::MAX).unwrap().is_empty());

    for sequence in 1..=4 {
        log.append(record(sequence)).unwrap();
    }

    let all = (1..=4).map(record).collect::<Vec<_>>();
    // From below the first record returns the whole log; the boundaries
    // themselves are inclusive at `from` and exclusive at `from + 1`.
    assert_eq!(log.read_from(0).unwrap(), all);
    assert_eq!(log.read_from(1).unwrap(), all);
    assert_eq!(
        log.read_from(2).unwrap(),
        (2..=4).map(record).collect::<Vec<_>>()
    );
    assert_eq!(
        log.read_from(3).unwrap(),
        (3..=4).map(record).collect::<Vec<_>>()
    );
    assert_eq!(log.read_from(4).unwrap(), vec![record(4)]);
    assert!(log.read_from(5).unwrap().is_empty());
    assert!(log.read_from(u64::MAX).unwrap().is_empty());
}

fn contract_non_contiguous_append_is_rejected(log: &mut dyn EventLog) {
    log.write_header(header()).unwrap();

    assert_eq!(
        log.append(record(2)),
        Err(DurableError::NonContiguousSequence {
            expected: 1,
            found: 2,
        })
    );
    log.append(record(1)).unwrap();
    assert_eq!(
        log.append(record(3)),
        Err(DurableError::NonContiguousSequence {
            expected: 2,
            found: 3,
        })
    );
    // A rejected append leaves the stored log untouched.
    assert_eq!(log.read_from(1).unwrap(), vec![record(1)]);
    assert!(log.read_from(2).unwrap().is_empty());

    log.append(record(2)).unwrap();
    assert_eq!(log.last_stored_sequence(), Some(2));

    // A duplicate of an already stored sequence is rejected as
    // non-contiguous, and still leaves the log unchanged.
    assert_eq!(
        log.append(record(2)),
        Err(DurableError::NonContiguousSequence {
            expected: 3,
            found: 2,
        })
    );
    assert_eq!(log.read_from(1).unwrap(), vec![record(1), record(2)]);
    assert_eq!(log.read_from(2).unwrap(), vec![record(2)]);
    assert!(log.read_from(3).unwrap().is_empty());
}

fn contract_empty_batch_is_a_noop(log: &mut dyn EventLog) {
    log.write_header(header()).unwrap();

    log.append_batch(Vec::new()).unwrap();
    assert_eq!(log.last_stored_sequence(), None);
    assert!(log.read_from(0).unwrap().is_empty());

    log.append(record(1)).unwrap();
    log.append_batch(Vec::new()).unwrap();
    assert_eq!(log.last_stored_sequence(), Some(1));
    assert_eq!(log.read_from(1).unwrap(), vec![record(1)]);
}

fn contract_batch_append(log: &mut dyn EventLog) {
    log.write_header(header()).unwrap();
    log.append_batch((1..=4).map(record).collect()).unwrap();

    assert_eq!(
        log.read_from(1).unwrap(),
        (1..=4).map(record).collect::<Vec<_>>()
    );
    assert_eq!(log.read_from(4).unwrap(), vec![record(4)]);
    assert!(log.read_from(5).unwrap().is_empty());
    assert_eq!(log.last_stored_sequence(), Some(4));
    assert_eq!(log.last_command_id(), Some(2));
}

#[test]
fn both_adapters_write_the_header_once() {
    contract_header_is_written_once(&mut InMemoryEventLog::new());

    let dir = tempdir().unwrap();
    let mut file = FileEventLog::open(dir.path().join("events.log")).unwrap();
    contract_header_is_written_once(&mut file);
}

#[test]
fn both_adapters_require_a_header_before_append() {
    contract_append_requires_a_header(&mut InMemoryEventLog::new());

    let dir = tempdir().unwrap();
    let mut file = FileEventLog::open(dir.path().join("events.log")).unwrap();
    contract_append_requires_a_header(&mut file);
}

#[test]
fn both_adapters_append_contiguously_and_report_bounds() {
    contract_append_and_bounds(&mut InMemoryEventLog::new());

    let dir = tempdir().unwrap();
    let mut file = FileEventLog::open(dir.path().join("events.log")).unwrap();
    contract_append_and_bounds(&mut file);
}

#[test]
fn both_adapters_read_from_in_append_order() {
    contract_read_from_is_ordered(&mut InMemoryEventLog::new());

    let dir = tempdir().unwrap();
    let mut file = FileEventLog::open(dir.path().join("events.log")).unwrap();
    contract_read_from_is_ordered(&mut file);
}

#[test]
fn both_adapters_reject_non_contiguous_appends() {
    contract_non_contiguous_append_is_rejected(&mut InMemoryEventLog::new());

    let dir = tempdir().unwrap();
    let mut file = FileEventLog::open(dir.path().join("events.log")).unwrap();
    contract_non_contiguous_append_is_rejected(&mut file);
}

#[test]
fn both_adapters_append_a_contiguous_batch() {
    contract_batch_append(&mut InMemoryEventLog::new());

    let dir = tempdir().unwrap();
    let mut file = FileEventLog::open(dir.path().join("events.log")).unwrap();
    contract_batch_append(&mut file);
}

#[test]
fn both_adapters_treat_an_empty_batch_as_a_noop() {
    contract_empty_batch_is_a_noop(&mut InMemoryEventLog::new());

    let dir = tempdir().unwrap();
    let mut file = FileEventLog::open(dir.path().join("events.log")).unwrap();
    contract_empty_batch_is_a_noop(&mut file);
}

// ---------------------------------------------------------------------------
// File-specific behavior
// ---------------------------------------------------------------------------

/// Offsets of the first byte of every line in `bytes`.
fn line_starts(bytes: &[u8]) -> Vec<usize> {
    let mut starts = vec![0];
    for (index, &byte) in bytes.iter().enumerate() {
        if byte == b'\n' && index + 1 < bytes.len() {
            starts.push(index + 1);
        }
    }
    starts
}

fn encode_line_json<T: serde::Serialize>(value: &T) -> Vec<u8> {
    let json = serde_json::to_vec(value).unwrap();
    let mut line = format!("{:08x} ", crc32fast::hash(&json)).into_bytes();
    line.extend_from_slice(&json);
    line.push(b'\n');
    line
}

/// Flip one hex digit of a line's checksum, keeping the line otherwise intact.
fn corrupt_line_crc(path: &Path, line_index: usize) {
    let mut bytes = fs::read(path).unwrap();
    let start = line_starts(&bytes)[line_index];
    bytes[start] = if bytes[start] == b'0' { b'1' } else { b'0' };
    fs::write(path, bytes).unwrap();
}

/// Replace one line's JSON with arbitrary bytes, using a matching checksum.
fn rewrite_line_json(path: &Path, line_index: usize, json: &[u8]) {
    let bytes = fs::read(path).unwrap();
    let starts = line_starts(&bytes);
    let start = starts[line_index];
    let end = starts.get(line_index + 1).copied().unwrap_or(bytes.len());

    let mut replacement = format!("{:08x} ", crc32fast::hash(json)).into_bytes();
    replacement.extend_from_slice(json);
    replacement.push(b'\n');

    let mut result = Vec::new();
    result.extend_from_slice(&bytes[..start]);
    result.extend_from_slice(&replacement);
    result.extend_from_slice(&bytes[end..]);
    fs::write(path, result).unwrap();
}

fn write_valid_log(path: &Path, records: &[EventRecord]) {
    let mut log = FileEventLog::open(path).unwrap();
    log.write_header(header()).unwrap();
    for record in records {
        log.append(record.clone()).unwrap();
    }
}

#[test]
fn open_creates_an_empty_log_without_a_header() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("events.log");
    assert!(!path.exists());

    let log = FileEventLog::open(&path).unwrap();

    assert!(path.exists(), "open must create the file when it is absent");
    assert_eq!(log.read_header().unwrap(), None);
    assert_eq!(log.first_sequence(), None);
    assert_eq!(log.last_stored_sequence(), None);
    assert_eq!(log.last_command_id(), None);
}

#[test]
fn acknowledged_records_are_read_back_in_order_after_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("events.log");
    let expected = (1..=5).map(record).collect::<Vec<_>>();
    write_valid_log(&path, &expected);

    let reopened = FileEventLog::open(&path).unwrap();

    assert_eq!(reopened.first_sequence(), Some(1));
    assert_eq!(reopened.last_stored_sequence(), Some(5));
    assert_eq!(reopened.last_command_id(), Some(3));
    assert_eq!(reopened.read_from(1).unwrap(), expected);
    assert_eq!(reopened.read_from(4).unwrap(), vec![record(4), record(5)]);
    assert!(reopened.read_from(6).unwrap().is_empty());
}

#[test]
fn a_contiguous_batch_is_durable_and_readable_after_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("events.log");
    let batch = (1..=4).map(record).collect::<Vec<_>>();

    let mut log = FileEventLog::open(&path).unwrap();
    log.write_header(header()).unwrap();
    log.append_batch(batch.clone()).unwrap();

    assert_eq!(log.first_sequence(), Some(1));
    assert_eq!(log.last_stored_sequence(), Some(4));
    assert_eq!(log.read_from(1).unwrap(), batch);

    // A crash is a drop and reopen; the batch must be on disk, not just cached.
    drop(log);
    let reopened = FileEventLog::open(&path).unwrap();
    assert_eq!(reopened.read_from(1).unwrap(), batch);
    assert_eq!(reopened.last_command_id(), Some(2));
}

#[test]
fn a_non_contiguous_batch_is_rejected_before_anything_is_written() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("events.log");
    let mut log = FileEventLog::open(&path).unwrap();
    log.write_header(header()).unwrap();
    log.append(record(1)).unwrap();
    let bytes_before = fs::metadata(&path).unwrap().len();

    assert_eq!(
        log.append_batch(vec![record(3), record(4)]),
        Err(DurableError::NonContiguousSequence {
            expected: 2,
            found: 3,
        })
    );
    assert_eq!(
        log.append_batch(vec![record(2), record(4)]),
        Err(DurableError::NonContiguousSequence {
            expected: 3,
            found: 4,
        })
    );

    assert_eq!(
        fs::metadata(&path).unwrap().len(),
        bytes_before,
        "a rejected batch must not write any line"
    );
    assert_eq!(log.last_stored_sequence(), Some(1));
}

#[test]
fn a_partial_trailing_line_is_truncated_on_open() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("events.log");
    write_valid_log(&path, &[record(1), record(2)]);
    let valid_len = fs::metadata(&path).unwrap().len();

    // Crash during an unacknowledged append: half a line, no newline.
    {
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"0badc0de {\"format_version\":2,\"sequence\":3")
            .unwrap();
        file.flush().unwrap();
    }

    let mut reopened = FileEventLog::open(&path).unwrap();

    assert_eq!(reopened.first_sequence(), Some(1));
    assert_eq!(reopened.last_stored_sequence(), Some(2));
    assert_eq!(reopened.read_from(1).unwrap(), vec![record(1), record(2)]);
    assert_eq!(
        fs::metadata(&path).unwrap().len(),
        valid_len,
        "the torn tail must be removed from the file"
    );

    // The truncated log still accepts the next contiguous record.
    reopened.append(record(3)).unwrap();
    assert_eq!(reopened.last_stored_sequence(), Some(3));
}

#[test]
fn a_crc_mismatch_in_a_middle_line_aborts_open() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("events.log");
    write_valid_log(&path, &[record(1), record(2), record(3)]);

    corrupt_line_crc(&path, 1);

    let error = FileEventLog::open(&path).unwrap_err();
    match error {
        DurableError::CorruptRecord { detail } => {
            assert!(
                detail.contains("crc32 mismatch"),
                "the detail should name the checksum failure: {detail}"
            );
        }
        other => panic!("expected corruption, got {other:?}"),
    }
}

#[test]
fn a_corrupt_middle_line_aborts_open_instead_of_skipping_history() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("events.log");
    write_valid_log(&path, &[record(1), record(2), record(3)]);

    // Valid checksum, undecodable JSON: a corrupt record, not a torn tail.
    rewrite_line_json(&path, 2, b"{\"format_version\":2,");

    let error = FileEventLog::open(&path).unwrap_err();
    assert!(
        matches!(error, DurableError::CorruptRecord { .. }),
        "a corrupt middle line must abort open, got {error:?}"
    );
}

#[test]
fn a_corrupt_last_complete_line_aborts_open() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("events.log");
    write_valid_log(&path, &[record(1), record(2)]);
    let len_before = fs::metadata(&path).unwrap().len();

    corrupt_line_crc(&path, 2);

    let error = FileEventLog::open(&path).unwrap_err();
    match error {
        DurableError::CorruptRecord { detail } => {
            assert!(
                detail.contains("line 3"),
                "the detail should name the terminal corrupt line: {detail}"
            );
            assert!(detail.contains("crc32 mismatch"));
        }
        other => panic!("expected corruption, got {other:?}"),
    }
    assert_eq!(
        fs::metadata(&path).unwrap().len(),
        len_before,
        "a refused open must not truncate acknowledged history"
    );
}

#[test]
fn a_non_utf8_middle_line_is_corruption_not_a_panic() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("events.log");
    write_valid_log(&path, &[record(1), record(2), record(3)]);

    rewrite_line_json(&path, 1, &[0xff, 0xfe, 0xfd]);

    let error = FileEventLog::open(&path).unwrap_err();
    assert!(
        matches!(error, DurableError::CorruptRecord { .. }),
        "non-UTF-8 bytes must be reported as corruption, got {error:?}"
    );
}

// ---------------------------------------------------------------------------
// Version refusal: v1 command logs are not event logs
// ---------------------------------------------------------------------------

#[test]
fn a_v1_header_is_refused_with_an_unsupported_version_error() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("events.log");
    let v1_header = LogHeader {
        format_version: 1,
        config: EngineConfig::default(),
    };
    fs::write(&path, encode_line_json(&v1_header)).unwrap();

    assert_eq!(
        FileEventLog::open(&path).unwrap_err(),
        DurableError::UnsupportedVersion {
            found: 1,
            supported: FORMAT_VERSION,
        }
    );
}

#[test]
fn a_v1_record_is_refused_with_an_unsupported_version_error() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("events.log");
    let mut bytes = encode_line_json(&header());
    // A v1-shaped record: the version probe refuses it before type decoding,
    // so it does not matter that this is not a valid v2 EventRecord.
    bytes.extend(encode_line_json(&serde_json::json!({
        "format_version": 1,
        "sequence": 1,
        "command": {"SubmitOrder": {"symbol": BTC}}
    })));
    fs::write(&path, bytes).unwrap();

    assert_eq!(
        FileEventLog::open(&path).unwrap_err(),
        DurableError::UnsupportedVersion {
            found: 1,
            supported: FORMAT_VERSION,
        }
    );
}

#[test]
fn a_v1_header_cannot_be_written_and_a_v1_record_cannot_be_appended() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("events.log");
    let mut log = FileEventLog::open(&path).unwrap();

    assert_eq!(
        log.write_header(LogHeader {
            format_version: 1,
            config: EngineConfig::default(),
        }),
        Err(DurableError::UnsupportedVersion {
            found: 1,
            supported: FORMAT_VERSION,
        })
    );

    log.write_header(header()).unwrap();
    let mut v1 = record(1);
    v1.format_version = 1;
    assert_eq!(
        log.append(v1),
        Err(DurableError::UnsupportedVersion {
            found: 1,
            supported: FORMAT_VERSION,
        })
    );
    assert_eq!(log.last_stored_sequence(), None);
}

#[test]
fn a_missing_format_version_is_corruption_not_an_unknown_version() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("events.log");
    fs::write(&path, encode_line_json(&serde_json::json!({"sequence": 1}))).unwrap();

    assert!(matches!(
        FileEventLog::open(&path).unwrap_err(),
        DurableError::CorruptRecord { .. }
    ));
}
