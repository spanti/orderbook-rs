//! Contract and durability tests for [`FileCommandLog`].
//!
//! The command-log contract (append, header-once, bounds, read-from ordering and
//! non-contiguous rejection) is exercised against both the in-memory reference
//! adapter and the file adapter through the trait. File-specific tests simulate
//! torn and corrupt records by editing the bytes on disk; a "crash" is a drop
//! and reopen, never a sleep.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

use orderbook_rs::durable::memory::InMemoryCommandLog;
use orderbook_rs::durable::{
    CommandLog, CommandRecord, DurableError, DurableMatchingEngine, FORMAT_VERSION, FileCommandLog,
    InMemorySnapshotStore, LogHeader,
};
use orderbook_rs::types::{
    EngineCommand, EngineConfig, ModifyRequest, OrderRequest, OrderType, Side, StopKind,
    StopOrderRequest, TimeInForce,
};
use tempfile::tempdir;

const BTC: &str = "BTC-USDT";
const ETH: &str = "ETH-USDT";

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn header() -> LogHeader {
    LogHeader {
        format_version: FORMAT_VERSION,
        config: EngineConfig::default(),
    }
}

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

/// A distinct, valid record for each sequence, used only for log assertions.
fn record(sequence: u64) -> CommandRecord {
    CommandRecord {
        format_version: FORMAT_VERSION,
        sequence,
        command: submit(BTC, limit(sequence, Side::Bid, 100 + sequence, sequence)),
    }
}

/// A mixed stream: resting orders, a trade, a modify, a cancel, a pending stop
/// and a rejected submission.
fn mixed_commands() -> Vec<EngineCommand> {
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
    ]
}

// ---------------------------------------------------------------------------
// Shared contract, run against both adapters
// ---------------------------------------------------------------------------

fn contract_header_is_written_once(log: &mut dyn CommandLog) {
    assert_eq!(log.read_header().unwrap(), None);
    log.write_header(header()).unwrap();
    assert_eq!(log.read_header().unwrap(), Some(header()));
    assert_eq!(
        log.write_header(header()),
        Err(DurableError::HeaderAlreadyPresent)
    );
}

fn contract_append_requires_a_header(log: &mut dyn CommandLog) {
    assert_eq!(log.append(record(1)), Err(DurableError::MissingHeader));
    log.write_header(header()).unwrap();
    log.append(record(1)).unwrap();
}

fn contract_append_and_bounds(log: &mut dyn CommandLog) {
    log.write_header(header()).unwrap();
    assert_eq!(log.first_sequence(), None);
    assert_eq!(log.last_sequence(), None);

    for sequence in 1..=3 {
        log.append(record(sequence)).unwrap();
    }

    assert_eq!(log.first_sequence(), Some(1));
    assert_eq!(log.last_sequence(), Some(3));
    assert_eq!(
        log.read_from(1).unwrap(),
        (1..=3).map(record).collect::<Vec<_>>()
    );
}

fn contract_read_from_is_ordered(log: &mut dyn CommandLog) {
    log.write_header(header()).unwrap();
    for sequence in 1..=4 {
        log.append(record(sequence)).unwrap();
    }

    assert_eq!(
        log.read_from(2).unwrap(),
        (2..=4).map(record).collect::<Vec<_>>()
    );
    assert_eq!(log.read_from(1).unwrap().len(), 4);
    assert_eq!(
        log.read_from(2)
            .unwrap()
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        vec![2, 3, 4]
    );
}

fn contract_read_from_past_the_end_is_empty(log: &mut dyn CommandLog) {
    log.write_header(header()).unwrap();
    for sequence in 1..=2 {
        log.append(record(sequence)).unwrap();
    }

    assert!(log.read_from(3).unwrap().is_empty());
    assert!(log.read_from(u64::MAX).unwrap().is_empty());
}

fn contract_append_batch_requires_a_header(log: &mut dyn CommandLog) {
    assert_eq!(
        log.append_batch(vec![record(1), record(2)]),
        Err(DurableError::MissingHeader)
    );
}

fn contract_non_contiguous_append_is_rejected(log: &mut dyn CommandLog) {
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
    log.append(record(2)).unwrap();
    assert_eq!(log.last_sequence(), Some(2));
}

#[test]
fn both_adapters_write_the_header_once() {
    contract_header_is_written_once(&mut InMemoryCommandLog::new());

    let dir = tempdir().unwrap();
    let mut file = FileCommandLog::open(dir.path().join("commands.log")).unwrap();
    contract_header_is_written_once(&mut file);
}

#[test]
fn both_adapters_require_a_header_before_append() {
    contract_append_requires_a_header(&mut InMemoryCommandLog::new());

    let dir = tempdir().unwrap();
    let mut file = FileCommandLog::open(dir.path().join("commands.log")).unwrap();
    contract_append_requires_a_header(&mut file);
}

#[test]
fn both_adapters_append_contiguously_and_report_bounds() {
    contract_append_and_bounds(&mut InMemoryCommandLog::new());

    let dir = tempdir().unwrap();
    let mut file = FileCommandLog::open(dir.path().join("commands.log")).unwrap();
    contract_append_and_bounds(&mut file);
}

#[test]
fn both_adapters_read_from_in_append_order() {
    contract_read_from_is_ordered(&mut InMemoryCommandLog::new());

    let dir = tempdir().unwrap();
    let mut file = FileCommandLog::open(dir.path().join("commands.log")).unwrap();
    contract_read_from_is_ordered(&mut file);
}

#[test]
fn both_adapters_read_from_past_the_end_is_empty() {
    contract_read_from_past_the_end_is_empty(&mut InMemoryCommandLog::new());

    let dir = tempdir().unwrap();
    let mut file = FileCommandLog::open(dir.path().join("commands.log")).unwrap();
    contract_read_from_past_the_end_is_empty(&mut file);
}

#[test]
fn both_adapters_reject_non_contiguous_appends() {
    contract_non_contiguous_append_is_rejected(&mut InMemoryCommandLog::new());

    let dir = tempdir().unwrap();
    let mut file = FileCommandLog::open(dir.path().join("commands.log")).unwrap();
    contract_non_contiguous_append_is_rejected(&mut file);
}

#[test]
fn both_adapters_require_a_header_before_batch_append() {
    contract_append_batch_requires_a_header(&mut InMemoryCommandLog::new());

    let dir = tempdir().unwrap();
    let mut file = FileCommandLog::open(dir.path().join("commands.log")).unwrap();
    contract_append_batch_requires_a_header(&mut file);
}

#[test]
fn both_adapters_append_a_contiguous_batch() {
    fn append_batch(log: &mut dyn CommandLog) {
        log.write_header(header()).unwrap();
        log.append_batch((1..=3).map(record).collect()).unwrap();
        assert_eq!(
            log.read_from(1).unwrap(),
            (1..=3).map(record).collect::<Vec<_>>()
        );
        assert_eq!(log.last_sequence(), Some(3));
    }

    append_batch(&mut InMemoryCommandLog::new());

    let dir = tempdir().unwrap();
    let mut file = FileCommandLog::open(dir.path().join("commands.log")).unwrap();
    append_batch(&mut file);
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

fn write_valid_log(path: &Path, records: &[CommandRecord]) {
    let mut log = FileCommandLog::open(path).unwrap();
    log.write_header(header()).unwrap();
    for record in records {
        log.append(record.clone()).unwrap();
    }
}

#[test]
fn open_creates_an_empty_log_without_a_header() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("commands.log");
    assert!(!path.exists());

    let log = FileCommandLog::open(&path).unwrap();

    assert!(path.exists(), "open must create the file when it is absent");
    assert_eq!(log.read_header().unwrap(), None);
    assert_eq!(log.first_sequence(), None);
    assert_eq!(log.last_sequence(), None);
}

#[test]
fn acknowledged_records_are_read_back_in_order_after_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("commands.log");
    let expected = (1..=5).map(record).collect::<Vec<_>>();
    write_valid_log(&path, &expected);

    let reopened = FileCommandLog::open(&path).unwrap();

    assert_eq!(reopened.first_sequence(), Some(1));
    assert_eq!(reopened.last_sequence(), Some(5));
    assert_eq!(reopened.read_from(1).unwrap(), expected);
    assert_eq!(reopened.read_from(4).unwrap(), vec![record(4), record(5)]);
    assert!(reopened.read_from(6).unwrap().is_empty());
}

#[test]
fn a_crc_mismatch_in_a_middle_line_is_reported_as_corruption() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("commands.log");
    write_valid_log(&path, &[record(1), record(2), record(3)]);

    corrupt_line_crc(&path, 1);

    let error = FileCommandLog::open(&path).unwrap_err();
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
    let path = dir.path().join("commands.log");
    write_valid_log(&path, &[record(1), record(2), record(3)]);

    // Valid checksum, undecodable JSON: a corrupt record, not a torn tail.
    rewrite_line_json(&path, 2, b"{\"format_version\":1,");

    let error = FileCommandLog::open(&path).unwrap_err();
    assert!(
        matches!(error, DurableError::CorruptRecord { .. }),
        "a corrupt middle line must abort open, got {error:?}"
    );
}

#[test]
fn a_non_utf8_middle_line_is_corruption_not_a_panic() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("commands.log");
    write_valid_log(&path, &[record(1), record(2), record(3)]);

    rewrite_line_json(&path, 1, &[0xff, 0xfe, 0xfd]);

    let error = FileCommandLog::open(&path).unwrap_err();
    assert!(
        matches!(error, DurableError::CorruptRecord { .. }),
        "non-UTF-8 bytes must be reported as corruption, got {error:?}"
    );
}

#[test]
fn a_contiguous_batch_is_durable_and_readable_after_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("commands.log");
    let batch = (1..=4).map(record).collect::<Vec<_>>();

    let mut log = FileCommandLog::open(&path).unwrap();
    log.write_header(header()).unwrap();
    log.append_batch(batch.clone()).unwrap();

    assert_eq!(log.first_sequence(), Some(1));
    assert_eq!(log.last_sequence(), Some(4));
    assert_eq!(log.read_from(1).unwrap(), batch);

    // A crash is a drop and reopen; the batch must be on disk, not just cached.
    drop(log);
    let reopened = FileCommandLog::open(&path).unwrap();
    assert_eq!(reopened.read_from(1).unwrap(), batch);
}

#[test]
fn a_non_contiguous_batch_is_rejected_before_anything_is_written() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("commands.log");
    let mut log = FileCommandLog::open(&path).unwrap();
    log.write_header(header()).unwrap();
    log.append(record(1)).unwrap();
    let bytes_before = fs::metadata(&path).unwrap().len();

    // The batch does not start at the next sequence.
    assert_eq!(
        log.append_batch(vec![record(3), record(4)]),
        Err(DurableError::NonContiguousSequence {
            expected: 2,
            found: 3,
        })
    );
    // A gap inside the batch is rejected as well.
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
    assert_eq!(log.last_sequence(), Some(1));
    assert_eq!(log.read_from(1).unwrap(), vec![record(1)]);

    // Rejection leaves the log usable for the next contiguous batch.
    log.append_batch(vec![record(2), record(3)]).unwrap();
    assert_eq!(log.last_sequence(), Some(3));
}

#[test]
fn a_partial_trailing_line_is_truncated_on_open() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("commands.log");
    write_valid_log(&path, &[record(1), record(2)]);
    let valid_len = fs::metadata(&path).unwrap().len();

    // Crash during an unacknowledged append: half a line, no newline.
    {
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"0badc0de {\"format_version\":1,\"sequence\":3,\"command\":")
            .unwrap();
        file.flush().unwrap();
    }

    let mut reopened = FileCommandLog::open(&path).unwrap();

    assert_eq!(reopened.first_sequence(), Some(1));
    assert_eq!(reopened.last_sequence(), Some(2));
    assert_eq!(reopened.read_from(1).unwrap(), vec![record(1), record(2)]);
    assert_eq!(
        fs::metadata(&path).unwrap().len(),
        valid_len,
        "the torn tail must be removed from the file"
    );

    // The truncated log still accepts the next contiguous record.
    reopened.append(record(3)).unwrap();
    assert_eq!(reopened.last_sequence(), Some(3));
}

#[test]
fn a_corrupt_last_complete_line_aborts_open() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("commands.log");
    write_valid_log(&path, &[record(1), record(2)]);
    let len_before = fs::metadata(&path).unwrap().len();

    // A completed, fsynced append cannot legitimately lose its checksum, so a
    // newline-terminated bad line is corruption, not an unacknowledged append.
    corrupt_line_crc(&path, 2);

    let error = FileCommandLog::open(&path).unwrap_err();
    match error {
        DurableError::CorruptRecord { detail } => {
            assert!(
                detail.contains("line 3"),
                "the detail should name the terminal corrupt line: {detail}"
            );
            assert!(
                detail.contains("crc32 mismatch"),
                "the detail should name the checksum failure: {detail}"
            );
        }
        other => panic!("expected corruption, got {other:?}"),
    }
    assert_eq!(
        fs::metadata(&path).unwrap().len(),
        len_before,
        "a refused open must not truncate acknowledged history"
    );
}

// ---------------------------------------------------------------------------
// End-to-end: a durable engine on a real file
// ---------------------------------------------------------------------------

#[test]
fn a_durable_engine_recovers_exactly_from_the_file_log() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("commands.log");
    let commands = mixed_commands();

    let before_crash = {
        let log = FileCommandLog::open(&path).expect("open the file log");
        let mut durable = DurableMatchingEngine::create(
            EngineConfig::default(),
            Box::new(log),
            Box::new(InMemorySnapshotStore::new()),
            0,
        )
        .expect("create writes the header");

        for command in &commands {
            durable
                .execute(command.clone())
                .expect("every command must become durable");
        }

        // Crash window: every command is durable and applied, but no snapshot
        // was taken. Recovery must replay the whole file log.
        durable.engine().capture_snapshot()
    };

    let log = FileCommandLog::open(&path).expect("reopen the file log");
    let mut reopened =
        DurableMatchingEngine::open(Box::new(log), Box::new(InMemorySnapshotStore::new()), 0)
            .expect("exact recovery from the file log must succeed");

    assert_eq!(reopened.applied_seq(), commands.len() as u64);
    assert_eq!(
        reopened.engine().capture_snapshot(),
        before_crash,
        "decision state must be identical after a real file restart"
    );
    assert_eq!(
        reopened
            .history_from(1)
            .unwrap()
            .into_iter()
            .map(|record| record.command)
            .collect::<Vec<_>>(),
        commands,
        "acknowledged commands must read back in append order"
    );

    // The recovered engine continues the global sequence.
    reopened
        .execute(submit(ETH, limit(7, Side::Ask, 300, 2)))
        .unwrap();
    assert_eq!(reopened.applied_seq(), commands.len() as u64 + 1);
}
