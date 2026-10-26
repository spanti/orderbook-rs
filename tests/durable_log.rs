//! Durable command-log behavior, exercised through the public durable surface.
//!
//! Every scenario uses the in-memory adapters. A "crash" is simulated by
//! dropping the durable engine and reopening from a second handle, never by
//! sleeping.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use orderbook_rs::durable::memory::{InMemoryCommandLog, InMemorySnapshotStore};
use orderbook_rs::durable::{
    CommandLog, CommandRecord, DurableError, DurableMatchingEngine, FORMAT_VERSION, LogHeader,
    SnapshotStore, StoredSnapshot,
};
use orderbook_rs::engine::MatchingEngine;
use orderbook_rs::types::{
    CommandOutcome, EngineCommand, EngineConfig, EngineError, ModifyRequest, OrderRequest, Side,
    StopKind, StopOrderRequest, TimeInForce,
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
fn commands_are_appended_before_apply_and_match_a_pure_engine() {
    let log = InMemoryCommandLog::new();
    let snapshots = InMemorySnapshotStore::new();
    let mut durable = DurableMatchingEngine::create(
        EngineConfig::default(),
        Box::new(log.handle()),
        Box::new(snapshots.handle()),
        0,
    )
    .expect("create must write the header");

    let commands = sample_commands();
    for command in &commands {
        durable
            .execute(command.clone())
            .expect("every command must become durable");
    }

    let records = log.read_from(1).expect("log must be readable");
    assert_eq!(records.len(), commands.len(), "one record per command");
    assert_eq!(log.first_sequence(), Some(1));
    assert_eq!(log.last_sequence(), Some(commands.len() as u64));
    assert_eq!(durable.applied_seq(), commands.len() as u64);
    assert!(
        records
            .iter()
            .all(|record| record.format_version == FORMAT_VERSION)
    );
    assert_eq!(
        records
            .iter()
            .map(|record| record.command.clone())
            .collect::<Vec<_>>(),
        commands
    );

    assert_eq!(
        durable.engine().capture_snapshot(),
        pure_engine_after(&commands).capture_snapshot(),
        "durable decision state equals a pure engine fed the same commands"
    );
}

#[test]
fn history_from_returns_durable_records_in_order() {
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
    for command in &commands {
        durable.execute(command.clone()).unwrap();
    }

    let all = durable.history_from(1).unwrap();
    assert_eq!(all.len(), commands.len());

    let tail = durable.history_from(3).unwrap();
    assert_eq!(tail.len(), commands.len() - 2);
    assert_eq!(
        tail.iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        (3..=commands.len() as u64).collect::<Vec<_>>()
    );
    assert_eq!(
        tail.iter()
            .map(|record| record.command.clone())
            .collect::<Vec<_>>(),
        commands[2..].to_vec()
    );
}

#[test]
fn rejected_command_is_appended_and_reported_as_rejected() {
    let log = InMemoryCommandLog::new();
    let snapshots = InMemorySnapshotStore::new();
    let mut durable = DurableMatchingEngine::create(
        EngineConfig::default(),
        Box::new(log.handle()),
        Box::new(snapshots.handle()),
        0,
    )
    .unwrap();

    let outcome = durable
        .execute(submit(BTC, limit(1, Side::Bid, 100, 0)))
        .expect("a rejection is a successful durable execution");

    assert_eq!(
        outcome,
        CommandOutcome::Rejected(EngineError::QuantityOutOfRange)
    );
    assert_eq!(durable.history_from(1).unwrap().len(), 1);
    assert_eq!(durable.applied_seq(), 1);
}

#[test]
fn reopening_from_the_log_alone_replays_from_sequence_one() {
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
    for command in &commands {
        durable.execute(command.clone()).unwrap();
    }
    let before_crash = durable.engine().capture_snapshot();

    // Crash window: every command was applied and durable, but no snapshot was
    // ever written. Recovery must rebuild state by replaying from sequence 1.
    drop(durable);

    let reopened =
        DurableMatchingEngine::open(Box::new(log.handle()), Box::new(snapshots.handle()), 0)
            .expect("recovery from the log alone must succeed");

    assert_eq!(reopened.applied_seq(), commands.len() as u64);
    assert_eq!(reopened.engine().capture_snapshot(), before_crash);
    assert_eq!(reopened.history_from(1).unwrap().len(), commands.len());
}

#[test]
fn reopened_engine_continues_the_command_sequence() {
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
    for command in &commands {
        durable.execute(command.clone()).unwrap();
    }
    drop(durable);

    let mut reopened =
        DurableMatchingEngine::open(Box::new(log.handle()), Box::new(snapshots.handle()), 0)
            .unwrap();

    reopened
        .execute(submit(ETH, limit(7, Side::Ask, 300, 2)))
        .expect("a recovered engine accepts new commands");

    let records = reopened.history_from(1).unwrap();
    assert_eq!(records.len(), commands.len() + 1);
    assert_eq!(records.last().unwrap().sequence, commands.len() as u64 + 1);
}

#[test]
fn from_commands_seeds_through_the_append_path() {
    let log = InMemoryCommandLog::new();
    let snapshots = InMemorySnapshotStore::new();
    let commands = sample_commands();

    let durable = DurableMatchingEngine::from_commands(
        EngineConfig::default(),
        &commands,
        Box::new(log.handle()),
        Box::new(snapshots.handle()),
        0,
    )
    .expect("seeding must succeed");

    assert_eq!(durable.applied_seq(), commands.len() as u64);
    assert_eq!(
        durable.engine().capture_snapshot(),
        pure_engine_after(&commands).capture_snapshot()
    );

    let records = log.read_from(1).unwrap();
    assert_eq!(records.len(), commands.len());
    assert_eq!(log.last_sequence(), Some(commands.len() as u64));
}

#[test]
fn rejected_commands_are_replayed_to_the_same_state() {
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
    for command in &commands {
        durable.execute(command.clone()).unwrap();
    }
    let before_crash = durable.engine().capture_snapshot();
    drop(durable);

    let reopened =
        DurableMatchingEngine::open(Box::new(log.handle()), Box::new(snapshots.handle()), 0)
            .unwrap();

    assert_eq!(reopened.applied_seq(), commands.len() as u64);
    assert_eq!(reopened.engine().capture_snapshot(), before_crash);
    assert_eq!(
        reopened.history_from(commands.len() as u64).unwrap()[0].command,
        commands.last().unwrap().clone(),
        "the rejected command stayed in the durable log"
    );
    // Replaying the rejected command deterministically produced no order: the
    // five ledger ids come from the two submits, modify, cancel-target and stop.
    assert_eq!(reopened.engine().order_count(), 5);
}

#[test]
fn create_refuses_a_log_that_already_has_a_header() {
    let log = InMemoryCommandLog::new();
    let snapshots = InMemorySnapshotStore::new();
    let _first = DurableMatchingEngine::create(
        EngineConfig::default(),
        Box::new(log.handle()),
        Box::new(snapshots.handle()),
        0,
    )
    .unwrap();

    let error = DurableMatchingEngine::create(
        EngineConfig::default(),
        Box::new(log.handle()),
        Box::new(InMemorySnapshotStore::new()),
        0,
    )
    .expect_err("a second create must not overwrite the header");

    assert_eq!(error, DurableError::HeaderAlreadyPresent);
}

// ---------------------------------------------------------------------------
// Fault-injection adapters. Each wraps an in-memory handle and changes only the
// behavior under test; the durable layer itself never changes.
// ---------------------------------------------------------------------------

/// A log whose append can be made to fail on demand.
struct FailingLog {
    inner: InMemoryCommandLog,
    fail_appends: Arc<AtomicBool>,
}

impl FailingLog {
    fn new(inner: InMemoryCommandLog, fail_appends: Arc<AtomicBool>) -> Self {
        Self {
            inner,
            fail_appends,
        }
    }
}

impl CommandLog for FailingLog {
    fn read_header(&self) -> Result<Option<LogHeader>, DurableError> {
        self.inner.read_header()
    }

    fn write_header(&mut self, header: LogHeader) -> Result<(), DurableError> {
        self.inner.write_header(header)
    }

    fn append(&mut self, record: CommandRecord) -> Result<(), DurableError> {
        if self.fail_appends.load(Ordering::SeqCst) {
            return Err(DurableError::AppendFailed(
                "injected append failure".to_string(),
            ));
        }
        self.inner.append(record)
    }

    fn read_from(&self, from: u64) -> Result<Vec<CommandRecord>, DurableError> {
        self.inner.read_from(from)
    }

    fn first_sequence(&self) -> Option<u64> {
        self.inner.first_sequence()
    }

    fn last_sequence(&self) -> Option<u64> {
        self.inner.last_sequence()
    }
}

/// A log that delivers one already-appended record twice.
struct DuplicateLog {
    inner: InMemoryCommandLog,
    duplicate: CommandRecord,
}

impl CommandLog for DuplicateLog {
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

/// A log that delivers an out-of-order record after a chosen sequence.
struct SplicedLog {
    inner: InMemoryCommandLog,
    after: u64,
    extra: CommandRecord,
}

impl CommandLog for SplicedLog {
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
        if self.extra.sequence >= from
            && let Some(position) = records
                .iter()
                .position(|record| record.sequence == self.after)
        {
            records.insert(position + 1, self.extra.clone());
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

/// A log that hides every record through `hide_through`, simulating a lost
/// prefix of the command history.
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

/// A log decorator that counts successful appends.
struct CountingLog {
    inner: InMemoryCommandLog,
    appends: Arc<AtomicUsize>,
}

impl CountingLog {
    fn new(inner: InMemoryCommandLog) -> Self {
        Self {
            inner,
            appends: Arc::new(AtomicUsize::new(0)),
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
        self.inner.append(record)?;
        self.appends.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn read_from(&self, from: u64) -> Result<Vec<CommandRecord>, DurableError> {
        self.inner.read_from(from)
    }

    fn first_sequence(&self) -> Option<u64> {
        self.inner.first_sequence()
    }

    fn last_sequence(&self) -> Option<u64> {
        self.inner.last_sequence()
    }
}

/// A snapshot store decorator that counts successful saves.
struct CountingSnapshotStore {
    inner: InMemorySnapshotStore,
    saves: Arc<AtomicUsize>,
}

impl CountingSnapshotStore {
    fn new(inner: InMemorySnapshotStore) -> Self {
        Self {
            inner,
            saves: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl SnapshotStore for CountingSnapshotStore {
    fn save(&mut self, snapshot: StoredSnapshot) -> Result<(), DurableError> {
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

// ---------------------------------------------------------------------------
// Crash windows and storage failures
// ---------------------------------------------------------------------------

#[test]
fn a_record_appended_without_being_applied_is_applied_on_recovery() {
    // Crash window: append returned durably, then the process died before the
    // engine applied the command. The raw adapter can produce exactly that.
    let mut log = InMemoryCommandLog::new();
    let snapshots = InMemorySnapshotStore::new();
    let durable = DurableMatchingEngine::create(
        EngineConfig::default(),
        Box::new(log.handle()),
        Box::new(snapshots.handle()),
        0,
    )
    .unwrap();
    drop(durable);

    let command = submit(BTC, limit(1, Side::Bid, 100, 5));
    log.append(CommandRecord {
        format_version: FORMAT_VERSION,
        sequence: 1,
        command: command.clone(),
    })
    .unwrap();

    let reopened =
        DurableMatchingEngine::open(Box::new(log.handle()), Box::new(snapshots.handle()), 0)
            .unwrap();

    assert_eq!(reopened.applied_seq(), 1);
    assert_eq!(
        reopened.engine().capture_snapshot(),
        pure_engine_after(std::slice::from_ref(&command)).capture_snapshot()
    );
}

#[test]
fn append_failure_leaves_the_command_unapplied_and_poisons_the_engine() {
    let raw_log = InMemoryCommandLog::new();
    let fail_appends = Arc::new(AtomicBool::new(false));
    let snapshots = InMemorySnapshotStore::new();
    let mut durable = DurableMatchingEngine::create(
        EngineConfig::default(),
        Box::new(FailingLog::new(raw_log.handle(), fail_appends.clone())),
        Box::new(snapshots.handle()),
        0,
    )
    .unwrap();

    durable
        .execute(submit(BTC, limit(1, Side::Bid, 100, 5)))
        .unwrap();
    let before_failure = durable.engine().capture_snapshot();

    fail_appends.store(true, Ordering::SeqCst);
    let error = durable
        .execute(submit(ETH, limit(2, Side::Ask, 200, 4)))
        .expect_err("the append must fail");
    assert_eq!(
        error,
        DurableError::AppendFailed("injected append failure".to_string())
    );

    assert_eq!(
        durable.engine().capture_snapshot(),
        before_failure,
        "the failed command must not reach the engine"
    );
    assert_eq!(durable.applied_seq(), 1);
    assert!(durable.is_failed());
    assert_eq!(raw_log.read_from(1).unwrap().len(), 1);
    assert!(
        snapshots.load_latest().unwrap().is_none(),
        "no snapshot may be written after a failed append"
    );

    let later = durable
        .execute(submit(BTC, limit(3, Side::Ask, 101, 1)))
        .expect_err("a failed engine accepts no more commands");
    assert_eq!(later, DurableError::EngineFailed);
    assert_eq!(raw_log.read_from(1).unwrap().len(), 1);
}

// ---------------------------------------------------------------------------
// Gap and ordering behavior
// ---------------------------------------------------------------------------

#[test]
fn a_duplicate_sequence_is_skipped_during_replay() {
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
    for command in &commands {
        durable.execute(command.clone()).unwrap();
    }
    let before_crash = durable.engine().capture_snapshot();
    drop(durable);

    let mut records = log.read_from(2).unwrap();
    let duplicate = records.remove(0);
    assert_eq!(duplicate.sequence, 2);

    let reopened = DurableMatchingEngine::open(
        Box::new(DuplicateLog {
            inner: log.handle(),
            duplicate,
        }),
        Box::new(snapshots.handle()),
        0,
    )
    .expect("an already-applied duplicate is skipped");

    assert_eq!(reopened.applied_seq(), commands.len() as u64);
    assert_eq!(
        reopened.engine().capture_snapshot(),
        before_crash,
        "double-applying a duplicate would change order or trade ids"
    );
}

#[test]
fn a_non_contiguous_sequence_refuses_to_open() {
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
    for command in &commands {
        durable.execute(command.clone()).unwrap();
    }
    drop(durable);

    let extra = CommandRecord {
        format_version: FORMAT_VERSION,
        sequence: 4,
        command: submit(ETH, limit(9, Side::Bid, 150, 1)),
    };
    let error = match DurableMatchingEngine::open(
        Box::new(SplicedLog {
            inner: log.handle(),
            after: 2,
            extra,
        }),
        Box::new(snapshots.handle()),
        0,
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
fn a_log_starting_above_sequence_one_refuses_to_open() {
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
    drop(durable);

    let error = match DurableMatchingEngine::open(
        Box::new(PrefixHiddenLog {
            inner: log.handle(),
            hide_through: 2,
        }),
        Box::new(snapshots.handle()),
        0,
    ) {
        Ok(_) => panic!("recovery must refuse a missing prefix"),
        Err(error) => error,
    };

    assert_eq!(
        error,
        DurableError::RecoveryGap {
            expected: 1,
            found: Some(3)
        }
    );
}

// ---------------------------------------------------------------------------
// Header and version refusal
// ---------------------------------------------------------------------------

#[test]
fn a_missing_log_header_refuses_to_open() {
    let error = match DurableMatchingEngine::open(
        Box::new(InMemoryCommandLog::new()),
        Box::new(InMemorySnapshotStore::new()),
        0,
    ) {
        Ok(_) => panic!("recovery must refuse a log without a header"),
        Err(error) => error,
    };

    assert_eq!(error, DurableError::MissingHeader);
}

#[test]
fn an_unsupported_header_version_refuses_to_open() {
    let mut log = InMemoryCommandLog::new();
    log.write_header(LogHeader {
        format_version: 2,
        config: EngineConfig::default(),
    })
    .unwrap();

    let error =
        match DurableMatchingEngine::open(Box::new(log), Box::new(InMemorySnapshotStore::new()), 0)
        {
            Ok(_) => panic!("recovery must refuse an unsupported version"),
            Err(error) => error,
        };

    assert_eq!(
        error,
        DurableError::UnsupportedVersion {
            found: 2,
            supported: FORMAT_VERSION
        }
    );
}

#[test]
fn an_unsupported_snapshot_version_refuses_to_open() {
    let mut log = InMemoryCommandLog::new();
    log.write_header(LogHeader {
        format_version: FORMAT_VERSION,
        config: EngineConfig::default(),
    })
    .unwrap();

    let mut snapshots = InMemorySnapshotStore::new();
    snapshots
        .save(StoredSnapshot {
            format_version: 2,
            applied_seq: 0,
            snapshot: MatchingEngine::new(EngineConfig::default()).capture_snapshot(),
        })
        .unwrap();

    let error = match DurableMatchingEngine::open(Box::new(log), Box::new(snapshots), 0) {
        Ok(_) => panic!("recovery must refuse an unsupported snapshot version"),
        Err(error) => error,
    };

    assert_eq!(
        error,
        DurableError::UnsupportedVersion {
            found: 2,
            supported: FORMAT_VERSION
        }
    );
}

// ---------------------------------------------------------------------------
// Adapter substitutability
// ---------------------------------------------------------------------------

#[test]
fn a_swapped_adapter_decorator_needs_no_durable_layer_changes() {
    let log = CountingLog::new(InMemoryCommandLog::new());
    let appends = log.appends.clone();
    let snapshots = CountingSnapshotStore::new(InMemorySnapshotStore::new());
    let saves = snapshots.saves.clone();

    let commands = sample_commands();
    let mut durable = DurableMatchingEngine::create(
        EngineConfig::default(),
        Box::new(log),
        Box::new(snapshots),
        0,
    )
    .unwrap();
    for command in &commands {
        durable.execute(command.clone()).unwrap();
    }
    durable.snapshot_now().unwrap();

    assert_eq!(appends.load(Ordering::SeqCst), commands.len());
    assert_eq!(saves.load(Ordering::SeqCst), 1);
    assert_eq!(
        durable.engine().capture_snapshot(),
        pure_engine_after(&commands).capture_snapshot()
    );
}
