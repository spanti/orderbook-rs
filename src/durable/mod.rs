//! Durable composition layer: append every command before applying it, then
//! recover exact decision state by replaying the command log.
//!
//! The layer is generic over two narrow, object-safe storage traits:
//! [`CommandLog`] for the single globally ordered command stream and
//! [`SnapshotStore`] for complete engine snapshots. Only
//! [`DurableMatchingEngine`] may append to the log; applying a command without
//! appending is reserved for the private recovery path.

use std::fmt;

use crate::engine::MatchingEngine;
use crate::snapshot::EngineSnapshot;
use crate::types::{CommandOutcome, EngineCommand, EngineConfig};

mod framing;

pub mod file_log;
pub mod file_snapshot;
pub mod memory;

pub use file_log::FileCommandLog;
pub use file_snapshot::FileSnapshotStore;
pub use memory::{InMemoryCommandLog, InMemorySnapshotStore};

/// Version written into every durable envelope. Incompatible versions are
/// refused rather than migrated.
pub const FORMAT_VERSION: u32 = 1;

/// The first record in a command log: format version plus the engine config
/// every record and snapshot must agree with.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LogHeader {
    pub format_version: u32,
    pub config: EngineConfig,
}

/// One appended command, with its globally ordered command sequence number.
///
/// The sequence is the durability and recovery boundary. It is unrelated to the
/// engine's event sequence (`EngineEvent::sequence_id`): one command can emit
/// several events.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CommandRecord {
    pub format_version: u32,
    /// Global **command** sequence, starting at 1. Not an event sequence.
    pub sequence: u64,
    pub command: EngineCommand,
}

/// A complete engine snapshot tagged with the last command sequence it covers.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StoredSnapshot {
    pub format_version: u32,
    /// **Command** sequence of the last command reflected in `snapshot`, not
    /// the engine's event sequence.
    pub applied_seq: u64,
    pub snapshot: EngineSnapshot,
}

/// Every way durable persistence or recovery can fail to preserve exact state.
#[derive(Debug, Clone, PartialEq)]
pub enum DurableError {
    /// The log has records but no header, or no header at all.
    MissingHeader,
    /// A header was written when one already existed.
    HeaderAlreadyPresent,
    /// A record, header or snapshot uses a format this build cannot read.
    UnsupportedVersion { found: u32, supported: u32 },
    /// The log header and the stored snapshot disagree about engine config.
    ConfigMismatch {
        header: EngineConfig,
        snapshot: EngineConfig,
    },
    /// The log does not begin at the sequence recovery needs.
    RecoveryGap { expected: u64, found: Option<u64> },
    /// The snapshot covers commands the log no longer contains.
    SnapshotAhead { applied_seq: u64, log_last_seq: u64 },
    /// A record arrived out of order and is not a duplicate.
    NonContiguousSequence { expected: u64, found: u64 },
    /// The snapshot store refused to replace a newer applied sequence.
    SnapshotBelowFloor { applied_seq: u64, floor: u64 },
    /// The command log could not make a record durable.
    AppendFailed(String),
    /// General storage adapter failure.
    Storage(String),
    /// A stored record could not be decoded or failed its integrity check.
    CorruptRecord { detail: String },
    /// The durable engine stopped accepting commands after an append failure.
    EngineFailed,
}

impl fmt::Display for DurableError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingHeader => write!(f, "command log header is missing"),
            Self::HeaderAlreadyPresent => write!(f, "command log header is already present"),
            Self::UnsupportedVersion { found, supported } => {
                write!(
                    f,
                    "unsupported format version {found} (supported: {supported})"
                )
            }
            Self::ConfigMismatch { header, snapshot } => write!(
                f,
                "log config {header:?} does not match snapshot config {snapshot:?}"
            ),
            Self::RecoveryGap { expected, found } => write!(
                f,
                "recovery gap: expected sequence {expected}, found {found:?}"
            ),
            Self::SnapshotAhead {
                applied_seq,
                log_last_seq,
            } => write!(
                f,
                "snapshot applied sequence {applied_seq} is ahead of log last sequence {log_last_seq}"
            ),
            Self::NonContiguousSequence { expected, found } => write!(
                f,
                "non-contiguous command sequence: expected {expected}, found {found}"
            ),
            Self::SnapshotBelowFloor { applied_seq, floor } => write!(
                f,
                "snapshot applied sequence {applied_seq} is below the floor {floor}"
            ),
            Self::AppendFailed(detail) => write!(f, "command append failed: {detail}"),
            Self::Storage(detail) => write!(f, "storage failure: {detail}"),
            Self::CorruptRecord { detail } => write!(f, "corrupt record: {detail}"),
            Self::EngineFailed => write!(f, "durable engine is failed and accepts no commands"),
        }
    }
}

impl std::error::Error for DurableError {}

/// The single globally ordered, append-only command stream.
///
/// Implementations make an appended record durable before returning, read
/// records back in append order, and never invent, reorder or deduplicate
/// sequences. Sequence bookkeeping stays visible to the durable layer so it can
/// detect gaps.
pub trait CommandLog {
    /// Read the log header, or `None` when the log has no header yet.
    fn read_header(&self) -> Result<Option<LogHeader>, DurableError>;

    /// Write the header. Fails with [`DurableError::HeaderAlreadyPresent`]
    /// when a header already exists.
    fn write_header(&mut self, header: LogHeader) -> Result<(), DurableError>;

    /// Append a record. Rejects a record whose sequence is not the next one.
    fn append(&mut self, record: CommandRecord) -> Result<(), DurableError>;

    /// Every record whose **command** sequence is `>= from`, in append order.
    fn read_from(&self, from: u64) -> Result<Vec<CommandRecord>, DurableError>;

    /// First stored **command** sequence, cached with no IO.
    fn first_sequence(&self) -> Option<u64>;

    /// Last stored **command** sequence, cached with no IO.
    fn last_sequence(&self) -> Option<u64>;
}

/// The latest complete engine snapshots. Only complete, decodable snapshots are
/// returned; the durable layer validates their format version and config.
pub trait SnapshotStore {
    /// Store a snapshot. A sequence below the current floor is refused; an
    /// equal sequence is allowed. Implementations keep the two newest.
    fn save(&mut self, snapshot: StoredSnapshot) -> Result<(), DurableError>;

    /// The newest complete snapshot, if any.
    fn load_latest(&self) -> Result<Option<StoredSnapshot>, DurableError>;

    /// The second-newest complete snapshot, if any.
    fn load_previous(&self) -> Result<Option<StoredSnapshot>, DurableError>;
}

/// A [`MatchingEngine`] that makes every command durable before applying it and
/// recovers by restoring the latest snapshot then replaying the log tail.
///
/// The sequence numbers on this surface (`applied_seq`, `CommandRecord` and
/// `StoredSnapshot`) are **command** sequences. They are distinct from the
/// engine's event sequence ([`MatchingEngine::last_sequence`]): one command can
/// emit several events, so only the command sequence is a recovery boundary.
pub struct DurableMatchingEngine {
    engine: MatchingEngine,
    log: Box<dyn CommandLog>,
    snapshots: Box<dyn SnapshotStore>,
    next_command_sequence: u64,
    applied_seq: u64,
    snapshot_cadence: u64,
    failed: bool,
    last_snapshot_error: Option<DurableError>,
}

impl fmt::Debug for DurableMatchingEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableMatchingEngine")
            .field("applied_seq", &self.applied_seq)
            .field("next_command_sequence", &self.next_command_sequence)
            .field("snapshot_cadence", &self.snapshot_cadence)
            .field("failed", &self.failed)
            .field("last_snapshot_error", &self.last_snapshot_error)
            .finish_non_exhaustive()
    }
}

impl DurableMatchingEngine {
    /// Create a fresh engine, writing the log header. The log must not already
    /// have a header and both stores must start empty.
    pub fn create(
        config: EngineConfig,
        mut log: Box<dyn CommandLog>,
        snapshots: Box<dyn SnapshotStore>,
        snapshot_cadence: u64,
    ) -> Result<Self, DurableError> {
        log.write_header(LogHeader {
            format_version: FORMAT_VERSION,
            config,
        })?;

        Ok(Self {
            engine: MatchingEngine::new(config),
            log,
            snapshots,
            next_command_sequence: 1,
            applied_seq: 0,
            snapshot_cadence,
            failed: false,
            last_snapshot_error: None,
        })
    }

    /// Recover from an existing log: read the header, restore the latest
    /// snapshot, then replay the log tail after it.
    pub fn open(
        log: Box<dyn CommandLog>,
        snapshots: Box<dyn SnapshotStore>,
        snapshot_cadence: u64,
    ) -> Result<Self, DurableError> {
        let header = log.read_header()?.ok_or(DurableError::MissingHeader)?;
        if header.format_version != FORMAT_VERSION {
            return Err(DurableError::UnsupportedVersion {
                found: header.format_version,
                supported: FORMAT_VERSION,
            });
        }

        let (engine, applied_seq) = match snapshots.load_latest()? {
            Some(stored) => {
                if stored.format_version != FORMAT_VERSION {
                    return Err(DurableError::UnsupportedVersion {
                        found: stored.format_version,
                        supported: FORMAT_VERSION,
                    });
                }
                if stored.snapshot.config != header.config {
                    return Err(DurableError::ConfigMismatch {
                        header: header.config,
                        snapshot: stored.snapshot.config,
                    });
                }
                match log.last_sequence() {
                    Some(last) if stored.applied_seq > last => {
                        return Err(DurableError::SnapshotAhead {
                            applied_seq: stored.applied_seq,
                            log_last_seq: last,
                        });
                    }
                    None if stored.applied_seq > 0 => {
                        return Err(DurableError::SnapshotAhead {
                            applied_seq: stored.applied_seq,
                            log_last_seq: 0,
                        });
                    }
                    _ => {}
                }

                (
                    MatchingEngine::restore_snapshot(stored.snapshot),
                    stored.applied_seq,
                )
            }
            None => (MatchingEngine::new(header.config), 0),
        };

        // The log must cover every command after the snapshot boundary. With no
        // snapshot that boundary is zero, so the log must start at sequence 1.
        let expected_first = applied_seq.saturating_add(1);
        if let Some(first) = log.first_sequence()
            && first > expected_first
        {
            return Err(DurableError::RecoveryGap {
                expected: expected_first,
                found: Some(first),
            });
        }

        let mut durable = Self {
            engine,
            log,
            snapshots,
            next_command_sequence: 1,
            applied_seq,
            snapshot_cadence,
            failed: false,
            last_snapshot_error: None,
        };

        let tail = durable
            .log
            .read_from(durable.applied_seq.saturating_add(1))?;
        for record in tail {
            if record.format_version != FORMAT_VERSION {
                return Err(DurableError::UnsupportedVersion {
                    found: record.format_version,
                    supported: FORMAT_VERSION,
                });
            }
            if record.sequence <= durable.applied_seq {
                // At-least-once delivery: this sequence is already applied.
                continue;
            }
            let expected = durable.applied_seq.saturating_add(1);
            if record.sequence != expected {
                return Err(DurableError::NonContiguousSequence {
                    expected,
                    found: record.sequence,
                });
            }
            durable.apply_replayed(record.command);
            durable.applied_seq = record.sequence;
        }

        // A recovered engine must continue after the log's tail even when the
        // tail was all duplicates, and after the snapshot when the tail is empty.
        durable.next_command_sequence = durable
            .log
            .last_sequence()
            .map(|last| last.saturating_add(1))
            .unwrap_or(1)
            .max(durable.applied_seq.saturating_add(1));

        Ok(durable)
    }

    /// Create a fresh durable engine and seed it through the append-then-apply
    /// path, so seeding is indistinguishable from live execution.
    pub fn from_commands(
        config: EngineConfig,
        commands: &[EngineCommand],
        log: Box<dyn CommandLog>,
        snapshots: Box<dyn SnapshotStore>,
        snapshot_cadence: u64,
    ) -> Result<Self, DurableError> {
        let mut durable = Self::create(config, log, snapshots, snapshot_cadence)?;
        for command in commands {
            durable.execute(command.clone())?;
        }
        Ok(durable)
    }

    /// Make a command durable, then apply it. A rejected command is still
    /// durable and returns [`CommandOutcome::Rejected`].
    pub fn execute(&mut self, command: EngineCommand) -> Result<CommandOutcome, DurableError> {
        if self.failed {
            return Err(DurableError::EngineFailed);
        }

        let sequence = self.next_command_sequence;
        let record = CommandRecord {
            format_version: FORMAT_VERSION,
            sequence,
            command: command.clone(),
        };

        if let Err(error) = self.log.append(record) {
            self.failed = true;
            return Err(error);
        }

        self.next_command_sequence = sequence.saturating_add(1);
        self.applied_seq = sequence;
        let outcome = self.engine.execute(command);

        // Automatic snapshots only run between commands, after a successful
        // apply. A failure never fails an already-durable command: it is kept
        // for `last_snapshot_error` and retried at the next cadence boundary.
        if self.snapshot_cadence > 0 && self.applied_seq % self.snapshot_cadence == 0 {
            let _ = self.snapshot_now();
        }

        Ok(outcome)
    }

    /// Private recovery path: apply a logged command without appending it.
    /// Only [`Self::open`] may call this, so no public caller can create
    /// unlogged engine state.
    fn apply_replayed(&mut self, command: EngineCommand) -> CommandOutcome {
        self.engine.execute(command)
    }

    /// Write a snapshot of the current decision state. A failure is stored on
    /// the engine and surfaced through [`Self::last_snapshot_error`]; it never
    /// undoes an already-durable command.
    pub fn snapshot_now(&mut self) -> Result<(), DurableError> {
        let stored = StoredSnapshot {
            format_version: FORMAT_VERSION,
            applied_seq: self.applied_seq,
            snapshot: self.engine.capture_snapshot(),
        };

        match self.snapshots.save(stored) {
            Ok(()) => {
                self.last_snapshot_error = None;
                Ok(())
            }
            Err(error) => {
                self.last_snapshot_error = Some(error.clone());
                Err(error)
            }
        }
    }

    /// Read-only access to the pure engine's decision state.
    pub fn engine(&self) -> &MatchingEngine {
        &self.engine
    }

    /// **Command** sequence of the last applied command; `0` before any command.
    ///
    /// This is the recovery and snapshot boundary. It is not
    /// [`MatchingEngine::last_sequence`], which counts derived events.
    pub fn applied_seq(&self) -> u64 {
        self.applied_seq
    }

    /// Durable command records whose **command** sequence is `from` onward, in
    /// sequence order.
    pub fn history_from(&self, from: u64) -> Result<Vec<CommandRecord>, DurableError> {
        self.log.read_from(from)
    }

    /// The last snapshot failure, if snapshotting is degraded.
    pub fn last_snapshot_error(&self) -> Option<&DurableError> {
        self.last_snapshot_error.as_ref()
    }

    /// Whether an append failure stopped this instance. Failed instances accept
    /// no further commands until reopened.
    pub fn is_failed(&self) -> bool {
        self.failed
    }
}
