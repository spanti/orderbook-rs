//! Durable composition layer: the event log is the only source of truth.
//!
//! The layer is generic over two narrow, object-safe storage traits:
//! [`EventLog`] for the single globally ordered event stream and
//! [`SnapshotStore`] for derived engine snapshots. Only
//! [`DurableMatchingEngine`] may append to the log; recovery folds events with
//! [`crate::engine::MatchingEngine::apply`] and never re-runs matching.

use std::fmt;

use crate::types::EngineConfig;

mod framing;

pub mod event_log;
pub mod event_snapshot;
pub mod file_event_log;
pub mod file_event_snapshot;
pub mod memory;
mod snapshot_files;

pub(crate) use event_log::recover;
pub use event_log::{DurableMatchingEngine, EventCursor, EventLog, EventRecord, FORMAT_VERSION};
pub use event_snapshot::{SnapshotStore, StoredSnapshot};
pub use file_event_log::FileEventLog;
pub use file_event_snapshot::FileSnapshotStore;
pub use memory::{InMemoryEventLog, InMemorySnapshotStore};

/// The first record in an event log: format version plus the engine config
/// every record and snapshot must agree with.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LogHeader {
    pub format_version: u32,
    pub config: EngineConfig,
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
    /// The snapshot covers events the log no longer contains.
    SnapshotAhead { applied_seq: u64, log_last_seq: u64 },
    /// A record arrived out of order and is not a duplicate.
    NonContiguousSequence { expected: u64, found: u64 },
    /// The snapshot store refused to replace a newer applied sequence.
    SnapshotBelowFloor { applied_seq: u64, floor: u64 },
    /// The event log could not make a record durable.
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
            Self::MissingHeader => write!(f, "event log header is missing"),
            Self::HeaderAlreadyPresent => write!(f, "event log header is already present"),
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
                "non-contiguous event sequence: expected {expected}, found {found}"
            ),
            Self::SnapshotBelowFloor { applied_seq, floor } => write!(
                f,
                "snapshot applied sequence {applied_seq} is below the floor {floor}"
            ),
            Self::AppendFailed(detail) => write!(f, "event append failed: {detail}"),
            Self::Storage(detail) => write!(f, "storage failure: {detail}"),
            Self::CorruptRecord { detail } => write!(f, "corrupt record: {detail}"),
            Self::EngineFailed => write!(f, "durable engine is failed and accepts no commands"),
        }
    }
}

impl std::error::Error for DurableError {}
