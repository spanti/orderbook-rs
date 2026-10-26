//! Event-sequence snapshots: derived caches tagged with an event boundary.
//!
//! [`EventSnapshot`] is distinct from the command-sourced
//! [`StoredSnapshot`](crate::durable::StoredSnapshot) while both paths coexist:
//! the event snapshot's `applied_seq` is an **event** sequence and it also
//! records the last `command_id` so command grouping continues after a restart
//! even when the log tail is empty. Ticket 09 owns unifying the record types.
//!
//! A snapshot is never a fact source. Recovery restores the latest complete
//! snapshot and folds the contiguous event-log tail after it.

use crate::durable::DurableError;
use crate::snapshot::EngineSnapshot;

/// A complete engine snapshot tagged with the event sequence it covers.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct EventSnapshot {
    pub format_version: u32,
    /// Last **event** sequence reflected in `snapshot`.
    pub applied_seq: u64,
    /// Last `command_id` reflected in `snapshot`; `0` before any command.
    ///
    /// Defaulted so a checksum-valid v1 snapshot still decodes and is refused
    /// by its format version instead of being silently skipped as corrupt.
    #[serde(default)]
    pub last_command_id: u64,
    pub snapshot: EngineSnapshot,
}

/// The latest complete event-sequence snapshots.
///
/// Only complete, decodable snapshots are returned; the durable layer validates
/// their format version and config. Implementations keep the two newest so a
/// corrupt newest snapshot can fall back to the previous one.
pub trait EventSnapshotStore: Send + 'static {
    /// Store a snapshot. A sequence below the current floor is refused; an
    /// equal sequence is allowed.
    fn save(&mut self, snapshot: EventSnapshot) -> Result<(), DurableError>;

    /// The newest complete snapshot, if any.
    fn load_latest(&self) -> Result<Option<EventSnapshot>, DurableError>;

    /// The second-newest complete snapshot, if any.
    fn load_previous(&self) -> Result<Option<EventSnapshot>, DurableError>;
}
