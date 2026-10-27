//! Event-sequence snapshots: derived caches tagged with an event boundary.
//!
//! [`StoredSnapshot`] records the event `applied_seq` and the last
//! `command_id` so command grouping continues after a restart even when the
//! log tail is empty.
//!
//! A snapshot is never a fact source. Recovery restores the latest complete
//! snapshot and folds the contiguous event-log tail after it.

use crate::durable::{DurableError, FORMAT_VERSION};
use crate::engine::MatchingEngine;
use crate::snapshot::EngineSnapshot;

/// A complete engine snapshot tagged with the event sequence it covers.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StoredSnapshot {
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

impl StoredSnapshot {
    /// Capture `engine` only when the applied and durable boundaries agree.
    ///
    /// A snapshot must never cover an event the log does not hold, so this is
    /// the single checked construction used by every production capture site.
    /// The format version is always stamped here.
    pub(crate) fn capture_checked(
        engine: &MatchingEngine,
        applied_seq: u64,
        durable_seq: u64,
        last_command_id: u64,
    ) -> Result<Self, DurableError> {
        if applied_seq != durable_seq {
            return Err(DurableError::Storage(format!(
                "cannot snapshot applied sequence {applied_seq} with durable sequence {durable_seq}"
            )));
        }

        Ok(Self {
            format_version: FORMAT_VERSION,
            applied_seq,
            last_command_id,
            snapshot: engine.capture_snapshot(),
        })
    }
}

/// The latest complete event-sequence snapshots.
///
/// Only complete, decodable snapshots are returned; the durable layer validates
/// their format version and config. Implementations keep the two newest so a
/// corrupt newest snapshot can fall back to the previous one.
pub trait SnapshotStore: Send + 'static {
    /// Store a snapshot. A sequence below the current floor is refused; an
    /// equal sequence is allowed.
    fn save(&mut self, snapshot: StoredSnapshot) -> Result<(), DurableError>;

    /// The newest complete snapshot, if any.
    fn load_latest(&self) -> Result<Option<StoredSnapshot>, DurableError>;

    /// The second-newest complete snapshot, if any.
    fn load_previous(&self) -> Result<Option<StoredSnapshot>, DurableError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{EngineCommand, EngineConfig, OrderRequest, OrderType, Side, TimeInForce};

    /// An engine with one resting order, so a captured snapshot is non-empty.
    fn engine_with_state() -> MatchingEngine {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        engine.execute(EngineCommand::SubmitOrder {
            symbol: "BTC-USDT".to_string(),
            request: OrderRequest {
                side: Side::Bid,
                price: 100,
                quantity: 5,
                timestamp: 1,
                order_type: OrderType::Limit,
                time_in_force: TimeInForce::GTC,
                post_only: false,
            },
        });
        engine
    }

    #[test]
    fn capture_checked_rejects_applied_seq_ahead_of_durability() {
        let engine = engine_with_state();

        let error = StoredSnapshot::capture_checked(&engine, 2, 1, 1)
            .expect_err("a snapshot must not cover an event the log may not hold");
        assert!(matches!(error, DurableError::Storage(_)));
    }

    #[test]
    fn capture_checked_rejects_applied_seq_behind_durability() {
        let engine = engine_with_state();

        let error = StoredSnapshot::capture_checked(&engine, 1, 2, 1)
            .expect_err("the applied and durable boundaries must agree");
        assert!(matches!(error, DurableError::Storage(_)));
    }

    #[test]
    fn capture_checked_accepts_equal_boundaries_and_stamps_the_format_version() {
        let engine = engine_with_state();

        let stored = StoredSnapshot::capture_checked(&engine, 3, 3, 7)
            .expect("equal boundaries must be snapshotable");

        assert_eq!(stored.format_version, FORMAT_VERSION);
        assert_eq!(stored.applied_seq, 3);
        assert_eq!(stored.last_command_id, 7);
        assert_eq!(stored.snapshot, engine.capture_snapshot());
    }

    #[test]
    fn capture_checked_captures_the_current_engine_state() {
        let engine = engine_with_state();

        let stored = StoredSnapshot::capture_checked(&engine, 0, 0, 0)
            .expect("an empty watermark is snapshotable");

        assert_eq!(stored.snapshot.next_order_id, 2);
        assert_eq!(stored.snapshot.ledger.len(), 1);
        assert_eq!(stored.snapshot.books.len(), 1);
        assert_eq!(stored.snapshot.books[0].symbol, "BTC-USDT");
        assert!(stored.snapshot.trades.is_empty());
    }
}
