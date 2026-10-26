//! Matching runtime vocabulary and public types.
//!
//! This module defines the data exchanged between producers, the matching
//! thread, and the journal thread. Threads and lifecycle live in the runtime
//! implementation; nothing here starts a worker.

pub mod envelope;
pub mod error;
pub mod idle;
pub(crate) mod journal;
pub(crate) mod matching;
pub mod metrics;
pub mod ticket;

pub use envelope::CommandEnvelope;
pub use error::{RuntimeError, SubmitError};
pub use idle::{Idle, IdleStrategy};
pub use metrics::RuntimeMetrics;
pub use ticket::{CommandReceipt, ResponseSlot, SlotHandle, SlotPool, SlotState, Ticket};

use std::sync::Arc;
use std::time::Duration;

/// Tuning for one matching runtime.
///
/// Defaults target a normal low-latency deployment; benchmarks use
/// [`IdleStrategy::Spin`] and a pinned matching core for the low-latency
/// target.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// Bounded ingress command queue capacity.
    pub queue_capacity: usize,
    /// How many journal batches may be in flight before application stops.
    pub journal_batch_capacity: usize,
    /// Maximum records per journal batch.
    pub apply_batch_max: usize,
    /// Maximum records coalesced into one journal append/fsync.
    pub journal_coalesce_max: usize,
    /// Upper bound on how long the first record in a group waits for a flush.
    pub journal_flush_interval: Duration,
    /// Automatic snapshot cadence in command sequences; `0` disables them.
    pub snapshot_cadence: u64,
    /// How the matching thread waits when the queue is empty.
    pub idle: IdleStrategy,
    /// Optional core to pin the matching thread to.
    pub matching_core: Option<usize>,
    /// Bound on shutdown's wait for durability.
    pub shutdown_timeout: Duration,
    /// Optional metrics sink. When `None`, the hot path skips clock reads and
    /// atomic updates.
    pub metrics: Option<Arc<RuntimeMetrics>>,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 1024,
            journal_batch_capacity: 64,
            apply_batch_max: 64,
            journal_coalesce_max: 1024,
            journal_flush_interval: Duration::from_micros(100),
            snapshot_cadence: 0,
            idle: IdleStrategy::Backoff,
            matching_core: None,
            shutdown_timeout: Duration::from_secs(5),
            metrics: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn runtime_config_defaults_match_the_spec() {
        let config = RuntimeConfig::default();

        assert_eq!(config.queue_capacity, 1024);
        assert_eq!(config.journal_batch_capacity, 64);
        assert_eq!(config.apply_batch_max, 64);
        assert_eq!(config.journal_coalesce_max, 1024);
        assert_eq!(config.journal_flush_interval, Duration::from_micros(100));
        assert_eq!(config.snapshot_cadence, 0);
        assert_eq!(config.idle, IdleStrategy::Backoff);
        assert_eq!(config.matching_core, None);
        assert_eq!(config.shutdown_timeout, Duration::from_secs(5));
        assert!(config.metrics.is_none());
    }
}
