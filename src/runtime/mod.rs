//! Matching runtime vocabulary and public types.
//!
//! An [`EngineRuntime`] is a single-writer command pipeline around
//! [`MatchingEngine`](crate::engine::MatchingEngine). Producers submit through
//! cloneable [`EngineHandle`]s; the runtime serializes every command onto one
//! matching thread, **applies it before it is durable**, and reports two
//! acknowledgement levels through [`Ticket`] and [`CommandReceipt`]. See
//! `docs/durable-storage.md` for the storage contract and
//! `docs/adr/0003-apply-first-runtime-with-bounded-queues.md` for the
//! decision to keep the synchronous path append-before-apply while the runtime
//! is apply-first.
//!
//! # Thread ownership
//!
//! - **Matching thread**: owns all mutable engine state, the applied and next
//!   command counters, the journal buffer, the pending-durable queue, and the
//!   snapshot gate. It is the only writer, so the single-writer invariant is a
//!   structural property rather than a convention.
//! - **Journal thread**: owns the [`CommandLog`](crate::durable::CommandLog).
//!   It coalesces batches, calls `append_batch`, and advances the shared
//!   durability watermark only after the append returns.
//! - **Snapshot worker**: owns the
//!   [`SnapshotStore`](crate::durable::SnapshotStore) and saves snapshots off
//!   the matching thread.
//! - **Producers**: own only [`EngineHandle`] clones. They never touch engine
//!   state.
//!
//! # Channel topology
//!
//! ```text
//! producers --try_submit--> ingress sync_channel(queue_capacity)
//!     --> matching thread --apply--> Applied Acknowledgement
//!         matching thread --Vec<CommandRecord>--> journal sync_channel(journal_batch_capacity)
//!             --> journal thread --append_batch + fsync--> durability watermark
//!                 --> matching thread --Durable Acknowledgement
//!         matching thread --StoredSnapshot--> snapshot sync_channel(1)
//!             --> snapshot worker --save--> SnapshotStore
//! ```
//!
//! Every queue is bounded. A full ingress or an exhausted response-slot pool
//! returns [`SubmitError::QueueFull`]; a full journal channel stops further
//! application until the journal catches up; a full snapshot channel keeps the
//! snapshot due and retries without stalling matching. A command accepted by
//! `try_submit` is never dropped.
//!
//! # Ordering guarantees
//!
//! - The matching thread assigns one global, contiguous command sequence in
//!   application order. Sequences are the recovery and snapshot boundary and
//!   are unrelated to event sequences: one command may emit several events.
//! - Commands submitted from one producer thread are applied in submission
//!   order because the std channel preserves per-sender FIFO. Commands from
//!   different producers interleave in whatever order the queue serializes;
//!   that interleaving is the engine's total order.
//! - `applied_seq` never decreases, and the durability watermark never exceeds
//!   it. A durable acknowledgement is sent only after its record is in the
//!   log.
//!
//! # Failure modes
//!
//! - `try_submit` returns [`SubmitError::QueueFull`], [`SubmitError::Shutdown`],
//!   or [`SubmitError::EngineFailed`]. A refused command is neither applied nor
//!   sequenced.
//! - A journal append failure fails the runtime closed: pending and queued
//!   commands receive [`RuntimeError::EngineFailed`] and later submissions are
//!   refused with [`SubmitError::EngineFailed`].
//! - A matching-thread panic drops the response-slot handles, so every waiter
//!   returns [`RuntimeError::EngineStopped`]. A journal failure or panic fails
//!   the runtime closed and completes pending and queued commands with
//!   [`RuntimeError::EngineFailed`]. Explicit shutdown reports a dead worker as
//!   [`RuntimeError::WorkerPanicked`] (`"matching"`, `"journal"`, or
//!   `"snapshot"`).
//! - Shutdown is bounded by [`RuntimeConfig::shutdown_timeout`]. A journal
//!   already blocked in an fsync cannot be interrupted, so the final join may
//!   still wait for that fsync.
//!
//! # Non-goals and known deviations
//!
//! - No read or query API through [`EngineHandle`]. There is no market-data
//!   output stream, and the runtime clears engine events after every command.
//! - No client idempotency or request deduplication; `request_id` is
//!   observability only.
//! - No per-symbol sharding, timers, GTT, or auction events.
//! - Ingress is a bounded `std::sync::mpsc::sync_channel`, not a custom
//!   Disruptor-style ring.
//! - One journal thread with group commit; no replication and no `io_uring`.
//!   There is no real-time priority or NUMA control, and core affinity is
//!   opt-in only.
//! - Trade history, the order ledger, and the command log grow for the process
//!   lifetime; snapshots grow with the ledger and trades. Retention and
//!   archival are out of scope.
//! - `wait_applied` is preliminary and can lose data on a crash between apply
//!   and fsync. `wait_durable` is the safe default.
//! - This is not HFT-grade latency until a custom ring, CPU isolation, and
//!   async IO exist.

pub mod envelope;
pub mod error;
pub mod idle;
pub(crate) mod journal;
pub(crate) mod lifecycle;
pub(crate) mod matching;
pub mod metrics;
pub(crate) mod snapshot;
pub mod ticket;

pub use envelope::CommandEnvelope;
pub use error::{RuntimeError, SubmitError};
pub use idle::{Idle, IdleStrategy};
#[doc(hidden)]
pub use lifecycle::start_with_matching_panic_for_test;
pub use lifecycle::{EngineHandle, EngineRuntime};
#[doc(hidden)]
pub use matching::enqueued_at_for_test;
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
    ///
    /// The value is an **index** into the list returned by
    /// [`core_affinity::get_core_ids`], not a raw platform processor id. An
    /// unsupported platform, an out-of-range index, or a failed bind makes
    /// startup fail with [`RuntimeError::Startup`]; the runtime never silently
    /// runs unpinned. The journal and snapshot threads are not pinned.
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
