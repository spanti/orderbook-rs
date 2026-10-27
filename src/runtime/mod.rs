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
//! - **Matching thread**: owns all mutable engine state, the next event and
//!   command counters, the journal buffer, the pending-durable queue, and the
//!   snapshot gate. It is the only writer, so the single-writer invariant is a
//!   structural property rather than a convention.
//! - **Journal thread**: owns the [`EventLog`](crate::durable::EventLog).
//!   It coalesces batches, calls `append_batch`, and advances the shared
//!   durability watermark only after the append returns.
//! - **Event Publisher**: runs on its own thread. It shares the log handle with
//!   the journal only to read committed records, and synchronously publishes
//!   durable [`EventRecord`](crate::durable::EventRecord)s on the
//!   [`MessageBus`](crate::bus::MessageBus). It starts at the recovered
//!   durability watermark and never replays history.
//! - **Snapshot worker**: owns the
//!   [`SnapshotStore`](crate::durable::SnapshotStore) and saves
//!   snapshots off the matching thread.
//! - **Producers**: own only [`EngineHandle`] clones. They never touch engine
//!   state.
//!
//! # Channel topology
//!
//! ```text
//! producers --try_submit--> ingress sync_channel(queue_capacity)
//!     --> matching thread --apply--> Applied Acknowledgement
//!         matching thread --Vec<EventRecord>--> journal sync_channel(journal_batch_capacity)
//!             --> journal thread --append_batch + fsync--> durability watermark
//!                 --> matching thread --Durable Acknowledgement
//!                 --> Event Publisher --read_from(watermark)--> MessageBus subscribers
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
//! - The matching thread's `Sequencer` assigns one global, contiguous **event**
//!   sequence in application order and one `command_id` per command. The event
//!   sequence is the recovery, durability, and snapshot boundary; `command_id`
//!   groups the events one command produced.
//! - Commands submitted from one producer thread are applied in submission
//!   order because the std channel preserves per-sender FIFO. Commands from
//!   different producers interleave in whatever order the queue serializes;
//!   that interleaving is the engine's total order.
//! - `applied_seq` never decreases, and the durability watermark never exceeds
//!   it. A durable acknowledgement is sent only after the command's **last**
//!   event sequence is in the log.
//! - The Event Publisher dispatches durable records in event-sequence order.
//!   The bus is synchronous, so a slow handler delays publication for every
//!   subscriber but never the matching thread; a handler panic is isolated.
//!   Consumers that miss events read `EventLog::read_from(last_seen + 1)`.
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
//!   still wait for that fsync. Publication is drained to the final durability
//!   watermark; a handler that cannot be interrupted is detached at the
//!   timeout, and consumers catch up from the event log.
//!
//! # Non-goals and known deviations
//!
//! - No read or query API through [`EngineHandle`]. Durable events are emitted
//!   on the [`MessageBus`](crate::bus::MessageBus); the applied receipt also
//!   carries the command's events. The bus is in-process, synchronous, and has
//!   no per-subscriber queue, lag detection, or command path.
//! - No client idempotency or request deduplication; `request_id` is
//!   observability only.
//! - No per-symbol sharding, timers, GTT, or auction events.
//! - Ingress is a bounded `std::sync::mpsc::sync_channel`, not a custom
//!   Disruptor-style ring.
//! - The response-slot pool removes per-command channel allocation, but the
//!   ingress channel still has bounded internal buffering and synchronization;
//!   the command itself may still allocate when constructed by the caller.
//! - One journal thread with group commit; no replication and no `io_uring`.
//!   There is no real-time priority or NUMA control, and core affinity is
//!   opt-in only.
//! - Trade history, the order ledger, and the event log grow for the process
//!   lifetime; snapshots grow with the ledger and trades. Retention and
//!   archival are out of scope.
//! - `wait_applied` is preliminary and can lose data on a crash between apply
//!   and fsync. `wait_durable` is the safe default.
//! - This is not HFT-grade latency until a custom ring, CPU isolation, and
//!   async IO exist.

pub(crate) mod assembly;
pub mod envelope;
pub mod error;
pub(crate) mod idle;
pub(crate) mod ingress_gauge;
pub(crate) mod journal;
pub(crate) mod lifecycle;
pub(crate) mod matching;
pub mod metrics;
pub(crate) mod publisher;
pub(crate) mod publisher_signal;
pub(crate) mod run_status;
pub(crate) mod shared;
pub(crate) mod snapshot;
pub(crate) mod snapshot_gate;
pub(crate) mod submitter;
pub(crate) mod sync;
pub(crate) mod ticket;
pub(crate) mod watermark;

pub use envelope::CommandEnvelope;
pub use error::{RuntimeError, SubmitError, Worker};
pub use idle::IdleStrategy;
#[doc(hidden)]
pub use lifecycle::start_with_matching_panic_for_test;
pub use lifecycle::{EngineHandle, EngineRuntime};
pub use metrics::RuntimeMetrics;
#[doc(hidden)]
pub use submitter::enqueued_at_for_test;
pub use ticket::{CommandReceipt, Ticket};

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
    /// Automatic snapshot cadence in **event** sequences; `0` disables them.
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
            journal_batch_capacity: 256,
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
        assert_eq!(config.journal_batch_capacity, 256);
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
