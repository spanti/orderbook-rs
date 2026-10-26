//! State shared by every matching-runtime component.
//!
//! The ingress submitters, the matching thread, the journal thread, and the
//! lifecycle all observe the same closed/failed flags, sequence counters, and
//! shutdown deadline. The cardinally shared values are atomics; the fatal error
//! and the shutdown deadline use a mutex because they are rare control-path
//! state.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::durable::DurableError;

use super::metrics::RuntimeMetrics;
use super::sync::lock;

/// State shared by the ingress submitters, the matching thread, the journal
/// thread and the runtime lifecycle.
#[derive(Clone)]
pub(crate) struct SharedState {
    closed: Arc<AtomicBool>,
    failed: Arc<AtomicBool>,
    fatal: Arc<Mutex<Option<DurableError>>>,
    durable_seq: Arc<AtomicU64>,
    applied_seq: Arc<AtomicU64>,
    applied_command_id: Arc<AtomicU64>,
    /// Deadline after which a closed matching thread stops waiting for the
    /// journal and aborts shutdown with a timeout.
    shutdown_deadline: Arc<Mutex<Option<Instant>>>,
    timed_out: Arc<AtomicBool>,
    /// Envelopes currently in the ingress command queue. Used only to report
    /// queue-depth metrics; it is not a correctness signal.
    pub(crate) ingress_depth: Arc<AtomicU64>,
    /// Optional metrics sink. Looked up on both submission and dequeue paths
    /// so a metrics-disabled runtime performs no depth atomics at all.
    pub(crate) metrics: Option<Arc<RuntimeMetrics>>,
}

impl SharedState {
    pub(crate) fn new(metrics: Option<Arc<RuntimeMetrics>>) -> Self {
        Self::with_durable_seq(metrics, Arc::new(AtomicU64::new(0)))
    }

    /// Build shared state whose watermark is owned elsewhere, as the journal
    /// thread owns it.
    pub(crate) fn with_durable_seq(
        metrics: Option<Arc<RuntimeMetrics>>,
        durable_seq: Arc<AtomicU64>,
    ) -> Self {
        Self {
            closed: Arc::new(AtomicBool::new(false)),
            failed: Arc::new(AtomicBool::new(false)),
            fatal: Arc::new(Mutex::new(None)),
            durable_seq,
            applied_seq: Arc::new(AtomicU64::new(0)),
            applied_command_id: Arc::new(AtomicU64::new(0)),
            shutdown_deadline: Arc::new(Mutex::new(None)),
            timed_out: Arc::new(AtomicBool::new(false)),
            ingress_depth: Arc::new(AtomicU64::new(0)),
            metrics,
        }
    }

    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Record the first fatal error and fail the runtime closed.
    pub(crate) fn fail(&self, error: DurableError) {
        let mut fatal = lock(&self.fatal);
        if fatal.is_none() {
            *fatal = Some(error);
        }
        drop(fatal);
        self.failed.store(true, Ordering::Release);
    }

    pub(crate) fn is_failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    pub(crate) fn failure(&self) -> Option<DurableError> {
        lock(&self.fatal).clone()
    }

    pub(crate) fn durable_seq(&self) -> u64 {
        self.durable_seq.load(Ordering::Acquire)
    }

    /// Advance the durability watermark to at least `sequence`.
    pub(crate) fn publish_durable(&self, sequence: u64) {
        self.durable_seq.fetch_max(sequence, Ordering::Release);
    }

    /// The last event sequence the matching thread applied.
    pub(crate) fn applied_seq(&self) -> u64 {
        self.applied_seq.load(Ordering::Acquire)
    }

    /// Publish the last applied event sequence.
    pub(crate) fn publish_applied(&self, sequence: u64) {
        self.applied_seq.store(sequence, Ordering::Release);
    }

    /// The `command_id` of the last command whose events were applied; `0`
    /// before any command.
    pub(crate) fn applied_command_id(&self) -> u64 {
        self.applied_command_id.load(Ordering::Acquire)
    }

    /// Publish the `command_id` of the last applied command.
    pub(crate) fn publish_applied_command_id(&self, command_id: u64) {
        self.applied_command_id.store(command_id, Ordering::Release);
    }

    /// Bound how long a closed matching thread waits for the journal.
    pub(crate) fn set_shutdown_deadline(&self, deadline: Instant) {
        *lock(&self.shutdown_deadline) = Some(deadline);
    }

    /// Whether shutdown's durability deadline has elapsed.
    pub(crate) fn shutdown_expired(&self) -> bool {
        lock(&self.shutdown_deadline).is_some_and(|deadline| Instant::now() >= deadline)
    }

    /// Record that shutdown stopped waiting for durability.
    pub(crate) fn mark_timed_out(&self) {
        self.timed_out.store(true, Ordering::Release);
    }

    /// Whether shutdown aborted because the durability deadline elapsed.
    pub(crate) fn timed_out(&self) -> bool {
        self.timed_out.load(Ordering::Acquire)
    }

    /// Count one envelope offered to the ingress queue.
    ///
    /// Metrics-only: when metrics are disabled this performs no atomic update.
    /// The increment happens *before* the send, so a consumer that dequeues
    /// immediately can never decrement the gauge below zero.
    pub(crate) fn note_ingress_enqueued(&self) {
        if self.metrics.is_some() {
            self.ingress_depth.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Undo [`Self::note_ingress_enqueued`] when the bounded channel refused
    /// the offer, so a refusal is not counted as queue depth.
    pub(crate) fn note_ingress_refused(&self) {
        if self.metrics.is_some() {
            self.ingress_depth.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Count one envelope removed from the ingress queue.
    ///
    /// Returns the depth observed before the decrement, or `None` when metrics
    /// are disabled. The decrement saturates at zero, so a spurious pair of
    /// decrements cannot wrap the gauge to `u64::MAX`.
    pub(crate) fn note_ingress_dequeued(&self) -> Option<u64> {
        if self.metrics.is_none() {
            return None;
        }
        let previous = self
            .ingress_depth
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |depth| {
                Some(depth.saturating_sub(1))
            })
            .unwrap_or(0);
        Some(previous)
    }
}
