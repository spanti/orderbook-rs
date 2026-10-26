//! Runtime metrics.
//!
//! Every field is an atomic so the matching and journal threads can update
//! their own counters without coordination. The counters are grouped into
//! cache-line padded blocks: counters written by different threads must not
//! share a cache line.
//!
//! Metrics are optional. When [`crate::runtime::RuntimeConfig::metrics`] is
//! `None`, callers skip both the clock reads and these updates.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[inline]
fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[inline]
fn count(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}

#[inline]
fn add(counter: &AtomicU64, value: u64) {
    counter.fetch_add(value, Ordering::Relaxed);
}

#[inline]
fn observe_max(counter: &AtomicU64, value: u64) {
    counter.fetch_max(value, Ordering::Relaxed);
}

/// Producer-side counters. Written by submission threads.
#[repr(align(64))]
#[derive(Debug, Default)]
struct IngressMetrics {
    submitted: AtomicU64,
    queue_full: AtomicU64,
    engine_failed: AtomicU64,
    stopped: AtomicU64,
}

/// Matching-thread counters.
#[repr(align(64))]
#[derive(Debug, Default)]
struct ApplyMetrics {
    applied: AtomicU64,
    queue_wait_ns_sum: AtomicU64,
    queue_wait_ns_max: AtomicU64,
    apply_ns_sum: AtomicU64,
    apply_ns_max: AtomicU64,
    queue_depth_max: AtomicU64,
}

/// Journal-thread and durable-acknowledgement counters.
#[repr(align(64))]
#[derive(Debug, Default)]
struct DurabilityMetrics {
    durable: AtomicU64,
    durable_wait_ns_sum: AtomicU64,
    durable_wait_ns_max: AtomicU64,
    journal_append_ns_sum: AtomicU64,
    journal_append_ns_max: AtomicU64,
    journal_append_count: AtomicU64,
    batch_records_sum: AtomicU64,
    batch_count: AtomicU64,
}

/// Counters and latency sums/maxima for one matching runtime.
///
/// The matching thread updates apply and durable-wait values; the journal
/// thread updates journal-append values; producers update ingress counts.
/// Readers may observe a value that is one update stale; metrics are
/// best-effort observability, not a correctness boundary.
#[repr(align(64))]
#[derive(Debug, Default)]
pub struct RuntimeMetrics {
    ingress: IngressMetrics,
    apply: ApplyMetrics,
    durability: DurabilityMetrics,
}

impl RuntimeMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    // Producer-side updates.

    pub fn record_submitted(&self) {
        count(&self.ingress.submitted);
    }

    pub fn record_queue_full(&self) {
        count(&self.ingress.queue_full);
    }

    pub fn record_engine_failed(&self) {
        count(&self.ingress.engine_failed);
    }

    pub fn record_stopped(&self) {
        count(&self.ingress.stopped);
    }

    // Matching-thread updates.

    pub fn record_applied(&self) {
        count(&self.apply.applied);
    }

    pub fn record_queue_wait(&self, wait: Duration) {
        let nanos = nanos(wait);
        add(&self.apply.queue_wait_ns_sum, nanos);
        observe_max(&self.apply.queue_wait_ns_max, nanos);
    }

    pub fn record_apply(&self, elapsed: Duration) {
        let nanos = nanos(elapsed);
        add(&self.apply.apply_ns_sum, nanos);
        observe_max(&self.apply.apply_ns_max, nanos);
    }

    pub fn observe_queue_depth(&self, depth: usize) {
        observe_max(
            &self.apply.queue_depth_max,
            u64::try_from(depth).unwrap_or(u64::MAX),
        );
    }

    // Durability updates.

    pub fn record_durable(&self) {
        count(&self.durability.durable);
    }

    pub fn record_durable_wait(&self, wait: Duration) {
        let nanos = nanos(wait);
        add(&self.durability.durable_wait_ns_sum, nanos);
        observe_max(&self.durability.durable_wait_ns_max, nanos);
    }

    /// One batch handed to the journal, counted at the handoff.
    ///
    /// The matching thread knows the batch size; append timing belongs to the
    /// journal thread. Ticket 06 must not count the same records again.
    pub fn record_journal_batch(&self, records: usize) {
        add(
            &self.durability.batch_records_sum,
            u64::try_from(records).unwrap_or(u64::MAX),
        );
        count(&self.durability.batch_count);
    }

    /// One journal append (which may cover many records) that took `elapsed`.
    pub fn record_journal_append(&self, elapsed: Duration, records: usize) {
        let nanos = nanos(elapsed);
        add(&self.durability.journal_append_ns_sum, nanos);
        observe_max(&self.durability.journal_append_ns_max, nanos);
        count(&self.durability.journal_append_count);
        add(
            &self.durability.batch_records_sum,
            u64::try_from(records).unwrap_or(u64::MAX),
        );
        count(&self.durability.batch_count);
    }

    // Readers.

    pub fn submitted(&self) -> u64 {
        self.ingress.submitted.load(Ordering::Relaxed)
    }

    pub fn queue_full(&self) -> u64 {
        self.ingress.queue_full.load(Ordering::Relaxed)
    }

    pub fn engine_failed(&self) -> u64 {
        self.ingress.engine_failed.load(Ordering::Relaxed)
    }

    pub fn stopped(&self) -> u64 {
        self.ingress.stopped.load(Ordering::Relaxed)
    }

    pub fn applied(&self) -> u64 {
        self.apply.applied.load(Ordering::Relaxed)
    }

    pub fn queue_wait_ns_sum(&self) -> u64 {
        self.apply.queue_wait_ns_sum.load(Ordering::Relaxed)
    }

    pub fn queue_wait_ns_max(&self) -> u64 {
        self.apply.queue_wait_ns_max.load(Ordering::Relaxed)
    }

    pub fn apply_ns_sum(&self) -> u64 {
        self.apply.apply_ns_sum.load(Ordering::Relaxed)
    }

    pub fn apply_ns_max(&self) -> u64 {
        self.apply.apply_ns_max.load(Ordering::Relaxed)
    }

    pub fn queue_depth_max(&self) -> u64 {
        self.apply.queue_depth_max.load(Ordering::Relaxed)
    }

    pub fn durable(&self) -> u64 {
        self.durability.durable.load(Ordering::Relaxed)
    }

    pub fn durable_wait_ns_sum(&self) -> u64 {
        self.durability.durable_wait_ns_sum.load(Ordering::Relaxed)
    }

    pub fn durable_wait_ns_max(&self) -> u64 {
        self.durability.durable_wait_ns_max.load(Ordering::Relaxed)
    }

    pub fn journal_append_ns_sum(&self) -> u64 {
        self.durability
            .journal_append_ns_sum
            .load(Ordering::Relaxed)
    }

    pub fn journal_append_ns_max(&self) -> u64 {
        self.durability
            .journal_append_ns_max
            .load(Ordering::Relaxed)
    }

    pub fn journal_append_count(&self) -> u64 {
        self.durability.journal_append_count.load(Ordering::Relaxed)
    }

    pub fn batch_records_sum(&self) -> u64 {
        self.durability.batch_records_sum.load(Ordering::Relaxed)
    }

    pub fn batch_count(&self) -> u64 {
        self.durability.batch_count.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn metrics_accumulate_counts_and_extremes() {
        let metrics = RuntimeMetrics::new();
        metrics.record_submitted();
        metrics.record_submitted();
        metrics.record_submitted();
        metrics.record_queue_full();
        metrics.record_engine_failed();
        metrics.record_stopped();

        metrics.record_queue_wait(Duration::from_nanos(120));
        metrics.record_queue_wait(Duration::from_nanos(80));
        metrics.record_apply(Duration::from_nanos(30));
        metrics.record_apply(Duration::from_nanos(45));
        metrics.record_durable_wait(Duration::from_micros(2));
        metrics.record_durable_wait(Duration::from_micros(1));
        metrics.observe_queue_depth(3);
        metrics.observe_queue_depth(9);
        metrics.observe_queue_depth(4);
        metrics.record_journal_append(Duration::from_micros(20), 10);
        metrics.record_journal_append(Duration::from_micros(5), 2);
        metrics.record_applied();
        metrics.record_durable();

        assert_eq!(metrics.submitted(), 3);
        assert_eq!(metrics.applied(), 1);
        assert_eq!(metrics.durable(), 1);
        assert_eq!(metrics.queue_full(), 1);
        assert_eq!(metrics.engine_failed(), 1);
        assert_eq!(metrics.stopped(), 1);
        assert_eq!(metrics.queue_wait_ns_sum(), 200);
        assert_eq!(metrics.queue_wait_ns_max(), 120);
        assert_eq!(metrics.apply_ns_sum(), 75);
        assert_eq!(metrics.apply_ns_max(), 45);
        assert_eq!(metrics.durable_wait_ns_sum(), 3_000);
        assert_eq!(metrics.durable_wait_ns_max(), 2_000);
        assert_eq!(metrics.queue_depth_max(), 9);
        assert_eq!(metrics.journal_append_ns_sum(), 25_000);
        assert_eq!(metrics.journal_append_ns_max(), 20_000);
        assert_eq!(metrics.journal_append_count(), 2);
        assert_eq!(metrics.batch_records_sum(), 12);
        assert_eq!(metrics.batch_count(), 2);
    }

    #[test]
    fn metrics_are_cache_line_grouped() {
        assert_eq!(std::mem::align_of::<RuntimeMetrics>(), 64);
        assert_eq!(std::mem::size_of::<RuntimeMetrics>() % 64, 0);
    }
}
