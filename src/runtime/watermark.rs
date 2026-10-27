//! The durability and application watermarks.
//!
//! The matching thread publishes the last event sequence it applied and the
//! `command_id` of the last command it applied; the journal thread advances the
//! durability watermark only after `append_batch` returns. Both are plain
//! atomics because every component reads them on the hot path.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// How far the log and the engine have progressed.
#[derive(Clone)]
pub(crate) struct Watermark {
    durable_seq: Arc<AtomicU64>,
    applied_seq: Arc<AtomicU64>,
    applied_command_id: Arc<AtomicU64>,
}

impl Watermark {
    pub(crate) fn new() -> Self {
        Self::with_durable_seq(Arc::new(AtomicU64::new(0)))
    }

    /// Build a watermark whose durability sequence is owned elsewhere, as the
    /// journal thread owns it.
    pub(crate) fn with_durable_seq(durable_seq: Arc<AtomicU64>) -> Self {
        Self {
            durable_seq,
            applied_seq: Arc::new(AtomicU64::new(0)),
            applied_command_id: Arc::new(AtomicU64::new(0)),
        }
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
}
