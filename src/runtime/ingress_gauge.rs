//! The metrics-only ingress queue-depth gauge.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Counts envelopes in the ingress command queue.
///
/// This is observability only, never a correctness signal. When metrics are
/// disabled the gauge performs no atomic updates at all.
#[derive(Clone)]
pub(crate) struct IngressGauge {
    /// Envelopes currently in the ingress command queue. Used only to report
    /// queue-depth metrics; it is not a correctness signal.
    depth: Arc<AtomicU64>,
    metrics_enabled: bool,
}

impl IngressGauge {
    pub(crate) fn new(metrics_enabled: bool) -> Self {
        Self {
            depth: Arc::new(AtomicU64::new(0)),
            metrics_enabled,
        }
    }

    /// Count one envelope offered to the ingress queue.
    ///
    /// Metrics-only: when metrics are disabled this performs no atomic update.
    /// The increment happens *before* the send, so a consumer that dequeues
    /// immediately can never decrement the gauge below zero.
    pub(crate) fn note_enqueued(&self) {
        if self.metrics_enabled {
            self.depth.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Undo [`Self::note_enqueued`] when the bounded channel refused the
    /// offer, so a refusal is not counted as queue depth.
    pub(crate) fn note_refused(&self) {
        if self.metrics_enabled {
            self.depth.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Count one envelope removed from the ingress queue.
    ///
    /// Returns the depth observed before the decrement, or `None` when metrics
    /// are disabled. The decrement saturates at zero, so a spurious pair of
    /// decrements cannot wrap the gauge to `u64::MAX`.
    pub(crate) fn note_dequeued(&self) -> Option<u64> {
        if !self.metrics_enabled {
            return None;
        }
        let previous = self
            .depth
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |depth| {
                Some(depth.saturating_sub(1))
            })
            .unwrap_or(0);
        Some(previous)
    }

    /// The current gauge value, for tests.
    #[cfg(test)]
    pub(crate) fn depth(&self) -> u64 {
        self.depth.load(Ordering::Relaxed)
    }
}
