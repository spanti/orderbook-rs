//! State shared by every matching-runtime component.
//!
//! The ingress submitters, the matching thread, the journal thread, and the
//! lifecycle all observe the same run state, sequence watermarks, publisher
//! signal, and ingress gauge. Each protocol has its own module and owns its
//! fields and semantics; [`SharedState`] is only a composition handle, so a
//! consumer names the protocol it uses through the matching accessor.
//!
//! The cardinally shared values are atomics; the fatal error and the shutdown
//! deadline use a mutex because they are rare control-path state.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use super::ingress_gauge::IngressGauge;
use super::metrics::RuntimeMetrics;
use super::publisher_signal::PublisherSignal;
use super::run_status::RunStatus;
use super::watermark::Watermark;

/// State shared by the ingress submitters, the matching thread, the journal
/// thread and the runtime lifecycle.
#[derive(Clone)]
pub(crate) struct SharedState {
    run: RunStatus,
    watermark: Watermark,
    publisher: PublisherSignal,
    ingress: IngressGauge,
    /// Optional metrics sink. Looked up on both submission and dequeue paths
    /// so a metrics-disabled runtime performs no depth atomics at all.
    metrics: Option<Arc<RuntimeMetrics>>,
}

impl SharedState {
    pub(crate) fn new(metrics: Option<Arc<RuntimeMetrics>>) -> Self {
        Self::with_watermark(metrics, Watermark::new())
    }

    /// Build shared state whose watermark is owned elsewhere, as the journal
    /// thread owns it.
    pub(crate) fn with_durable_seq(
        metrics: Option<Arc<RuntimeMetrics>>,
        durable_seq: Arc<AtomicU64>,
    ) -> Self {
        Self::with_watermark(metrics, Watermark::with_durable_seq(durable_seq))
    }

    fn with_watermark(metrics: Option<Arc<RuntimeMetrics>>, watermark: Watermark) -> Self {
        Self {
            run: RunStatus::new(),
            watermark,
            publisher: PublisherSignal::default(),
            ingress: IngressGauge::new(metrics.is_some()),
            metrics,
        }
    }

    pub(crate) fn run(&self) -> &RunStatus {
        &self.run
    }

    pub(crate) fn watermark(&self) -> &Watermark {
        &self.watermark
    }

    pub(crate) fn publisher(&self) -> &PublisherSignal {
        &self.publisher
    }

    pub(crate) fn ingress(&self) -> &IngressGauge {
        &self.ingress
    }

    pub(crate) fn metrics(&self) -> Option<&Arc<RuntimeMetrics>> {
        self.metrics.as_ref()
    }
}
