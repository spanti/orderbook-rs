//! The producer-side ingress submitter.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{SyncSender, TrySendError};
use std::time::Instant;

use crate::types::EngineCommand;

use super::envelope::CommandEnvelope;
use super::error::SubmitError;
use super::metrics::RuntimeMetrics;
use super::shared::SharedState;
use super::ticket::{SlotPool, Ticket};

/// The internal submitter used by [`crate::runtime::EngineHandle`].
///
/// It checks out a pre-allocated response slot, builds a
/// [`CommandEnvelope`], and offers it to the bounded ingress queue without
/// blocking. Pool exhaustion and a full channel both surface as
/// [`SubmitError::QueueFull`]; the command is not applied and consumes no
/// command sequence.
#[derive(Clone)]
pub(crate) struct IngressSubmitter {
    sender: SyncSender<CommandEnvelope>,
    pool: SlotPool,
    shared: SharedState,
    next_request_id: Arc<AtomicU64>,
}

impl IngressSubmitter {
    pub(crate) fn new(
        sender: SyncSender<CommandEnvelope>,
        pool: SlotPool,
        shared: SharedState,
    ) -> Self {
        Self {
            sender,
            pool,
            shared,
            next_request_id: Arc::new(AtomicU64::new(1)),
        }
    }

    pub(crate) fn submit(&self, command: EngineCommand) -> Result<Ticket, SubmitError> {
        if let Some(error) = self.shared.failure() {
            if let Some(metrics) = &self.shared.metrics {
                metrics.record_engine_failed();
            }
            return Err(SubmitError::EngineFailed(error));
        }
        if self.shared.is_closed() {
            if let Some(metrics) = &self.shared.metrics {
                metrics.record_stopped();
            }
            return Err(SubmitError::Shutdown);
        }

        // No slot means the response pool is exhausted; treat it exactly like
        // a full ingress queue so producers shed load instead of allocating.
        let Some((ticket, slot)) = self.pool.checkout() else {
            if let Some(metrics) = &self.shared.metrics {
                metrics.record_queue_full();
            }
            return Err(SubmitError::QueueFull);
        };

        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let enqueued_at = self.shared.metrics.as_ref().map(|_| Instant::now());
        let envelope = CommandEnvelope::new(request_id, enqueued_at, command, slot);

        // Count the offer before sending: the consumer may dequeue and
        // decrement before this thread resumes, which would underflow a
        // post-send increment and poison `queue_depth_max`.
        self.shared.note_ingress_enqueued();
        match self.sender.try_send(envelope) {
            Ok(()) => {
                if let Some(metrics) = &self.shared.metrics {
                    metrics.record_submitted();
                }
                Ok(ticket)
            }
            Err(TrySendError::Full(_envelope)) => {
                self.shared.note_ingress_refused();
                // Dropping the returned envelope releases its slot handle; the
                // ticket is dropped on return, so the slot goes back to the
                // pool. The command was never accepted.
                if let Some(metrics) = &self.shared.metrics {
                    metrics.record_queue_full();
                }
                Err(SubmitError::QueueFull)
            }
            Err(TrySendError::Disconnected(_envelope)) => {
                self.shared.note_ingress_refused();
                if let Some(metrics) = &self.shared.metrics {
                    metrics.record_stopped();
                }
                Err(SubmitError::Shutdown)
            }
        }
    }
}

/// Test-only probe: run one command through the real ingress submitter and
/// return the envelope's `enqueued_at`.
///
/// `None` proves that metrics-disabled submission performs no clock read. This
/// is hidden test support, not part of the supported API.
#[doc(hidden)]
pub fn enqueued_at_for_test(metrics: Option<Arc<RuntimeMetrics>>) -> Option<Instant> {
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    let submitter = IngressSubmitter::new(sender, SlotPool::new(1), SharedState::new(metrics));
    let ticket = submitter
        .submit(EngineCommand::CancelOrder {
            symbol: "BTC-USDT".to_string(),
            order_id: 1,
        })
        .expect("the probe ingress has room");
    let envelope = receiver.recv().expect("the probe envelope");
    let enqueued_at = envelope.enqueued_at;
    drop(envelope);
    drop(ticket);
    enqueued_at
}
