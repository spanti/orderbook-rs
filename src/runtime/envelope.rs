//! The ingress command envelope.

use std::time::Instant;

use crate::types::EngineCommand;

use super::ticket::SlotHandle;

/// A command travelling through the ingress command queue.
///
/// The envelope owns the command and the worker-side slot handle. The matching
/// thread can move the command into a durable record after applying it by
/// reference, so the runtime never clones a command.
#[derive(Debug)]
pub struct CommandEnvelope {
    /// Producer-assigned id for observability. It is not an idempotency key;
    /// the runtime performs no deduplication.
    pub request_id: u64,
    /// When the command was enqueued, or `None` when metrics are disabled.
    pub enqueued_at: Option<Instant>,
    pub command: EngineCommand,
    /// The response slot the ticket observes; moved to the worker.
    pub(crate) slot: SlotHandle,
}

impl CommandEnvelope {
    pub(crate) fn new(
        request_id: u64,
        enqueued_at: Option<Instant>,
        command: EngineCommand,
        slot: SlotHandle,
    ) -> Self {
        Self {
            request_id,
            enqueued_at,
            command,
            slot,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::ticket::{ResponseSlot, SlotHandle};
    use crate::types::EngineCommand;
    use std::sync::Arc;
    use std::time::Instant;

    #[test]
    fn envelope_carries_request_metadata_and_its_slot() {
        let slot = Arc::new(ResponseSlot::new());
        let command = EngineCommand::CancelOrder {
            symbol: "BTC-USDT".to_string(),
            order_id: 7,
        };
        let enqueued_at = Some(Instant::now());

        let envelope = CommandEnvelope {
            request_id: 42,
            enqueued_at,
            command: command.clone(),
            slot: SlotHandle::new(slot),
        };

        assert_eq!(envelope.request_id, 42);
        assert_eq!(envelope.enqueued_at, enqueued_at);
        assert_eq!(envelope.command, command);
    }

    #[test]
    fn envelope_allows_missing_enqueue_time_when_metrics_are_off() {
        let slot = Arc::new(ResponseSlot::new());
        let envelope = CommandEnvelope {
            request_id: 1,
            enqueued_at: None,
            command: EngineCommand::CancelOrder {
                symbol: "BTC-USDT".to_string(),
                order_id: 1,
            },
            slot: SlotHandle::new(slot),
        };

        assert!(envelope.enqueued_at.is_none());
    }
}
