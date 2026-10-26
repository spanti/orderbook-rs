//! Response slots and the caller-visible ticket.
//!
//! A command carries a pre-allocated [`ResponseSlot`] through the runtime.
//! The matching thread writes the applied result, the journal path later marks
//! durability, and the caller observes either level through a [`Ticket`].
//!
//! Ticket 03 defines the slot state and wait protocol only. The pool and its
//! free list belong to the runtime implementation (ticket 04).

use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use crate::types::CommandOutcome;

use super::error::RuntimeError;

/// The engine's result for one command, tagged with its command sequence.
#[derive(Debug, Clone, PartialEq)]
pub struct CommandReceipt {
    pub outcome: CommandOutcome,
    /// Global command sequence assigned by the matching thread. Not an event
    /// sequence: one command may emit several events.
    pub command_sequence: u64,
}

/// Results written into one response slot.
///
/// `applied` is the preliminary acknowledgement; `durable` is set once the
/// record is confirmed in the command log. `stopped` unblocks waiters when a
/// worker disappears before both results are written.
#[derive(Debug, Default)]
pub struct SlotState {
    pub applied: Option<Result<CommandReceipt, RuntimeError>>,
    pub durable: Option<Result<(), RuntimeError>>,
    pub stopped: bool,
}

impl SlotState {
    /// Clear every result so the slot can be reused from the pool.
    pub fn reset(&mut self) {
        *self = Self::default();
    }

    fn is_complete(&self) -> bool {
        self.applied.is_some() && self.durable.is_some()
    }
}

/// One pre-allocated response slot: a mutex-protected state plus a condvar.
#[derive(Debug)]
pub struct ResponseSlot {
    state: Mutex<SlotState>,
    ready: Condvar,
}

impl ResponseSlot {
    /// Create an empty slot. The runtime pre-allocates these in its pool.
    pub fn new() -> Self {
        Self {
            state: Mutex::new(SlotState::default()),
            ready: Condvar::new(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, SlotState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Write the applied acknowledgement and wake every waiter.
    pub fn set_applied(&self, result: Result<CommandReceipt, RuntimeError>) {
        let mut state = self.lock();
        state.applied = Some(result);
        self.ready.notify_all();
    }

    /// Mark durability and wake every waiter.
    pub fn set_durable(&self, result: Result<(), RuntimeError>) {
        let mut state = self.lock();
        state.durable = Some(result);
        self.ready.notify_all();
    }

    /// Unblock waiters with [`RuntimeError::EngineStopped`].
    pub fn stop(&self) {
        let mut state = self.lock();
        state.stopped = true;
        self.ready.notify_all();
    }

    /// Clear the slot for reuse. Only valid when no handle or ticket remains.
    pub fn reset(&self) {
        self.lock().reset();
    }
}

impl Default for ResponseSlot {
    fn default() -> Self {
        Self::new()
    }
}

/// The worker's reference to a response slot.
///
/// Dropping it before both results are written marks the slot stopped, so a
/// panicking or aborting worker never leaves a caller waiting forever.
#[derive(Debug)]
pub struct SlotHandle {
    slot: Arc<ResponseSlot>,
}

impl SlotHandle {
    /// Create the worker's handle for `slot`.
    pub fn new(slot: Arc<ResponseSlot>) -> Self {
        Self { slot }
    }

    pub fn set_applied(&self, result: Result<CommandReceipt, RuntimeError>) {
        self.slot.set_applied(result);
    }

    pub fn set_durable(&self, result: Result<(), RuntimeError>) {
        self.slot.set_durable(result);
    }

    pub fn slot(&self) -> &Arc<ResponseSlot> {
        &self.slot
    }
}

impl Drop for SlotHandle {
    fn drop(&mut self) {
        let mut state = self.slot.lock();
        if !state.is_complete() {
            state.stopped = true;
            self.slot.ready.notify_all();
        }
    }
}

/// The caller's handle to one command's results.
///
/// Both waits take `&self`, so a caller may observe the applied and durable
/// levels on the same command. Dropping a ticket never affects the command.
#[derive(Debug)]
pub struct Ticket {
    slot: Arc<ResponseSlot>,
}

impl Ticket {
    /// Create a ticket observing `slot`.
    pub fn new(slot: Arc<ResponseSlot>) -> Self {
        Self { slot }
    }

    /// Wait for the applied acknowledgement (preliminary; may be lost on a
    /// crash before durability).
    pub fn wait_applied(&self) -> Result<CommandReceipt, RuntimeError> {
        self.wait(WaitLevel::Applied, None)
    }

    /// Wait for the applied acknowledgement, or return
    /// [`RuntimeError::Timeout`] after `timeout`.
    pub fn wait_applied_timeout(&self, timeout: Duration) -> Result<CommandReceipt, RuntimeError> {
        self.wait(WaitLevel::Applied, Some(timeout))
    }

    /// Wait until the command record is durable in the command log.
    pub fn wait_durable(&self) -> Result<CommandReceipt, RuntimeError> {
        self.wait(WaitLevel::Durable, None)
    }

    /// Wait for durability, or return [`RuntimeError::Timeout`] after
    /// `timeout`.
    pub fn wait_durable_timeout(&self, timeout: Duration) -> Result<CommandReceipt, RuntimeError> {
        self.wait(WaitLevel::Durable, Some(timeout))
    }

    fn wait(
        &self,
        level: WaitLevel,
        timeout: Option<Duration>,
    ) -> Result<CommandReceipt, RuntimeError> {
        let mut state = self.slot.lock();
        let deadline = timeout.and_then(|limit| Instant::now().checked_add(limit));

        loop {
            if let Some(result) = read_result(&state, level) {
                return result;
            }

            match deadline {
                None => {
                    state = self
                        .slot
                        .ready
                        .wait(state)
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                }
                Some(deadline) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return Err(RuntimeError::Timeout);
                    }
                    let (next, _) = self
                        .slot
                        .ready
                        .wait_timeout(state, deadline - now)
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    state = next;
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum WaitLevel {
    Applied,
    Durable,
}

fn read_result(
    state: &SlotState,
    level: WaitLevel,
) -> Option<Result<CommandReceipt, RuntimeError>> {
    match level {
        WaitLevel::Applied => {
            if let Some(result) = &state.applied {
                return Some(result.clone());
            }
        }
        WaitLevel::Durable => {
            if let Some(result) = &state.durable {
                return Some(match result {
                    Ok(()) => match &state.applied {
                        Some(Ok(receipt)) => Ok(receipt.clone()),
                        // Durability without an applied receipt violates the
                        // slot protocol; treat it like a stopped worker
                        // instead of inventing a receipt.
                        Some(Err(_)) | None => Err(RuntimeError::EngineStopped),
                    },
                    Err(error) => Err(error.clone()),
                });
            }
        }
    }

    if state.stopped {
        Some(Err(RuntimeError::EngineStopped))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durable::DurableError;
    use crate::types::EngineError;
    use std::sync::Arc;
    use std::time::Duration;

    fn receipt(sequence: u64) -> CommandReceipt {
        CommandReceipt {
            outcome: CommandOutcome::Rejected(EngineError::UnknownOrder),
            command_sequence: sequence,
        }
    }

    #[test]
    fn command_receipt_clones_and_compares_by_value() {
        let first = receipt(7);
        let clone = first.clone();

        assert_eq!(first, clone);
        assert_ne!(first, receipt(8));
    }

    #[test]
    fn ticket_observes_applied_then_durable() {
        let slot = Arc::new(ResponseSlot::new());
        let ticket = Ticket::new(Arc::clone(&slot));
        let handle = SlotHandle::new(Arc::clone(&slot));

        handle.set_applied(Ok(receipt(1)));
        assert_eq!(ticket.wait_applied().unwrap(), receipt(1));

        handle.set_durable(Ok(()));
        assert_eq!(ticket.wait_durable().unwrap(), receipt(1));
    }

    #[test]
    fn ticket_propagates_a_durable_failure() {
        let slot = Arc::new(ResponseSlot::new());
        let ticket = Ticket::new(Arc::clone(&slot));
        let handle = SlotHandle::new(Arc::clone(&slot));
        let error = RuntimeError::EngineFailed(DurableError::AppendFailed("disk".into()));

        handle.set_applied(Ok(receipt(2)));
        handle.set_durable(Err(error.clone()));

        assert_eq!(ticket.wait_durable(), Err(error));
    }

    #[test]
    fn dropping_an_incomplete_worker_handle_stops_waiters() {
        let slot = Arc::new(ResponseSlot::new());
        let ticket = Ticket::new(Arc::clone(&slot));
        let handle = SlotHandle::new(Arc::clone(&slot));

        drop(handle);

        assert_eq!(ticket.wait_applied(), Err(RuntimeError::EngineStopped));
        assert_eq!(ticket.wait_durable(), Err(RuntimeError::EngineStopped));
    }

    #[test]
    fn dropping_a_completed_worker_handle_does_not_stop_waiters() {
        let slot = Arc::new(ResponseSlot::new());
        let ticket = Ticket::new(Arc::clone(&slot));
        let handle = SlotHandle::new(Arc::clone(&slot));

        handle.set_applied(Ok(receipt(3)));
        handle.set_durable(Ok(()));
        drop(handle);

        assert_eq!(ticket.wait_applied().unwrap(), receipt(3));
        assert_eq!(ticket.wait_durable().unwrap(), receipt(3));
    }

    #[test]
    fn wait_timeout_returns_timeout_without_a_result() {
        let slot = Arc::new(ResponseSlot::new());
        let ticket = Ticket::new(Arc::clone(&slot));
        let _handle = SlotHandle::new(Arc::clone(&slot));

        assert_eq!(
            ticket.wait_applied_timeout(Duration::from_millis(5)),
            Err(RuntimeError::Timeout)
        );
        assert_eq!(
            ticket.wait_durable_timeout(Duration::from_millis(5)),
            Err(RuntimeError::Timeout)
        );
    }

    #[test]
    fn ticket_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Ticket>();
    }
}
