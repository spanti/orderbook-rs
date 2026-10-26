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

use super::RuntimeConfig;
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
    _lease: Option<Arc<SlotLease>>,
}

impl SlotHandle {
    /// Create the worker's handle for `slot`.
    ///
    /// Handles created this way do not belong to a pool. Runtime envelopes use
    /// [`SlotPool::checkout`], which keeps the slot reserved until both sides
    /// are dropped.
    pub fn new(slot: Arc<ResponseSlot>) -> Self {
        Self { slot, _lease: None }
    }

    fn with_lease(slot: Arc<ResponseSlot>, lease: Arc<SlotLease>) -> Self {
        Self {
            slot,
            _lease: Some(lease),
        }
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

/// One checked-out slot's shared reservation.
///
/// The ticket and the worker's handle each hold an `Arc` to the lease, so the
/// slot returns to the pool only when both sides are gone. The worker's drop
/// behavior (marking the slot stopped) is separate and runs first.
#[derive(Debug)]
struct SlotLease {
    pool: Arc<SlotPoolInner>,
    index: u32,
    slot: Arc<ResponseSlot>,
}

impl Drop for SlotLease {
    fn drop(&mut self) {
        self.slot.reset();
        self.pool.release(self.index);
    }
}

#[derive(Debug)]
struct SlotPoolInner {
    slots: Vec<Arc<ResponseSlot>>,
    free: Mutex<Vec<u32>>,
}

impl SlotPoolInner {
    fn acquire(&self) -> Option<u32> {
        self.free
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .pop()
    }

    fn release(&self, index: u32) {
        self.free
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(index);
    }
}

/// A fixed-capacity pool of pre-allocated response slots.
///
/// Producers check out a slot per submitted command. The slot is reserved
/// until both the caller's [`Ticket`] and the worker's [`SlotHandle`] are
/// dropped, at which point it is reset and returned to the free list. An
/// exhausted pool reports `None`; callers map that to
/// [`crate::runtime::SubmitError::QueueFull`].
#[derive(Debug, Clone)]
pub struct SlotPool {
    inner: Arc<SlotPoolInner>,
}

impl SlotPool {
    /// Pre-allocate `capacity` slots and put them all on the free list.
    pub fn new(capacity: usize) -> Self {
        let slots = (0..capacity)
            .map(|_| Arc::new(ResponseSlot::new()))
            .collect::<Vec<_>>();
        let free = (0..capacity).map(|index| index as u32).collect();

        Self {
            inner: Arc::new(SlotPoolInner {
                slots,
                free: Mutex::new(free),
            }),
        }
    }

    /// Build a pool sized for `config`.
    pub fn from_config(config: &RuntimeConfig) -> Self {
        Self::new(Self::capacity_for(config))
    }

    /// The spec's fixed capacity:
    /// `queue_capacity + journal_batch_capacity * apply_batch_max + 64`.
    pub fn capacity_for(config: &RuntimeConfig) -> usize {
        config.queue_capacity + config.journal_batch_capacity * config.apply_batch_max + 64
    }

    /// Total number of slots, checked out or free.
    pub fn capacity(&self) -> usize {
        self.inner.slots.len()
    }

    /// Number of slots currently free for checkout.
    pub fn available(&self) -> usize {
        self.inner
            .free
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }

    /// Reserve one slot, or `None` when every slot is checked out.
    pub fn checkout(&self) -> Option<(Ticket, SlotHandle)> {
        let index = self.inner.acquire()?;
        let slot = Arc::clone(&self.inner.slots[index as usize]);
        let lease = Arc::new(SlotLease {
            pool: Arc::clone(&self.inner),
            index,
            slot: Arc::clone(&slot),
        });

        Some((
            Ticket::with_lease(Arc::clone(&slot), Arc::clone(&lease)),
            SlotHandle::with_lease(slot, lease),
        ))
    }
}

/// The caller's handle to one command's results.
///
/// Both waits take `&self`, so a caller may observe the applied and durable
/// levels on the same command. Dropping a ticket never affects the command.
#[derive(Debug)]
pub struct Ticket {
    slot: Arc<ResponseSlot>,
    _lease: Option<Arc<SlotLease>>,
}

impl Ticket {
    /// Create a ticket observing `slot`.
    ///
    /// Tickets created this way do not belong to a pool; runtime producers use
    /// [`SlotPool::checkout`].
    pub fn new(slot: Arc<ResponseSlot>) -> Self {
        Self { slot, _lease: None }
    }

    fn with_lease(slot: Arc<ResponseSlot>, lease: Arc<SlotLease>) -> Self {
        Self {
            slot,
            _lease: Some(lease),
        }
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
    fn durable_wait_parks_until_applied_and_durable_are_written() {
        use std::sync::mpsc;
        use std::thread;

        let slot = Arc::new(ResponseSlot::new());
        let ticket = Ticket::new(Arc::clone(&slot));
        let handle = SlotHandle::new(Arc::clone(&slot));
        let (started_tx, started_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();

        // The waiter parks on durability before either result exists.
        let waiter = thread::spawn(move || {
            started_tx.send(()).expect("report waiter start");
            let result = ticket.wait_durable();
            result_tx.send(result).expect("report durable result");
        });

        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("waiter started");
        assert!(
            result_rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "durable wait must park while the slot is empty"
        );

        // An applied receipt alone must not release a durable waiter.
        handle.set_applied(Ok(receipt(4)));
        assert!(
            result_rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "applied must not satisfy a durable wait"
        );

        handle.set_durable(Ok(()));
        let result = result_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("waiter wakes for durability");

        assert_eq!(result.unwrap(), receipt(4));
        waiter.join().expect("waiter thread");
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

    #[test]
    fn pool_checks_out_and_reclaims_after_both_handles_drop() {
        let pool = SlotPool::new(1);
        assert_eq!(pool.available(), 1);

        let (ticket, handle) = pool.checkout().expect("pre-allocated slot");
        assert_eq!(pool.available(), 0);

        handle.set_applied(Ok(receipt(10)));
        handle.set_durable(Ok(()));
        assert_eq!(ticket.wait_durable().unwrap(), receipt(10));

        drop(handle);
        drop(ticket);
        assert_eq!(pool.available(), 1);
    }

    #[test]
    fn exhausted_pool_returns_none() {
        let pool = SlotPool::new(2);
        let first = pool.checkout().expect("first slot");
        let second = pool.checkout().expect("second slot");

        assert!(pool.checkout().is_none());

        drop(first);
        assert!(pool.checkout().is_some());
        drop(second);
    }

    #[test]
    fn dropping_only_the_ticket_keeps_the_command_alive_and_slot_reserved() {
        let pool = SlotPool::new(1);
        let (ticket, handle) = pool.checkout().expect("slot");

        drop(ticket);

        assert_eq!(pool.available(), 0);
        assert!(pool.checkout().is_none());

        // The worker can still complete: the slot was neither reset nor
        // marked stopped by the caller dropping its ticket.
        handle.set_applied(Ok(receipt(11)));
        handle.set_durable(Ok(()));
        drop(handle);

        assert_eq!(pool.available(), 1);
    }

    #[test]
    fn dropping_only_the_worker_handle_stops_waiters_and_keeps_slot_reserved() {
        let pool = SlotPool::new(1);
        let (ticket, handle) = pool.checkout().expect("slot");

        drop(handle);

        assert_eq!(ticket.wait_applied(), Err(RuntimeError::EngineStopped));
        assert_eq!(ticket.wait_durable(), Err(RuntimeError::EngineStopped));
        assert_eq!(pool.available(), 0);
        assert!(pool.checkout().is_none());

        drop(ticket);
        assert_eq!(pool.available(), 1);
    }

    #[test]
    fn pool_capacity_is_derived_from_runtime_config() {
        let mut config = crate::runtime::RuntimeConfig::default();
        config.queue_capacity = 10;
        config.journal_batch_capacity = 3;
        config.apply_batch_max = 4;

        assert_eq!(SlotPool::capacity_for(&config), 10 + 3 * 4 + 64);

        let pool = SlotPool::from_config(&config);
        assert_eq!(pool.capacity(), 86);
        assert_eq!(pool.available(), 86);

        let defaults = SlotPool::from_config(&crate::runtime::RuntimeConfig::default());
        assert_eq!(defaults.capacity(), 1024 + 64 * 64 + 64);
    }

    #[test]
    fn dropping_both_handles_returns_the_slot_for_reuse() {
        let pool = SlotPool::new(1);

        for sequence in 0..3 {
            let (ticket, handle) = pool.checkout().expect("slot reused");
            handle.set_applied(Ok(receipt(sequence)));
            handle.set_durable(Ok(()));
            assert_eq!(ticket.wait_durable().unwrap(), receipt(sequence));

            drop(ticket);
            drop(handle);

            assert_eq!(pool.available(), 1);
        }
    }

    #[test]
    fn concurrent_checkout_and_return_is_safe() {
        use std::sync::Mutex as StdMutex;
        use std::sync::mpsc;

        let capacity = 4;
        let pool = SlotPool::new(capacity);
        let (slot_tx, slot_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let release_rx = Arc::new(StdMutex::new(release_rx));

        let workers: Vec<_> = (0..capacity)
            .map(|_| {
                let pool = pool.clone();
                let slot_tx = slot_tx.clone();
                let release_rx = Arc::clone(&release_rx);

                std::thread::spawn(move || {
                    let pair = pool.checkout().expect("one slot per producer");
                    slot_tx
                        .send(Arc::as_ptr(pair.1.slot()) as usize)
                        .expect("report checked-out slot");
                    release_rx
                        .lock()
                        .expect("release channel")
                        .recv()
                        .expect("release");
                    pair
                })
            })
            .collect();
        drop(slot_tx);

        let mut pointers = (0..capacity)
            .map(|_| slot_rx.recv().expect("producer checked out a slot"))
            .collect::<Vec<_>>();
        pointers.sort_unstable();
        pointers.dedup();
        assert_eq!(
            pointers.len(),
            capacity,
            "each producer gets a distinct slot"
        );

        assert_eq!(pool.available(), 0);
        assert!(pool.checkout().is_none());

        for _ in 0..capacity {
            release_tx.send(()).expect("release producer");
        }
        drop(release_tx);

        let pairs = workers
            .into_iter()
            .map(|worker| worker.join().expect("producer thread"))
            .collect::<Vec<_>>();
        drop(pairs);

        assert_eq!(pool.available(), capacity);
    }
}
