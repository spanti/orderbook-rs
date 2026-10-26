# 04: Pre-allocated response slot pool

**What to build:** A bounded pool of pre-allocated response slots so `try_submit` and the matching thread do not create a channel or allocate per command. `Ticket` waits on a slot; the worker completes applied and durable states; dropping both handles returns the slot.

**Blocked by:** 03.

**Status:** todo

- [ ] Implement `ResponseSlot` with `Mutex<SlotState>` + `Condvar`; `SlotState` holds applied (`Option<Result<CommandReceipt, RuntimeError>>`), durable (`Option<Result<(), RuntimeError>>`), and `stopped`.
- [ ] Implement `SlotPool` with a fixed capacity of `queue_capacity + journal_batch_capacity * apply_batch_max + 64`, a `Mutex<Vec<u32>>` free list, checkout, and return.
- [ ] Implement `Ticket` with `wait_applied`, `wait_applied_timeout`, `wait_durable`, `wait_durable_timeout`; `wait_durable` returns the stored applied receipt on success.
- [ ] The worker's slot handle marks the slot `stopped` and notifies when dropped before both results are set; the ticket side must not overwrite results.
- [ ] Tests: applied then durable resolution; durable-first waiting; timeout; stopped unblocks both waits with `EngineStopped`; slot reuse after both handles drop; pool exhaustion reports `QueueFull`.
