# 08: Lifecycle, failure semantics, and optional affinity

**What to build:** The public lifecycle (`create`, `open`, `shutdown`, `Drop`), fail-closed behavior, panic unblocking, startup error reporting, and optional `core_affinity` binding for the matching thread.

**Blocked by:** 05, 06, 07.

**Status:** done

- [x] Implement `EngineRuntime::create` and `open` using the shared recovery helper; move the engine, log, and snapshot store into their threads.
- [x] Add `core_affinity = "0.8"` and bind the matching thread when `matching_core` is set; report invalid cores as `RuntimeError::Startup` through a startup channel.
- [x] Implement `shutdown(self)`: set closed, drain, flush, wait for durability with `shutdown_timeout`, write the final snapshot, join all threads, return the first error.
- [x] Implement `Drop`: same stop without the final snapshot, ignore errors and panics.
- [x] Implement the closed/failed flags: new submissions return `Shutdown` or `EngineFailed`; queued commands receive `EngineFailed` on fatal failure.
- [x] Ensure a matching-thread panic marks slots stopped so every waiter returns `EngineStopped`; a journal-thread panic fails the runtime closed.
- [x] Tests: shutdown drains and joins; `Drop` drains without a final snapshot; panics do not hang waiters; invalid affinity fails startup; open recovers the expected state and watermark.
