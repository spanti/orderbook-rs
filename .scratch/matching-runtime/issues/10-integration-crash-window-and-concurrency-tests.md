# 10: Integration, crash-window, and concurrency tests

**What to build:** End-to-end tests that prove the runtime's contracts: preliminary applied acknowledgements, durable acknowledgements gated by the watermark, exact replay, concurrent producers, backpressure, panic behavior, and snapshot gating.

**Blocked by:** 05, 06, 07, 08.

**Status:** done

- [x] Single-producer equality: runtime execution and direct execution produce the same receipts, command log, and final snapshot.
- [x] Applied-vs-durable window: block the journal append, assert `wait_applied` returns while `wait_durable` blocks, assert the log is empty for that command, then release and assert durability.
- [x] Crash recovery: reopen from the log at the blocked point and prove the applied-but-not-durable command is absent; after durable ack, prove it survives.
- [x] Concurrent producers: N threads × M commands, unique contiguous sequences, every command applied exactly once, replay of the log reproduces the final snapshot.
- [x] Backpressure: saturate the ingress (test pause hook or a stalled journal) until `try_submit` returns `QueueFull`; assert no sequence is consumed for rejected submissions.
- [x] Failure: journal append failure and journal panic fail closed; matching-thread panic unblocks every waiter; submissions after failure are rejected.
- [x] Metrics sanity: counts match submitted/accepted commands; queue wait, apply, and durable wait are non-negative; metrics disabled reads no clock (compile-time or test-double assertion).
- [x] Affinity: valid core binds, invalid core fails startup, unsupported platform skips.
