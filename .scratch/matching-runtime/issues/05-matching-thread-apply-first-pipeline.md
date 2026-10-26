# 05: Matching Thread apply-first pipeline

**What to build:** The single matching thread that drains the ingress, applies one command at a time, publishes applied receipts, buffers command records for the journal, tracks pending durable acknowledgements, and applies ingress/journal backpressure. Use a `JournalSink` seam so the loop can be tested before the real Journal Thread exists.

**Blocked by:** 03, 04.

**Status:** done

- [x] Add a `JournalSink` trait (`try_send_batch`, disconnect/failure state) and a no-op test implementation.
- [x] Implement the loop from the spec: observe failure, drain durable completions, flush a full journal buffer, process one command, flush when idle, run the snapshot gate hook, check shutdown, idle.
- [x] Use `MatchingEngine::execute_ref`, assign command sequences, build `CommandReceipt`, complete the slot's applied state, move the command into `CommandRecord`, and clear events.
- [x] Flush the journal buffer when it reaches `apply_batch_max`, when `journal_flush_interval` elapses since its first record, or when the ingress is empty.
- [x] Enforce the lead bound: never apply more commands while the journal buffer cannot be flushed; retry with the idle strategy.
- [x] Update metrics only when enabled: queue wait, apply time, batch sizes, queue depth, applied count.
- [x] Single-producer test: a sequence of submits/cancels produces the same receipts, command log, and final `EngineSnapshot` as direct `MatchingEngine` execution.
- [x] Per-producer FIFO test: one producer's commands keep their submission order in the applied sequence.
