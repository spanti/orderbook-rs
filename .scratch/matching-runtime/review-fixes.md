# Matching Runtime: Two-Axis Review Fixes

Status: merged and verified.

- Review/fix branch: `ticket/12-review-fixes` at `9e132a6`, based on `ac046dd`.
- Merge commit: `d5ac088` (`merge: code-review fixes`).
- Integration branch: `feature/matching-runtime`.
- Spec: [`spec.md`](spec.md).

The review was run on two axes. **Standards** covered module boundaries, public
API surface, duplication, dead code, naming, and documentation. **Spec** checked
conformance to `spec.md`: apply-first durability, journal group commit, snapshot
gating, slot-pool sizing, failure semantics, and the dual SLO contract. Findings
are labelled a-o below; all were fixed, and the only deferrals are items the spec
already lists as explicit follow-ups/non-goals.

## Resolutions a-o

| # | Finding | Resolution | Pointer |
|---|---------|------------|---------|
| a | A journal handoff, or a batch drained while coalescing, could push `append_batch` past `journal_coalesce_max`; a single fsync could cover more records than the documented cap. | Split the received batch at the cap and carry the remainder into the next group, preserving order, so every append/fsync is bounded. | `src/runtime/journal.rs` (`JournalThread::run`); tests `journal_splits_an_oversized_batch_at_the_coalesce_cap`, `journal_splits_a_mixed_sequence_of_batches_at_the_coalesce_cap`, `journal_handles_a_coalesce_cap_smaller_than_one_batch` |
| b | A snapshot left `Due` because the worker channel was full was retried on later polls, so an automatic snapshot could be captured off a cadence boundary. | Retry a due snapshot only at the next cadence boundary; intake is still never paused for a full worker channel. | `src/runtime/snapshot_gate.rs` (`SnapshotPending::Due` arm); `docs/durable-storage.md` |
| c | The ingress-depth gauge could underflow/wrap (post-send increment raced a dequeue) and was updated even when metrics were disabled. | Increment before `try_send`, undo on refusal, saturating decrement on dequeue, and gate every depth atomic on `metrics.is_some()`. | `src/runtime/shared.rs` (`note_ingress_*`), `src/runtime/submitter.rs`; `tests/runtime_integration.rs` bounds `queue_depth_max` |
| d | The response-slot pool was sized only from the journal channel, so it could exhaust before the ingress was genuinely full, making the two overload signals indistinguishable. | Size the pool for the full applied-not-durable lead: ingress + journal channel + coalesced group + matching buffer + split carry + 64 slack. | `src/runtime/ticket.rs` (`SlotPool::capacity_for`); tests `pool_covers_the_full_applied_but_not_durable_lead`, `slot_pool_does_not_exhaust_before_the_ingress_is_full` |
| e | SLO evaluation marked PASS from p99 alone, so a run that shed load with `QueueFull` could still claim the latency target. | PASS now requires `p99 <= target` **and** `queue_full == 0`; queue-full is reported in text and JSON. | `src/bench/report.rs` (`SloCheck::new`); test `slo_pass_requires_zero_queue_full` |
| f | The durable SLO was reported PASS/FAIL with `--log memory`, where no fsync exists. | With an in-memory log the durable SLO is `NOT APPLICABLE`; it remains judgeable with `--log file`. | `src/bench/runtime_path.rs`, `src/bench/report.rs`; tests `report_marks_the_durable_slo_not_applicable_with_the_memory_log`, `report_marks_the_durable_slo_judgeable_with_the_file_log` |
| g | Worker panic identity was an untyped `&'static str` in `RuntimeError::WorkerPanicked`. | Added `Worker::{Matching, Journal, Snapshot}` with `Display`; lifecycle and tests use the typed value. | `src/runtime/error.rs`, `src/runtime/lifecycle.rs` |
| h | Applied/durable/next sequences travelled as three loose `u64`s, inviting transposition across recovery and startup. | Bundled them in `CommandCursor` (with `fresh()`); recovery, the synchronous facade, and the matching thread pass one value. | `src/durable/mod.rs` (`CommandCursor`), `src/runtime/lifecycle.rs`, `src/runtime/matching.rs` |
| i | `matching.rs` had become a god module holding shared state, the submitter, the snapshot gate, and the matching loop. | Split into `shared.rs`, `snapshot_gate.rs`, `submitter.rs`, and `sync.rs`; `matching.rs` keeps the loop and its seams. | `src/runtime/mod.rs`, the four new modules |
| j | Internal runtime types (`SlotPool`, `SlotHandle`, `SlotState`, `ResponseSlot`, `Idle`, `CommandEnvelope::new`) leaked through the public API. | Made them `pub(crate)` and narrowed re-exports to the supported surface (`Ticket`, `CommandReceipt`, `IdleStrategy`, `Worker`). | `src/runtime/mod.rs`, `src/runtime/envelope.rs`, `src/runtime/ticket.rs`, `src/runtime/idle.rs` |
| k | The bench duplicated the symbol-list construction and the `BenchConfig -> RuntimeConfig` mapping. | Added `symbol_list()` and `BenchConfig::runtime_config()` as the single conversion points, with tests. | `src/bench/run.rs`, `src/bench/config.rs`, `src/bench/runtime_path.rs` |
| l | Documentation still stated the old slot-pool formula. | Updated to the full lead formula in the spec, durable-storage doc, runtime module doc, and `SlotPool` doc. | `spec.md` (Response slots), `docs/durable-storage.md`, `src/runtime/mod.rs` |
| m | SLO docs did not state the `queue_full == 0` requirement or the memory-log applicability rule. | Documented both, including the required Linux release/spin/pinned/NVMe setup and JSON `status`. | `docs/durable-storage.md` (SLO measurement), `src/bench/report.rs`, `src/runtime/mod.rs` |
| n | The known-deviations list omitted that the ingress channel still buffers/synchronizes and the command may allocate. | Added the deviation to the module docs and durable-storage doc. | `src/runtime/mod.rs` (Non-goals), `docs/durable-storage.md` (Known deviations) |
| o | Dead/test-only helpers remained on the production surface (`ResponseSlot::stop`, `Idle::strategy`, `SlotHandle::new`, unused `SnapshotSink::last_error`, duplicated `lock` helpers). | Removed dead helpers or gated them with `#[cfg(test)]`; consolidated poisoned-lock recovery into `sync::lock`. | `src/runtime/ticket.rs`, `src/runtime/idle.rs`, `src/runtime/snapshot.rs`, `src/runtime/sync.rs` |

## Deferred / documented, not defects

- Sharding or lock-free replacing the `Mutex<Vec<u32>>` slot free list: spec
  follow-up only if the bench shows producer contention; no such evidence in
  this run. Pointer: `spec.md` (Response slots), `docs/durable-storage.md`.
- Custom ingress ring, `io_uring`, replication, NUMA/real-time control,
  isolated-core management, trade/ledger/log retention: declared non-goals.
  Pointer: `spec.md` (Known deviations and non-goals), `src/runtime/mod.rs`.
- A full snapshot-worker channel defers the capture to the next cadence boundary.
  This is the intended best-effort behavior, not an open finding.
- Numeric SLOs are not claimed on Windows; the report remains informational there.

No finding a-o was left unfixed in code.

## Verification (integration worktree)

- `cargo test`: pass, 263 lib tests + 138 integration tests (9 suites) + 0 doc tests,
  0 failures, no warnings.
- `cargo doc --no-deps`: pass, no warnings.
- `cargo check --all-targets`: pass, no warnings.
- Stress: `cargo test --lib -- --test-threads=16` x5 and `--test-threads=24` x3
  (263 passed each); `cargo test --test runtime_integration` x3 (11 passed each).
- No global `cargo fmt` was run.
