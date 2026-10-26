# Matching Runtime: Two-Axis Review Fixes

Status: merged and verified.

- Review/fix branch: `ticket/12-review-fixes` at `9e132a6`, based on `ac046dd`.
- Merge commit: `d5ac088` (`merge: code-review fixes`).
- Integration branch: `feature/matching-runtime`.
- Spec: [`spec.md`](spec.md).

The review ran on two axes. **Standards** checked documented conventions plus the
Fowler smell baseline. **Spec** checked conformance to `spec.md`: apply-first
durability, journal group commit, snapshot gating, slot-pool sizing, failure
semantics, and the dual SLO contract. Every finding is listed below with its
axis and resolution.

## Standards — hard violations

| Finding | Resolution | Pointer |
|---|---|---|
| A journal handoff could push `append_batch` past `journal_coalesce_max`, so one fsync covered more records than documented. | Split the received batch at the cap and carry the remainder into the next group, preserving order. | `src/runtime/journal.rs` (`JournalThread::run`); `journal_splits_an_oversized_batch_at_the_coalesce_cap`, `journal_splits_a_mixed_sequence_of_batches_at_the_coalesce_cap`, `journal_handles_a_coalesce_cap_smaller_than_one_batch` |
| A snapshot left `Due` by a full worker channel was retried at any later equality point, so automatic snapshots could be captured off a cadence boundary. | Retry a due snapshot only at the next cadence boundary; a full worker never pauses intake. | `src/runtime/snapshot_gate.rs` (`SnapshotPending::Due`); `a_due_snapshot_retries_only_at_the_next_cadence_boundary` |

## Standards — judgement calls

| Finding | Resolution | Pointer |
|---|---|---|
| `Recovered.config` was dead outside tests. | Removed the field; recovery consumers keep only the engine and cursor. | `src/durable/mod.rs` |
| Applied/durable/next sequences were three loose `u64`s passed through several layers. | Bundled into `CommandCursor` and passed as one value. | `src/durable/mod.rs`, `src/runtime/lifecycle.rs`, `src/runtime/matching.rs` |
| Duplicated drain/fail loops, `lock` helpers, and bench symbol-list construction. | Extracted `MatchingThread::drain_ingress`, `sync::lock`, and `bench::run::symbol_list`. | `src/runtime/matching.rs`, `src/runtime/sync.rs`, `src/bench/run.rs` |
| `matching.rs` mixed shared state, submitter, snapshot gate, and the loop. | Split into `shared.rs`, `submitter.rs`, `snapshot_gate.rs`, `sync.rs`; `matching.rs` keeps the loop. | `src/runtime/` |
| Each bench knob was mirrored through five places. | Added `BenchConfig::runtime_config()` as the single conversion point; flat CLI fields stay for parser/JSON stability. | `src/bench/config.rs`, `src/bench/runtime_path.rs` |
| `WorkerPanicked(&'static str)` carried stringly identities. | Added typed `Worker::{Matching, Journal, Snapshot}`. | `src/runtime/error.rs`, `src/runtime/lifecycle.rs` |
| `request_id` competed with the glossary's “Request” term. | Added a `Request Id` glossary entry (observability only, not idempotency). | `GLOSSARY.md` |

## Spec — missing or partial

| Finding | Resolution | Pointer |
|---|---|---|
| Metrics-off still performed ingress-depth atomics. | Every depth atomic is gated on `metrics.is_some()`; submission/dequeue touch no counter when disabled. | `src/runtime/shared.rs`, `src/runtime/submitter.rs`; `metrics_disabled_submission_and_dequeue_touch_no_depth_counter` |
| SLO PASS/FAIL ignored `queue_full == 0`, and the durable SLO ran with the memory log. | PASS requires p99 ≤ target and `queue_full == 0`; durable is `NOT APPLICABLE` with `--log memory`, judgeable with `--log file`; the required setup is printed. | `src/bench/report.rs`, `src/bench/runtime_path.rs`; `slo_pass_requires_zero_queue_full`, `report_marks_the_durable_slo_not_applicable_with_the_memory_log` |
| Known deviations omitted the bounded ingress buffering/allocation caveat. | Added to the module docs and the durable guide. | `src/runtime/mod.rs`, `docs/durable-storage.md` |

## Spec — scope creep

| Finding | Resolution | Pointer |
|---|---|---|
| Internal slot/idle types leaked through the public API. | `Idle`, `ResponseSlot`, `SlotHandle`, `SlotPool`, `SlotState`, and `CommandEnvelope::new` are `pub(crate)`; the supported surface is narrowed to `Ticket`, `CommandReceipt`, `IdleStrategy`, `Worker`, handles and config. The two `#[doc(hidden)]` test hooks remain. | `src/runtime/mod.rs`, `src/runtime/envelope.rs`, `src/runtime/ticket.rs`, `src/runtime/idle.rs` |

## Spec — implemented but wrong

| Finding | Resolution | Pointer |
|---|---|---|
| Ingress-depth gauge could wrap: post-send increment raced a dequeue, poisoning `queue_depth_max`. | Increment before `try_send`, undo on refusal, saturating decrement on dequeue; high-concurrency test asserts the bound. | `src/runtime/shared.rs`, `src/runtime/submitter.rs`; `high_concurrency_ingress_depth_never_wraps_or_exceeds_the_queue_bound` |
| Snapshot retry could capture off-cadence. | Fixed with the Standards hard finding above. | `src/runtime/snapshot_gate.rs` |
| Lead bound understated the applied-but-not-durable records; the slot pool could exhaust before the ingress was full. | Pool capacity is now `queue_capacity + journal_batch_capacity*apply_batch_max + journal_coalesce_max + 2*apply_batch_max + 64` (effective, saturating); spec and guides state the same formula. | `src/runtime/ticket.rs` (`SlotPool::capacity_for`); `pool_covers_the_full_applied_but_not_durable_lead`, `slot_pool_does_not_exhaust_before_the_ingress_is_full` |

## Deferred / documented, not defects

- Sharding or a lock-free replacement for the `Mutex<Vec<u32>>` slot free list:
  a spec follow-up only if the bench shows producer contention.
- Custom ingress ring, `io_uring`, replication, NUMA/real-time control,
  isolated-core management, trade/ledger/log retention: declared non-goals.
- A full snapshot-worker channel defers the capture to the next cadence
  boundary; this is the intended best-effort behavior.
- Numeric SLOs are not claimed on Windows; the report is informational there.

No finding was left unfixed in code.

## Verification (integration worktree)

- `cargo test`: pass, 263 lib tests + 138 integration tests (9 suites) + 0 doc
  tests, 0 failures, no warnings.
- `cargo doc --no-deps`: pass, no warnings.
- `cargo check --all-targets`: pass, no warnings.
- Stress: `cargo test --lib -- --test-threads=16` ×5 and `--test-threads=24` ×3
  (263 passed each); `cargo test --test runtime_integration` ×3 (11 passed each).
- No global `cargo fmt` was run.
