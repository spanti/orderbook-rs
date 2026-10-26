# Matching Runtime: Single-Writer Command Queue, Apply-First Durability, Dual Acknowledgement

Status: ready-for-agent

## Problem Statement

撮合引擎目前只有一种使用方式：调用方直接持有 `&mut MatchingEngine`（或 `&mut DurableMatchingEngine`）并同步调用 `execute`。这带来三个问题：

1. **并发安全靠调用方自觉。** ADR 0002 已经决定"单线程、无内部并行"，但没有任何组件把 single-writer 变成结构约束；多线程误用只能靠外层加锁补救。
2. **生产端与撮合耦合。** 突发流量直接压在撮合线程上，没有有界缓冲，也没有明确的过载语义。
3. **持久化路径决定延迟下限。** `DurableMatchingEngine` 是 append-before-apply，每条命令都要等 fsync；`submit → applied` 不可能进入微秒级。

目标是给引擎加一个**单线程撮合运行时**：命令经有界队列进入唯一的 Matching Thread，生产端通过可克隆的 Engine Handle 提交；同时提供真正的低延迟路径——先应用、后落盘，回执分 applied（preliminary）与 durable 两级，配 Journal Thread + group commit、预分配回执槽、可选绑核。结构上对齐 LMAX/CME/Eurex 的单写者共性；性能上以两套 SLO 验收，而不是笼统宣称 "low latency"。

## Solution

新增 `src/runtime/`：

- `EngineRuntime`：拥有 Matching Thread、Journal Thread、Snapshot Worker，负责启动、恢复、关闭。
- `EngineHandle`：`Clone + Send + Sync` 的提交入口；`try_submit` 立即返回 `Ticket`，另有 `submit_and_wait`（durable）与 `submit_and_wait_applied` 便利封装。
- `CommandEnvelope`：命令 + 预分配回执槽 + 可选入队时间 + request id。
- `Ticket`：可分别等待 `wait_applied` 与 `wait_durable`；回执是 `CommandReceipt { outcome, command_sequence }`。
- `RuntimeMetrics`：提交/应用/持久化计数、排队等待、应用服务时间、durable 等待、QueueFull、批次大小、journal append 时间。

数据路径：

```
producer thread(s)
    │ try_submit（有界 MPSC，满则 QueueFull）
    ▼
Command Queue (std::sync::mpsc::sync_channel)
    ▼
Matching Thread ── apply ──► Applied Acknowledgement (preliminary)
    │                         │
    │ 批量 CommandRecord       └─ pending durable slots
    ▼
Journal Thread ── append_batch + 一次 fsync ──► Durability Watermark (durable_seq)
    │
    └─► Durable Acknowledgement
                                   Snapshot Worker
Matching Thread ── Engine Snapshot（仅 durable_seq 处）──► Snapshot Store
```

原有的同步 `DurableMatchingEngine`（append-before-apply）保留，供直接调用与测试；运行时使用独立的 apply-first 管线，二者共享恢复算法与存储 trait。详细取舍见 ADR 0003。

## User Stories

1. As a developer, I want one Matching Thread per runtime to own all mutable engine state, so single-writer is a structural constraint, not a convention.
2. As a developer, I want producers to submit through a cloneable handle, so concurrent callers never touch engine state.
3. As a developer, I want a bounded ingress queue, so overload is explicit and memory is bounded.
4. As a developer, I want `try_submit` to return `QueueFull` immediately when the queue is full, so producers can shed load without blocking.
5. As a developer, I want a `Ticket` returned immediately, so I can choose when and whether to wait.
6. As a developer, I want `wait_applied` to return after the engine applies the command, so latency-sensitive callers do not wait for fsync.
7. As a developer, I want `wait_durable` to return only after the command record is durable, so durability-sensitive callers are safe.
8. As a developer, I want a receipt carrying the engine-assigned command sequence, so responses can be ordered and reconciled.
9. As a developer, I want a rejected command to still receive a command sequence and a durable record, so replay reproduces the rejection.
10. As a developer, I want one producer's commands to apply in submission order, so per-client FIFO holds.
11. As a developer, I want cross-producer order to be whatever the queue serializes, so the engine still has a total order.
12. As a developer, I want "enqueue is not acceptance" documented, so nobody mistakes the queue for durability.
13. As an operator, I want the ingress queue to reject rather than grow, so overload is visible and bounded.
14. As an operator, I want journal backpressure to stop further application, so applied state cannot run away from the log.
15. As a developer, I want fsync on a Journal Thread, so it is not on the matching thread.
16. As a developer, I want group commit, so many commands share one fsync.
17. As an operator, I want a durability watermark, so I know exactly what is recoverable.
18. As a developer, I want durable acknowledgements only up to the watermark, so no caller is told a lost command is durable.
19. As an operator, I want snapshots only at or below the watermark, so recovery never sees a snapshot ahead of the log.
20. As an operator, I want snapshot writes on a separate worker, so they do not add IO spikes to matching.
21. As an operator, I want a snapshot failure not to fail durable commands, so degraded snapshots do not stop trading.
22. As an operator, I want recovery to keep the existing header/gap/snapshot-ahead checks, so the runtime does not weaken durability.
23. As a developer, I want the synchronous `DurableMatchingEngine` unchanged, so existing direct users keep append-before-apply.
24. As a developer, I want one shared recovery helper, so both write paths decode the log identically.
25. As a developer, I want a pre-allocated response-slot pool, so submit/apply does not allocate a channel per command.
26. As a developer, I want the command moved into the log record after applying, so the runtime does not clone commands.
27. As a developer, I want configurable idle strategies, so I can trade CPU for latency.
28. As a developer, I want pure spin for benchmarking and bounded backoff for normal use.
29. As an operator, I want optional core affinity, so the matching thread can be pinned.
30. As an operator, I want affinity misconfiguration to fail startup loudly.
31. As a developer, I want metrics for queue wait, apply time, durable wait, QueueFull, batch size, and journal append time.
32. As a developer, I want metrics disabled to avoid clock reads and atomics on the hot path.
33. As an operator, I want explicit shutdown to stop intake, drain, flush, snapshot, and join.
34. As an operator, I want `Drop` to drain and join without a final snapshot.
35. As a developer, I want a worker panic to unblock every waiting ticket with `EngineStopped`, so callers never hang.
36. As an operator, I want an append failure to fail-stop and reply `EngineFailed` to pending and queued commands.
37. As a developer, I want submissions after a fatal failure to be rejected, so the runtime fails closed.
38. As a test author, I want a single-producer runtime test to match direct execution exactly.
39. As a test author, I want a crash-window test proving applied-but-not-durable commands are lost after restart.
40. As a test author, I want concurrent-producer tests with unique, contiguous command sequences and exact replay.
41. As a test author, I want QueueFull, panic, shutdown, snapshot-gating, and affinity tests.
42. As a developer, I want the bench to compare direct and runtime paths with the same workload.
43. As an operator, I want the bench to report both SLOs and the fsync share, so latency claims are honest.
44. As a maintainer, I want ADR 0003 to record the invariant change, so it is not mistaken for a bug.
45. As a developer, I want the runtime to clear engine events, so a long-running process does not accumulate output history.
46. As a maintainer, I want known deviations listed, so nobody calls this HFT-grade without the missing pieces.

## Implementation Decisions

### Terminology

Use the glossary terms: Command Queue, Command Envelope, Ticket, Matching Thread, Engine Handle, Matching Runtime, Journal Thread, Snapshot Worker, Applied Acknowledgement, Durable Acknowledgement, Durability Watermark. Never use bare "queue" for the ingress. The price-level FIFO queue inside the book is unrelated.

### Threads, ownership, and channels

- **Matching Thread** owns `MatchingEngine`, the applied/next command counters, the journal buffer, the pending-durable queue, and the snapshot gate.
- **Journal Thread** owns the `CommandLog`. It receives batches of `CommandRecord`, calls `append_batch`, and advances the shared durability watermark.
- **Snapshot Worker** owns the `SnapshotStore`. It receives `StoredSnapshot` jobs and saves them.
- **Producers** only hold `EngineHandle` clones.
- Ingress: `std::sync::mpsc::sync_channel(queue_capacity)` of `CommandEnvelope`; producers use `try_send`. `queue_capacity` default 1024.
- Journal input: `sync_channel(journal_batch_capacity)` of `Vec<CommandRecord>`; `journal_batch_capacity` default 64 batches. The maximum number of records that may be applied but not yet handed to the journal is `journal_batch_capacity * apply_batch_max`; `apply_batch_max` default 64.
- Snapshot jobs: `sync_channel(1)`; a full channel keeps the snapshot due and retries at a later durability boundary rather than blocking or growing. Automatic snapshots are best effort and never stall matching.
- Shared state: `Arc<AtomicBool>` closed, `Arc<AtomicBool>` failed, `Arc<Mutex<Option<DurableError>>>` fatal error, `Arc<AtomicU64>` applied sequence, `Arc<AtomicU64>` durable watermark, a shutdown deadline plus timed-out flag (bounded shutdown), an ingress-depth counter used only for metrics, `Arc<RuntimeMetrics>` optional, and the response-slot pool.

### Public API

```rust
pub struct RuntimeConfig {
    pub queue_capacity: usize,          // 1024
    pub journal_batch_capacity: usize,  // 64
    pub apply_batch_max: usize,         // 64
    pub journal_coalesce_max: usize,    // 1024 records per fsync
    pub journal_flush_interval: Duration, // 100µs upper bound on buffering
    pub snapshot_cadence: u64,          // 0 = no automatic snapshots
    pub idle: IdleStrategy,             // Backoff
    pub matching_core: Option<usize>,   // None; index into core_affinity::get_core_ids()
    pub shutdown_timeout: Duration,     // 5s
    pub metrics: Option<Arc<RuntimeMetrics>>,
}

impl EngineRuntime {
    pub fn create(config, engine_config, log, snapshots) -> Result<(EngineHandle, Self), RuntimeError>;
    pub fn open(config, log, snapshots) -> Result<(EngineHandle, Self), RuntimeError>;
    pub fn shutdown(self) -> Result<(), RuntimeError>;
    pub fn metrics(&self) -> Option<&Arc<RuntimeMetrics>>;
}

#[derive(Clone)]
pub struct EngineHandle { /* ingress sender, pool, flags, metrics, request-id counter */ }

impl EngineHandle {
    pub fn try_submit(&self, command: EngineCommand) -> Result<Ticket, SubmitError>;
    pub fn submit_and_wait(&self, command: EngineCommand) -> Result<CommandReceipt, RuntimeError>;
    pub fn submit_and_wait_applied(&self, command: EngineCommand) -> Result<CommandReceipt, RuntimeError>;
    pub fn is_failed(&self) -> bool;
    pub fn metrics(&self) -> Option<&Arc<RuntimeMetrics>>;
}

pub struct Ticket { /* one checked-out response slot */ }

impl Ticket {
    pub fn wait_applied(&self) -> Result<CommandReceipt, RuntimeError>;
    pub fn wait_applied_timeout(&self, timeout: Duration) -> Result<CommandReceipt, RuntimeError>;
    pub fn wait_durable(&self) -> Result<CommandReceipt, RuntimeError>;
    pub fn wait_durable_timeout(&self, timeout: Duration) -> Result<CommandReceipt, RuntimeError>;
}

pub struct CommandReceipt {
    pub outcome: CommandOutcome,
    pub command_sequence: u64,
}

pub enum SubmitError { QueueFull, Shutdown, EngineFailed(DurableError) }

pub enum RuntimeError {
    EngineFailed(DurableError),
    QueueFull,
    Shutdown,
    EngineStopped,
    Timeout,
    WorkerPanicked(&'static str),
    SnapshotFailed(DurableError),
    Startup(String),
}

impl From<SubmitError> for RuntimeError {
    fn from(error: SubmitError) -> Self {
        match error {
            SubmitError::QueueFull => Self::QueueFull,
            SubmitError::Shutdown => Self::Shutdown,
            SubmitError::EngineFailed(error) => Self::EngineFailed(error),
        }
    }
}
```

- `try_submit` takes ownership of the command and does not return it on `QueueFull`; callers that retry clone it first (`EngineCommand: Clone`).
- `submit_and_wait` = `try_submit` + `wait_durable`; `submit_and_wait_applied` = `try_submit` + `wait_applied`. Submission failures are preserved, not collapsed into a wait failure: `QueueFull` becomes `RuntimeError::QueueFull`, `Shutdown` becomes `RuntimeError::Shutdown`, and `EngineFailed` becomes `RuntimeError::EngineFailed`.
- `Ticket` waits take `&self`, so a caller may observe both levels on the same command.
- A `Ticket` holds a response slot until it is dropped and the worker is finished with it. Callers must drop tickets; the pool is bounded and its exhaustion is reported as `QueueFull`.

### Response slots

- At startup, pre-allocate `queue_capacity + journal_batch_capacity * apply_batch_max + 64` response slots. Each slot has a `Mutex<SlotState>` + `Condvar`; no per-command allocation is performed by the runtime.
- `SlotState` holds `applied: Option<Result<CommandReceipt, RuntimeError>>`, `durable: Option<Result<(), RuntimeError>>`, and `stopped: bool`.
- The producer checks out a slot from a free list; if none is available, `try_submit` returns `QueueFull` and increments the QueueFull metric.
- The worker writes the applied result and notifies; later it marks durability and notifies. `wait_durable` returns the stored applied receipt when durability succeeds.
- When both the `Ticket` and the worker's slot handle are dropped, the slot is reset and returned to the free list. The worker's handle marks the slot `stopped` if it is dropped before both results are set, so a panic or abort unblocks waiters with `EngineStopped`.
- The free list is a `Mutex<Vec<u32>>` for v1. If bench shows producer contention, sharding it is a follow-up.

### Matching Thread loop

1. Observe and clear the fatal error. If failed, drain the ingress, replying `EngineFailed` to every envelope, then exit.
2. Read `durable_seq`; complete and notify every pending-durable slot whose sequence is `<= durable_seq`.
3. If the journal buffer holds `apply_batch_max` records, `journal_flush_interval` has elapsed since its first record, or the ingress is empty and the buffer is non-empty, flush it. A flush retries `try_send` with the idle strategy until the journal accepts it; do not apply more commands while the buffer is full.
4. `try_recv` one envelope. If present:
   - record dequeue time (metrics on only),
   - `engine.execute_ref(&envelope.command)` (apply by reference; no clone),
   - assign `command_sequence = next_command_sequence; next_command_sequence += 1`,
   - set `applied_seq`,
   - write `CommandReceipt { outcome, command_sequence }` into the slot and notify applied waiters,
   - push the record `CommandRecord { format_version, sequence, command: envelope.command }` into the journal buffer,
   - push `(sequence, slot_handle)` onto the pending-durable queue,
   - `engine.clear_events()` so output history does not grow,
   - idle.reset().
5. If the ingress is empty, flush a non-empty journal buffer.
6. Run the snapshot gate (below).
7. If closed and the ingress is empty, the journal buffer is empty, and the pending-durable queue is empty, exit.
8. Otherwise apply the idle strategy (`Spin` or `Backoff`).

- Applying one command per iteration (rather than draining a batch of commands) keeps `submit → applied` latency independent of batch size. Batching applies only to journal records.
- `journal_flush_interval` bounds durable latency: `apply_batch_max` is a cap, not the only trigger. Without it, a continuous 100k/s stream with `apply_batch_max = 64` would delay the first record in a group by ~640µs, missing the durable SLO.
- `MatchingEngine` gains `execute_ref(&mut self, command: &EngineCommand) -> CommandOutcome` and `clear_events(&mut self)`.

### Journal Thread and group commit

- Receive one `Vec<CommandRecord>` (blocking `recv`), then drain further batches with `try_recv` until `journal_coalesce_max` records or the channel is momentarily empty.
- Call `CommandLog::append_batch(records)`. On success, store the highest sequence into the durability watermark with `Release` and `unpark` the matching thread. On failure, store the error, set failed, `unpark` the matching thread, and exit.
- Record journal append duration and append count in metrics. Batch size and batch count are recorded once by the matching thread at handoff, so coalesced groups are not double-counted.
- `CommandLog` gains `append_batch` and the supertraits `Send + 'static`:

```rust
pub trait CommandLog: Send + 'static {
    fn append_batch(&mut self, records: Vec<CommandRecord>) -> Result<(), DurableError> {
        for record in records { self.append(record)?; }
        Ok(())
    }
    // existing methods unchanged
}
```

- The default `append_batch` preserves per-record durability and is a correctness fallback. `FileCommandLog` overrides it to write all lines, flush once, and fsync once. Its contract: `Ok` means every record in the batch is durable; `Err` may leave a prefix written, and the caller must fail-stop.
- `SnapshotStore` also gains `Send + 'static`.

### Durability watermark and acknowledgements

- `applied_seq` is the last command applied; `durable_seq` is the last command confirmed durable. Always `durable_seq <= applied_seq`.
- The matching thread never confirms `wait_durable` before `durable_seq >= receipt.command_sequence`.
- On journal failure, every pending-durable command receives `Err(EngineFailed(...))`; every queued-but-not-applied envelope receives `Err(EngineFailed(...))` for both levels. New submissions return `SubmitError::EngineFailed`.
- On matching-thread panic, the response-slot handles drop and mark slots `stopped`; all waiters return `EngineStopped`. `shutdown`/join reports `WorkerPanicked("matching")`.
- On journal-thread panic, the matching thread observes a disconnected journal channel or the failed flag and fail-stops; `shutdown` reports `WorkerPanicked("journal")`.
- On snapshot-worker panic, automatic snapshots stop; durable commands are unaffected and `shutdown` reports `WorkerPanicked("snapshot")`.

### Snapshot gate and Snapshot Worker

- Automatic snapshots run when `snapshot_cadence > 0`, `applied_seq > 0`, `applied_seq % snapshot_cadence == 0`, and `applied_seq == durable_seq`.
- If the cadence boundary is reached while `applied_seq > durable_seq`, set `snapshot_due` and stop draining new commands until `durable_seq == applied_seq`; then capture and resume. This is a deliberate, infrequent pause: without it, a continuously loaded engine could starve snapshots forever because applied always leads durable.
- Capture `engine.capture_snapshot()` on the matching thread, build `StoredSnapshot { format_version, applied_seq, snapshot }`, and `try_send` it to the snapshot worker. If the worker's channel is full, keep the snapshot due and retry at a later durability boundary without pausing intake; snapshots are best effort and never stall matching.
- Track `last_snapshot_seq` so a boundary is not captured repeatedly.
- The snapshot worker calls `SnapshotStore::save`. Failures are stored in a shared `last_snapshot_error` and never fail an already-durable command.
- Explicit `shutdown` waits for `durable_seq == applied_seq` (bounded by `shutdown_timeout`), captures a final snapshot regardless of cadence, and waits for the worker to save it. A final snapshot failure is returned as `SnapshotFailed`.
- `Drop` does the same drain/join but skips the final snapshot and ignores errors.

### Recovery and the synchronous façade

- Extract the existing `DurableMatchingEngine::open` logic into a shared helper returning `(MatchingEngine, EngineConfig, applied_seq, durable_seq, next_command_sequence)`. `DurableMatchingEngine::open` delegates to it and keeps append-before-apply.
- `EngineRuntime::open` calls the helper, clears replayed events, then moves the engine to the matching thread, the log to the journal thread, and the snapshot store to the snapshot worker.
- `EngineRuntime::create` writes the log header and starts from an empty engine (`applied_seq = 0`, `durable_seq = 0`, `next_command_sequence = 1`).
- Recovery keeps every existing check: missing header, unsupported version, config mismatch, recovery gap, snapshot ahead, non-contiguous sequence, corrupt record.
- Recovery does not replay unacknowledged applied commands, because they were never written. This is the preliminary semantic.

### Shutdown and lifecycle

- `shutdown(self)` sets closed, rejects new submissions, and joins the matching thread, which drains the ingress, flushes the journal buffer, and waits for durability up to the deadline. If `applied_seq == durable_seq`, shutdown then captures the final snapshot through the snapshot worker and waits for its save; it finally drops the sinks and joins the journal and snapshot threads.
- `Drop` performs the same stop without a final snapshot and ignores errors/panics; it blocks until the threads exit.
- If `shutdown_timeout` expires while waiting for durability, stop applying, skip the final snapshot, and return `Timeout`. The timeout bounds the matching thread's wait; a journal thread already blocked in fsync cannot be interrupted, so the final join may still take as long as that fsync. Document this rather than pretending shutdown is hard-real-time.
- The runtime holds an ingress sender clone so the channel does not disconnect while handles exist; shutdown is signalled by the closed flag, not by sender drop.

### Metrics

`RuntimeMetrics` uses atomics grouped to avoid false sharing (`#[repr(align(64))]` wrappers where practical):

- `submitted`, `applied`, `durable`, `queue_full`, `engine_failed`, `stopped`
- `queue_wait_ns_sum/max`, `apply_ns_sum/max`, `durable_wait_ns_sum/max`
- `journal_append_ns_sum/max/count`, `batch_records_sum`, `batch_count`, `queue_depth_max`

- When metrics are `None`, the handle does not read the clock and the slot's `enqueued_at` is `None`; the worker skips timing and atomics.
- Queue wait is `dequeue - enqueued_at`; apply time is `after apply - dequeue`; durable wait is `durable observed - enqueued_at`.

### Idle strategy and affinity

- `IdleStrategy::{Spin, Backoff}`. `Backoff` spins `spin_iters` (10_000), then `yield_now`, then `park_timeout(50µs)`. `Spin` only calls `spin_loop()`.
- Default is `Backoff`; bench uses `Spin` to measure the low-latency target.
- The Journal Thread `unpark`s the matching thread after advancing the watermark, so a parked matching thread wakes immediately.
- `matching_core: Option<usize>` is an **index** into `core_affinity::get_core_ids()`, not a raw platform processor id; the mapping is stable only for the process lifetime. Add `core_affinity = "0.8"` to `Cargo.toml`. An unsupported platform, an out-of-range index, or a failed bind fails startup (`RuntimeError::Startup`); the runtime never silently runs unpinned. Do not pin journal/snapshot threads; document external isolation (`isolcpus`, `taskset`, Windows affinity) and that a busy-spin core must not be shared.

### Bench integration

- Extend the existing bench with `--path direct|runtime` (default `direct`).
- Runtime flags: `--queue-capacity`, `--journal-batch-capacity`, `--apply-batch-max`, `--journal-coalesce-max`, `--journal-flush-us`, `--idle spin|backoff` (bench default `spin`), `--affinity <CORE_INDEX>`, `--snapshot-cadence`, `--log memory|file`, `--data-dir <PATH>`, and `--in-flight <N>` (default 1 for latency). `--path direct|runtime` defaults to `direct`.
- Runtime path uses the same workload generator; because the runtime has no read API, the single producer maintains a shadow `MatchingEngine` updated after each receipt to drive book-aware actions. The shadow work happens outside the timed window.
- Report, in addition to existing fields: `applied_latency` (submit → applied), `durable_latency` (submit → durable), `queue_wait`, `apply_service`, `durable_wait`, `journal_append`, `batch_size_mean` and `batch_count`, `queue_depth_max`, `queue_full`, `applied_count`, `durable_count`, `fsync_share` (journal append time / durable wait), and an `slo` object with each level's target, p50/p99/p99.9, and pass/fail. The global `service` row mirrors applied for the runtime path; engine-service time is `apply_service`, measured inside the matching thread.
- `direct` path output and behavior remain unchanged.

### Acceptance SLOs

Measured per command in-process from submission to receipt with the monotonic clock, on Linux, release build, one producer, `--path runtime --idle spin`, matching thread pinned:

- `submit → applied` p99 ≤ 10µs at 100k commands/s with `InMemoryCommandLog`; `queue_full == 0`.
- `submit → durable` p99 ≤ 500µs at the same rate with `FileCommandLog` on NVMe; `queue_full == 0`.
- The report prints both levels' p50/p99/p99.9 and marks PASS when p99 ≤ target. Matching service time and journal append time are reported separately (`apply_service`, `journal_append`); durable latency must be explainable as queue wait + apply + journal append + wakeup.
- Windows is for functional validation only (QPC granularity is about 100ns); do not claim the numeric SLOs there. The PASS/FAIL line is informational outside the Linux release/pinned/spin setup.

### Known deviations and non-goals

- Ingress is std `sync_channel`, not a custom Disruptor ring.
- The response slot pool removes per-command channel allocation, but the ingress channel still has bounded internal buffering and synchronization; the command itself may still allocate when constructed by the caller.
- One journal thread, group commit, no replication, no `io_uring`.
- No real-time priority, NUMA control, or isolated-core management; affinity is opt-in.
- No market-data output stream; engine events are cleared. No read/query API through the handle.
- No client idempotency or deduplication; `request_id` is observability only.
- No per-symbol sharding, no timer/GTT/auction events.
- Trade history, order ledger, and the command log grow for the process lifetime; retention/archival is out of scope.
- `wait_applied` is preliminary and can lose data on crash; `wait_durable` is the safe default.
- Two write paths exist (synchronous façade and runtime); only recovery and storage traits are shared.
- Not HFT-grade until a custom ring, CPU isolation, and async IO are addressed.

## Testing

- Single-producer runtime execution matches direct `MatchingEngine` execution: same receipt sequence, same final `EngineSnapshot`, same command log.
- `wait_applied` returns before a deliberately blocked journal append; `wait_durable` remains blocked; after release, `wait_durable` returns and the record is in the log.
- Crash window: while the journal is blocked, the log is empty for the applied command; reopening from that log recovers without the command. This proves preliminary semantics.
- Concurrent producers: every command is applied exactly once; command sequences are unique and contiguous; replaying the log reproduces the runtime's final snapshot.
- `QueueFull` when the ingress is saturated; the command is not applied and no sequence is consumed.
- Journal append failure: applied waiters may already have a receipt; durable waiters get `EngineFailed`; subsequent submissions get `EngineFailed`.
- Matching thread panic: all waiters return `EngineStopped`; runtime join reports the panic. Journal thread panic fails the runtime closed.
- Shutdown drains queued commands, flushes the journal, writes the final snapshot, and joins. `Drop` drains and joins without a final snapshot.
- Snapshot gate: a snapshot is never saved with `applied_seq > durable_seq`; a slow snapshot store does not block matching; a snapshot failure does not fail durable commands.
- Metrics counts and latencies are internally consistent; metrics disabled performs no clock reads.
- Affinity: a valid core binds; an invalid core fails startup; skip on platforms without core ids.
- Existing tests, benches, and the synchronous durable test suite still pass.

## Further Notes

- The current `MatchingEngine` keeps every event, trade, and ledger record. The runtime clears events after each command, but trades and ledger retention remain unbounded; document this and treat retention as a separate feature.
- The file command log is append-only and is not truncated after snapshots; the snapshot floor rule from the durable spec still applies.
- If bench shows the `Mutex<Vec<u32>>` slot free list is a producer bottleneck, shard it per producer or replace it with a lock-free stack; do not add a custom ring until the std channel is proven to be the bottleneck.
- If durable ack p99 misses 500µs, measure the fsync distribution first; a separate journal thread with `io_uring` is the next step, not a rewrite of the matching loop.
