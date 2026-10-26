# Durable storage

Two write paths share the storage contract. The synchronous
`DurableMatchingEngine` makes every command durable before applying it
(append-before-apply). `EngineRuntime` (the Matching Runtime) applies first and
acknowledges at two levels (apply-first). Both recover exact decision state by
restoring the latest snapshot and replaying the command-log tail after it. This
page describes the storage contract, both write paths, the shipped file
formats, and what a future adapter must guarantee.

## Commands are the source of truth

The durable log stores `EngineCommand` values, not `EngineEvent` values. Events
are derived outputs: they lack the side, price, and sometimes symbol needed to
rebuild the book, so they cannot be replayed into state. See
[ADR 0001](adr/0001-command-log-as-source-of-truth.md).

Each `DurableMatchingEngine` owns exactly one globally ordered command log. Order
ids, trade ids, event sequences, and the snapshot's applied command sequence are
global, so all symbols share one sequence space. See
[ADR 0002](adr/0002-single-global-ordered-log.md).

## On-disk formats

### Command log

The file adapter (`FileCommandLog`) writes a header line followed by one record
per line. Every line, header included, is:

```
{crc32:08x} {json}\n
```

The CRC-32 covers exactly the JSON bytes (not the checksum, space, or newline).
Appends write, flush, and fsync before returning, so an acknowledged append is
durable. The header is written through the same durable append path.

`append_batch` validates the whole batch for sequence contiguity and encodes
every line before writing, then writes all lines, flushes once, and fsyncs once.
`Ok` means every record in the batch is durable; `Err` may leave a prefix of the
batch written, so the caller must fail-stop rather than retry the same batch.
The default trait implementation falls back to one `append` per record, keeping
per-record durability without group commit.

On open:

- A final fragment without a terminating newline is truncated. It is an append
  that was never acknowledged.
- Any newline-terminated line that fails its CRC or JSON decoding aborts `open`
  with a corruption error, **including the last line**. A completed, fsynced
  append cannot legitimately lose its checksum; treating it as a torn write
  would silently drop applied history.

### Snapshots

The file adapter (`FileSnapshotStore`) writes one file per snapshot, named
`snapshot-{applied_seq:020}.json`, using the same `{crc32} {json}` line
convention around a `StoredSnapshot`. Saving writes a temporary file in the same
directory, flushes and fsyncs it, atomically renames it over the target, syncs
the directory on a best-effort basis, and then prunes older snapshots, keeping
the two newest. Loading sorts candidates newest-first and skips a file that
fails its checksum or JSON decoding, so a corrupt newest snapshot falls back to
the previous one.

## Runtime path: apply-first with dual acknowledgement

`EngineRuntime` is the second write path. It uses the same `CommandLog` and
`SnapshotStore` traits and the same recovery helper, but it changes the
write-order invariant:

- `DurableMatchingEngine` is **append-before-apply**. `execute` appends and
  fsyncs the command record, then applies it. Applied state can never be ahead
  of the log.
- `EngineRuntime` is **apply-first**. The matching thread applies the command
  and sends a preliminary applied acknowledgement before the journal thread
  writes it. The record is durable only after the journal's `append_batch`
  returns.

The synchronous façade keeps append-before-apply; the runtime does not. The
runtime does not weaken the durable level: no caller is told a command is
durable before its record is in the log. The decision and its trade-offs are
recorded in
[ADR 0003](adr/0003-apply-first-runtime-with-bounded-queues.md).

### Applied versus durable acknowledgement

- **Applied Acknowledgement** (`Ticket::wait_applied`, preliminary): the engine
  has changed state and assigned a command sequence. A crash before the journal
  fsyncs can lose the command even though the caller saw this receipt.
- **Durable Acknowledgement** (`Ticket::wait_durable`): the command record is in
  the command log and will be replayed on recovery. It implies the applied
  receipt. `submit_and_wait` waits for this level;
  `submit_and_wait_applied` waits only for the preliminary level.

A successful `try_submit` is neither acknowledgement. It only means the command
entered the bounded ingress queue; enqueue is not acceptance, and the command
may still be lost in the crash window before it is durable.

### Durability watermark and snapshot gating

`applied_seq` is the last command sequence applied. The **durability watermark**
(`durable_seq`) is the last sequence whose record is confirmed in the log.
`durable_seq <= applied_seq` always holds. The journal thread advances the
watermark with a release store after `append_batch` returns; the matching thread
then completes the durable receipts for every pending sequence at or below the
watermark, in order.

Automatic snapshots are captured only at a cadence boundary and only when
`applied_seq == durable_seq`. If a boundary is reached while the journal lags,
the matching thread pauses intake until the watermark catches up, then captures
and resumes. This is a deliberate, infrequent pause: without it, continuous load
could starve snapshots forever because applied normally leads durable. Recovery
therefore never sees a snapshot ahead of the log. Capture happens on the matching
thread; the `SnapshotStore::save` call happens on the snapshot worker, so
snapshot IO is not on the matching path.

Automatic snapshots are best effort. The worker channel holds one queued job;
when it is full the gate keeps the snapshot due and retries at the next
durability boundary instead of stalling matching. A snapshot failure is recorded
and never fails an already-durable command.

Explicit shutdown drains the ingress, waits for `durable_seq == applied_seq`
(bounded by `shutdown_timeout`), captures a final snapshot at that boundary, and
waits for the worker to save it. If the durability wait times out, shutdown skips
the final snapshot and returns `RuntimeError::Timeout`. `Drop` performs the same
drain and join without a final snapshot.

Recovery is unchanged and shared with the synchronous path: restore the latest
snapshot, reject a snapshot ahead of the log, then replay the contiguous log
tail. A command that received only an applied acknowledgement was never written,
so recovery correctly omits it. That omission is the preliminary semantic, not a
recovery bug.

### Bounded queues and backpressure

- The ingress Command Queue is `std::sync::mpsc::sync_channel(queue_capacity)`
  (default 1024). `try_submit` rejects with `SubmitError::QueueFull` instead of
  growing without bound; the command is not applied and consumes no sequence.
- The response-slot pool is pre-allocated with
  `queue_capacity + journal_batch_capacity * apply_batch_max +
  journal_coalesce_max + 2 * apply_batch_max + 64` slots. The extra terms cover
  the matching thread's journal buffer and the journal thread's split
  remainder, not only the journal channel. Exhausting the pool is also reported
  as `QueueFull`, so the hot path never allocates a response channel. The pool
  cannot exhaust before the ingress Command Queue is genuinely full, which keeps
  the two overload signals distinguishable.
- The journal batch channel holds `journal_batch_capacity` batches (default 64)
  of up to `apply_batch_max` records (default 64). If the journal will not accept
  a flush, the matching thread stops applying; applied state cannot run away from
  the log.
- The journal coalesces up to `journal_coalesce_max` records (default 1024) per
  `append_batch`/fsync, and `journal_flush_interval` (default 100 µs) bounds how
  long the first record in a group waits for a flush. A handoff larger than the
  remaining room is split at the cap; its remainder is appended in order in the
  next group, so no `append_batch`/fsync ever exceeds the cap.
- The snapshot worker channel holds one job; a full channel is retried, never
  waited on. An automatic snapshot is still captured only on a cadence boundary,
  so a full channel defers it to the next boundary rather than capturing an
  off-cadence sequence.

### SLO measurement

The runtime benchmark (`bench --path runtime`) reports two independent SLOs,
measured per command in-process from submission to receipt with the monotonic
clock:

- `submit -> applied` p99 <= 10 µs with `--log memory`, one producer,
  `--in-flight 1`, `--idle spin`, the matching thread pinned, and
  `queue_full == 0`.
- `submit -> durable` p99 <= 500 µs at 100k commands/s with `--log file` on
  NVMe, with the same spin/pinned setup and `queue_full == 0`.

A PASS requires both `p99 <= target` **and** `queue_full == 0`: a refusal means
the producer could not sustain the offered rate, so the measured latency does not
describe the acceptance workload. The required setup is a Linux release build,
`--idle spin`, a pinned matching core, and `FileCommandLog` on NVMe for the
durable level. With `--log memory` the durable SLO is marked **NOT APPLICABLE**
rather than PASS/FAIL, because an in-memory log cannot exercise fsync. The report
prints p50/p99/p99.9 for both levels, the queue-wait, apply-service, durable-wait,
and journal-append components, the mean batch size, maximum queue depth,
`queue_full`, and the fsync share (journal append time as a share of durable
wait).

**Windows is functional-only.** Its clocks (QPC) typically tick at about 100 ns,
so sub-tick differences are not meaningful; do not claim the numeric SLOs there.
Run the functional test suite on Windows and measure the SLOs on Linux.

### Known deviations

These are deliberate gaps relative to an HFT-grade engine; do not describe this
runtime as HFT-grade until they are closed:

- Ingress is bounded `std::sync::mpsc`, not a custom Disruptor-style ring.
- The response-slot pool removes per-command channel allocation, but the ingress
  channel still has bounded internal buffering and synchronization; the command
  itself may still allocate when constructed by the caller.
- One journal thread with group commit; no replication and no `io_uring`.
- Core affinity is opt-in; there is no real-time priority, NUMA control, or
  isolated-core management.
- No market-data output stream and no read/query API; engine events are cleared.
- No client idempotency or deduplication; request ids are observability only.
- No per-symbol sharding, timers, GTT, or auction events.
- Trade history, the order ledger, and the command log are retained for the
  process lifetime; snapshots grow with the ledger and trades. Retention and
  archival are out of scope.
- Applied acknowledgements are preliminary; `wait_durable` is the safe default.
- The synchronous façade and the runtime share only recovery and the storage
  traits; they are two distinct write paths.

## Writing a `CommandLog` adapter

A `CommandLog` implementation must:

- Return from `append` only after the record is durable.
- Return from `append_batch` only when every record in the batch is durable, and
  never report success for a partially durable batch.
- Read records back in append order.
- Never invent, reorder, or deduplicate sequences. Sequence gaps and duplicates
  are the durable layer's concern; the adapter reports exactly what is stored.
- Keep physical bounds private. `first_sequence`/`last_sequence` are command
  sequences; a Kafka offset or file offset is an adapter detail.

### Kafka (requirements, no implementation ships)

A Kafka adapter must use:

- `acks=all` and an idempotent producer;
- a single partition per engine instance, so the log has one global order;
- replication factor at least 3 and `min.insync.replicas` at least 2;
- delete-style retention with log compaction disabled.

Weakening any of these breaks the contract. `acks` below `all` can acknowledge
a record a broker has not durably replicated. A non-idempotent producer can
duplicate or reorder records on retry. Multiple partitions destroy the single
global order that command sequences, trade ids, and deterministic replay depend
on. A replication factor or `min.insync.replicas` below these values allows an
acknowledged record to be lost when a broker fails. Log compaction removes
individual records and creates sequence gaps; retention must keep the history
recovery needs.

### Redis

Redis is a `SnapshotStore`, not a `CommandLog`. Its default persistence can lose
recent writes, and its data is memory-bound, so it cannot be the system of
record for the command stream. It is acceptable for snapshots because a lost
snapshot only lengthens replay from the command log.

## Client idempotency is out of scope

Log-level sequence deduplication handles at-least-once **log delivery only**: a
duplicate record already applied is skipped during recovery. It is not client
request idempotency. A client retry that produces a new command creates a new
order. Callers and gateways must deduplicate by their own client order id, or
must not retry a request whose outcome is unknown.

## Format upgrades

`FORMAT_VERSION` is written into the header, every command record, and every
stored snapshot. An unknown version is refused with `UnsupportedVersion`; it is
never upcast or guessed.

To upgrade a format: keep the old command log and snapshots, run a migration
step or tool that replays the old command log and writes a new-format snapshot,
and only after that snapshot is durable may old logs or snapshots be cleaned up.
No migration tool ships in this release; the procedure is a requirement on
whoever changes the format.

A version-mismatched snapshot is deliberately **not** skipped by the stores. A
`SnapshotStore` returns the newest checksum-valid, decodable snapshot, and the
durable layer validates its format version and config against the log header.
An incompatible snapshot therefore fails startup loudly instead of silently
falling back to an older snapshot that might omit history or disagree with the
log.

## Known limits

- The order ledger and trade history grow without bound inside the engine, and
  both are included in snapshots, so snapshots grow with them.
- Event history also grows without bound in a long-lived process, but events are
  not persisted and are not recovered. The runtime clears events after each
  command, so its audit history does not grow; its ledger, trades, snapshots,
  and command log have the same unbounded retention as the synchronous path.
- The command log itself has no truncation or archival in this release.
