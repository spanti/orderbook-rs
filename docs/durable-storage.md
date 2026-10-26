# Durable storage

Two write paths share the event storage contract. The synchronous
`DurableMatchingEngine` decides, appends events, then applies them
(append-before-apply). `EngineRuntime` (the Matching Runtime) applies first and
acknowledges at two levels (apply-first). Both recover exact decision state by
restoring the latest snapshot and folding the event-log tail after it.

## Events are the source of truth

The durable log stores `DomainEvent` values wrapped in
`EventRecord { format_version, sequence, command_id, event }`, not commands.
Commands are ingress only: they drive `decide`, are not persisted, and cannot
rebuild state. See [ADR 0004](adr/0004-event-sourced-order-log.md).

One command emits one or more events. `command_id` groups the events produced by
one command; `sequence` is the global event sequence and the recovery boundary.
A command's events are contiguous in the log. `command_id` is persisted in
`StoredSnapshot.last_command_id`, and recovery continues from
`max(snapshot.last_command_id, last log record.command_id) + 1`, so grouping
stays unique across restarts.

`OrderRejected` is the only event with no state effect. It is logged for audit
and published to the bus; replay ignores it. A rejected command consumes an
event sequence but allocates no order id.

Each engine instance owns exactly one globally ordered event log. Order ids,
trade ids, and the snapshot's applied sequence are global, so all symbols share
one event sequence space. See [ADR 0002](adr/0002-single-global-ordered-log.md).

## Event catalog and per-command order

```
OrderAccepted { symbol, order, post_only }   creates the ledger record; no book effect
OrderRested   { symbol, order }              inserts order.quantity (remaining) into the book
TradeExecuted { trade }                      maker book/ledger down, taker ledger up, tape + last price
OrderCanceled { symbol, order_id, canceled_quantity, reason }
StopPlaced    { symbol, stop }               stop book + PendingTrigger ledger record
StopTriggered { symbol, order, last_price }  removes the stop; activates the ledger record
StopCanceled  { symbol, order_id }
OrderRejected { symbol, reason, command }    audit only; replay no-op
```

`OrderCanceled.reason` is `UserCancel`, `UnfilledResidual`, or `Modified`. The
event stream has one property that recovery and consumers rely on: **every
prefix is a valid engine state**. That is why `OrderAccepted` does not insert
into the book, `OrderRested` is a separate event, and `StopTriggered` carries the
converted `Order` and performs the whole activation itself.

Per command:

- submit: `OrderAccepted` -> `TradeExecuted`* -> `OrderRested` or
  `OrderCanceled{UnfilledResidual}`; a full fill has neither trailing event.
- cancel: `OrderCanceled{UserCancel}`; cancel of a pending stop:
  `StopCanceled`.
- modify (cancel old + new id, as before): `OrderCanceled{Modified}` ->
  `OrderAccepted` (new id) -> `TradeExecuted`* -> `OrderRested` or
  `OrderCanceled{UnfilledResidual}`.
- submit stop: `StopPlaced`.
- trigger stop: `StopTriggered` -> `TradeExecuted`* -> `OrderRested` or
  `OrderCanceled{UnfilledResidual}`.
- rejection: `OrderRejected`.

## On-disk formats

### Event log

The file adapter (`FileEventLog`) writes a header line followed by one
`EventRecord` per line. Every line, header included, is:

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
The default trait implementation falls back to one `append` per event record,
keeping per-record durability without group commit.

A batch may contain a prefix of one command's events. Because every event is an
independent state delta, replaying that prefix produces a valid state; it simply
corresponds to a command that never received a durable acknowledgement.

On open:

- A final fragment without a terminating newline is truncated. It is an append
  that was never acknowledged.
- Any newline-terminated line that fails its CRC or JSON decoding aborts `open`
  with a corruption error, **including the last line**. A completed, fsynced
  append cannot legitimately lose its checksum.

### Snapshots

The file adapter (`FileSnapshotStore`) writes one file per snapshot, named
`snapshot-{applied_seq:020}.json`, using the same `{crc32} {json}` line
convention around a `StoredSnapshot`. Saving writes a temporary file in the same
directory, flushes and fsyncs it, atomically renames it over the target, syncs
the directory on a best-effort basis, and then prunes older snapshots, keeping
the two newest. Loading sorts candidates newest-first and skips a file that
fails its checksum or JSON decoding, so a corrupt newest snapshot falls back to
the previous one.

`StoredSnapshot.applied_seq` is an **event** sequence: the last event reflected
in the snapshot. `StoredSnapshot.last_command_id` records the last `command_id`
reflected in the snapshot, so command grouping continues after a restart even
when the log tail is empty.

## Recovery

Recovery restores the latest snapshot, then applies every log record after it:

- read and validate the header version;
- load the latest snapshot, validate its version and config against the header,
  and reject a snapshot ahead of the log;
- restore the snapshot or create an empty engine;
- enforce `first_sequence <= applied_seq + 1` (gap check);
- read the tail from `applied_seq + 1`; skip duplicates
  (`record.sequence <= applied_seq`), reject non-contiguous sequences, and call
  `apply(event, sequence)` for each record;
- compute `durable_seq` and `next_sequence` from the log tail.

`OrderRejected` is applied as a no-op. The recovered engine's decision state is
bit-for-bit equal to folding the same event log from an empty engine. Replay
never re-runs matching and never re-decides a rejection.

## Runtime path: apply-first with dual acknowledgement

`EngineRuntime` is the second write path. It uses the same `EventLog` and
`SnapshotStore` traits and the same recovery helper, but changes the write-order
invariant:

- `DurableMatchingEngine` is **append-before-apply**. `execute` decides, appends
  and fsyncs the command's event records, then applies them. Applied state can
  never be ahead of the log.
- `EngineRuntime` is **apply-first**. The matching thread decides, applies all
  events of the command, and sends a preliminary applied acknowledgement before
  the journal thread writes them. The events are durable only after the
  journal's `append_batch` returns.

The synchronous façade keeps append-before-apply; the runtime does not. The
runtime does not weaken the durable level: no caller is told a command is
durable before **all of its events** are in the log. The decision and its
trade-offs are recorded in
[ADR 0003](adr/0003-apply-first-runtime-with-bounded-queues.md).

### Applied versus durable acknowledgement

- **Applied Acknowledgement** (`Ticket::wait_applied`, preliminary): the engine
  has applied all events of the command and assigned their event sequences. A
  crash before the journal fsyncs can lose the command even though the caller saw
  this receipt. The receipt carries the command's events and the first/last
  event sequence.
- **Durable Acknowledgement** (`Ticket::wait_durable`): every event of the
  command is in the event log and will be replayed on recovery. It implies the
  applied receipt. `submit_and_wait` waits for this level;
  `submit_and_wait_applied` waits only for the preliminary level.

A successful `try_submit` is neither acknowledgement. It only means the command
entered the bounded ingress queue; enqueue is not acceptance, and the command
may still be lost in the crash window before it is durable.

### Durability watermark and snapshot gating

`applied_seq` is the last event sequence applied. The **durability watermark**
(`durable_seq`) is the last event sequence whose record is confirmed in the log.
`durable_seq <= applied_seq` always holds. The journal thread advances the
watermark with a release store after `append_batch` returns; the matching thread
then completes the durable receipts for every pending command whose last event
sequence is at or below the watermark, in order.

Automatic snapshots are captured only at a command boundary and only when
`applied_seq == durable_seq`. `snapshot_cadence` is measured in event
sequences; when the threshold is crossed, the gate waits for the current
command's events to finish applying and for durability to catch up, then
captures at that boundary. If a boundary is reached while the journal lags, the
matching thread pauses intake until the watermark catches up, then captures and
resumes. This is a deliberate, infrequent pause: without it, continuous load
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
snapshot, reject a snapshot ahead of the log, then fold the contiguous event-log
tail. A command that received only an applied acknowledgement was never written,
so recovery correctly omits it. That omission is the preliminary semantic, not a
recovery bug.

## MessageBus publication

After the journal fsyncs a batch, it publishes a watermark/wake-up signal. A
separate Event Publisher reads `EventLog::read_from(last_published + 1)` up to
`durable_seq` and calls `try_publish_any("events", record)` in sequence order.
The publisher starts at the recovered durable watermark, so it publishes only
live events; history is the consumer's job via `EventLog::read_from`.

The bus is synchronous: handlers run on the publisher thread with `&dyn Any`,
and a slow handler delays publication for every subscriber but never the
matching thread. A panicking handler is caught and its endpoint is marked
failed. The bus does not persist, does not queue per subscriber, and does not
carry commands. See [ADR 0005](adr/0005-in-process-message-bus.md).

## Bounded queues and backpressure

- The ingress Command Queue is `std::sync::mpsc::sync_channel(queue_capacity)`
  (default 1024). `try_submit` rejects with `SubmitError::QueueFull` instead of
  growing without bound; the command is not applied and consumes no event
  sequence.
- The response-slot pool is pre-allocated with
  `queue_capacity + journal_batch_capacity * apply_batch_max +
  journal_coalesce_max + 2 * apply_batch_max + 64` slots. The extra terms cover
  the matching thread's journal buffer and the journal thread's split
  remainder, not only the journal channel. Exhausting the pool is also reported
  as `QueueFull`, so the hot path never allocates a response channel.
- The journal batch channel holds `journal_batch_capacity` batches (default 64)
  of up to `apply_batch_max` **event records** (default 64). If the journal will
  not accept a flush, the matching thread stops applying; applied state cannot
  run away from the log.
- The journal coalesces up to `journal_coalesce_max` event records (default
  1024) per `append_batch`/fsync, and `journal_flush_interval` (default 100 µs)
  bounds how long the first record in a group waits for a flush. A handoff
  larger than the remaining room is split at the cap; its remainder is appended
  in order in the next group.
- The snapshot worker channel holds one job; a full channel is retried, never
  waited on. An automatic snapshot is still captured only at a command boundary,
  so a full channel defers it to the next boundary rather than capturing an
  off-cadence state.

## SLO measurement

The runtime benchmark (`bench --path runtime`) reports two independent SLOs,
measured per command in-process from submission to receipt with the monotonic
clock:

- `submit -> applied` p99 <= 10 µs with `--log memory`, one producer,
  `--in-flight 1`, `--idle spin`, the matching thread pinned, and
  `queue_full == 0`. The measurement includes applying every event of the
  command.
- `submit -> durable` p99 <= 500 µs at 100k commands/s with `--log file` on
  NVMe, with the same spin/pinned setup and `queue_full == 0`. It waits for the
  command's last event to be durable.

A PASS requires both `p99 <= target` **and** `queue_full == 0`: a refusal means
the producer could not sustain the offered rate, so the measured latency does not
describe the acceptance workload. The required setup is a Linux release build,
`--idle spin`, a pinned matching core, and `FileEventLog` on NVMe for the
durable level. With `--log memory` the durable SLO is marked **NOT APPLICABLE**
rather than PASS/FAIL, because an in-memory log cannot exercise fsync. The report
prints p50/p99/p99.9 for both levels, the queue-wait, apply-service, durable-wait,
and journal-append components, the mean batch size, maximum queue depth,
`queue_full`, and the fsync share (journal append time as a share of durable
wait). Event volume per command affects the journal append component.

**Windows is functional-only.** Its clocks (QPC) typically tick at about 100 ns,
so sub-tick differences are not meaningful; do not claim the numeric SLOs there.
Run the functional test suite on Windows and measure the SLOs on Linux.

### Known deviations

These are deliberate gaps relative to an HFT-grade engine; do not describe this
runtime as HFT-grade until they are closed:

- Ingress is bounded `std::sync::mpsc`, not a custom Disruptor-style ring.
- The response-slot pool removes per-command channel allocation, but the ingress
  channel still has bounded internal buffering and synchronization.
- One journal thread with group commit; no replication and no `io_uring`.
- Core affinity is opt-in; there is no real-time priority, NUMA control, or
  isolated-core management.
- The MessageBus is synchronous and in-process. It has no per-subscriber queue,
  no lag detection, and no external broker adapter yet.
- `send_any` and `register_response_handler` are declared but unimplemented;
  there is no request/response or command path on the bus.
- No per-symbol sharding, timers, GTT, or auction events.
- The event log, trade history, and order ledger are retained for the process
  lifetime; there is no truncation or archival. Snapshots grow with the ledger
  and trades.
- Applied acknowledgements are preliminary; `wait_durable` is the safe default.
- The synchronous façade and the runtime share only decide/apply, recovery, and
  the storage traits; they are two distinct write paths.
- The event log is larger than the old command log because one command emits
  multiple events and rejected commands are logged.

## Writing an `EventLog` adapter

An `EventLog` implementation must:

- Return from `append` only after the record is durable.
- Return from `append_batch` only when every record in the batch is durable, and
  never report success for a partially durable batch.
- Read records back in append order.
- Never invent, reorder, or deduplicate sequences. Sequence gaps and duplicates
  are the durable layer's concern; the adapter reports exactly what is stored.
- Keep physical bounds private. `first_sequence`/`last_sequence` are event
  sequences; a Kafka offset or file offset is an adapter detail.
- Preserve `command_id`; it is metadata, but consumers use it to group a
  command's events.

### Kafka (requirements, no implementation ships)

A Kafka adapter must use:

- `acks=all` and an idempotent producer;
- a single partition per engine instance, so the log has one global order;
- replication factor at least 3 and `min.insync.replicas` at least 2;
- delete-style retention with log compaction disabled.

Weakening any of these breaks the contract. `acks` below `all` can acknowledge a
record a broker has not durably replicated. A non-idempotent producer can
duplicate or reorder records on retry. Multiple partitions destroy the single
global order that event sequences, trade ids, and deterministic replay depend
on. A replication factor or `min.insync.replicas` below these values allows an
acknowledged record to be lost when a broker fails. Log compaction removes
individual records and creates sequence gaps; retention must keep the history
recovery needs.

### Redis

Redis is a `SnapshotStore`, not an `EventLog`. Its default persistence can lose
recent writes, and its data is memory-bound, so it cannot be the system of
record for the event stream. It is acceptable for snapshots because a lost
snapshot only lengthens replay from the event log.

## Client idempotency is out of scope

Log-level sequence deduplication handles at-least-once **log delivery only**: a
duplicate record already applied is skipped during recovery. It is not client
request idempotency. A client retry that produces a new command creates a new
order. Callers and gateways must deduplicate by their own client order id, or
must not retry a request whose outcome is unknown.

## Format upgrades

`FORMAT_VERSION` is 2. It is written into the header, every event record, and
every stored snapshot. An unknown version is refused with `UnsupportedVersion`;
it is never upcast or guessed. Version 1 command logs are not readable and no
migration tool ships.

To upgrade a format: keep the old event log and snapshots, run a migration step
or tool that folds the old event log and writes a new-format snapshot, and only
after that snapshot is durable may old logs or snapshots be cleaned up. No
migration tool ships in this release; the procedure is a requirement on whoever
changes the format.

A version-mismatched snapshot is deliberately **not** skipped by the stores. A
`SnapshotStore` returns the newest checksum-valid, decodable snapshot, and the
durable layer validates its format version and config against the log header.
An incompatible snapshot therefore fails startup loudly instead of silently
falling back to an older snapshot that might omit history or disagree with the
log.

## Known limits

- The order ledger and trade history grow without bound inside the engine, and
  both are included in snapshots, so snapshots grow with them.
- The event log grows faster than the old command log: every order has at least
  an `OrderAccepted`, resting orders add `OrderRested`, and rejected commands
  occupy a record. There is no truncation or archival in this release.
- A snapshot is a derived cache and cannot be the only fact source; recovery
  still needs the event log tail after the snapshot.
- Event schemas may only grow with optional fields in this release. A breaking
  change requires a format-version bump and an explicit rebuild.
