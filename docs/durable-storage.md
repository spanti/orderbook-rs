# Durable storage

`DurableMatchingEngine` makes every command durable before applying it and
recovers exact decision state by restoring the latest snapshot and replaying the
command-log tail after it. This page describes the storage contract, the shipped
file formats, and what a future adapter must guarantee.

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
  not persisted and are not recovered.
- The command log itself has no truncation or archival in this release.
