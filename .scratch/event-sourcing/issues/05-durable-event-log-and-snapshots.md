# 05: Durable event log and event-sequence snapshots

**What to build:** The on-disk event log and event-sequence snapshots. A process can append `EventRecord`s durably, restart from disk, restore the latest snapshot, and fold the event tail to the exact same state. Version 1 logs are refused, the format is explicitly v2, and `command_id` stays continuous across restarts.

**Blocked by:** 01

**Status:** done

- [x] A file-backed `EventLog` writes a v2 header and reads back `EventRecord`s in append order; `append` makes the record durable before returning.
- [x] `append_batch` makes the whole batch durable before returning; a partial batch fails the caller closed.
- [x] A v1 header, v1 record, or v1 snapshot is refused with an unsupported-version error.
- [x] An unterminated final fragment is truncated; a terminated line with a bad checksum or bad JSON aborts open.
- [x] `StoredSnapshot` carries an event-sequence `applied_seq` and `last_command_id`; `EngineSnapshot` no longer carries an event counter.
- [x] Recovery with snapshot plus event tail equals folding the full log from empty; a snapshot ahead of the log is refused; duplicates are skipped; gaps and non-contiguous sequences abort.
- [x] `command_id` continues across restart even when the log tail is empty.
- [x] File snapshots keep the v2 framing and version checks; a corrupt newest snapshot falls back to the previous one.
- [x] The tests use the resting-submit event path and can run independently of 02–04.

## Comments

- Snapshot-record criterion is satisfied on the event-sourced path by a distinct `EventSnapshot { format_version, applied_seq, last_command_id, snapshot }` rather than by changing the shared `StoredSnapshot`; `EngineSnapshot.next_event_sequence` was left in place so the old command-sourced path stays green. Type unification is deferred to ticket 09. The file snapshot mechanics (v2 framing, atomic save, retention, corrupt-newest fallback) are shared by both paths in `src/durable/snapshot_files.rs`.
