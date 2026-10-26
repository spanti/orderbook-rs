# 02: Extract a shared recovery helper

**What to build:** Move the recovery logic out of `DurableMatchingEngine::open` into a helper that returns the restored engine, config, applied sequence, durability watermark, and next command sequence. Both the synchronous façade and the runtime use it, so the two write paths cannot drift on log decoding or recovery checks.

**Blocked by:** None.

**Status:** todo

- [ ] Add a crate-internal `recover(log: &dyn CommandLog, snapshots: &dyn SnapshotStore) -> Result<Recovered, DurableError>` with `Recovered { engine, config, applied_seq, durable_seq, next_command_sequence }`.
- [ ] Move the existing checks into it unchanged: missing header, unsupported version, config mismatch, snapshot ahead, recovery gap, non-contiguous sequence, duplicate skip.
- [ ] Set `durable_seq` to the log's last sequence (or `applied_seq` when the log is empty), and `next_command_sequence` to the greater of log-last + 1 and applied + 1.
- [ ] Make `DurableMatchingEngine::open` delegate to the helper and keep append-before-apply behavior byte-for-byte.
- [ ] Tests: the existing recovery tests still pass; the helper returns the expected `durable_seq` with and without a snapshot; a snapshot at the log tail yields `durable_seq == applied_seq`.
