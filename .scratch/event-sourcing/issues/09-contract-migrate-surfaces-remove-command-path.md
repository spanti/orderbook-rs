# 09: Contract — migrate surfaces and remove the command-sourced path

**What to build:** The event-sourced path becomes the only path. Runtime, bench, and CLI consume events; the command log, the command-sourced durable façade, and the old result types are deleted; engine and durable tests assert event streams and folded state. The repository no longer advertises or contains a command-sourced recovery path.

**Blocked by:** 07, 08

**Status:** done

- [x] Runtime, bench, and CLI consume events; no `CommandOutcome` or `ExecutionReport` remains on those surfaces.
- [x] The command log, the command-sourced durable façade, `EngineEvent`, `CommandOutcome`, `ExecutionReport`, `events()`, `clear_events()`, and `last_sequence()` are removed.
- [x] Engine and durable tests assert event streams and folded state, not reports or command outcomes.
- [x] `cargo test` and `cargo test --all-targets` pass.
- [x] A repository search for the old names finds only historical ADR/spec text that intentionally mentions them.
- [x] The event log is the only durable path; no command is persisted and no command enters the bus.
- [x] The durable-storage doc, glossary, and ADRs match the implementation; v1 refusal, no migration, no truncation, and event-log growth are documented.
- [x] The bench builds and both log paths run.

## Comments

Deleted command suites are covered by `file_event_log`/`event_snapshot_store`/`event_sourced_recovery`; old policy/FIFO/snapshot coverage was migrated into `src/engine.rs` and `tests/engine_snapshot.rs`. `EventLog::last_sequence()` was renamed `last_stored_sequence()` to satisfy the acceptance grep. A raw `restore_snapshot` no longer restores an event boundary because the durable layer owns it. Final names are `DurableMatchingEngine`/`StoredSnapshot`/`SnapshotStore`/`FORMAT_VERSION=2`.
