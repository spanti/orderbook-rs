# 09: Contract — migrate surfaces and remove the command-sourced path

**What to build:** The event-sourced path becomes the only path. Runtime, bench, and CLI consume events; the command log, the command-sourced durable façade, and the old result types are deleted; engine and durable tests assert event streams and folded state. The repository no longer advertises or contains a command-sourced recovery path.

**Blocked by:** 07, 08

**Status:** ready-for-agent

- [ ] Runtime, bench, and CLI consume events; no `CommandOutcome` or `ExecutionReport` remains on those surfaces.
- [ ] The command log, the command-sourced durable façade, `EngineEvent`, `CommandOutcome`, `ExecutionReport`, `events()`, `clear_events()`, and `last_sequence()` are removed.
- [ ] Engine and durable tests assert event streams and folded state, not reports or command outcomes.
- [ ] `cargo test` and `cargo test --all-targets` pass.
- [ ] A repository search for the old names finds only historical ADR/spec text that intentionally mentions them.
- [ ] The event log is the only durable path; no command is persisted and no command enters the bus.
- [ ] The durable-storage doc, glossary, and ADRs match the implementation; v1 refusal, no migration, no truncation, and event-log growth are documented.
- [ ] The bench builds and both log paths run.
