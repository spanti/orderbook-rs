# 03: Runtime vocabulary, public types, and engine hooks

**What to build:** The `src/runtime/` module skeleton and its public vocabulary, plus the two small `MatchingEngine` hooks the runtime needs. No threads are started yet.

**Blocked by:** None.

**Status:** done

- [x] Create `src/runtime/{mod,envelope,ticket,metrics,error,idle}.rs` and export it from `lib.rs`.
- [x] Define `RuntimeConfig` with the defaults from the spec: `queue_capacity = 1024`, `journal_batch_capacity = 64`, `apply_batch_max = 64`, `journal_coalesce_max = 1024`, `journal_flush_interval = 100µs`, `snapshot_cadence = 0`, `idle = Backoff`, `matching_core = None`, `shutdown_timeout = 5s`, `metrics = None`.
- [x] Define `CommandEnvelope` (request id, optional enqueue time, command, slot handle), `CommandReceipt { outcome, command_sequence }`, `SubmitError`, `RuntimeError`.
- [x] Define `RuntimeMetrics` as atomics with cache-line grouping; all updates are no-ops when the metrics handle is absent.
- [x] Define `IdleStrategy::{Spin, Backoff}` with a resettable backoff counter.
- [x] Add `MatchingEngine::execute_ref(&mut self, command: &EngineCommand) -> CommandOutcome` that applies without cloning, and `MatchingEngine::clear_events(&mut self)`.
- [x] Unit tests: default config, error display, `CommandReceipt` equality/clone, backoff reset, `execute_ref` produces the same outcome and state as `execute`, `clear_events` does not change state or the event sequence counter.
