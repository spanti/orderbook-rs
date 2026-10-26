# Event-Sourced Order Log

Status: ready-for-agent

## Problem Statement

The matching engine's durable fact source is currently the command log. `EngineCommand` values are persisted and replay re-executes them. The engine's `EngineEvent` values are only result notifications: `OrderAccepted` does not carry side or price, `OrderRejected` does not even carry a symbol, and there is no event for an IOC/market residual being discarded. Events therefore cannot rebuild the book, so the command log has to stay.

Two needs make that arrangement obsolete:

1. The order events must be complete enough to apply as state deltas. Replay should fold the events that were already produced, not re-run matching.
2. A future market-data module and a post-trade risk module need to consume the event stream. Today the runtime clears events after every command and publishes nothing.

The user wants event-sourcing to replace command-sourcing: the event log becomes the only durable source of truth, commands only drive the matching engine, and durable events are published to a MessageBus.

## Solution

Redesign the event vocabulary so every event is a replayable state delta, then make the event log the durable log:

- `OrderAccepted` creates the ledger record but does not touch the book; `OrderRested` inserts the resting remainder; `TradeExecuted` applies market-maker/taker fills; `OrderCanceled` carries the cancel quantity and reason; `StopPlaced`/`StopTriggered`/`StopCanceled` make stop orders replayable; `OrderRejected` carries the rejected command as audit payload and is the only event with no state effect.
- `MatchingEngine` splits into a pure `decide` (command -> events) and an `apply` (event -> state). Replay only calls `apply`.
- `EventRecord` wraps an event with a global event sequence and a `command_id`. The event sequence is the recovery and snapshot boundary; `command_id` groups a command's events.
- The event log replaces the command log. `FORMAT_VERSION` becomes 2; v1 logs are refused; no migration tool ships.
- Snapshots become derived caches tagged with an event sequence, captured only at command boundaries.
- The runtime applies events first, journals `EventRecord`s, acknowledges durability only when a command's last event is durable, and publishes durable events through an in-process synchronous `MessageBus` whose consumers catch up from the event log.
- `ExecutionReport` and `CommandOutcome` are removed; command results are the events themselves.

## User Stories

1. As a developer, I want the durable log to store events rather than commands, so that the log is the domain's fact stream.
2. As an operator, I want recovery to fold events into state, so that a restart reproduces the exact pre-crash decision state.
3. As a developer, I want every event to carry its full order attributes, so that replay never needs the original command.
4. As a developer, I want `OrderAccepted` not to insert into the book, so that a partially applied command is still a valid state.
5. As a developer, I want `OrderRested` as a separate event, so that book insertion is an atomic delta.
6. As a developer, I want a residual-discard event, so that IOC and market leftovers are replayable.
7. As a developer, I want `TradeExecuted` to carry the full trade, so that maker/taker ledger and book changes replay exactly.
8. As a developer, I want `StopPlaced` to carry the full stop order, so that pending stops survive replay.
9. As a developer, I want `StopTriggered` to carry the converted order, so that activation is self-contained.
10. As a developer, I want `StopCanceled`, so that cancelling a pending stop is replayable.
11. As a developer, I want modify represented as cancel-old plus accept-new, so that the existing priority semantics are preserved.
12. As a developer, I want every rejected command emitted as `OrderRejected`, so that the event stream has one unified fact type.
13. As an operator, I want `OrderRejected` to be the only no-op event, so that the exception is explicit and auditable.
14. As a developer, I want a global contiguous event sequence, so that replay can detect gaps and duplicates.
15. As a developer, I want a `command_id`, so that consumers can group all events produced by one command.
16. As an operator, I want `command_id` to continue across restarts, so that grouping never collides after recovery.
17. As a developer, I want a pure `decide` step, so that event generation is testable without IO or hidden mutation.
18. As a developer, I want `apply` to be the only state write entry, so that replay and live execution share one path.
19. As a developer, I want every prefix of the event stream to be a valid engine state, so that a torn command batch is still recoverable.
20. As an operator, I want a snapshot tagged with an event sequence, so that recovery resumes at the right point.
21. As an operator, I want snapshots captured only at command boundaries, so that no half-command state is stored.
22. As an operator, I want snapshot cadence measured in event sequences, so that recovery time is bounded by the replay cost.
23. As a developer, I want snapshots to be caches, so that the event log remains the only fact source.
24. As a developer, I want order and trade ids derived from applied events, so that counters do not need their own events.
25. As an operator, I want old v1 command logs refused, so that incompatible data fails loudly instead of being misread.
26. As a developer, I want `FORMAT_VERSION` 2, so that the format break is explicit in every header, record, and snapshot.
27. As a developer, I want no v1 migration tool, so that the scope stays honest and no untested migration path is advertised.
28. As an operator, I want no event-log truncation in this release, so that full history remains available for replay and consumers.
29. As a developer, I want additive-only event schema changes, so that future consumers do not break silently.
30. As a runtime user, I want the applied receipt to carry the command's events and event sequence range, so that I can observe the decision without reading the log.
31. As a runtime user, I want the durable receipt to wait for the command's last event, so that durability is never overstated.
32. As a developer, I want the journal to write `EventRecord`s, so that the runtime path and the durable layer share one format.
33. As an operator, I want the durability watermark to be an event sequence, so that snapshots, acknowledgements, and recovery agree.
34. As a developer, I want the publisher to run off the matching thread, so that consumers cannot block matching.
35. As a market-data module, I want events published only after durability, so that I never see a phantom event.
36. As a market-data module, I want `OrderRested` and `TradeExecuted` events, so that I can maintain books and last trade price.
37. As a risk module, I want `OrderRejected` and `TradeExecuted` events, so that I can audit rejections and update post-trade exposure.
38. As a consumer, I want to catch up from the event log by sequence, so that a missed publish or a restart does not lose data.
39. As a developer, I want a synchronous `MessageBus` with `&dyn Any` handlers, so that module integration stays simple.
40. As a developer, I want handler panics isolated, so that one bad consumer cannot kill publication.
41. As a developer, I want the bus to reject commands by contract, so that it cannot become a second command ingress.
42. As a developer, I want `send_any` and `register_response_handler` declared but unsupported, so that the future API shape is visible without untested behavior.
43. As a developer, I want `ExecutionReport` and `CommandOutcome` removed, so that events are the only command result.
44. As a test author, I want in-memory `EventLog` and `SnapshotStore` adapters, so that recovery and replay tests stay fast.
45. As a test author, I want file `EventLog` tests for checksum, torn-tail, and corrupt-line cases, so that durability is proven rather than assumed.
46. As a benchmark author, I want the runtime SLOs to include event application and event-volume reporting, so that event sourcing cannot silently regress latency.
47. As a maintainer, I want the ADRs and glossary updated, so that future readers know why commands are no longer persisted.
48. As a maintainer, I want event-log growth and rejection cost documented, so that operations know the retention gap.
49. As an operator, I want the publisher to drain to the durable watermark on shutdown, so that live consumers do not miss acknowledged events.
50. As a developer, I want recovery to skip duplicates and abort on gaps, so that at-least-once log delivery remains safe.
51. As a developer, I want an internal invariant violation to panic rather than become `OrderRejected`, so that real bugs are not hidden as business rejections.
52. As a developer, I want the pure `decide` path measured against the applied SLO, so that the event-sourcing refactor does not destroy the low-latency target.

## Implementation Decisions

### Event model

- The event catalog is: `OrderAccepted`, `OrderRested`, `TradeExecuted`, `OrderCanceled`, `StopPlaced`, `StopTriggered`, `StopCanceled`, `OrderRejected`. `Order` carries id/side/price/quantity/timestamp/type/TIF; the event carries the symbol because `Order` does not. `OrderAccepted` additionally carries `post_only` and `StopTriggered` additionally carries the triggering `last_price`; both are consumer metadata that `apply` does not read.
- `OrderRested` and `StopTriggered` carry an `Order` whose `quantity` is the remaining quantity at that point.
- Per-command sequence:
  - submit: `OrderAccepted` -> `TradeExecuted`* -> `OrderRested` or `OrderCanceled{UnfilledResidual}`; a full fill has neither trailing event.
  - cancel: `OrderCanceled{UserCancel}`; pending stop: `StopCanceled`.
  - modify: `OrderCanceled{Modified}` -> `OrderAccepted` (new id) -> `TradeExecuted`* -> `OrderRested` or `OrderCanceled{UnfilledResidual}`.
  - submit stop: `StopPlaced`.
  - trigger stop: `StopTriggered` -> `TradeExecuted`* -> `OrderRested` or `OrderCanceled{UnfilledResidual}`.
  - rejection: `OrderRejected` only; no order id is allocated.
- `OrderCanceled.reason` is `UserCancel`, `UnfilledResidual`, or `Modified`.
- Every prefix of the event stream must be a valid state. This is why `OrderAccepted` has no book effect, `OrderRested` is separate, and `StopTriggered` performs the whole activation instead of relying on a following `OrderAccepted`.

### Decide and apply

- `MatchingEngine::decide` is pure: it reads state, allocates order/trade ids in local variables, and produces the full event list without mutation.
- `MatchingEngine::apply` is the only state write entry; it folds one event and records the last applied event sequence for book snapshots.
- `execute` is a convenience composition of `decide` and `apply`.
- Internal invariant violations panic; they are not converted to `OrderRejected`.
- The pure `decide` must not clone the whole engine on the hot path. The plan is a decision scratch/overlay over the touched symbol's book, stop book, and a copy-on-write ledger overlay, gated by the existing applied SLO. If that cannot meet the target, the fallback is an explicit ADR amendment, not a silent switch to apply-first on every path.

### Event log

- The log unit is `EventRecord { format_version, sequence, command_id, event }`.
- `sequence` is assigned by the unique writer and is the recovery/snapshot boundary. `DomainEvent` itself has no sequence field.
- `command_id` is assigned per command and stamped on all of its events. It is grouping and acknowledgement metadata, not a recovery boundary. It must continue across restarts: the stored snapshot records `last_command_id`, and recovery continues from the maximum of the snapshot value and the last log record's `command_id`, plus one.
- The event log replaces the command log. `FORMAT_VERSION` is 2. Old v1 command logs are refused; there is no migration tool.
- Event schemas may only grow with optional fields in this release. A breaking change requires a format-version bump and an explicit rebuild.
- There is no event-log truncation in this release.

### Recovery and snapshots

- `EngineSnapshot` is a derived cache. It no longer stores an event sequence counter; `StoredSnapshot.applied_seq` is the event sequence reflected in the snapshot.
- Snapshots are captured only at a command boundary and only when applied and durable event sequences are equal.
- Snapshot cadence is measured in event sequences and captured at the next command boundary after the threshold.
- Recovery restores the latest snapshot, checks config and version, rejects a snapshot ahead of the log, checks the first-sequence gap, reads the tail, skips duplicates, aborts on non-contiguous sequences, and calls `apply` for every record.
- `OrderRejected` is a no-op during recovery. Exact Recovery means the state equals folding the same event log from an empty engine.

### Runtime

- The matching thread decides, assigns a contiguous event sequence range and `command_id`, applies every event, sends an applied acknowledgement carrying the events and the first/last event sequence, then moves the records to the journal.
- The pending-durable entry stores the command's last event sequence. A durable acknowledgement is sent only when the watermark reaches it.
- The journal batches `EventRecord`s and advances the durability watermark after `append_batch` returns.
- The snapshot gate uses event sequences and captures only at command boundaries.
- The publisher is a separate thread; it reads `EventLog::read_from(last_published + 1)` up to the durable watermark and publishes in sequence order. It starts at the recovered durable watermark and does not replay history.

### MessageBus

- The bus exposes `register_any`, `register_response_handler`, `send_any`, and `try_publish_any`; messages are `&dyn Any`.
- This release implements `register_any` and `try_publish_any`. `send_any` and `register_response_handler` return an `Unsupported` error and have no command path.
- `register_any` allows multiple handlers per endpoint name. `try_publish_any` synchronously calls every handler under the topic on the caller's thread.
- A handler panic is caught and the endpoint is marked failed; other handlers continue.
- The concrete event type is `EventRecord`. Commands are not allowed on the bus; `&dyn Any` cannot enforce it, so this is a contract and review constraint.
- The bus does not persist, queue per subscriber, or detect lag. Consumers save their last seen sequence and catch up from the event log.

### Public API

- `ExecutionReport`, `CommandOutcome`, `events()`, `clear_events()`, and `last_sequence()` are removed.
- `execute` and `decide` return the events; the runtime receipt carries the event list and first/last event sequence.
- `CommandLog`, `CommandRecord`, and `CommandCursor` are renamed to `EventLog`, `EventRecord`, and `EventCursor`; history read returns `EventRecord`s.
- The synchronous durable façade remains append-before-apply: decide, append every event, then apply. The runtime remains apply-first.

## Testing Decisions

- Test external behavior, not implementation details. Assert event streams, recovered state, acknowledgement levels, and bus delivery, not private fields or helper calls.
- Seams, highest first:
  1. `MatchingEngine::decide` / `apply` — the pure engine seam. This is the primary seam: command in, event list out; event list in, state out.
  2. `DurableMatchingEngine` with in-memory and file `EventLog` + `SnapshotStore` — proves recovery, format, and append-before-apply.
  3. `EngineRuntime` with an in-memory `MessageBus` — proves applied/durable acknowledgements, journal ordering, snapshot gating, publisher behavior, and consumer catch-up.
- Prefer these existing seams. `MessageBus` is the only new seam, and it should stay narrow: a test subscriber that records delivered records and can restart from a sequence.
- Modules covered: matching engine (event catalog, per-command sequences, prefix validity, counters, stop cascades), durable layer (log framing, recovery, snapshots, version/gap/duplicate checks, command id continuity), runtime (acknowledgement ranges, crash window, concurrency, shutdown, publisher), bus (order, panics, unsupported methods), benchmark (SLOs and event volume).
- Prior art: the existing durable-log, durable-snapshot, runtime-integration, engine-snapshot, stop-order, and file-log tests. Reuse their scenarios and migrate their assertions from command/outcome shapes to event shapes.
- Required test themes:
  - exact event sequence for each command variant, including modify, stop trigger, residual discard, and rejection;
  - prefix-validity tests over every prefix of representative event streams;
  - live execution versus event-fold equality for state, counters, trades, last prices, and FIFO priority;
  - crash-window tests for applied-but-not-durable and durable-but-not-snapshotted;
  - duplicate and gap recovery;
  - v1 header/snapshot refusal;
  - bus sequence order, handler panic isolation, and catch-up after missed publication;
  - benchmark gate that includes applying all events of a command and reports event volume.

## Out of Scope

- Implementing a market-data module or a risk module.
- Pre-trade risk inside the engine; risk is a post-trade consumer.
- An external broker adapter (Kafka/NATS) or a network-transparent bus.
- Asynchronous bus dispatch, per-subscriber queues, or lag detection.
- Implementing `send_any` or `register_response_handler`.
- Event-log truncation, retention, or compaction.
- Migrating v1 command logs.
- Per-event schema versioning.
- Bus filtering or routing by symbol/event kind.
- Client request idempotency.
- Any claim of HFT-grade latency.

## Further Notes

- `OrderRejected` is the only event that does not change state. It carries the full rejected command as audit payload; replay treats it as a no-op. Rejected commands consume an event sequence but no order id.
- The event log is larger than the command log: a resting order is at least `OrderAccepted` + `OrderRested`, and a sweeping command emits many `TradeExecuted` records. There is no truncation in this release.
- The bus is synchronous. A slow handler delays publication for every subscriber, but not the matching thread. Consumers that fall behind must read the event log.
- `&dyn Any` loses type safety. The concrete event type is `EventRecord`; a failed downcast is a consumer bug and is not detected by the bus.
- The pure `decide` performance is the main implementation risk. The overlay/scratch approach must be measured against the applied SLO; the fallback requires an ADR amendment.
- Architectural context lives in ADR 0004 (event-sourced order log) and ADR 0005 (in-process MessageBus); the storage contract is in `docs/durable-storage.md`.
