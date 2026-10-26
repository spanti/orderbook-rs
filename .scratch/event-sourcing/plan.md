# Event-Sourced Order Log: Implementation Plan

Status: ready-for-agent
Date: 2026-10-26
ADRs: [0004](../../docs/adr/0004-event-sourced-order-log.md), [0005](../../docs/adr/0005-in-process-message-bus.md)

## Goal

Replace command-sourcing with event-sourcing:

- the durable log stores `DomainEvent` records, not `EngineCommand` values;
- replay folds events into state and never re-runs matching;
- commands are ingress only and are not persisted;
- events are published to an in-process `MessageBus` after they are durable;
- every event carries enough information to be applied as a state delta.

## Non-goals

- Implementing a market-data module or a risk module.
- Pre-trade risk inside the engine (risk stays a post-trade consumer).
- An external broker adapter (Kafka/NATS); the bus is in-process.
- Async bus dispatch or per-subscriber queues (`&dyn Any` rules it out).
- Implementing `send_any` or `register_response_handler`.
- Event-log truncation/retention.
- Migrating v1 command logs.
- Per-event schema versioning.
- Client request idempotency.

## Invariants

1. **Single fact source.** The event log is the only durable source of engine state; snapshots are caches.
2. **Replay applies, never decides.** Recovery calls `apply`, never `decide` or the matching logic.
3. **Every prefix is valid.** Applying any prefix of the event stream yields an invariant-preserving engine state. This is why `OrderAccepted` does not touch the book, `OrderRested` exists, and `StopTriggered` carries the converted order.
4. **Rejections are events.** `OrderRejected` is logged and published; it is the only event with no state effect and replay treats it as a no-op.
5. **Contiguous event sequence.** The log has one global, gap-free event sequence; duplicates at or below the applied sequence are skipped, gaps abort recovery.
6. **Command grouping.** `command_id` is assigned per command and stamped on all of its events; it is grouping/ack metadata, not a recovery boundary.
7. **Durable means every event.** A durable acknowledgement is sent only after the command's last event sequence is in the log.
8. **Snapshots at command boundaries.** A snapshot is captured only after all events of a command are applied and durable; it is tagged with an event sequence.
9. **Publication follows durability.** The bus receives only durable events, in sequence order.
10. **Commands never enter the log or the bus.** `OrderRejected` carries the rejected command as audit payload; that is the only place a command shape appears.
11. **The matching thread never waits on consumers.** The publisher is a separate thread; a slow handler delays publication only.

## Event catalog

`Order` already carries `id`, `side`, `price`, `quantity`, `timestamp`, `order_type`, `time_in_force`; `symbol` is carried by the event.

| Event | Payload | Apply effect |
| --- | --- | --- |
| `OrderAccepted` | `symbol`, `order: Order` (original quantity), `post_only` | create a ledger record in `New`; no book effect |
| `OrderRested` | `symbol`, `order: Order` (`quantity` = remaining) | insert the order into the book |
| `TradeExecuted` | `trade: Trade` | maker book/ledger down, taker ledger up, append the trade, update last trade price |
| `OrderCanceled` | `symbol`, `order_id`, `canceled_quantity`, `reason` | remove from the book if present; mark the ledger canceled |
| `StopPlaced` | `symbol`, `stop: StopOrder` | insert the stop and a `PendingTrigger` ledger record |
| `StopTriggered` | `symbol`, `order: Order` (converted), `last_price` | remove the stop; activate the ledger record |
| `StopCanceled` | `symbol`, `order_id` | remove the stop; mark the ledger canceled |
| `OrderRejected` | `symbol`, `reason: EngineError`, `command: EngineCommand` | no-op (audit only) |

`OrderCanceled.reason`: `UserCancel`, `UnfilledResidual`, `Modified`.

## Per-command event sequences

- **Submit** (fresh order):
  `OrderAccepted` -> `TradeExecuted`* -> if remaining > 0 and the order rests, `OrderRested`; if remaining > 0 and it does not rest, `OrderCanceled{UnfilledResidual}`; a full fill has neither trailing event.
- **Cancel**:
  open book order -> `OrderCanceled{UserCancel}`;
  pending stop -> `StopCanceled`.
- **Modify** (keeps the current cancel-old + new-id semantics):
  `OrderCanceled{Modified}` -> `OrderAccepted` (new id) -> `TradeExecuted`* -> `OrderRested` or `OrderCanceled{UnfilledResidual}`.
- **Submit stop**:
  `StopPlaced`.
- **Trigger stop** (after the command's own events, possibly cascading):
  `StopTriggered` -> `TradeExecuted`* -> `OrderRested` or `OrderCanceled{UnfilledResidual}`.
- **Rejection**:
  `OrderRejected` only; no order id is allocated and no state changes.

## Architecture

### Decide and apply

- `MatchingEngine::decide(&self, command) -> Vec<DomainEvent>` is pure: it reads state, allocates ids/trade ids in local variables, and produces the full event list without mutating the engine.
- `MatchingEngine::apply(&mut self, event, sequence)` is the only state write entry. It folds one event and records `last_applied_sequence` for `BookSnapshot.sequence`.
- `MatchingEngine::execute` is a convenience composition: decide, then apply every event.
- Internal invariant violations panic; they are not converted to `OrderRejected`.

**The hardest part of Slice 1 is a pure `decide` that does not clone the whole engine on the hot path.** The matching logic currently mutates `OrderBook`, `OrderLedger`, `StopBook`, `last_trades` and counters while it runs. Proposed approach:

1. A `DecisionScratch` (or overlay) clones only the touched symbol's `OrderBook` and `StopBook`; the ledger uses a copy-on-write overlay so only touched records are copied.
2. The existing matching logic runs against the scratch and emits events; the scratch is discarded after `decide`.
3. `apply` then folds the events into the real state.
4. A benchmark gate: `submit -> applied` p99 must stay within the existing 10 µs target with the standard runtime benchmark. If the overlay cannot meet it, the fallback is to let the runtime apply directly and emit events, and restrict the pure `decide` to the synchronous façade and tests; that fallback must be recorded as an ADR amendment rather than done silently.

`OrderBook`, `BookSide`, `PriceLevel` already derive `Clone`; `OrderLedger` and `StopBook` need `Clone` or a COW wrapper.

### Event record and log

- `EventRecord { format_version, sequence, command_id, event }`.
- `DomainEvent` has no sequence field. The unique writer assigns `sequence` and `command_id`.
- `command_id` must survive restarts: `StoredSnapshot` gains `last_command_id`; `EventCursor` gains `next_command_id`; recovery takes `max(snapshot.last_command_id, last log record.command_id) + 1`.
- `EventLog` trait mirrors the old `CommandLog`: header, append, append_batch, read_from, first_sequence, last_sequence; add a cached `last_command_id` or derive it from the last record.
- `EventCursor { applied_seq, durable_seq, next_sequence, next_command_id }`, all event sequences except `next_command_id`.
- `FORMAT_VERSION = 2`. v1 logs are refused; no migration tool; no truncation.

### Runtime

- Matching thread: `decide` -> assign a contiguous event sequence range and `command_id` -> `apply` each event -> applied acknowledgement with events and first/last event sequence -> move `EventRecord`s to the journal buffer.
- `PendingDurable` stores the command's **last event sequence**; durable acknowledgement is sent when the watermark reaches it.
- `CommandReceipt { events: Vec<DomainEvent>, first_event_sequence, last_event_sequence }`.
- Snapshot gate: cadence in event sequences; capture only at a command boundary when `applied_seq == durable_seq`.
- Journal: batches `EventRecord`s; after fsync, advances the watermark and signals the publisher.
- Publisher: reads `EventLog::read_from(last_published + 1)` up to `durable_seq` and calls `try_publish_any("events", record)`; starts at the recovered durable watermark.

### MessageBus

- Four trait methods; `register_any` and `try_publish_any` are implemented this slice, `send_any` and `register_response_handler` return `BusError::Unsupported`.
- `register_any(endpoint, Box<dyn Fn(&dyn Any) + Send + 'static>)`; multiple handlers per name.
- `try_publish_any(topic, &dyn Any)` synchronously calls every handler registered under the topic; handler panics are caught and the endpoint is marked failed.
- The concrete event type is `EventRecord`; commands are not allowed on the bus.
- No per-subscriber queue, no lag detection; consumers catch up via `EventLog::read_from`.

## Slice 1: Domain events and the decision engine

**Files**

- `src/event.rs` (new): `DomainEvent`, `OrderCancelReason`.
- `src/types.rs`: remove `EngineEvent`, `ExecutionReport`, `CommandOutcome`, `CancelReport`; keep `Fill` internal to the book; keep `Trade`, `Order`, `OrderRecord`, `EngineCommand`, `EngineError`, `EngineConfig`, `StopOrder`, `StopKind`, `OrderRequest`, `ModifyRequest`, `OrderStatus`.
- `src/engine.rs`: replace the event buffer/sequence counter with `decide`/`apply`/`execute`; emit the new event sequences; make `StopTriggered` carry the converted `Order`; emit `OrderRested`, `OrderCanceled{UnfilledResidual}`, `StopPlaced`, `StopCanceled`, `OrderRejected`; derive `next_order_id`/`next_trade_id` from applied ids; remove `events()`, `clear_events()`, `last_sequence()`, `emit_*`, `allocate_sequence_id`.
- `src/ledger.rs`: rename `apply_maker_fill` to `apply_fill`; add a `resume_stop_as_active` transition; remove `to_report`; keep `export_records`/`from_records`.
- `src/stop_book.rs`: add `Clone` or a scratch wrapper; no behavioral change.
- `src/orderbook.rs`: expose what the scratch needs to clone/run; add `Clone` where missing.
- `src/snapshot.rs`: remove `next_event_sequence`; keep `next_order_id`/`next_trade_id` as cached state; document that restore sets the counters from the snapshot.
- `src/engine.rs` tests, `src/types.rs` serialization tests, `tests/engine_snapshot.rs`, `tests/stop_orders.rs`.

**Acceptance**

- Every command produces the exact event sequence above.
- Applying any prefix leaves a valid state (book/ledger/stop invariants hold).
- Folding all events from an empty engine equals live execution state, including counters, trades, last price, stop books, and FIFO priority.
- Modify produces `Canceled{Modified}` + `Accepted(new id)`; stop triggers are self-contained.
- Rejected commands produce exactly one `OrderRejected` and allocate no order id.
- `ExecutionReport`/`CommandOutcome` no longer exist in the public API.

## Slice 2: Event log, recovery, and snapshots

**Files**

- `src/durable/mod.rs`: `EventRecord`, `EventLog`, `EventCursor`, `Recovered`; `FORMAT_VERSION = 2`; recovery folds events with `apply`; `DurableMatchingEngine::execute` decides, appends, then applies; `history_from` returns `Vec<EventRecord>`.
- `src/durable/file_log.rs` -> `src/durable/file_event_log.rs` (`FileEventLog`): same framing/CRC/fsync contract; header and records carry v2.
- `src/durable/memory.rs`: `InMemoryEventLog`.
- `src/durable/framing.rs`: unchanged.
- `src/durable/file_snapshot.rs`: `StoredSnapshot { format_version, applied_seq, last_command_id, snapshot }`; `applied_seq` is an event sequence.
- `src/snapshot.rs`: `EngineSnapshot` without `next_event_sequence`.
- `tests/file_command_log.rs` -> `tests/file_event_log.rs`; `tests/durable_log.rs`, `tests/durable_snapshot.rs`, `tests/file_snapshot_store.rs`, `tests/engine_snapshot.rs`.

**Acceptance**

- A file event log survives restart, detects corrupt terminated lines, truncates only an unterminated final fragment, and rejects v1 headers/records.
- Recovery restores snapshot + tail, rejects a snapshot ahead of the log, skips duplicates, aborts on gaps/non-contiguous sequences, and ignores `OrderRejected` for state.
- The recovered state equals folding the same events from empty.
- The synchronous façade preserves append-before-apply: a failed append leaves state unchanged and fails the instance.
- `last_command_id`/`next_command_id` continue across snapshot + tail.

## Slice 3: Runtime pipeline and MessageBus

**Files**

- `src/runtime/journal.rs`: `JournalSink` batches `Vec<EventRecord>`; watermark is an event sequence; signal the publisher after fsync.
- `src/runtime/matching.rs`: decide/apply; assign event range + `command_id`; applied receipt with events; pending durable by last event sequence; remove `clear_events`.
- `src/runtime/ticket.rs`: `CommandReceipt { events, first_event_sequence, last_event_sequence }`; durable result.
- `src/runtime/snapshot_gate.rs`: cadence in event sequences; command-boundary capture.
- `src/runtime/shared.rs`: applied/durable event watermarks; publisher wake-up.
- `src/runtime/lifecycle.rs`: start the publisher; shutdown drains to `durable_seq`; error handling.
- `src/runtime/mod.rs`: update the channel/ordering/failure docs.
- `src/bus/mod.rs` (new) + `src/runtime/publisher.rs` (new): `MessageBus` trait, `BusError`, in-process registry, synchronous dispatch, panic isolation, log tailer.
- `src/lib.rs`: export the bus.
- `src/bench/run.rs`, `src/bench/runtime_path.rs`, `src/bench/workload.rs`: derive accepted/rejected from events; receipts carry events.
- `tests/runtime_integration.rs`, `tests/runtime_lifecycle.rs`, `tests/bench_cli.rs`; new bus tests.

**Acceptance**

- A command's applied receipt arrives after all its events are applied and carries the event list plus first/last sequence.
- A durable receipt arrives only after the last event sequence is at or below the watermark.
- Crash between apply and append loses the command's events; recovery omits them.
- Snapshot capture never lands mid-command and never exceeds the watermark.
- The publisher emits durable events in sequence order; a panicking handler is isolated; a slow handler delays publication but not matching.
- A test subscriber can subscribe, miss events, and catch up from `EventLog::read_from`.
- `send_any`/`register_response_handler` return `Unsupported`; no command path exists on the bus.
- The existing SLOs still pass; event volume per command is reported.

## Slice 4: Cleanup, docs, and migration of the remaining surface

**Files**

- Remove all remaining `CommandLog`/`CommandRecord`/`CommandCursor`/`FileCommandLog`/`InMemoryCommandLog` names and doc strings.
- `tests/bench_cli.rs`, `src/bin/bench.rs`, `src/bench/*` updated.
- `docs/adr/0001` status; `0002`, `0003` updated; `0004`, `0005` added.
- `docs/durable-storage.md` rewritten.
- `GLOSSARY.md` final pass.
- `.scratch/event-replay/spec.md` gets a `superseded by ADR 0004` note; historical content stays.

**Acceptance**

- `cargo test` and `cargo test --all-targets` pass.
- `rg` for `CommandLog|CommandRecord|CommandSequence|EngineEvent|CommandOutcome|ExecutionReport` finds only historical docs/ADRs that intentionally mention them.
- The bench builds and the runtime path still reports both SLOs.

## Test migration matrix

| Existing test area | New focus |
| --- | --- |
| `engine` unit tests | exact per-command event sequences; prefix validity; decide purity; apply fold |
| `engine_snapshot` | snapshot round-trip without event counter; restore + event fold equality |
| `durable_log` | event append/apply; rejected no-op; duplicate skip; gap abort; append-before-apply |
| `durable_snapshot` | snapshot + event tail; applied-not-snapshotted; event sequence continuity; `command_id` continuity |
| `file_command_log` -> `file_event_log` | CRC, torn tail, corrupt terminated line, v1 refusal |
| `file_snapshot_store` | v2 snapshot framing and version checks |
| `runtime_integration` | applied/durable by event range; crash window; exactly-once application; snapshot gating |
| `runtime_lifecycle` | publisher start/drain; shutdown timeout; worker failures |
| `stop_orders` | `StopPlaced`/`StopTriggered`/`StopCanceled` sequences and replay |
| `bench_cli` | events-derived outcomes; SLO report; queue-full behavior |

## Acceptance criteria (whole change)

- Replaying a recorded event log from empty reproduces the live engine's state, including FIFO priority, ledger, stops, trades, last prices, and counters.
- Every event in the log is applicable; `OrderRejected` is the only no-op.
- No command is persisted or replayed; no command enters the bus.
- Old v1 logs fail loudly; no migration path is advertised.
- The event log is the only fact source; deleting snapshots still recovers from the log.
- The bus only publishes durable events; matching never blocks on a consumer.
- Documentation, glossary, and ADRs match the implementation.

## Risks

- **Pure `decide` performance.** The overlay may be the hardest part; gate it with the 10 µs applied benchmark. Fallback requires an explicit ADR amendment.
- **Event volume.** A resting order is at least two events; sweeping commands emit many trades. Journal batch sizing and metrics must be in event counts.
- **Rejected traffic.** Rejections consume log space and event sequences; this is the agreed cost of a unified event stream.
- **`&dyn Any` type safety.** Downcast failures are silent consumer bugs; document the concrete `EventRecord` contract.
- **Slow handler.** Synchronous dispatch means one slow subscriber delays all publication; matching is unaffected.
- **Test churn.** The removal of `ExecutionReport`/`CommandOutcome` touches every durable/runtime/bench test; migrate in Slice 1/2, not at the end.
- **Partial command batches.** A durable prefix of a command is a valid state but an incomplete group; consumers must tolerate it and use `command_id`.
- **`command_id` continuity.** Must be persisted in the snapshot; otherwise grouping breaks after restart.
