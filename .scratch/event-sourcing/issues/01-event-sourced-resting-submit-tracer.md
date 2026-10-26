# 01: Event-sourced resting submit tracer with decision scratch

**What to build:** A non-crossing limit submit that travels the complete new path — pure `decide`, `EventRecord` journaling, append-before-apply, and recovery — for one command. The engine gains a decision scratch/overlay so `decide` does not clone the whole book. The new event-sourced durable façade appends the events before applying, and reopening from the in-memory event log alone yields the same book, FIFO priority, ledger, last applied event sequence, and counters. The existing command-sourced path stays green.

**Blocked by:** None (can start immediately)

**Status:** done

- [x] `decide` for a non-crossing limit submit returns `OrderAccepted` followed by `OrderRested`; no other event kind is emitted.
- [x] `OrderAccepted` creates the ledger record but does not insert into the book; `OrderRested` inserts the order at its price level with FIFO priority.
- [x] `apply` is the only state write entry and folds both events; applying either prefix leaves a valid engine state.
- [x] Each `EventRecord` carries format version 2, a contiguous event sequence, and the same `command_id` for both events.
- [x] The event-sourced durable façade appends both records before applying; a failed append applies nothing and fails the instance closed.
- [x] Reopening from the in-memory event log alone reproduces the book, FIFO priority, ledger, trades, last price, counters, and last applied event sequence.
- [x] `BookSnapshot.sequence` reports the last applied event sequence.
- [x] Unsupported commands are not accepted by the event-sourced path with a clear failure; no command is silently dropped.
- [x] The decision scratch/overlay is exercised by a benchmark baseline; a whole-engine clone is not on the path.
- [x] All existing command-sourced tests still pass.
