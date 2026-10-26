# 03: Event-sourced stops

**What to build:** Stop orders join the event-sourced path end to end. Submitting a stop registers it, triggering it activates it against the book, and cancelling it removes it; every step is journaled as an event that can be folded back without re-running the trigger decision. Trigger cascades replay in the same order.

**Blocked by:** 02

**Status:** done

- [x] Submitting a stop emits `StopPlaced` carrying the full stop order; applying it inserts the stop and the PendingTrigger ledger record.
- [x] Triggering a stop emits `StopTriggered` carrying the converted order (side, target price, market+IOC or limit+GTC, original quantity); applying it removes the stop and activates the ledger record, without a separate `OrderAccepted`.
- [x] The events after `StopTriggered` are the normal matching events (trades, rest, residual discard) and replay exactly.
- [x] A cascade of triggers emits its events in trigger order and replays to the same state.
- [x] Cancelling a pending stop emits `StopCanceled` and removes it from the stop book and ledger.
- [x] Every prefix is valid; replay equals live execution for the stop book, ledger, book, trades, last price, and counters.
- [x] Existing stop-order tests still pass.

## Comments

Merged in ticket 03. The cascade runs after `SubmitOrder` (the only command
that can change the last trade price); stop submit/cancel cannot newly make a
stop eligible because placement validation rejects already-triggerable stops,
matching the old path.
