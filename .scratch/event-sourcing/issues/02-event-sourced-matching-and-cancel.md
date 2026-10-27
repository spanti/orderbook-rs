# 02: Event-sourced matching and cancel

**What to build:** The matching half of the event-sourced path: crossing submissions, partial and full fills, IOC/market residual discard, and user cancel. A command can now be executed, journaled as events, and replayed to the same book, ledger, trade tape, and last trade price as live execution.

**Blocked by:** 01

**Status:** done

- [x] A crossing limit submit emits `OrderAccepted`, one `TradeExecuted` per fill in price-time order, and then `OrderRested` when a remainder rests or `OrderCanceled{UnfilledResidual}` when the remainder is discarded.
- [x] A fully filled taker emits no trailing rest or cancel event and ends in the filled ledger status.
- [x] A market or IOC order with insufficient liquidity emits `OrderAccepted` then `OrderCanceled{UnfilledResidual}` carrying the discarded quantity.
- [x] `TradeExecuted` applies the maker's book and ledger reduction, the taker's ledger increase, the trade tape, and the last trade price.
- [x] A user cancel of an open order emits `OrderCanceled{UserCancel}` and removes it from the book with the correct canceled quantity.
- [x] Every prefix of each event stream is a valid state; replay from empty equals live execution for the book, FIFO priority, ledger, trades, last price, and counters.
- [x] Existing command-sourced tests still pass; modify, stop, and rejection remain on the old path until their tickets land.

## Comments

Pending stop triggers remain ticket 03's scope. Until then, the event path rejects a command that would trigger a pending stop with `UnsupportedCommand` rather than emitting a stream that omits `StopTriggered`. The live-vs-command-sourced state comparison deliberately ignores `next_event_sequence`, because the old command path emits one output event per transition while the event path also emits rest/residual-cancel events.
