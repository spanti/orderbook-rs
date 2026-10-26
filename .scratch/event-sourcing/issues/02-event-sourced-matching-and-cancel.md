# 02: Event-sourced matching and cancel

**What to build:** The matching half of the event-sourced path: crossing submissions, partial and full fills, IOC/market residual discard, and user cancel. A command can now be executed, journaled as events, and replayed to the same book, ledger, trade tape, and last trade price as live execution.

**Blocked by:** 01

**Status:** ready-for-agent

- [ ] A crossing limit submit emits `OrderAccepted`, one `TradeExecuted` per fill in price-time order, and then `OrderRested` when a remainder rests or `OrderCanceled{UnfilledResidual}` when the remainder is discarded.
- [ ] A fully filled taker emits no trailing rest or cancel event and ends in the filled ledger status.
- [ ] A market or IOC order with insufficient liquidity emits `OrderAccepted` then `OrderCanceled{UnfilledResidual}` carrying the discarded quantity.
- [ ] `TradeExecuted` applies the maker's book and ledger reduction, the taker's ledger increase, the trade tape, and the last trade price.
- [ ] A user cancel of an open order emits `OrderCanceled{UserCancel}` and removes it from the book with the correct canceled quantity.
- [ ] Every prefix of each event stream is a valid state; replay from empty equals live execution for the book, FIFO priority, ledger, trades, last price, and counters.
- [ ] Existing command-sourced tests still pass; modify, stop, and rejection remain on the old path until their tickets land.
