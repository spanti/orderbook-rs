# 04: Event-sourced modify and rejection

**What to build:** Modify joins the event-sourced path with the existing cancel-old plus accept-new semantics, and every rejected command becomes an auditable event. After this ticket all four command variants have a complete, replayable event stream, and the event log records both accepted state changes and rejected attempts.

**Blocked by:** 03

**Status:** ready-for-agent

- [ ] A modify emits `OrderCanceled{Modified}` for the old order, then `OrderAccepted` with a new order id, then the normal matching events, then `OrderRested` or `OrderCanceled{UnfilledResidual}`.
- [ ] Every rejected command — submit, modify, cancel, and stop submit — emits exactly one `OrderRejected` carrying the symbol, the reason, and the full rejected `EngineCommand`.
- [ ] A rejected command consumes one event sequence and one `command_id` but allocates no order id.
- [ ] `OrderRejected` is journaled and published like any event; replay applies it as a no-op and state is unchanged.
- [ ] An internal invariant violation panics instead of becoming `OrderRejected`.
- [ ] Replaying a stream containing rejections, modifies, stops, fills, and cancels equals live execution.
- [ ] Every prefix is valid.
- [ ] All four command variants now have an event stream; the old command path is no longer required for them.
