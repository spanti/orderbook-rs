# 07: Runtime hardening, metrics, and SLO gate

**What to build:** The event-sourced runtime is hardened against the failure windows, concurrency, and overload that the old runtime already covered, and its latency claims are re-established. A command that is applied but not durable is lost on crash; a command that is durable but not snapshotted recovers from the tail; concurrent producers do not double-apply; queues stay bounded; the benchmark reports event volume and verifies both SLOs.

**Blocked by:** 06

**Status:** ready-for-agent

- [ ] A crash between apply and append loses the command's events; recovery omits the command and the caller never received a durable receipt.
- [ ] A crash after durability but before a snapshot recovers from the event-log tail.
- [ ] Concurrent producers apply each command exactly once; one producer's commands keep their submission order.
- [ ] Ingress and journal queues stay bounded; overload returns `QueueFull` or stops intake and never grows without bound.
- [ ] Metrics report event volume per command, journal batch size in event records, queue depth, apply time, and both acknowledgement levels.
- [ ] The `submit -> applied` and `submit -> durable` SLOs pass under the documented benchmark setup.
- [ ] The pure `decide` path meets the applied SLO; if it does not, the ticket records the measured shortfall and stops rather than silently switching to apply-first.
- [ ] Event-volume numbers are reported alongside the SLOs.
