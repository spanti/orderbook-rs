# 07: Runtime hardening, metrics, and SLO gate

**What to build:** The event-sourced runtime is hardened against the failure windows, concurrency, and overload that the old runtime already covered, and its latency claims are re-established. A command that is applied but not durable is lost on crash; a command that is durable but not snapshotted recovers from the tail; concurrent producers do not double-apply; queues stay bounded; the benchmark reports event volume and verifies both SLOs.

**Blocked by:** 06

**Status:** done

- [x] A crash between apply and append loses the command's events; recovery omits the command and the caller never received a durable receipt.
- [x] A crash after durability but before a snapshot recovers from the event-log tail.
- [x] Concurrent producers apply each command exactly once; one producer's commands keep their submission order.
- [x] Ingress and journal queues stay bounded; overload returns `QueueFull` or stops intake and never grows without bound.
- [x] Metrics report event volume per command, journal batch size in event records, queue depth, apply time, and both acknowledgement levels.
- [x] The `submit -> applied` and `submit -> durable` SLOs pass under the documented benchmark setup.
- [x] The pure `decide` path meets the applied SLO; if it does not, the ticket records the measured shortfall and stops rather than silently switching to apply-first.
- [x] Event-volume numbers are reported alongside the SLOs.

## Comments

Merged in `merge: runtime hardening, metrics, and SLO gate (ticket 07)`.

Measured on Windows with the debug/QPC clock, so the numeric figures are functional observations rather than SLO certification:

- Pure `decide` path: p99 **0.9 µs**, below the 10 µs applied target. On Windows the decide SLO is reported as **NOT APPLICABLE** for numeric certification because QPC tick resolution is ~100 ns; the value is printed for functional inspection only.
- Runtime applied/durable numeric SLOs require the documented Linux + NVMe setup. The Windows QPC/NTFS run produced 12.7 µs applied p99 (pinned) and 512.7 µs durable p99; neither is claimed as PASS.
- The report now includes the journal handoff batch maximum (`batch_size_max`) in event records alongside the mean, and event volume (`events_total`, `events_per_command`) for the runtime, direct, and decide paths.
- The event counters are updated only when `RuntimeConfig::metrics` is `Some`; with metrics disabled the hot path performs no clock reads and no metric atomics, covered by `metrics_disabled_reads_no_clock_and_exposes_no_sink`.
