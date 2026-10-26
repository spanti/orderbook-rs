# 09: Bench runtime path and dual-SLO reporting

**What to build:** Extend the benchmark CLI with a runtime path that uses the same workload generator, reports applied and durable latency separately, and exposes queue wait, apply service, journal append, batch size, queue depth, and QueueFull. The direct path stays unchanged.

**Blocked by:** 06, 07, 08.

**Status:** done

- [x] Add `--path direct|runtime` (default `direct`) and runtime flags: `--queue-capacity`, `--journal-batch-capacity`, `--apply-batch-max`, `--journal-coalesce-max`, `--journal-flush-us`, `--idle spin|backoff` (runtime default `spin`), `--affinity <CORE>`, `--snapshot-cadence`, `--log memory|file`, `--data-dir <PATH>`, `--in-flight <N>`.
- [x] Validate the new flags and add parser tests.
- [x] Add a runtime driver that submits the workload, measures `submit → applied` and `submit → durable`, and supports one-in-flight latency mode plus `--in-flight N` throughput mode.
- [x] Since the runtime has no read API, maintain a single-producer shadow `MatchingEngine` updated from receipts to drive book-aware workload actions outside the timed window.
- [x] Extend the report with applied/durable latency, queue wait, apply service, durable wait, journal append, batch size, queue depth, QueueFull, and fsync share; keep direct output compatible.
- [x] Print the two SLO targets with the measured p50/p99/p99.9 and a pass/fail line; do not fail the process on a miss.
- [x] Tests: a tiny runtime run completes and reports both latency families; the direct path output is unchanged; invalid runtime flags are usage errors.
