# 06: Journal Thread, group commit, and durable acknowledgements

**What to build:** The Journal Thread that owns the `CommandLog`, coalesces batches, performs one `append_batch`/fsync per group, advances the durability watermark, wakes the matching thread, and publishes durable acknowledgements. Wire it into runtime startup and shutdown.

**Blocked by:** 01, 03, 05.

**Status:** done

- [x] Add the bounded journal channel of `Vec<CommandRecord>` and the shared `durable_seq: Arc<AtomicU64>`.
- [x] Implement the Journal Thread: blocking receive, then drain up to `journal_coalesce_max` records, call `append_batch`, store the watermark with `Release`, `unpark` the matching thread, and record append metrics.
- [x] On append failure: store the error, set the failed flag, unpark the matching thread, and exit.
- [x] Complete pending durable slots in the matching thread when `durable_seq >= command_sequence`; never complete them earlier.
- [x] On shutdown, drain the journal input, flush the final group, and exit when the matching thread drops its sender.
- [x] Tests with a counting/slow `CommandLog`: N commands produce one `append_batch` per coalesced group; durable waiters unblock only after the watermark advances; a failed append produces `EngineFailed` for pending and queued commands; a journal panic fails the runtime closed.
