# 01: Make the storage traits thread-safe and support batch append

**What to build:** `CommandLog` and `SnapshotStore` become `Send + 'static`, and `CommandLog` gains `append_batch` so one fsync can cover a whole group. The synchronous `append` path keeps per-record durability.

**Blocked by:** None.

**Status:** done

- [x] Add `Send + 'static` supertraits to `CommandLog` and `SnapshotStore`; confirm `Box<dyn CommandLog>` / `Box<dyn SnapshotStore>` still compile at every existing call site.
- [x] Add `CommandLog::append_batch(&mut self, records: Vec<CommandRecord>) -> Result<(), DurableError>`, with a default implementation that calls `append` per record (correctness fallback, no group commit).
- [x] Override `append_batch` in `FileCommandLog`: validate sequence contiguity before writing, encode all lines, write once, flush once, fsync once, and update the cached records only after durability.
- [x] Document the contract: `Ok` means every record is durable; `Err` may leave a prefix written and the caller must fail-stop.
- [x] Keep `FileCommandLog::append` unchanged (one write + flush + fsync per record).
- [x] Tests: a contiguous batch is durable and readable after reopen; a non-contiguous batch is rejected without writing; the default implementation matches per-record `append`; existing durable-log tests pass.
