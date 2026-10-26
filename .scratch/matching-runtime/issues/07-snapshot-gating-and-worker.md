# 07: Snapshot gating and Snapshot Worker

**What to build:** Capture engine snapshots only at or below the durability watermark, write them on a separate Snapshot Worker, and write a final snapshot on explicit shutdown. Snapshot failures degrade observability but never fail durable commands.

**Blocked by:** 02, 06.

**Status:** done

- [x] Add the snapshot gate to the matching thread: cadence boundary, `applied_seq == durable_seq`, `snapshot_due`, and `last_snapshot_seq`. When a snapshot is due and durability lags, stop draining new commands until the watermark catches up, then capture and resume; otherwise continuous load starves snapshots.
- [x] Capture `engine.capture_snapshot()` on the matching thread and `try_send` a `StoredSnapshot` to the worker; a full channel keeps `snapshot_due` and drops the job (best effort).
- [x] Implement the Snapshot Worker: own the `SnapshotStore`, save jobs, store the last error, and keep running after failures.
- [ ] On explicit shutdown: wait for `durable_seq == applied_seq` (bounded by `shutdown_timeout`), capture and save the final snapshot regardless of cadence, and return `SnapshotFailed` on error.
  - Not ticked: ticket 07 provides `SnapshotSink::capture_final` (refuses `applied_seq != durable_seq`, captures/saves regardless of cadence, waits for the save, and returns the save error) plus the `RuntimeError::SnapshotFailed` variant. The bounded `shutdown_timeout` wait and the public `shutdown` that maps the error belong to ticket 08 (spec "shutdown(self) ... waits for durability, sends the final snapshot").
- [x] Tests: a snapshot is never saved with `applied_seq > durable_seq`; a snapshot due while durability lags is deferred and captured after the watermark catches up; a slow store does not stall matching; a failing store leaves durable commands successful; the final snapshot is present after shutdown.
