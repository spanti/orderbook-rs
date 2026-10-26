//! Snapshot gating: when the matching thread may capture a snapshot.
//!
//! The gate owns only the cadence policy. Capture happens on the matching
//! thread; the `SnapshotStore::save` call happens on the snapshot worker
//! through [`SnapshotSink`].

use crate::durable::{FORMAT_VERSION, StoredSnapshot};
use crate::engine::MatchingEngine;

use super::snapshot::{SnapshotSend, SnapshotSink};

/// Whether the matching thread may keep draining the ingress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GateDecision {
    Continue,
    Pause,
}

/// Snapshot-gate seam. A snapshot is only captured when
/// `applied_seq == durable_seq`, pausing intake at a cadence boundary while
/// durability catches up.
pub(crate) trait SnapshotGate: Send + 'static {
    fn poll(&mut self, engine: &MatchingEngine, applied_seq: u64, durable_seq: u64)
    -> GateDecision;
}

/// Automatic snapshots at a fixed command-sequence cadence.
///
/// The gate captures only when `applied_seq == durable_seq`. At a cadence
/// boundary with durability lagging, it pauses intake until the watermark
/// catches up; a full worker channel instead keeps the snapshot due without
/// pausing, because snapshots are best effort and must never stall matching.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnapshotPending {
    /// No snapshot is waiting.
    None,
    /// A boundary was reached while durability lagged; intake is paused.
    Watermark,
    /// A capture is pending because the worker channel was full; keep
    /// matching and retry at the next cadence boundary.
    Due,
}

pub(crate) struct CadenceSnapshotGate {
    sink: SnapshotSink,
    cadence: u64,
    pending: SnapshotPending,
    last_snapshot_seq: u64,
}

impl CadenceSnapshotGate {
    pub(crate) fn new(cadence: u64, sink: SnapshotSink) -> Self {
        Self {
            sink,
            cadence,
            pending: SnapshotPending::None,
            last_snapshot_seq: 0,
        }
    }

    /// The last command sequence this gate handed to the worker.
    #[cfg(test)]
    pub(crate) fn last_snapshot_seq(&self) -> u64 {
        self.last_snapshot_seq
    }

    fn try_capture(&mut self, engine: &MatchingEngine, applied_seq: u64) -> bool {
        if applied_seq <= self.last_snapshot_seq {
            self.pending = SnapshotPending::None;
            return true;
        }

        let stored = StoredSnapshot {
            format_version: FORMAT_VERSION,
            applied_seq,
            snapshot: engine.capture_snapshot(),
        };

        match self.sink.try_send(stored) {
            SnapshotSend::Sent => {
                self.last_snapshot_seq = applied_seq;
                self.pending = SnapshotPending::None;
                true
            }
            SnapshotSend::Full | SnapshotSend::Disconnected => {
                self.pending = SnapshotPending::Due;
                false
            }
        }
    }
}

impl SnapshotGate for CadenceSnapshotGate {
    fn poll(
        &mut self,
        engine: &MatchingEngine,
        applied_seq: u64,
        durable_seq: u64,
    ) -> GateDecision {
        if self.cadence == 0 {
            return GateDecision::Continue;
        }

        match self.pending {
            SnapshotPending::Watermark => {
                if applied_seq > durable_seq {
                    return GateDecision::Pause;
                }
                // The watermark caught up; capturing may still find the
                // worker channel full, in which case matching continues and
                // the capture stays due.
                self.try_capture(engine, applied_seq);
                GateDecision::Continue
            }
            SnapshotPending::Due => {
                // A full worker channel must never stall matching. Automatic
                // snapshots still belong only on a cadence boundary: retry at
                // the next boundary rather than capturing whatever sequence
                // happens to be durable now.
                let at_boundary = applied_seq > 0 && applied_seq % self.cadence == 0;
                if at_boundary && applied_seq > self.last_snapshot_seq && applied_seq == durable_seq
                {
                    self.try_capture(engine, applied_seq);
                }
                GateDecision::Continue
            }
            SnapshotPending::None => {
                let at_boundary = applied_seq > 0 && applied_seq % self.cadence == 0;
                if !at_boundary || applied_seq <= self.last_snapshot_seq {
                    return GateDecision::Continue;
                }

                if applied_seq > durable_seq {
                    // Deliberate, infrequent pause so continuous load cannot
                    // starve snapshots forever.
                    self.pending = SnapshotPending::Watermark;
                    return GateDecision::Pause;
                }

                self.try_capture(engine, applied_seq);
                GateDecision::Continue
            }
        }
    }
}

/// A gate that never pauses. Correct when snapshots are disabled.
#[cfg(test)]
pub(crate) struct NoopSnapshotGate;

#[cfg(test)]
impl SnapshotGate for NoopSnapshotGate {
    fn poll(
        &mut self,
        _engine: &MatchingEngine,
        _applied_seq: u64,
        _durable_seq: u64,
    ) -> GateDecision {
        GateDecision::Continue
    }
}
