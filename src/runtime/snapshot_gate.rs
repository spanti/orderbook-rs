//! Snapshot gating: when the matching thread may capture a snapshot.
//!
//! The gate owns only the cadence policy. Capture happens on the matching
//! thread at a command boundary; the `SnapshotStore::save` call happens on
//! the snapshot worker through [`SnapshotSink`].
//!
//! `snapshot_cadence` is measured in **event** sequences: a snapshot is due once
//! at least `cadence` events have been applied since the last capture. It is
//! captured at the next command boundary where `applied_seq == durable_seq`, so
//! a snapshot never covers a half-applied command or an event the log does not
//! hold.

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
/// `applied_seq == durable_seq`, pausing intake at a due command boundary while
/// durability catches up.
pub(crate) trait SnapshotGate: Send + 'static {
    fn poll(
        &mut self,
        engine: &MatchingEngine,
        applied_seq: u64,
        durable_seq: u64,
        last_command_id: u64,
    ) -> GateDecision;
}

/// Automatic snapshots at a cadence measured in event sequences.
///
/// The gate captures only when `applied_seq == durable_seq` and at least
/// `cadence` events were applied since the last capture. A due boundary with
/// durability lagging pauses intake until the watermark catches up; a full
/// worker channel instead keeps the snapshot due without pausing, because
/// snapshots are best effort and must never stall matching.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnapshotPending {
    /// No snapshot is waiting.
    None,
    /// A due boundary was reached while durability lagged; intake is paused.
    Watermark,
    /// A capture is pending because the worker channel was full; keep
    /// matching and retry at the next durability boundary.
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

    /// The last event sequence this gate handed to the worker.
    #[cfg(test)]
    pub(crate) fn last_snapshot_seq(&self) -> u64 {
        self.last_snapshot_seq
    }

    /// Whether enough events were applied since the last capture.
    fn is_due(&self, applied_seq: u64) -> bool {
        applied_seq > self.last_snapshot_seq
            && applied_seq.saturating_sub(self.last_snapshot_seq) >= self.cadence
    }

    fn try_capture(
        &mut self,
        engine: &MatchingEngine,
        applied_seq: u64,
        last_command_id: u64,
    ) -> bool {
        if applied_seq <= self.last_snapshot_seq {
            self.pending = SnapshotPending::None;
            return true;
        }

        let stored = StoredSnapshot {
            format_version: FORMAT_VERSION,
            applied_seq,
            last_command_id,
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
        last_command_id: u64,
    ) -> GateDecision {
        if self.cadence == 0 {
            return GateDecision::Continue;
        }

        // `poll` is called between commands, so `applied_seq` is always a
        // command boundary. Without enough applied events since the last
        // capture there is nothing due.
        if !self.is_due(applied_seq) {
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
                self.try_capture(engine, applied_seq, last_command_id);
                GateDecision::Continue
            }
            SnapshotPending::Due => {
                // A full worker channel must never stall matching. Retry at
                // the next boundary where durability has caught up.
                if applied_seq == durable_seq {
                    self.try_capture(engine, applied_seq, last_command_id);
                }
                GateDecision::Continue
            }
            SnapshotPending::None => {
                if applied_seq > durable_seq {
                    // Deliberate, infrequent pause so continuous load cannot
                    // starve snapshots forever.
                    self.pending = SnapshotPending::Watermark;
                    return GateDecision::Pause;
                }

                self.try_capture(engine, applied_seq, last_command_id);
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
        _last_command_id: u64,
    ) -> GateDecision {
        GateDecision::Continue
    }
}
