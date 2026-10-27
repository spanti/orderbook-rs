//! Event-sourced durable layer: the event log is the fact source.
//!
//! [`DurableMatchingEngine`] composes the pure engine decision with an
//! append-before-apply write path:
//!
//! 1. `decide` produces the command's events without mutating the engine;
//! 2. the records are assigned a contiguous event-sequence range and one
//!    `command_id`, then appended as a batch;
//! 3. only after the append succeeds are the events folded through `apply`.
//!
//! Recovery reads the header, folds every contiguous record through `apply`,
//! skips duplicates and aborts on a gap. It never re-runs matching.

use std::fmt;

use crate::durable::{DurableError, LogHeader, Sequencer, SnapshotStore, StoredSnapshot};
use crate::engine::MatchingEngine;
use crate::event::DomainEvent;
use crate::types::{EngineCommand, EngineConfig};

/// Format version written into every event-log header, record and snapshot.
///
/// Version 1 was the retired command-log format; it is refused rather than
/// migrated.
pub const FORMAT_VERSION: u32 = 2;

/// One event in the durable log.
///
/// The event itself carries no sequence: the unique writer assigns `sequence`
/// and `command_id`. `sequence` is the recovery and snapshot boundary;
/// `command_id` groups all events produced by one command.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct EventRecord {
    pub format_version: u32,
    pub sequence: u64,
    pub command_id: u64,
    pub event: DomainEvent,
}

impl EventRecord {
    /// Build the contiguous records for one command's events.
    ///
    /// `first_sequence` is the event sequence assigned to the first event;
    /// each following event takes the next one, and every record shares
    /// `command_id`. Sequence assignment stays with the single writer, which
    /// supplies the cursor values.
    pub(crate) fn batch(events: &[DomainEvent], first_sequence: u64, command_id: u64) -> Vec<Self> {
        events
            .iter()
            .enumerate()
            .map(|(offset, event)| Self {
                format_version: FORMAT_VERSION,
                sequence: first_sequence.saturating_add(offset as u64),
                command_id,
                event: event.clone(),
            })
            .collect()
    }
}

/// The single globally ordered, append-only event stream.
///
/// Implementations make an appended record durable before returning, never
/// invent or reorder sequences, and report gaps to the caller.
pub trait EventLog: Send + 'static {
    /// Read the log header, or `None` when the log has no header yet.
    fn read_header(&self) -> Result<Option<LogHeader>, DurableError>;

    /// Write the header. Fails when a header already exists.
    fn write_header(&mut self, header: LogHeader) -> Result<(), DurableError>;

    /// Append one record. Rejects a sequence that is not the next one.
    fn append(&mut self, record: EventRecord) -> Result<(), DurableError>;

    /// Append a contiguous batch, making every record durable before returning.
    ///
    /// `Ok(())` means every record is durable. `Err` may leave a prefix of the
    /// batch written, so the caller must fail-stop rather than retry.
    fn append_batch(&mut self, records: Vec<EventRecord>) -> Result<(), DurableError> {
        for record in records {
            self.append(record)?;
        }
        Ok(())
    }

    /// Every record whose event `sequence` is `>= from`, in append order.
    fn read_from(&self, from: u64) -> Result<Vec<EventRecord>, DurableError>;

    /// First stored event sequence, cached with no IO.
    fn first_sequence(&self) -> Option<u64>;

    /// Last stored event sequence, cached with no IO.
    fn last_stored_sequence(&self) -> Option<u64>;

    /// `command_id` of the last stored record, cached with no IO.
    fn last_command_id(&self) -> Option<u64>;
}

/// The recovery state an event-sourced writer must agree on.
///
/// It carries the last event sequence applied to the engine, the last event
/// sequence known durable, the event sequence the next record will receive,
/// and the `command_id` the next command will receive. All but
/// `next_command_id` are event sequences; `next_command_id` is grouping
/// metadata that must continue across restarts.
///
/// Recovery reconstructs this cursor from the latest snapshot plus the log
/// tail, and both the synchronous façade and the runtime continue from it.
/// The spec defines these ids as primitive `u64`s, so they are deliberately
/// not newtypes in this release.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventCursor {
    /// Last event sequence applied to the engine.
    pub applied_seq: u64,
    /// Last event sequence known durable.
    pub durable_seq: u64,
    /// Sequence the next event will receive.
    pub next_sequence: u64,
    /// Command id the next command will receive.
    pub next_command_id: u64,
}

impl EventCursor {
    /// Cursor for an empty event log whose header has just been written.
    pub fn fresh() -> Self {
        Self {
            applied_seq: 0,
            durable_seq: 0,
            next_sequence: 1,
            next_command_id: 1,
        }
    }
}

/// A [`MatchingEngine`] whose durable source of truth is an event log.
///
/// The synchronous façade keeps append-before-apply: a failed append applies
/// no event and fails the instance closed.
pub struct DurableMatchingEngine {
    engine: MatchingEngine,
    log: Box<dyn EventLog>,
    snapshots: Box<dyn SnapshotStore>,
    sequencer: Sequencer,
    failed: bool,
    last_snapshot_error: Option<DurableError>,
}

impl fmt::Debug for DurableMatchingEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableMatchingEngine")
            .field(
                "cursor",
                &self.sequencer.cursor(self.sequencer.applied_seq()),
            )
            .field("failed", &self.failed)
            .field("last_snapshot_error", &self.last_snapshot_error)
            .finish_non_exhaustive()
    }
}

/// Restore engine state from the latest event snapshot plus the contiguous
/// event-log tail.
///
/// Every recovery check lives here so the snapshot boundary, duplicate, gap and
/// command-id rules cannot drift between the synchronous façade and the
/// matching runtime. Borrows both stores and never writes.
pub(crate) fn recover(
    log: &dyn EventLog,
    snapshots: &dyn SnapshotStore,
) -> Result<(MatchingEngine, EventCursor), DurableError> {
    let header = log.read_header()?.ok_or(DurableError::MissingHeader)?;
    if header.format_version != FORMAT_VERSION {
        return Err(DurableError::UnsupportedVersion {
            found: header.format_version,
            supported: FORMAT_VERSION,
        });
    }

    let (mut engine, mut applied_seq, mut next_command_id) = match snapshots.load_latest()? {
        Some(stored) => {
            if stored.format_version != FORMAT_VERSION {
                return Err(DurableError::UnsupportedVersion {
                    found: stored.format_version,
                    supported: FORMAT_VERSION,
                });
            }
            if stored.snapshot.config != header.config {
                return Err(DurableError::ConfigMismatch {
                    header: header.config,
                    snapshot: stored.snapshot.config,
                });
            }

            // A snapshot may never cover events the log no longer contains.
            let log_last_seq = log.last_stored_sequence().unwrap_or(0);
            if stored.applied_seq > log_last_seq {
                return Err(DurableError::SnapshotAhead {
                    applied_seq: stored.applied_seq,
                    log_last_seq,
                });
            }

            let mut engine = MatchingEngine::restore_snapshot(stored.snapshot);
            engine.set_last_applied_sequence(stored.applied_seq);
            (
                engine,
                stored.applied_seq,
                stored.last_command_id.saturating_add(1).max(1),
            )
        }
        None => (MatchingEngine::new(header.config), 0, 1),
    };

    // The log must cover every event after the snapshot boundary. With no
    // snapshot that boundary is zero, so the log must start at sequence 1.
    let expected_first = applied_seq.saturating_add(1);
    if let Some(first) = log.first_sequence()
        && first > expected_first
    {
        return Err(DurableError::RecoveryGap {
            expected: expected_first,
            found: Some(first),
        });
    }

    for record in log.read_from(expected_first)? {
        if record.format_version != FORMAT_VERSION {
            return Err(DurableError::UnsupportedVersion {
                found: record.format_version,
                supported: FORMAT_VERSION,
            });
        }

        // At-least-once delivery: a sequence at or below the applied boundary
        // is a duplicate and is skipped, not re-applied.
        if record.sequence <= applied_seq {
            continue;
        }

        let expected = applied_seq.saturating_add(1);
        if record.sequence != expected {
            return Err(DurableError::NonContiguousSequence {
                expected,
                found: record.sequence,
            });
        }

        engine.apply(&record.event, record.sequence);
        applied_seq = record.sequence;
        next_command_id = next_command_id.max(record.command_id.saturating_add(1));
    }

    // A recovered writer must continue after the log's tail even when the tail
    // was all duplicates, and after the snapshot when the tail is empty.
    let log_last = log.last_stored_sequence();
    let durable_seq = log_last.unwrap_or(applied_seq);
    let next_sequence = log_last
        .map(|last| last.saturating_add(1))
        .unwrap_or(1)
        .max(applied_seq.saturating_add(1));
    if let Some(last_id) = log.last_command_id() {
        next_command_id = next_command_id.max(last_id.saturating_add(1));
    }

    Ok((
        engine,
        EventCursor {
            applied_seq,
            durable_seq,
            next_sequence,
            next_command_id,
        },
    ))
}

impl DurableMatchingEngine {
    /// Create a fresh event-sourced engine and write the v2 log header.
    pub fn create(
        config: EngineConfig,
        mut log: Box<dyn EventLog>,
        snapshots: Box<dyn SnapshotStore>,
    ) -> Result<Self, DurableError> {
        log.write_header(LogHeader {
            format_version: FORMAT_VERSION,
            config,
        })?;

        Ok(Self {
            engine: MatchingEngine::new(config),
            log,
            snapshots,
            sequencer: Sequencer::from_cursor(EventCursor::fresh()),
            failed: false,
            last_snapshot_error: None,
        })
    }

    /// Recover from the latest event snapshot plus the contiguous log tail.
    ///
    /// The snapshot is validated against the log header and the log tail; a
    /// snapshot ahead of the log, a version or config mismatch, a gap or a
    /// non-contiguous sequence aborts recovery.
    pub fn open(
        log: Box<dyn EventLog>,
        snapshots: Box<dyn SnapshotStore>,
    ) -> Result<Self, DurableError> {
        let (engine, cursor) = recover(log.as_ref(), snapshots.as_ref())?;

        Ok(Self {
            engine,
            log,
            snapshots,
            sequencer: Sequencer::from_cursor(cursor),
            failed: false,
            last_snapshot_error: None,
        })
    }

    /// Write a snapshot of the current decision state at the applied event
    /// boundary.
    ///
    /// `execute` applies a whole command before returning, so this is always a
    /// command boundary. A failure is recorded on the engine and surfaced
    /// through [`Self::last_snapshot_error`]; it never undoes a durable command.
    pub fn snapshot_now(&mut self) -> Result<(), DurableError> {
        let stored = match StoredSnapshot::capture_checked(
            &self.engine,
            self.sequencer.applied_seq(),
            self.log.last_stored_sequence().unwrap_or(0),
            self.sequencer.last_command_id(),
        ) {
            Ok(stored) => stored,
            Err(error) => {
                self.last_snapshot_error = Some(error.clone());
                return Err(error);
            }
        };

        match self.snapshots.save(stored) {
            Ok(()) => {
                self.last_snapshot_error = None;
                Ok(())
            }
            Err(error) => {
                self.last_snapshot_error = Some(error.clone());
                Err(error)
            }
        }
    }

    /// The last snapshot failure, if snapshotting is degraded.
    pub fn last_snapshot_error(&self) -> Option<&DurableError> {
        self.last_snapshot_error.as_ref()
    }

    /// Decide, append every event, then apply it.
    ///
    /// Every command produces at least one event. A business rejection is a
    /// successful, event-producing execute: it journals one
    /// [`DomainEvent::OrderRejected`] and changes no engine state. A failed
    /// append applies nothing and leaves the instance failed closed.
    pub fn execute(&mut self, command: EngineCommand) -> Result<Vec<DomainEvent>, DurableError> {
        if self.failed {
            return Err(DurableError::EngineFailed);
        }

        let events = self.engine.decide(&command);
        let Some(batch) = self.sequencer.assign(&events) else {
            return Ok(events);
        };

        // Append-before-apply: a failed append must leave engine state and the
        // sequencing cursor intact.
        if let Err(error) = self.log.append_batch(batch.records.clone()) {
            self.failed = true;
            return Err(error);
        }

        let engine = &mut self.engine;
        self.sequencer.fold(&batch, |record| {
            engine.apply(&record.event, record.sequence)
        });
        Ok(events)
    }

    /// Durable records whose event sequence is `from` onward.
    pub fn history_from(&self, from: u64) -> Result<Vec<EventRecord>, DurableError> {
        self.log.read_from(from)
    }

    /// Read-only access to the pure engine's decision state.
    pub fn engine(&self) -> &MatchingEngine {
        &self.engine
    }

    /// Last event sequence applied to the engine.
    pub fn applied_seq(&self) -> u64 {
        self.sequencer.applied_seq()
    }

    /// The current sequence/command-id cursor.
    pub fn cursor(&self) -> EventCursor {
        // Append-before-apply: after every successful append, durable == applied.
        self.sequencer.cursor(self.sequencer.applied_seq())
    }

    /// Whether an append failure stopped this instance.
    pub fn is_failed(&self) -> bool {
        self.failed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{EngineCommand, EngineError};

    fn rejected() -> DomainEvent {
        DomainEvent::OrderRejected {
            symbol: "BTC-USDT".to_string(),
            reason: EngineError::UnknownOrder,
            command: EngineCommand::CancelOrder {
                symbol: "BTC-USDT".to_string(),
                order_id: 7,
            },
        }
    }

    #[test]
    fn batch_assigns_contiguous_sequences_and_one_command_id() {
        let events = vec![rejected(), rejected()];

        let records = EventRecord::batch(&events, 10, 4);

        assert_eq!(
            records
                .iter()
                .map(|record| (record.format_version, record.sequence, record.command_id))
                .collect::<Vec<_>>(),
            vec![(FORMAT_VERSION, 10, 4), (FORMAT_VERSION, 11, 4)]
        );
        assert_eq!(
            records
                .iter()
                .map(|record| record.event.clone())
                .collect::<Vec<_>>(),
            events
        );
    }
}
