//! Event sequencing: assign contiguous event sequences, group one command's
//! events under one `command_id`, and fold an assigned batch into the engine.
//!
//! The Sequencer is the single owner of the sequencing cursor. It deliberately
//! does not append: the synchronous façade appends before folding, while the
//! matching runtime folds before handing records to the Journal. Both paths
//! call [`Sequencer::assign`] and [`Sequencer::fold`] in their own order.

use crate::durable::{EventCursor, EventRecord};
use crate::event::DomainEvent;

/// The single owner of the event-sequence and `command_id` cursors.
#[derive(Debug)]
pub(crate) struct Sequencer {
    applied_seq: u64,
    next_sequence: u64,
    next_command_id: u64,
}

/// One command's events with their assigned global event-sequence range.
#[derive(Debug)]
pub(crate) struct EventBatch {
    pub(crate) records: Vec<EventRecord>,
    pub(crate) command_id: u64,
    pub(crate) first_event_sequence: u64,
    pub(crate) last_event_sequence: u64,
}

impl Sequencer {
    /// Seed the cursor from recovery.
    ///
    /// `durable_seq` is not owned here: the runtime keeps it in the durability
    /// watermark, and the synchronous façade has `durable == applied` after
    /// every successful append.
    pub(crate) fn from_cursor(cursor: EventCursor) -> Self {
        Self {
            applied_seq: cursor.applied_seq,
            next_sequence: cursor.next_sequence,
            next_command_id: cursor.next_command_id,
        }
    }

    /// Assign this command's events a contiguous range and one `command_id`.
    ///
    /// Pure: it reads the cursor and reserves nothing. A command with no events
    /// returns `None`; `decide` never produces one, but both write paths keep
    /// their defensive empty branch.
    pub(crate) fn assign(&self, events: &[DomainEvent]) -> Option<EventBatch> {
        if events.is_empty() {
            return None;
        }

        let command_id = self.next_command_id;
        let first_event_sequence = self.next_sequence;
        let records = EventRecord::batch(events, first_event_sequence, command_id);
        let last_event_sequence = records
            .last()
            .map(|record| record.sequence)
            .expect("a non-empty event list produced no records");

        Some(EventBatch {
            records,
            command_id,
            first_event_sequence,
            last_event_sequence,
        })
    }

    /// Fold an assigned batch into the caller's state and commit the cursor.
    ///
    /// The caller supplies the state effect, so the Sequencer owns the
    /// sequencing invariant without depending on a concrete engine. This is
    /// the only cursor commit point; the debug assertions make a stale or
    /// duplicated assignment a programming error at the seam rather than a
    /// silent gap in the event stream.
    pub(crate) fn fold(&mut self, batch: &EventBatch, mut apply: impl FnMut(&EventRecord)) {
        debug_assert_eq!(
            batch.first_event_sequence, self.next_sequence,
            "batch starts at an unassigned sequence"
        );
        debug_assert_eq!(
            batch.command_id, self.next_command_id,
            "batch carries an unassigned command id"
        );

        for record in &batch.records {
            apply(record);
        }

        self.applied_seq = batch.last_event_sequence;
        self.next_sequence = batch.last_event_sequence.saturating_add(1);
        self.next_command_id = batch.command_id.saturating_add(1);
    }

    /// Last event sequence folded into the engine.
    pub(crate) fn applied_seq(&self) -> u64 {
        self.applied_seq
    }

    /// `command_id` of the last assigned command; `0` before any command.
    pub(crate) fn last_command_id(&self) -> u64 {
        self.next_command_id.saturating_sub(1)
    }

    /// Rebuild the recovery DTO. `durable_seq` is supplied by the caller.
    pub(crate) fn cursor(&self, durable_seq: u64) -> EventCursor {
        EventCursor {
            applied_seq: self.applied_seq,
            durable_seq,
            next_sequence: self.next_sequence,
            next_command_id: self.next_command_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::MatchingEngine;
    use crate::types::{EngineCommand, EngineConfig, EngineError};

    fn at(applied_seq: u64, next_sequence: u64, next_command_id: u64) -> EventCursor {
        EventCursor {
            applied_seq,
            durable_seq: applied_seq,
            next_sequence,
            next_command_id,
        }
    }

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
    fn assign_returns_none_for_an_empty_event_list() {
        let sequencer = Sequencer::from_cursor(at(0, 1, 1));
        assert!(sequencer.assign(&[]).is_none());
    }

    #[test]
    fn assign_reserves_a_contiguous_range_and_one_command_id() {
        let sequencer = Sequencer::from_cursor(at(0, 10, 4));
        let batch = sequencer
            .assign(&[rejected(), rejected()])
            .expect("two events");

        assert_eq!(batch.command_id, 4);
        assert_eq!(batch.first_event_sequence, 10);
        assert_eq!(batch.last_event_sequence, 11);
        assert_eq!(
            batch
                .records
                .iter()
                .map(|record| (record.sequence, record.command_id))
                .collect::<Vec<_>>(),
            vec![(10, 4), (11, 4)]
        );
    }

    #[test]
    fn fold_advances_all_three_counters() {
        let mut sequencer = Sequencer::from_cursor(at(0, 1, 1));
        let mut engine = MatchingEngine::new(EngineConfig::default());
        let batch = sequencer.assign(&[rejected()]).expect("one event");

        sequencer.fold(&batch, |record| {
            engine.apply(&record.event, record.sequence)
        });

        assert_eq!(sequencer.applied_seq(), 1);
        assert_eq!(sequencer.last_command_id(), 1);
        assert_eq!(sequencer.cursor(1).next_sequence, 2);
        assert_eq!(sequencer.cursor(1).next_command_id, 2);
        assert_eq!(engine.last_applied_sequence(), 1);
    }

    #[test]
    fn fold_invokes_the_supplied_apply_for_each_record_in_order() {
        let mut sequencer = Sequencer::from_cursor(at(0, 1, 1));
        let batch = sequencer
            .assign(&[rejected(), rejected()])
            .expect("two events");

        // No MatchingEngine: the fold effect is supplied by the caller.
        let mut applied = Vec::new();
        sequencer.fold(&batch, |record| applied.push(record.sequence));

        assert_eq!(applied, vec![1, 2]);
        assert_eq!(sequencer.applied_seq(), 2);
    }

    #[test]
    fn consecutive_batches_are_contiguous() {
        let mut sequencer = Sequencer::from_cursor(at(0, 1, 1));
        let mut engine = MatchingEngine::new(EngineConfig::default());

        let first = sequencer.assign(&[rejected()]).expect("one event");
        sequencer.fold(&first, |record| {
            engine.apply(&record.event, record.sequence)
        });
        let second = sequencer
            .assign(&[rejected(), rejected()])
            .expect("two events");
        sequencer.fold(&second, |record| {
            engine.apply(&record.event, record.sequence)
        });

        assert_eq!(first.first_event_sequence, 1);
        assert_eq!(second.first_event_sequence, 2);
        assert_eq!(second.last_event_sequence, 3);
        assert_eq!(sequencer.applied_seq(), 3);
        assert_eq!(sequencer.last_command_id(), 2);
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "unassigned")]
    fn fold_rejects_a_batch_from_a_stale_position() {
        let mut sequencer = Sequencer::from_cursor(at(0, 1, 1));
        let mut engine = MatchingEngine::new(EngineConfig::default());
        let batch = sequencer.assign(&[rejected()]).expect("one event");

        sequencer.fold(&batch, |record| {
            engine.apply(&record.event, record.sequence)
        });
        sequencer.fold(&batch, |record| {
            engine.apply(&record.event, record.sequence)
        });
    }

    #[test]
    fn cursor_round_trips_the_assignment_position() {
        let sequencer = Sequencer::from_cursor(at(7, 8, 3));
        let cursor = sequencer.cursor(7);

        assert_eq!(cursor.applied_seq, 7);
        assert_eq!(cursor.durable_seq, 7);
        assert_eq!(cursor.next_sequence, 8);
        assert_eq!(cursor.next_command_id, 3);
    }
}
