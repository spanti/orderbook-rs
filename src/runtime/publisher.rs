//! The Event Publisher: durable events leave the engine here.
//!
//! # Read strategy
//!
//! The journal writer and the publisher share **one** event-log handle
//! ([`SharedEventLog`]). The journal takes the handle's mutex only for
//! `append_batch`; the publisher takes it only to clone the records in
//! `read_from`. Handler dispatch happens afterwards, with no log lock held, so
//! a slow subscriber cannot delay the journal or the matching thread.
//!
//! The journal signals a durability-watermark advance with
//! [`SharedState::notify_publisher`], but the signal is advisory: the publisher
//! always re-reads `durable_seq` and calls
//! [`EventLog::read_from(last_published + 1)`](crate::durable::EventLog::read_from),
//! clamping the records to the watermark. A lost wake-up is therefore
//! harmless, and an event is never published before it is durable.
//!
//! # Recovery and catch-up
//!
//! The publisher starts at the recovered durable watermark
//! ([`EventCursor::durable_seq`](crate::durable::EventCursor)) and never
//! republishes history. A subscriber that missed a publication, or that started
//! after the publisher passed it, saves its last seen sequence and reads
//! `EventLog::read_from(last_seen + 1)` itself. The bus keeps no history and no
//! per-subscriber queue.
//!
//! # Shutdown
//!
//! Shutdown sets [`SharedState::finish_publisher`] only after the journal has
//! been joined, so no further watermark advance is possible. The publisher
//! drains to that final watermark and then exits. A log read failure stops the
//! publisher without failing the engine; the log remains the catch-up source.

use std::sync::Arc;
use std::sync::mpsc::Sender;
use std::thread::{self, JoinHandle};

use crate::bus::{EVENTS_TOPIC, MessageBus};
use crate::durable::DurableError;

use super::journal::SharedEventLog;
use super::shared::SharedState;
use super::sync::lock;

struct EventPublisher {
    log: SharedEventLog,
    shared: SharedState,
    bus: Arc<dyn MessageBus>,
    /// The last event sequence this publisher has dispatched.
    last_published: u64,
    /// A log read or ordering failure that stopped publication. Publication is
    /// best effort: consumers can always catch up from the event log.
    #[allow(dead_code)]
    last_error: Option<DurableError>,
}

impl EventPublisher {
    fn run(mut self, done: Sender<()>) {
        loop {
            // Read the generation before draining. A notification that races
            // with the drain then makes the next wait return immediately
            // instead of parking on a stale generation.
            let seen = self.shared.publisher_generation();
            let healthy = self.drain();

            if self.shared.publisher_finished() && self.last_published >= self.shared.durable_seq()
            {
                break;
            }
            if !healthy {
                break;
            }

            self.shared.wait_for_publisher(seen);
        }

        let _ = done.send(());
    }

    /// Publish every durable record after `last_published`, in sequence order.
    ///
    /// Returns `false` when publication cannot continue safely (a log read
    /// error or a sequence gap). The caller keeps the durability watermark as
    /// the final authority either way.
    fn drain(&mut self) -> bool {
        let durable = self.shared.durable_seq();
        if durable <= self.last_published {
            return true;
        }

        let read = lock(&self.log).read_from(self.last_published.saturating_add(1));
        let records = match read {
            Ok(records) => records,
            Err(error) => {
                self.last_error = Some(error);
                return false;
            }
        };

        for record in records {
            // Past the captured watermark means the writer advanced while we
            // were reading; the next wake-up covers it.
            if record.sequence > durable {
                break;
            }
            if record.sequence <= self.last_published {
                continue;
            }
            if record.sequence != self.last_published.saturating_add(1) {
                self.last_error = Some(DurableError::RecoveryGap {
                    expected: self.last_published.saturating_add(1),
                    found: Some(record.sequence),
                });
                return false;
            }

            // The bus isolates handler panics; an `Err` here (only possible if
            // a future implementation reports one) must not stop the stream.
            let _ = self.bus.try_publish_any(EVENTS_TOPIC, &record);
            self.last_published = record.sequence;
        }
        true
    }
}

/// Start the Event Publisher.
///
/// `start_at` is the recovered durable watermark: the publisher dispatches only
/// records after it. Returns the join handle and a completion receiver that
/// fires when the thread exits (including on a publication error), so shutdown
/// can wait without assuming the thread always finishes.
pub(crate) fn spawn(
    log: SharedEventLog,
    shared: SharedState,
    bus: Arc<dyn MessageBus>,
    start_at: u64,
) -> (JoinHandle<()>, std::sync::mpsc::Receiver<()>) {
    let (done_sender, done_receiver) = std::sync::mpsc::channel();
    let publisher = EventPublisher {
        log,
        shared,
        bus,
        last_published: start_at,
        last_error: None,
    };
    let handle = thread::Builder::new()
        .name("publisher".to_string())
        .spawn(move || publisher.run(done_sender))
        .expect("start the Event Publisher thread");
    (handle, done_receiver)
}
