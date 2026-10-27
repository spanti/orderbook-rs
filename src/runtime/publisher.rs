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
//! [`super::publisher_signal::PublisherSignal::notify`], but the signal is
//! advisory: the publisher always re-reads `durable_seq` and calls
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
//! Shutdown sets [`super::publisher_signal::PublisherSignal::finish`] only
//! after the journal has been joined, so no further watermark advance is
//! possible. The publisher drains to that final watermark and then exits. A log
//! read failure stops the publisher without failing the engine; the log remains
//! the catch-up source.

use std::sync::Arc;
use std::sync::mpsc::Sender;
use std::thread::{self, JoinHandle};

use crate::bus::{EVENTS_TOPIC, MessageBus};

use super::journal::SharedEventLog;
use super::shared::SharedState;
use super::sync::lock;

struct EventPublisher {
    log: SharedEventLog,
    shared: SharedState,
    bus: Arc<dyn MessageBus>,
    /// The last event sequence this publisher has dispatched.
    last_published: u64,
}

impl EventPublisher {
    fn run(mut self, done: Sender<()>) {
        loop {
            // Read the generation before draining. A notification that races
            // with the drain then makes the next wait return immediately
            // instead of parking on a stale generation.
            let seen = self.shared.publisher().generation();
            let healthy = self.drain();

            if self.shared.publisher().finished()
                && self.last_published >= self.shared.watermark().durable_seq()
            {
                break;
            }
            if !healthy {
                break;
            }

            self.shared.publisher().wait_for_change(seen);
        }

        let _ = done.send(());
    }

    /// Publish every durable record after `last_published`, in sequence order.
    ///
    /// Returns `false` when publication cannot continue safely (a log read
    /// error or a sequence gap). The caller keeps the durability watermark as
    /// the final authority either way.
    fn drain(&mut self) -> bool {
        let durable = self.shared.watermark().durable_seq();
        if durable <= self.last_published {
            return true;
        }

        // Nobody is listening: skip the log read and clone entirely. A
        // consumer that registers later receives live events only and catches
        // up from the log, exactly as a consumer that misses a publication.
        if !self.bus.has_subscribers(EVENTS_TOPIC) {
            self.last_published = durable;
            return true;
        }

        let read = lock(&self.log).read_from(self.last_published.saturating_add(1));
        let records = match read {
            Ok(records) => records,
            // Publication is best effort: consumers catch up from the log, so
            // a read failure only stops this publisher.
            Err(_) => return false,
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
                // A gap means the log cannot be published in order. Consumers
                // still read the log directly.
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
    };
    let handle = thread::Builder::new()
        .name("publisher".to_string())
        .spawn(move || publisher.run(done_sender))
        .expect("start the Event Publisher thread");
    (handle, done_receiver)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::Any;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::bus::BusError;
    use crate::durable::{DurableError, EventLog, EventRecord, FORMAT_VERSION, LogHeader};
    use crate::event::DomainEvent;
    use crate::types::{EngineCommand, EngineError};

    fn record(sequence: u64) -> EventRecord {
        EventRecord {
            format_version: FORMAT_VERSION,
            sequence,
            command_id: sequence,
            event: DomainEvent::OrderRejected {
                symbol: "BTC-USDT".to_string(),
                reason: EngineError::UnknownOrder,
                command: EngineCommand::CancelOrder {
                    symbol: "BTC-USDT".to_string(),
                    order_id: sequence,
                },
            },
        }
    }

    struct CountingLog {
        reads: Arc<AtomicUsize>,
        records: Vec<EventRecord>,
    }

    impl EventLog for CountingLog {
        fn read_header(&self) -> Result<Option<LogHeader>, DurableError> {
            Ok(None)
        }

        fn write_header(&mut self, _header: LogHeader) -> Result<(), DurableError> {
            Ok(())
        }

        fn append(&mut self, _record: EventRecord) -> Result<(), DurableError> {
            Ok(())
        }

        fn read_from(&self, from: u64) -> Result<Vec<EventRecord>, DurableError> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            Ok(self
                .records
                .iter()
                .filter(|record| record.sequence >= from)
                .cloned()
                .collect())
        }

        fn first_sequence(&self) -> Option<u64> {
            self.records.first().map(|record| record.sequence)
        }

        fn last_stored_sequence(&self) -> Option<u64> {
            self.records.last().map(|record| record.sequence)
        }

        fn last_command_id(&self) -> Option<u64> {
            self.records.last().map(|record| record.command_id)
        }
    }

    struct TestBus {
        subscribers: bool,
        published: AtomicUsize,
    }

    impl MessageBus for TestBus {
        fn register_any(
            &self,
            _endpoint: &str,
            _handler: Box<dyn Fn(&dyn Any) + Send + 'static>,
        ) -> Result<(), BusError> {
            Ok(())
        }

        fn register_response_handler(
            &self,
            _correlation_id: u64,
            _handler: Box<dyn Fn(&dyn Any) + Send + 'static>,
        ) -> Result<(), BusError> {
            Err(BusError::Unsupported)
        }

        fn send_any(&self, _endpoint: &str, _message: &dyn Any) -> Result<(), BusError> {
            Err(BusError::Unsupported)
        }

        fn try_publish_any(&self, _topic: &str, _message: &dyn Any) -> Result<(), BusError> {
            self.published.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        fn has_subscribers(&self, _topic: &str) -> bool {
            self.subscribers
        }
    }

    fn publisher(subscribers: bool) -> (EventPublisher, Arc<AtomicUsize>, Arc<TestBus>) {
        let reads = Arc::new(AtomicUsize::new(0));
        let log = CountingLog {
            reads: Arc::clone(&reads),
            records: (1..=3).map(record).collect(),
        };
        let bus = Arc::new(TestBus {
            subscribers,
            published: AtomicUsize::new(0),
        });
        let bus_dyn: Arc<dyn MessageBus> = bus.clone();
        let shared = SharedState::new(None);
        shared.watermark().publish_durable(3);

        let publisher = EventPublisher {
            log: Arc::new(Mutex::new(Box::new(log) as Box<dyn EventLog>)),
            shared,
            bus: bus_dyn,
            last_published: 0,
        };
        (publisher, reads, bus)
    }

    #[test]
    fn drain_skips_the_log_when_nobody_is_subscribed() {
        let (mut publisher, reads, bus) = publisher(false);

        assert!(publisher.drain());
        assert_eq!(
            reads.load(Ordering::Relaxed),
            0,
            "an unsubscribed publisher must not read or clone the log"
        );
        assert_eq!(bus.published.load(Ordering::Relaxed), 0);
        assert_eq!(publisher.last_published, 3);
    }

    #[test]
    fn drain_reads_and_publishes_when_a_subscriber_exists() {
        let (mut publisher, reads, bus) = publisher(true);

        assert!(publisher.drain());
        assert_eq!(reads.load(Ordering::Relaxed), 1);
        assert_eq!(bus.published.load(Ordering::Relaxed), 3);
        assert_eq!(publisher.last_published, 3);
    }
}
