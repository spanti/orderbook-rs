//! End-to-end publisher contracts on the public runtime seam.
//!
//! The Event Publisher is started by [`EngineRuntime`]. Its only observable
//! surface is a [`MessageBus`] subscriber: durable `EventRecord`s arrive in
//! sequence order, only after they are in the log, and a subscriber that missed
//! events catches up with `EventLog::read_from`.

use std::any::Any;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use orderbook_rs::bus::{EVENTS_TOPIC, InProcessBus, MessageBus};
use orderbook_rs::durable::{
    DurableError, EventLog, EventRecord, EventSourcedEngine, InMemoryEventLog,
    InMemoryEventSnapshotStore, LogHeader,
};
use orderbook_rs::runtime::{EngineRuntime, RuntimeConfig};
use orderbook_rs::types::{
    EngineCommand, EngineConfig, OrderRequest, OrderType, Side, TimeInForce,
};

const WAIT: Duration = Duration::from_secs(10);

fn submit_order(timestamp: u64) -> EngineCommand {
    EngineCommand::SubmitOrder {
        symbol: "BTC-USDT".to_string(),
        request: OrderRequest {
            side: Side::Bid,
            price: 100,
            quantity: 1,
            timestamp,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::GTC,
            post_only: false,
        },
    }
}

fn cancel_order(order_id: u64) -> EngineCommand {
    EngineCommand::CancelOrder {
        symbol: "BTC-USDT".to_string(),
        order_id,
    }
}

fn wait_until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !condition() {
        assert!(Instant::now() < deadline, "condition was not met in time");
        thread::sleep(Duration::from_millis(1));
    }
}

/// A subscriber that records every `EventRecord` it receives.
#[derive(Clone, Default)]
struct Recorder {
    records: Arc<Mutex<Vec<EventRecord>>>,
}

impl Recorder {
    fn subscribe(&self, bus: &InProcessBus) {
        let records = Arc::clone(&self.records);
        bus.register_any(
            EVENTS_TOPIC,
            Box::new(move |message: &dyn Any| {
                let record = message
                    .downcast_ref::<EventRecord>()
                    .expect("the events topic carries EventRecord")
                    .clone();
                records.lock().unwrap().push(record);
            }),
        )
        .expect("subscribe to the events endpoint");
    }

    fn len(&self) -> usize {
        self.records.lock().unwrap().len()
    }

    fn sequences(&self) -> Vec<u64> {
        self.records
            .lock()
            .unwrap()
            .iter()
            .map(|record| record.sequence)
            .collect()
    }
}

#[test]
fn subscribers_receive_durable_events_in_sequence_order() {
    let bus = Arc::new(InProcessBus::new());
    let recorder = Recorder::default();
    recorder.subscribe(&bus);

    let (handle, runtime) = EngineRuntime::create_with_bus(
        RuntimeConfig::default(),
        EngineConfig::default(),
        Box::new(InMemoryEventLog::new()),
        Box::new(InMemoryEventSnapshotStore::new()),
        Arc::clone(&bus) as Arc<dyn MessageBus>,
    )
    .expect("create a runtime with a bus");

    for timestamp in 1..=3 {
        handle
            .submit_and_wait(submit_order(timestamp))
            .expect("durable submission");
    }

    // Each resting order emits `OrderAccepted` + `OrderRested`.
    wait_until(|| recorder.len() == 6);
    assert_eq!(
        recorder.sequences(),
        (1..=6).collect::<Vec<_>>(),
        "the publisher emits every durable event once, in sequence order"
    );

    runtime.shutdown().expect("shutdown");
}

#[test]
fn every_handler_on_the_events_endpoint_receives_every_event() {
    let bus = Arc::new(InProcessBus::new());
    let first = Recorder::default();
    let second = Recorder::default();
    first.subscribe(&bus);
    second.subscribe(&bus);

    let (handle, runtime) = EngineRuntime::create_with_bus(
        RuntimeConfig::default(),
        EngineConfig::default(),
        Box::new(InMemoryEventLog::new()),
        Box::new(InMemoryEventSnapshotStore::new()),
        Arc::clone(&bus) as Arc<dyn MessageBus>,
    )
    .expect("create");

    handle
        .submit_and_wait(submit_order(1))
        .expect("durable submission");

    wait_until(|| first.len() == 2 && second.len() == 2);
    assert_eq!(first.sequences(), second.sequences());

    runtime.shutdown().expect("shutdown");
}

/// A log whose `append_batch` blocks until the gate opens.
#[derive(Clone)]
struct BlockingLog {
    inner: InMemoryEventLog,
    gate: Arc<Gate>,
    entered: Arc<AtomicBool>,
}

impl BlockingLog {
    fn new(inner: InMemoryEventLog, gate: Arc<Gate>) -> Self {
        Self {
            inner,
            gate,
            entered: Arc::new(AtomicBool::new(false)),
        }
    }

    fn entered(&self) -> bool {
        self.entered.load(Ordering::Acquire)
    }
}

impl EventLog for BlockingLog {
    fn read_header(&self) -> Result<Option<LogHeader>, DurableError> {
        self.inner.read_header()
    }

    fn write_header(&mut self, header: LogHeader) -> Result<(), DurableError> {
        self.inner.write_header(header)
    }

    fn append(&mut self, record: EventRecord) -> Result<(), DurableError> {
        self.inner.append(record)
    }

    fn append_batch(&mut self, records: Vec<EventRecord>) -> Result<(), DurableError> {
        self.entered.store(true, Ordering::Release);
        self.gate.wait();
        self.inner.append_batch(records)
    }

    fn read_from(&self, from: u64) -> Result<Vec<EventRecord>, DurableError> {
        self.inner.read_from(from)
    }

    fn first_sequence(&self) -> Option<u64> {
        self.inner.first_sequence()
    }

    fn last_sequence(&self) -> Option<u64> {
        self.inner.last_sequence()
    }

    fn last_command_id(&self) -> Option<u64> {
        self.inner.last_command_id()
    }
}

#[derive(Default)]
struct Gate {
    open: Mutex<bool>,
    ready: Condvar,
}

impl Gate {
    fn wait(&self) {
        let mut open = self.open.lock().unwrap_or_else(|p| p.into_inner());
        while !*open {
            open = self.ready.wait(open).unwrap_or_else(|p| p.into_inner());
        }
    }

    fn open(&self) {
        *self.open.lock().unwrap_or_else(|p| p.into_inner()) = true;
        self.ready.notify_all();
    }
}

#[test]
fn only_events_at_or_below_the_durable_watermark_are_published() {
    let bus = Arc::new(InProcessBus::new());
    let recorder = Recorder::default();
    recorder.subscribe(&bus);

    let gate = Arc::new(Gate::default());
    let log = InMemoryEventLog::new();
    let blocking = BlockingLog::new(log.handle(), Arc::clone(&gate));
    let probe = blocking.clone();
    let config = RuntimeConfig {
        apply_batch_max: 1,
        journal_batch_capacity: 1,
        journal_flush_interval: Duration::from_millis(1),
        ..RuntimeConfig::default()
    };

    let (handle, runtime) = EngineRuntime::create_with_bus(
        config,
        EngineConfig::default(),
        Box::new(blocking),
        Box::new(InMemoryEventSnapshotStore::new()),
        Arc::clone(&bus) as Arc<dyn MessageBus>,
    )
    .expect("create");

    let ticket = handle.try_submit(submit_order(1)).expect("submit");
    ticket
        .wait_applied_timeout(WAIT)
        .expect("the command is applied before the journal writes it");
    wait_until(|| probe.entered());
    thread::sleep(Duration::from_millis(50));
    assert_eq!(
        recorder.len(),
        0,
        "an applied-but-not-durable event must never be published"
    );

    gate.open();
    ticket.wait_durable_timeout(WAIT).expect("durable");
    wait_until(|| recorder.len() == 2);
    assert_eq!(recorder.sequences(), vec![1, 2]);

    runtime.shutdown().expect("shutdown");
}

#[test]
fn the_publisher_starts_at_the_recovered_watermark_and_does_not_replay_history() {
    let log = InMemoryEventLog::new();
    {
        let mut seeded = EventSourcedEngine::create(
            EngineConfig::default(),
            Box::new(log.handle()),
            Box::new(InMemoryEventSnapshotStore::new()),
        )
        .expect("seed the event log");
        for timestamp in 1..=3 {
            seeded
                .execute(submit_order(timestamp))
                .expect("append-before-apply seed");
        }
    }
    assert_eq!(log.last_sequence(), Some(6));

    let bus = Arc::new(InProcessBus::new());
    let recorder = Recorder::default();
    // Register before `open` so history would be observed if it were replayed.
    recorder.subscribe(&bus);

    let (handle, runtime) = EngineRuntime::open_with_bus(
        RuntimeConfig::default(),
        Box::new(log.handle()),
        Box::new(InMemoryEventSnapshotStore::new()),
        Arc::clone(&bus) as Arc<dyn MessageBus>,
    )
    .expect("recover from the seeded log");

    handle
        .submit_and_wait(cancel_order(1))
        .expect("continue after recovery");

    wait_until(|| recorder.len() == 1);
    thread::sleep(Duration::from_millis(50));
    assert_eq!(
        recorder.sequences(),
        vec![7],
        "only the live event is published; the durable history is not replayed"
    );

    runtime.shutdown().expect("shutdown");
}

#[test]
fn a_consumer_that_missed_publications_catches_up_from_the_event_log() {
    let bus = Arc::new(InProcessBus::new());
    let online = Arc::new(AtomicBool::new(true));
    let seen = Arc::new(Mutex::new(Vec::<u64>::new()));
    {
        let online = Arc::clone(&online);
        let seen = Arc::clone(&seen);
        bus.register_any(
            EVENTS_TOPIC,
            Box::new(move |message: &dyn Any| {
                if !online.load(Ordering::Acquire) {
                    return;
                }
                let record = message.downcast_ref::<EventRecord>().expect("EventRecord");
                seen.lock().unwrap().push(record.sequence);
            }),
        )
        .expect("register the consumer");
    }
    // A second, always-on subscriber lets the test wait until the publisher
    // really passed the events the consumer missed.
    let published = Recorder::default();
    published.subscribe(&bus);

    let log = InMemoryEventLog::new();
    let (handle, runtime) = EngineRuntime::create_with_bus(
        RuntimeConfig::default(),
        EngineConfig::default(),
        Box::new(log.handle()),
        Box::new(InMemoryEventSnapshotStore::new()),
        Arc::clone(&bus) as Arc<dyn MessageBus>,
    )
    .expect("create");

    handle
        .submit_and_wait(submit_order(1))
        .expect("durable submission");
    wait_until(|| *seen.lock().unwrap() == vec![1, 2]);
    let last_seen = 2u64;

    // The consumer goes offline and misses the next command's publications.
    online.store(false, Ordering::Release);
    handle
        .submit_and_wait(submit_order(2))
        .expect("durable submission while offline");
    wait_until(|| published.len() == 4);
    assert_eq!(
        *seen.lock().unwrap(),
        vec![1, 2],
        "the offline consumer missed events 3 and 4"
    );

    online.store(true, Ordering::Release);
    handle
        .submit_and_wait(submit_order(3))
        .expect("durable submission after coming back");
    wait_until(|| *seen.lock().unwrap() == vec![1, 2, 5, 6]);

    // Catch up from the durable log using its last seen sequence. The missed
    // records are still there, in order, up to the durability watermark.
    let caught_up = log.read_from(last_seen + 1).expect("read the event log");
    assert_eq!(
        caught_up
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        vec![3, 4, 5, 6],
        "a consumer catches up from the log, not from a bus queue"
    );

    runtime.shutdown().expect("shutdown");
}

/// Blocks the publisher's handler until the test releases it, including on an
/// assertion panic, so a failing test cannot leave the publisher thread parked
/// forever.
struct SlowHandlerGate {
    open: Mutex<bool>,
    ready: Condvar,
}

impl SlowHandlerGate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            open: Mutex::new(false),
            ready: Condvar::new(),
        })
    }

    fn wait(&self) {
        let mut open = self.open.lock().unwrap_or_else(|p| p.into_inner());
        while !*open {
            open = self.ready.wait(open).unwrap_or_else(|p| p.into_inner());
        }
    }

    fn open(&self) {
        *self.open.lock().unwrap_or_else(|p| p.into_inner()) = true;
        self.ready.notify_all();
    }
}

struct ReleaseOnDrop(Arc<SlowHandlerGate>);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.open();
    }
}

#[test]
fn a_slow_handler_delays_publication_but_not_the_matching_thread() {
    let bus = Arc::new(InProcessBus::new());
    let gate = SlowHandlerGate::new();
    let entered = Arc::new(AtomicBool::new(false));
    {
        let gate = Arc::clone(&gate);
        let entered = Arc::clone(&entered);
        bus.register_any(
            EVENTS_TOPIC,
            Box::new(move |_: &dyn Any| {
                entered.store(true, Ordering::Release);
                gate.wait();
            }),
        )
        .expect("register the slow handler");
    }

    let published = Recorder::default();
    published.subscribe(&bus);
    let config = RuntimeConfig {
        apply_batch_max: 1,
        journal_flush_interval: Duration::from_millis(1),
        ..RuntimeConfig::default()
    };
    let (handle, runtime) = EngineRuntime::create_with_bus(
        config,
        EngineConfig::default(),
        Box::new(InMemoryEventLog::new()),
        Box::new(InMemoryEventSnapshotStore::new()),
        Arc::clone(&bus) as Arc<dyn MessageBus>,
    )
    .expect("create");
    // Declared after the runtime so an assertion panic releases the handler
    // before `Drop` waits for the publisher.
    let release = ReleaseOnDrop(Arc::clone(&gate));

    // Get the publisher into the slow handler.
    handle
        .submit_and_wait(submit_order(1))
        .expect("durable submission");
    wait_until(|| entered.load(Ordering::Acquire));

    // The publisher is parked. Matching must continue to apply and journal new
    // commands without waiting for the consumer.
    let started = Instant::now();
    let tickets = (2..=9)
        .map(|timestamp| handle.try_submit(submit_order(timestamp)).expect("submit"))
        .collect::<Vec<_>>();
    for ticket in &tickets {
        ticket
            .wait_applied_timeout(Duration::from_secs(2))
            .expect("matching is not blocked by a slow handler");
    }
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "applying commands waited on the slow subscriber"
    );
    for ticket in &tickets {
        ticket
            .wait_durable_timeout(Duration::from_secs(2))
            .expect("the journal is not blocked by a slow handler");
    }

    // Publication is ordered but stalled behind the slow handler.
    assert_eq!(
        published.len(),
        0,
        "the first event is still inside the slow handler"
    );

    release.0.open();
    wait_until(|| published.len() == 18);
    assert_eq!(
        published.sequences(),
        (1..=18).collect::<Vec<_>>(),
        "publication resumes in sequence order after the slow handler returns"
    );

    runtime.shutdown().expect("shutdown drains the publisher");
}
