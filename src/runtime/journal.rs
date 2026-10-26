//! The journal thread: group commit and the durability watermark.
//!
//! The matching thread hands batches of [`EventRecord`] to a bounded channel
//! through [`JournalThreadSink`]. The journal thread owns the
//! [`EventLog`], blocks on the first batch, coalesces everything queued
//! behind it up to [`RuntimeConfig::journal_coalesce_max`] records, and calls
//! [`EventLog::append_batch`] once per group. Only after that call returns
//! does it advance the shared durability watermark and wake the matching
//! thread and the Event Publisher, so no caller is ever told a lost event is
//! durable and no subscriber sees a record above the watermark.
//!
//! The log is a [`SharedEventLog`]: the runtime shares one handle between this
//! writer and the publisher's reader.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::{self, JoinHandle, Thread};
use std::time::Instant;

use crate::durable::{DurableError, EventLog, EventRecord};

use super::RuntimeConfig;
use super::matching::JournalSink;
use super::metrics::RuntimeMetrics;
use super::shared::SharedState;
use super::sync::lock;

/// The event log as shared by the journal writer and the publisher reader.
///
/// The two threads use the same log handle so the publisher needs no adapter
/// hook and cannot observe a different log instance. The mutex covers only one
/// `append_batch` or one `read_from`; synchronous handler dispatch happens
/// after the read lock is released, so a slow consumer cannot block the
/// journal.
pub(crate) type SharedEventLog = Arc<Mutex<Box<dyn EventLog>>>;

/// Something the journal thread can wake after it publishes a watermark
/// advance.
///
/// The production implementation is [`ThreadWaker`], which unparks the matching
/// thread. Tests use a counting waker to observe one wake per advance without
/// running a real matching thread.
pub(crate) trait Waker: Send + 'static {
    fn wake(&self);
}

/// Unparks one fixed thread.
///
/// The matching thread installs its own [`Thread`] handle with
/// [`ThreadWaker::attach`] when it starts; the runtime passes a clone of the
/// same waker to [`spawn`]. Waking before a handle is attached is a no-op.
#[derive(Clone, Default)]
pub(crate) struct ThreadWaker {
    thread: Arc<OnceLock<Thread>>,
}

impl ThreadWaker {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Attach the thread to wake. The first attachment wins; a runtime installs
    /// exactly one matching thread.
    pub(crate) fn attach(&self, thread: Thread) {
        let _ = self.thread.set(thread);
    }
}

impl Waker for ThreadWaker {
    fn wake(&self) {
        if let Some(thread) = self.thread.get() {
            thread.unpark();
        }
    }
}

/// The matching thread's handle to the journal batch channel.
///
/// It never blocks: a full channel returns the batch so the matching thread
/// keeps ownership and retries. `is_disconnected` turns true when the sender
/// observes the receiver gone or when the journal thread exits, which lets the
/// matching thread fail closed even after its last successful handoff.
pub(crate) struct JournalThreadSink {
    sender: SyncSender<Vec<EventRecord>>,
    shared: SharedState,
    alive: Arc<AtomicBool>,
    disconnected: bool,
}

impl JournalSink for JournalThreadSink {
    fn try_send_batch(&mut self, batch: Vec<EventRecord>) -> Result<(), Vec<EventRecord>> {
        match self.sender.try_send(batch) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(batch)) => Err(batch),
            Err(TrySendError::Disconnected(batch)) => {
                self.disconnected = true;
                Err(batch)
            }
        }
    }

    fn is_disconnected(&self) -> bool {
        self.disconnected || !self.alive.load(Ordering::Acquire)
    }

    fn failure(&self) -> Option<DurableError> {
        self.shared.failure()
    }
}

/// Clears the shared "journal alive" flag when the journal thread exits,
/// including on panic. The matching thread polls the flag through the sink.
struct ExitSignal(Arc<AtomicBool>);

impl Drop for ExitSignal {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

struct JournalThread<W: Waker> {
    log: SharedEventLog,
    receiver: Receiver<Vec<EventRecord>>,
    shared: SharedState,
    metrics: Option<Arc<RuntimeMetrics>>,
    coalesce_max: usize,
    waker: W,
    _alive: ExitSignal,
}

impl<W: Waker> JournalThread<W> {
    fn run(mut self) {
        // Records already received but held back because the next group is
        // already at the coalesce cap. They are appended immediately in the
        // next iteration, so no record waits for a new handoff.
        let mut carry: Vec<EventRecord> = Vec::new();

        loop {
            let mut records = if carry.is_empty() {
                match self.receiver.recv() {
                    Ok(batch) => batch,
                    Err(_) => return,
                }
            } else {
                std::mem::take(&mut carry)
            };

            // A single handoff may itself exceed the cap; split it before it
            // reaches `append_batch` so no fsync ever covers more records than
            // the documented maximum.
            if records.len() > self.coalesce_max {
                carry = records.split_off(self.coalesce_max);
            }

            // Everything the matching thread queued while the journal was
            // idle (or busy appending) joins this group, up to the record cap.
            // A batch that does not fit is split; its remainder is carried
            // over and appended in order in the next group.
            let mut disconnected = false;
            while carry.is_empty() && records.len() < self.coalesce_max {
                match self.receiver.try_recv() {
                    Ok(mut batch) => {
                        let room = self.coalesce_max - records.len();
                        if batch.len() <= room {
                            records.append(&mut batch);
                        } else {
                            carry = batch.split_off(room);
                            records.append(&mut batch);
                        }
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        disconnected = true;
                        break;
                    }
                }
            }

            let appended = self.append(records);
            if !appended || (disconnected && carry.is_empty()) {
                return;
            }
        }
    }

    /// Append one coalesced group. Returns `false` when the log failed and the
    /// journal must exit.
    fn append(&mut self, records: Vec<EventRecord>) -> bool {
        let Some(last_sequence) = records.last().map(|record| record.sequence) else {
            // An empty handoff advances nothing; keep waiting for real work.
            return true;
        };

        let started = self.metrics.as_ref().map(|_| Instant::now());
        let result = lock(&self.log).append_batch(records);
        if let (Some(metrics), Some(started)) = (self.metrics.as_ref(), started) {
            metrics.record_journal_append(started.elapsed());
        }

        match result {
            Ok(()) => {
                // The batch is durable now; only now may anyone observe it.
                // The publisher is woken after the watermark advance; it
                // re-reads the log up to that watermark.
                self.shared.publish_durable(last_sequence);
                self.shared.notify_publisher();
                self.waker.wake();
                true
            }
            Err(error) => {
                self.shared.fail(error);
                self.waker.wake();
                false
            }
        }
    }
}

/// Start the journal thread with a log the caller also shares with the
/// publisher.
///
/// The runtime creates the shared log once, passes it here and to
/// [`super::publisher::spawn`]. Tests that only exercise the journal use
/// [`spawn`].
pub(crate) fn spawn_shared<W: Waker>(
    log: SharedEventLog,
    shared: SharedState,
    config: &RuntimeConfig,
    waker: W,
) -> (JournalThreadSink, JoinHandle<()>) {
    let (sender, receiver) = mpsc::sync_channel(config.journal_batch_capacity.max(1));
    let alive = Arc::new(AtomicBool::new(true));
    let sink = JournalThreadSink {
        sender,
        shared: shared.clone(),
        alive: Arc::clone(&alive),
        disconnected: false,
    };
    let thread = JournalThread {
        log,
        receiver,
        shared,
        metrics: config.metrics.clone(),
        coalesce_max: config.journal_coalesce_max.max(1),
        waker,
        _alive: ExitSignal(alive),
    };
    let handle = thread::Builder::new()
        .name("journal".to_string())
        .spawn(move || thread.run())
        .expect("start the journal thread");
    (sink, handle)
}

/// Start the journal thread for a caller that does not need the shared log.
///
/// Returns the matching thread's sink and the journal's join handle. The
/// runtime creates the waker first, passes one clone here and attaches the
/// matching thread's handle to the other.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn spawn<W: Waker>(
    log: Box<dyn EventLog>,
    shared: SharedState,
    config: &RuntimeConfig,
    waker: W,
) -> (JournalThreadSink, JoinHandle<()>) {
    spawn_shared(Arc::new(Mutex::new(log)), shared, config, waker)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durable::{
        DurableError, EVENT_FORMAT_VERSION, EventCursor, EventLog, EventRecord, LogHeader,
    };
    use crate::engine::MatchingEngine;
    use crate::runtime::RuntimeError;
    use crate::runtime::SubmitError;
    use crate::runtime::matching::MatchingThread;
    use crate::runtime::metrics::RuntimeMetrics;
    use crate::runtime::shared::SharedState;
    use crate::runtime::snapshot_gate::NoopSnapshotGate;
    use crate::runtime::submitter::IngressSubmitter;
    use crate::runtime::sync::lock;
    use crate::runtime::ticket::SlotPool;
    use crate::types::{EngineCommand, EngineConfig, OrderRequest, OrderType, Side, TimeInForce};
    use std::ops::RangeInclusive;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::mpsc::{self, Sender, sync_channel};
    use std::sync::{Arc, Condvar, Mutex};
    use std::thread::{self, JoinHandle};
    use std::time::{Duration, Instant};

    const WAIT: Duration = Duration::from_secs(3);

    fn wait_until(mut condition: impl FnMut() -> bool) {
        let deadline = Instant::now() + WAIT;
        while !condition() {
            assert!(Instant::now() < deadline, "condition was not met in time");
            thread::sleep(Duration::from_millis(1));
        }
    }

    fn command(timestamp: u64) -> EngineCommand {
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

    fn record(sequence: u64) -> EventRecord {
        EventRecord {
            format_version: EVENT_FORMAT_VERSION,
            sequence,
            command_id: sequence,
            event: crate::event::DomainEvent::OrderRejected {
                symbol: "BTC-USDT".to_string(),
                reason: crate::types::EngineError::UnknownOrder,
                command: command(sequence),
            },
        }
    }

    fn records(range: RangeInclusive<u64>) -> Vec<EventRecord> {
        range.map(record).collect()
    }

    /// A gate the test log blocks in `append_batch` until the test opens it.
    #[derive(Clone, Default)]
    struct Gate {
        open: Arc<Mutex<bool>>,
        ready: Arc<Condvar>,
    }

    impl Gate {
        fn wait(&self) {
            let mut open = lock(&self.open);
            while !*open {
                open = self
                    .ready
                    .wait(open)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
        }

        fn open(&self) {
            *lock(&self.open) = true;
            self.ready.notify_all();
        }
    }

    #[derive(Default)]
    struct SpyInner {
        batch_sizes: Vec<usize>,
        records: Vec<EventRecord>,
        gate: Option<Gate>,
        notify: Option<Sender<()>>,
        fail_on_call: Option<usize>,
        fail_error: Option<DurableError>,
        panic_on_call: Option<usize>,
    }

    /// An `EventLog` that records batch sizes and can block, fail, or panic on
    /// a chosen `append_batch` call.
    #[derive(Clone, Default)]
    struct SpyLog {
        inner: Arc<Mutex<SpyInner>>,
    }

    impl SpyLog {
        fn with_gate(gate: Gate) -> Self {
            let log = Self::default();
            lock(&log.inner).gate = Some(gate);
            log
        }

        fn failing_on_first(error: DurableError) -> Self {
            let log = Self::default();
            {
                let mut inner = lock(&log.inner);
                inner.fail_on_call = Some(0);
                inner.fail_error = Some(error);
            }
            log
        }

        fn panicking_on_first() -> Self {
            let log = Self::default();
            lock(&log.inner).panic_on_call = Some(0);
            log
        }

        fn notify_on_append(&self, sender: Sender<()>) {
            lock(&self.inner).notify = Some(sender);
        }

        fn batch_sizes(&self) -> Vec<usize> {
            lock(&self.inner).batch_sizes.clone()
        }

        fn records(&self) -> Vec<EventRecord> {
            lock(&self.inner).records.clone()
        }
    }

    impl EventLog for SpyLog {
        fn read_header(&self) -> Result<Option<LogHeader>, DurableError> {
            Ok(None)
        }

        fn write_header(&mut self, _header: LogHeader) -> Result<(), DurableError> {
            Ok(())
        }

        fn append(&mut self, record: EventRecord) -> Result<(), DurableError> {
            self.append_batch(vec![record])
        }

        fn append_batch(&mut self, records: Vec<EventRecord>) -> Result<(), DurableError> {
            let (gate, notify, fail, panic) = {
                let mut inner = lock(&self.inner);
                let call = inner.batch_sizes.len();
                inner.batch_sizes.push(records.len());
                (
                    inner.gate.clone(),
                    inner.notify.clone(),
                    inner.fail_on_call == Some(call),
                    inner.panic_on_call == Some(call),
                )
            };

            if let Some(notify) = notify {
                let _ = notify.send(());
            }
            if panic {
                panic!("journal log panic");
            }
            if let Some(gate) = gate {
                gate.wait();
            }
            if fail {
                return Err(lock(&self.inner)
                    .fail_error
                    .clone()
                    .unwrap_or_else(|| DurableError::AppendFailed("test".to_string())));
            }

            lock(&self.inner).records.extend(records);
            Ok(())
        }

        fn read_from(&self, from: u64) -> Result<Vec<EventRecord>, DurableError> {
            Ok(lock(&self.inner)
                .records
                .iter()
                .filter(|record| record.sequence >= from)
                .cloned()
                .collect())
        }

        fn first_sequence(&self) -> Option<u64> {
            lock(&self.inner)
                .records
                .first()
                .map(|record| record.sequence)
        }

        fn last_sequence(&self) -> Option<u64> {
            lock(&self.inner)
                .records
                .last()
                .map(|record| record.sequence)
        }

        fn last_command_id(&self) -> Option<u64> {
            lock(&self.inner)
                .records
                .last()
                .map(|record| record.command_id)
        }
    }

    /// Counts wakeups so tests can assert one per watermark advance.
    #[derive(Clone, Default)]
    struct CountingWaker {
        wakes: Arc<AtomicU64>,
    }

    impl CountingWaker {
        fn count(&self) -> u64 {
            self.wakes.load(Ordering::SeqCst)
        }
    }

    impl Waker for CountingWaker {
        fn wake(&self) {
            self.wakes.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn config() -> RuntimeConfig {
        RuntimeConfig {
            journal_batch_capacity: 16,
            journal_coalesce_max: 1024,
            ..RuntimeConfig::default()
        }
    }

    /// Start a journal, matching thread, and ingress. The matching thread
    /// attaches its real thread handle to the waker, so this exercises the
    /// production wake path.
    #[allow(clippy::type_complexity)]
    fn spawn_runtime(
        log: SpyLog,
        config: RuntimeConfig,
        metrics: Option<Arc<RuntimeMetrics>>,
    ) -> (
        IngressSubmitter,
        SlotPool,
        SharedState,
        JoinHandle<()>,
        JoinHandle<MatchingEngine>,
    ) {
        let (ingress_sender, ingress_receiver) = sync_channel(config.queue_capacity.max(1));
        let shared = SharedState::new(metrics);
        let waker = ThreadWaker::new();
        let (sink, journal_handle) = spawn(Box::new(log), shared.clone(), &config, waker.clone());
        let pool = SlotPool::from_config(&config);
        let submitter = IngressSubmitter::new(ingress_sender, pool.clone(), shared.clone());
        let matching = MatchingThread::new(
            MatchingEngine::new(EngineConfig::default()),
            ingress_receiver,
            sink,
            NoopSnapshotGate,
            shared.clone(),
            config,
            EventCursor {
                applied_seq: 0,
                durable_seq: 0,
                next_sequence: 1,
                next_command_id: 1,
            },
        );
        let matching_handle = thread::spawn(move || {
            waker.attach(thread::current());
            matching.run()
        });
        (submitter, pool, shared, journal_handle, matching_handle)
    }

    #[test]
    fn journal_sink_returns_batches_when_full_and_reflects_disconnection() {
        let shared = SharedState::new(None);
        let alive = Arc::new(AtomicBool::new(true));
        let (sender, receiver) = sync_channel(1);
        let mut sink = JournalThreadSink {
            sender,
            shared: shared.clone(),
            alive: Arc::clone(&alive),
            disconnected: false,
        };

        sink.try_send_batch(records(1..=1)).expect("channel room");
        let returned = sink
            .try_send_batch(records(2..=2))
            .expect_err("a full channel returns the batch");
        assert_eq!(returned.len(), 1);
        assert!(!sink.is_disconnected());
        assert!(sink.failure().is_none());

        drop(receiver);
        assert!(
            sink.try_send_batch(records(3..=3)).is_err(),
            "a dropped receiver refuses the batch"
        );
        assert!(sink.is_disconnected());

        shared.fail(DurableError::Storage("boom".to_string()));
        assert!(sink.failure().is_some());
    }

    #[test]
    fn journal_sink_reports_a_dead_thread_even_without_a_failed_send() {
        let (sender, _receiver) = sync_channel(1);
        let shared = SharedState::new(None);
        let alive = Arc::new(AtomicBool::new(false));

        let sink = JournalThreadSink {
            sender,
            shared,
            alive,
            disconnected: false,
        };

        assert!(
            sink.is_disconnected(),
            "the exit signal fail-stops the matching thread"
        );
    }

    #[test]
    fn journal_coalesces_queued_batches_into_one_append() {
        let gate = Gate::default();
        let log = SpyLog::with_gate(gate.clone());
        let (started_tx, started_rx) = mpsc::channel();
        log.notify_on_append(started_tx);

        let shared = SharedState::new(None);
        let waker = CountingWaker::default();
        let (mut sink, handle) = spawn(
            Box::new(log.clone()),
            shared.clone(),
            &config(),
            waker.clone(),
        );

        // The first batch starts an append that blocks, letting the rest queue.
        sink.try_send_batch(records(1..=2)).expect("send A");
        started_rx.recv_timeout(WAIT).expect("first append started");
        sink.try_send_batch(records(3..=3)).expect("send B");
        sink.try_send_batch(records(4..=5)).expect("send C");
        sink.try_send_batch(records(6..=6)).expect("send D");

        gate.open();
        wait_until(|| log.records().len() == 6);
        drop(sink);
        handle.join().expect("journal thread");

        assert_eq!(
            log.batch_sizes(),
            vec![2, 4],
            "queued batches coalesce into one append"
        );
        assert_eq!(shared.durable_seq(), 6);
        assert_eq!(waker.count(), 2, "one wake per watermark advance");
        assert!(!shared.is_failed());
    }

    #[test]
    fn journal_stops_draining_at_the_coalesce_maximum() {
        let gate = Gate::default();
        let log = SpyLog::with_gate(gate.clone());
        let (started_tx, started_rx) = mpsc::channel();
        log.notify_on_append(started_tx);

        let shared = SharedState::new(None);
        let config = RuntimeConfig {
            journal_coalesce_max: 3,
            ..config()
        };
        let (mut sink, handle) = spawn(
            Box::new(log.clone()),
            shared.clone(),
            &config,
            CountingWaker::default(),
        );

        sink.try_send_batch(records(1..=1)).expect("send A");
        started_rx.recv_timeout(WAIT).expect("first append started");
        sink.try_send_batch(records(2..=3)).expect("send B");
        sink.try_send_batch(records(4..=4)).expect("send C");
        sink.try_send_batch(records(5..=5)).expect("send D");

        gate.open();
        wait_until(|| log.records().len() == 5);
        drop(sink);
        handle.join().expect("journal thread");

        assert_eq!(
            log.batch_sizes(),
            vec![1, 3, 1],
            "each group stops at the record cap"
        );
        assert_eq!(shared.durable_seq(), 5);
    }

    fn logged_sequences(records: &[EventRecord]) -> Vec<u64> {
        records.iter().map(|record| record.sequence).collect()
    }

    #[test]
    fn journal_splits_an_oversized_batch_at_the_coalesce_cap() {
        let log = SpyLog::default();
        let shared = SharedState::new(None);
        let config = RuntimeConfig {
            journal_coalesce_max: 3,
            ..config()
        };
        let (mut sink, handle) = spawn(
            Box::new(log.clone()),
            shared.clone(),
            &config,
            CountingWaker::default(),
        );

        sink.try_send_batch(records(1..=7)).expect("send");
        drop(sink);
        handle.join().expect("journal thread");

        assert_eq!(
            log.batch_sizes(),
            vec![3, 3, 1],
            "one oversized handoff must be split so every append stays within the cap"
        );
        assert_eq!(shared.durable_seq(), 7);
        assert_eq!(
            logged_sequences(&log.records()),
            (1..=7).collect::<Vec<_>>()
        );
    }

    #[test]
    fn journal_splits_a_mixed_sequence_of_batches_at_the_coalesce_cap() {
        let gate = Gate::default();
        let log = SpyLog::with_gate(gate.clone());
        let (started_tx, started_rx) = mpsc::channel();
        log.notify_on_append(started_tx);
        let shared = SharedState::new(None);
        let config = RuntimeConfig {
            journal_coalesce_max: 4,
            ..config()
        };
        let (mut sink, handle) = spawn(
            Box::new(log.clone()),
            shared.clone(),
            &config,
            CountingWaker::default(),
        );

        // Hold the first append open so the following handoffs queue up.
        sink.try_send_batch(records(1..=3)).expect("send A");
        started_rx.recv_timeout(WAIT).expect("first append started");
        sink.try_send_batch(records(4..=8)).expect("send B");
        sink.try_send_batch(records(9..=10)).expect("send C");

        gate.open();
        wait_until(|| log.records().len() == 10);
        drop(sink);
        handle.join().expect("journal thread");

        assert_eq!(
            log.batch_sizes(),
            vec![3, 4, 3],
            "a batch larger than the remaining room is split, and its remainder is kept"
        );
        assert_eq!(shared.durable_seq(), 10);
        assert_eq!(
            logged_sequences(&log.records()),
            (1..=10).collect::<Vec<_>>(),
            "splitting preserves record order"
        );
    }

    #[test]
    fn journal_handles_a_coalesce_cap_smaller_than_one_batch() {
        let log = SpyLog::default();
        let shared = SharedState::new(None);
        let config = RuntimeConfig {
            journal_coalesce_max: 2,
            ..config()
        };
        let (mut sink, handle) = spawn(
            Box::new(log.clone()),
            shared.clone(),
            &config,
            CountingWaker::default(),
        );

        sink.try_send_batch(records(1..=5)).expect("send");
        drop(sink);
        handle.join().expect("journal thread");

        assert_eq!(log.batch_sizes(), vec![2, 2, 1]);
        assert_eq!(shared.durable_seq(), 5);
        assert_eq!(
            logged_sequences(&log.records()),
            (1..=5).collect::<Vec<_>>()
        );
    }

    #[test]
    fn slot_pool_does_not_exhaust_before_the_ingress_is_full() {
        let gate = Gate::default();
        let log = SpyLog::with_gate(gate.clone());
        let (started_tx, started_rx) = mpsc::channel();
        log.notify_on_append(started_tx);

        let metrics = Arc::new(RuntimeMetrics::new());
        let config = RuntimeConfig {
            queue_capacity: 16,
            apply_batch_max: 64,
            journal_batch_capacity: 64,
            journal_coalesce_max: 1024,
            journal_flush_interval: Duration::from_secs(10),
            metrics: Some(Arc::clone(&metrics)),
            ..config()
        };
        let (submitter, pool, shared, journal_handle, matching_handle) =
            spawn_runtime(log, config, Some(Arc::clone(&metrics)));

        let mut tickets = Vec::new();
        let mut timestamp = 1u64;
        let deadline = Instant::now() + WAIT;

        // Feed commands until the matching thread has a full batch and the
        // journal enters its blocked append.
        while started_rx.try_recv().is_err() {
            match submitter.submit(command(timestamp)) {
                Ok(ticket) => {
                    tickets.push(ticket);
                    timestamp += 1;
                }
                Err(SubmitError::QueueFull) => thread::sleep(Duration::from_millis(1)),
                Err(other) => panic!("unexpected submit error: {other:?}"),
            }
            assert!(
                Instant::now() < deadline,
                "the journal never started its first append"
            );
        }

        // Keep offering commands. The journal is blocked, so applying either
        // stops because the journal channel is full (ingress overload) or
        // because the response-slot pool is exhausted. Only the first is a
        // legitimate `QueueFull`: the ingress must fill first.
        loop {
            match submitter.submit(command(timestamp)) {
                Ok(ticket) => {
                    tickets.push(ticket);
                    timestamp += 1;
                }
                Err(SubmitError::QueueFull) => {
                    // Probe whether the matching thread has genuinely stalled.
                    let applied = metrics.applied();
                    thread::sleep(Duration::from_millis(25));
                    if metrics.applied() == applied {
                        break;
                    }
                }
                Err(other) => panic!("unexpected submit error: {other:?}"),
            }
            assert!(timestamp < 1_000_000, "the ingress never filled");
            assert!(
                Instant::now() < deadline,
                "the ingress never reached a stalled full state"
            );
        }

        assert!(
            pool.available() > 0,
            "the response-slot pool was exhausted before the ingress was full \
             (accepted {})",
            tickets.len()
        );

        gate.open();
        shared.close();
        drop(submitter);
        drop(tickets);
        matching_handle.join().expect("matching thread");
        journal_handle.join().expect("journal thread");
    }

    #[test]
    fn watermark_advances_only_after_append_batch_returns() {
        let gate = Gate::default();
        let log = SpyLog::with_gate(gate.clone());
        let (started_tx, started_rx) = mpsc::channel();
        log.notify_on_append(started_tx);

        let shared = SharedState::new(None);
        let (mut sink, handle) = spawn(
            Box::new(log.clone()),
            shared.clone(),
            &config(),
            CountingWaker::default(),
        );

        sink.try_send_batch(records(1..=3)).expect("send");
        started_rx.recv_timeout(WAIT).expect("append started");

        assert_eq!(
            shared.durable_seq(),
            0,
            "the watermark must not move while append_batch is running"
        );

        gate.open();
        wait_until(|| shared.durable_seq() == 3);
        drop(sink);
        handle.join().expect("journal thread");
    }

    #[test]
    fn append_failure_fails_shared_state_and_exits() {
        let error = DurableError::AppendFailed("disk".to_string());
        let log = SpyLog::failing_on_first(error.clone());
        let shared = SharedState::new(None);
        let waker = CountingWaker::default();
        let (mut sink, handle) = spawn(Box::new(log), shared.clone(), &config(), waker.clone());

        sink.try_send_batch(records(1..=2)).expect("send");
        wait_until(|| shared.is_failed());

        assert_eq!(shared.failure(), Some(error));
        assert_eq!(shared.durable_seq(), 0);
        assert_eq!(waker.count(), 1, "the failure wakes the matching thread");

        handle.join().expect("journal exits after a failed append");
        assert!(sink.is_disconnected());
    }

    #[test]
    fn dropping_the_sink_drains_and_exits_with_every_record() {
        let log = SpyLog::default();
        let shared = SharedState::new(None);
        let (mut sink, handle) = spawn(
            Box::new(log.clone()),
            shared.clone(),
            &config(),
            CountingWaker::default(),
        );

        sink.try_send_batch(records(1..=2)).expect("send A");
        sink.try_send_batch(records(3..=3)).expect("send B");
        sink.try_send_batch(records(4..=5)).expect("send C");
        drop(sink);

        handle.join().expect("journal drains and exits");

        let logged = log.records();
        assert_eq!(logged.len(), 5);
        assert_eq!(
            logged
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );
        assert_eq!(shared.durable_seq(), 5);
        assert!(!shared.is_failed());
    }

    #[test]
    fn durable_acknowledgements_wait_for_the_watermark() {
        let gate = Gate::default();
        let log = SpyLog::with_gate(gate.clone());
        let (started_tx, started_rx) = mpsc::channel();
        log.notify_on_append(started_tx);

        let config = RuntimeConfig {
            apply_batch_max: 1,
            journal_flush_interval: Duration::from_secs(10),
            ..config()
        };
        let (submitter, _pool, shared, journal_handle, matching_handle) =
            spawn_runtime(log.clone(), config, None);

        let ticket = submitter.submit(command(1)).expect("submit");
        ticket.wait_applied_timeout(WAIT).expect("applied");
        started_rx.recv_timeout(WAIT).expect("append started");

        assert_eq!(
            ticket.wait_durable_timeout(Duration::from_millis(50)),
            Err(RuntimeError::Timeout),
            "handoff alone must not be acknowledged as durable"
        );

        gate.open();
        ticket.wait_durable_timeout(WAIT).expect("durable");
        assert_eq!(log.records().len(), 2);

        shared.close();
        drop(submitter);
        matching_handle.join().expect("matching thread");
        journal_handle.join().expect("journal thread");
    }

    #[test]
    fn journal_panic_fails_the_matching_thread_closed() {
        let config = RuntimeConfig {
            apply_batch_max: 1,
            journal_flush_interval: Duration::from_secs(10),
            ..config()
        };
        let (submitter, _pool, shared, journal_handle, matching_handle) =
            spawn_runtime(SpyLog::panicking_on_first(), config, None);

        let ticket = submitter.submit(command(1)).expect("submit");
        ticket.wait_applied_timeout(WAIT).expect("applied");

        let durable = ticket.wait_durable_timeout(WAIT);
        assert!(
            matches!(durable, Err(RuntimeError::EngineFailed(_))),
            "a dead journal must fail the pending durable waiter, got {durable:?}"
        );
        assert!(shared.is_failed());

        matching_handle.join().expect("matching thread");
        assert!(
            journal_handle.join().is_err(),
            "the journal thread panicked"
        );
        assert!(matches!(
            submitter.submit(command(2)),
            Err(SubmitError::EngineFailed(_))
        ));
    }

    #[test]
    fn journal_metrics_count_batch_records_once() {
        let metrics = Arc::new(RuntimeMetrics::new());
        let config = RuntimeConfig {
            apply_batch_max: 2,
            journal_flush_interval: Duration::from_secs(10),
            metrics: Some(Arc::clone(&metrics)),
            ..config()
        };
        let (submitter, _pool, shared, journal_handle, matching_handle) =
            spawn_runtime(SpyLog::default(), config, Some(Arc::clone(&metrics)));

        let tickets = (1..=5)
            .map(|timestamp| submitter.submit(command(timestamp)).expect("submit"))
            .collect::<Vec<_>>();
        for ticket in &tickets {
            ticket.wait_durable_timeout(WAIT).expect("durable");
        }

        shared.close();
        drop(submitter);
        matching_handle.join().expect("matching thread");
        journal_handle.join().expect("journal thread");

        assert_eq!(metrics.applied(), 5);
        assert_eq!(
            metrics.batch_records_sum(),
            10,
            "each event record is counted once, at handoff"
        );
        assert!(metrics.batch_count() >= 1);
        assert!(metrics.journal_append_count() >= 1);
        assert!(
            metrics.journal_append_count() <= metrics.batch_count(),
            "appends can coalesce handoffs but never outnumber them"
        );
    }
}
