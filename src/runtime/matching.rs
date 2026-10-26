//! The matching thread: an apply-first event pipeline.
//!
//! The thread owns [`MatchingEngine`] state, takes one command at a time from
//! the ingress command queue, decides its events, assigns a contiguous event
//! sequence range and one `command_id`, applies every event, publishes an
//! applied receipt carrying the events and range, then hands the
//! [`EventRecord`]s to the journal as a batch. Durable receipts are completed
//! from the shared durability watermark, never from a guess about the log.
//!
//! This module deliberately does not start threads or implement the public
//! `EngineRuntime` lifecycle: [`MatchingThread::run`] is the loop, and the
//! journal and snapshot sides are narrow seams.

use std::collections::VecDeque;
use std::sync::mpsc::{Receiver, TryRecvError};
use std::time::{Duration, Instant};

use crate::durable::{DurableError, EVENT_FORMAT_VERSION, EventCursor, EventRecord};
use crate::engine::MatchingEngine;

use super::RuntimeConfig;
use super::envelope::CommandEnvelope;
use super::error::RuntimeError;
use super::idle::Idle;
use super::shared::SharedState;
use super::snapshot_gate::{GateDecision, SnapshotGate};
use super::ticket::{CommandReceipt, SlotHandle};

/// The journal-side seam.
///
/// The real implementation in ticket 06 wraps the journal batch channel. The
/// matching thread never blocks on the journal: when [`JournalSink::try_send_batch`]
/// reports that the batch cannot be handed off, it returns the batch so the
/// matching thread can retry without losing records or applying more commands.
pub(crate) trait JournalSink: Send + 'static {
    /// Try to hand `batch` to the journal.
    ///
    /// `Err(batch)` means the batch was not accepted (for example because the
    /// journal batch channel is full); the caller keeps ownership and retries.
    fn try_send_batch(&mut self, batch: Vec<EventRecord>) -> Result<(), Vec<EventRecord>>;

    /// Whether the journal is gone. A disconnected journal fail-stops.
    fn is_disconnected(&self) -> bool;

    /// The journal's fatal error, if it failed asynchronously.
    fn failure(&self) -> Option<DurableError>;
}

/// A sink that accepts and drops every batch. For tests and plumbing that do
/// not need journal behavior.
#[cfg(test)]
pub(crate) struct NoopJournalSink;

#[cfg(test)]
impl JournalSink for NoopJournalSink {
    fn try_send_batch(&mut self, _batch: Vec<EventRecord>) -> Result<(), Vec<EventRecord>> {
        Ok(())
    }

    fn is_disconnected(&self) -> bool {
        false
    }

    fn failure(&self) -> Option<DurableError> {
        None
    }
}

struct PendingDurable {
    /// Last event sequence of the command. The watermark must reach it.
    last_event_sequence: u64,
    enqueued_at: Option<Instant>,
    slot: SlotHandle,
}

/// The single matching-thread loop.
///
/// The loop applies one command per iteration so `submit -> applied` latency
/// does not depend on batch size. Journal records are batched separately.
pub(crate) struct MatchingThread<J, G> {
    engine: MatchingEngine,
    ingress: Receiver<CommandEnvelope>,
    journal: J,
    snapshot_gate: G,
    shared: SharedState,
    idle: Idle,
    apply_batch_max: usize,
    journal_flush_interval: Duration,

    next_sequence: u64,
    next_command_id: u64,
    applied_seq: u64,
    journal_buffer: Vec<EventRecord>,
    buffer_started_at: Option<Instant>,
    pending_durable: VecDeque<PendingDurable>,
    ingress_empty: bool,
}

impl<J: JournalSink, G: SnapshotGate> MatchingThread<J, G> {
    pub(crate) fn new(
        engine: MatchingEngine,
        ingress: Receiver<CommandEnvelope>,
        journal: J,
        snapshot_gate: G,
        shared: SharedState,
        config: RuntimeConfig,
        cursor: EventCursor,
    ) -> Self {
        Self {
            engine,
            ingress,
            journal,
            snapshot_gate,
            shared,
            idle: Idle::new(config.idle),
            apply_batch_max: config.apply_batch_max.max(1),
            journal_flush_interval: config.journal_flush_interval,
            next_sequence: cursor.next_sequence,
            next_command_id: cursor.next_command_id,
            applied_seq: cursor.applied_seq,
            journal_buffer: Vec::with_capacity(config.apply_batch_max),
            buffer_started_at: None,
            pending_durable: VecDeque::new(),
            ingress_empty: false,
        }
    }

    /// Run until shutdown: closed, ingress empty, journal buffer flushed, and
    /// no pending durable acknowledgements. Returns the engine so the caller
    /// can snapshot or inspect the final decision state.
    pub(crate) fn run(mut self) -> MatchingEngine {
        loop {
            self.observe_failure();
            if self.shared.is_failed() {
                self.fail_stop();
                return self.engine;
            }

            // Shutdown is bounded: once the deadline elapses the matching
            // thread stops waiting for an unfinished journal instead of
            // spinning forever. All work already completed still counts as a
            // clean shutdown.
            if self.shared.is_closed() && self.shared.shutdown_expired() {
                if self.can_exit() {
                    return self.engine;
                }
                // The deadline can expire in the same iteration that the last
                // command became durable. Confirm the ingress is really empty
                // before reporting a timeout.
                if self.pending_durable.is_empty() && self.journal_buffer.is_empty() {
                    match self.ingress.try_recv() {
                        Err(TryRecvError::Empty | TryRecvError::Disconnected) => {
                            self.ingress_empty = true;
                            return self.engine;
                        }
                        Ok(envelope) => {
                            self.shared.note_ingress_dequeued();
                            envelope.slot.set_applied(Err(RuntimeError::EngineStopped));
                            envelope.slot.set_durable(Err(RuntimeError::EngineStopped));
                        }
                    }
                }
                self.abort_timeout();
                return self.engine;
            }

            // Durable acknowledgements are driven only by the watermark.
            self.drain_durable();

            // A full buffer, an expired flush interval, or an idle ingress all
            // force a flush. If the journal will not take the batch, applying
            // more commands would let applied state run away from the log.
            if self.should_flush_journal() && !self.flush_journal() {
                self.idle.idle();
                continue;
            }

            // The snapshot gate may pause intake while durability catches up.
            if self.gate_decision() == GateDecision::Pause {
                if self.can_exit() {
                    return self.engine;
                }
                self.idle.idle();
                continue;
            }

            let mut progressed = false;
            match self.ingress.try_recv() {
                Ok(envelope) => {
                    self.note_dequeue();
                    self.apply_envelope(envelope);
                    progressed = true;
                }
                Err(TryRecvError::Empty) => {
                    self.ingress_empty = true;
                    if !self.journal_buffer.is_empty() && !self.flush_journal() {
                        self.idle.idle();
                        continue;
                    }
                }
                Err(TryRecvError::Disconnected) => {
                    // No producer can enqueue anything else. With the runtime
                    // holding a sender clone this only happens after teardown.
                    self.ingress_empty = true;
                    self.shared.close();
                    if !self.journal_buffer.is_empty() && !self.flush_journal() {
                        self.idle.idle();
                        continue;
                    }
                }
            }

            if self.can_exit() {
                return self.engine;
            }

            if progressed {
                self.idle.reset();
            } else {
                self.idle.idle();
            }
        }
    }

    fn observe_failure(&mut self) {
        if self.shared.is_failed() {
            return;
        }
        if let Some(error) = self.journal.failure() {
            self.shared.fail(error);
        } else if self.journal.is_disconnected() {
            self.shared.fail(DurableError::Storage(
                "journal thread disconnected".to_string(),
            ));
        }
    }

    fn drain_durable(&mut self) {
        let durable_seq = self.shared.durable_seq();
        while let Some(front) = self.pending_durable.front() {
            if front.last_event_sequence > durable_seq {
                break;
            }
            let pending = self
                .pending_durable
                .pop_front()
                .expect("checked the pending front");
            pending.slot.set_durable(Ok(()));
            if let Some(metrics) = &self.shared.metrics {
                metrics.record_durable();
                if let Some(enqueued_at) = pending.enqueued_at {
                    metrics.record_durable_wait(enqueued_at.elapsed());
                }
            }
        }
    }

    fn should_flush_journal(&self) -> bool {
        if self.journal_buffer.is_empty() {
            return false;
        }
        if self.journal_buffer.len() >= self.apply_batch_max {
            return true;
        }
        self.buffer_started_at
            .is_some_and(|started| started.elapsed() >= self.journal_flush_interval)
    }

    /// Try one flush. Returns `false` when the journal did not accept the
    /// batch; the records are restored in order for the next attempt.
    fn flush_journal(&mut self) -> bool {
        if self.journal_buffer.is_empty() {
            return true;
        }

        let batch = std::mem::take(&mut self.journal_buffer);
        let records = batch.len();
        match self.journal.try_send_batch(batch) {
            Ok(()) => {
                self.buffer_started_at = None;
                if let Some(metrics) = &self.shared.metrics {
                    metrics.record_journal_batch(records);
                }
                true
            }
            Err(batch) => {
                debug_assert!(self.journal_buffer.is_empty());
                self.journal_buffer = batch;
                false
            }
        }
    }

    fn gate_decision(&mut self) -> GateDecision {
        self.snapshot_gate.poll(
            &self.engine,
            self.applied_seq,
            self.shared.durable_seq(),
            self.next_command_id.saturating_sub(1),
        )
    }

    fn note_dequeue(&mut self) {
        if let Some(previous) = self.shared.note_ingress_dequeued()
            && let Some(metrics) = &self.shared.metrics
        {
            metrics.observe_queue_depth(usize::try_from(previous).unwrap_or(usize::MAX));
        }
    }

    fn apply_envelope(&mut self, envelope: CommandEnvelope) {
        let CommandEnvelope {
            enqueued_at,
            command,
            slot,
            ..
        } = envelope;

        let metrics = self.shared.metrics.clone();
        let dequeued_at = metrics.as_ref().map(|_| Instant::now());
        if let (Some(metrics), Some(dequeued_at), Some(enqueued_at)) =
            (metrics.as_ref(), dequeued_at, enqueued_at)
        {
            metrics.record_queue_wait(dequeued_at.saturating_duration_since(enqueued_at));
        }

        let apply_started = metrics.as_ref().map(|_| Instant::now());
        let events = self.engine.decide(&command);

        // `decide` always emits at least one event (a rejection is an event).
        // Keep the empty branch defensive: no event means nothing to journal.
        if events.is_empty() {
            slot.set_applied(Ok(CommandReceipt {
                events,
                first_event_sequence: self.applied_seq,
                last_event_sequence: self.applied_seq,
            }));
            slot.set_durable(Ok(()));
            return;
        }

        let command_id = self.next_command_id;
        let first_event_sequence = self.next_sequence;
        let records: Vec<EventRecord> = events
            .iter()
            .enumerate()
            .map(|(offset, event)| EventRecord {
                format_version: EVENT_FORMAT_VERSION,
                sequence: first_event_sequence.saturating_add(offset as u64),
                command_id,
                event: event.clone(),
            })
            .collect();
        let last_event_sequence = records
            .last()
            .map(|record| record.sequence)
            .expect("a non-empty event list produced no records");

        for record in &records {
            self.engine.apply(&record.event, record.sequence);
        }

        self.next_sequence = last_event_sequence.saturating_add(1);
        self.next_command_id = command_id.saturating_add(1);
        self.applied_seq = last_event_sequence;
        self.shared.publish_applied(last_event_sequence);
        self.shared.publish_applied_command_id(command_id);

        slot.set_applied(Ok(CommandReceipt {
            events,
            first_event_sequence,
            last_event_sequence,
        }));

        if let (Some(metrics), Some(apply_started)) = (metrics.as_ref(), apply_started) {
            metrics.record_apply(apply_started.elapsed());
            metrics.record_applied();
        }

        if self.journal_buffer.is_empty() {
            self.buffer_started_at = Some(Instant::now());
        }
        self.journal_buffer.extend(records);
        self.pending_durable.push_back(PendingDurable {
            last_event_sequence,
            enqueued_at,
            slot,
        });
    }

    /// Reply with `error` at both receipt levels to every envelope still in the
    /// ingress, and mark the ingress empty.
    fn drain_ingress(&mut self, error: &RuntimeError) {
        while let Ok(envelope) = self.ingress.try_recv() {
            self.shared.note_ingress_dequeued();
            envelope.slot.set_applied(Err(error.clone()));
            envelope.slot.set_durable(Err(error.clone()));
        }
        self.ingress_empty = true;
    }

    /// Fail every command that cannot become durable and every command still
    /// waiting in the ingress.
    fn fail_stop(&mut self) {
        let error = self.shared.failure().unwrap_or(DurableError::EngineFailed);
        let runtime_error = RuntimeError::EngineFailed(error);

        self.drain_ingress(&runtime_error);
        while let Some(pending) = self.pending_durable.pop_front() {
            pending.slot.set_durable(Err(runtime_error.clone()));
        }

        self.journal_buffer.clear();
        self.buffer_started_at = None;
    }

    /// Stop waiting for durability at the shutdown deadline.
    ///
    /// Every command still queued or awaiting its durable acknowledgement is
    /// released with [`RuntimeError::EngineStopped`]; nothing is applied after
    /// this point.
    fn abort_timeout(&mut self) {
        let stopped = RuntimeError::EngineStopped;
        self.shared.mark_timed_out();

        self.drain_ingress(&stopped);
        while let Some(pending) = self.pending_durable.pop_front() {
            pending.slot.set_durable(Err(stopped.clone()));
        }

        self.journal_buffer.clear();
        self.buffer_started_at = None;
    }

    fn can_exit(&self) -> bool {
        self.shared.is_closed()
            && self.ingress_empty
            && self.journal_buffer.is_empty()
            && self.pending_durable.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durable::{
        DurableError, EVENT_FORMAT_VERSION, EventCursor, EventRecord, EventSnapshot,
        EventSnapshotStore, InMemoryEventSnapshotStore,
    };
    use crate::event::DomainEvent;
    use crate::runtime::error::SubmitError;
    use crate::runtime::metrics::RuntimeMetrics;
    use crate::runtime::snapshot::{self, SnapshotSend};
    use crate::runtime::snapshot_gate::{CadenceSnapshotGate, NoopSnapshotGate};
    use crate::runtime::submitter::IngressSubmitter;
    use crate::runtime::sync::lock;
    use crate::runtime::ticket::{SlotPool, Ticket};
    use crate::types::{
        EngineCommand, EngineConfig, ModifyRequest, OrderRequest, OrderType, Side, TimeInForce,
    };
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::mpsc::sync_channel;
    use std::sync::{Arc, Mutex};
    use std::thread;

    const WAIT: Duration = Duration::from_secs(3);

    fn limit(timestamp: u64, side: Side, price: u64, quantity: u64) -> OrderRequest {
        OrderRequest {
            side,
            price,
            quantity,
            timestamp,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::GTC,
            post_only: false,
        }
    }

    fn submit(
        symbol: &str,
        timestamp: u64,
        side: Side,
        price: u64,
        quantity: u64,
    ) -> EngineCommand {
        EngineCommand::SubmitOrder {
            symbol: symbol.to_string(),
            request: limit(timestamp, side, price, quantity),
        }
    }

    fn cancel(symbol: &str, order_id: u64) -> EngineCommand {
        EngineCommand::CancelOrder {
            symbol: symbol.to_string(),
            order_id,
        }
    }

    fn simple_command(timestamp: u64) -> EngineCommand {
        submit("BTC-USDT", timestamp, Side::Bid, 100, 1)
    }

    fn wait_until(mut condition: impl FnMut() -> bool) {
        let deadline = Instant::now() + WAIT;
        while !condition() {
            assert!(Instant::now() < deadline, "condition was not met in time");
            thread::sleep(Duration::from_millis(1));
        }
    }

    /// A journal sink that records batches and, when asked, advances the
    /// durability watermark as a real journal thread would.
    #[derive(Clone, Default)]
    struct TestJournal {
        batches: Arc<Mutex<Vec<Vec<EventRecord>>>>,
        watermark: Option<Arc<AtomicU64>>,
        watermark_paused: Arc<AtomicBool>,
        accepted_max: Arc<AtomicU64>,
        blocked: Arc<AtomicBool>,
        disconnected: Arc<AtomicBool>,
        failure: Arc<Mutex<Option<DurableError>>>,
    }

    impl TestJournal {
        fn with_watermark() -> Self {
            Self {
                watermark: Some(Arc::new(AtomicU64::new(0))),
                ..Self::default()
            }
        }

        fn block(&self) {
            self.blocked.store(true, Ordering::Release);
        }

        fn unblock(&self) {
            self.blocked.store(false, Ordering::Release);
        }

        fn disconnect(&self) {
            self.disconnected.store(true, Ordering::Release);
        }

        /// Accept batches without moving the watermark, simulating a journal
        /// that has received records but has not made them durable yet.
        fn pause_watermark(&self) {
            self.watermark_paused.store(true, Ordering::Release);
        }

        /// Make every accepted record durable and resume normal advancement.
        fn release_watermark(&self) {
            self.watermark_paused.store(false, Ordering::Release);
            if let Some(watermark) = &self.watermark {
                watermark.fetch_max(self.accepted_max.load(Ordering::Acquire), Ordering::Release);
            }
        }

        fn set_failure(&self, error: DurableError) {
            *lock(&self.failure) = Some(error);
        }

        fn batches(&self) -> Vec<Vec<EventRecord>> {
            lock(&self.batches).clone()
        }

        fn records(&self) -> Vec<EventRecord> {
            lock(&self.batches).iter().flatten().cloned().collect()
        }

        fn record_count(&self) -> usize {
            lock(&self.batches).iter().map(Vec::len).sum()
        }
    }

    impl JournalSink for TestJournal {
        fn try_send_batch(&mut self, batch: Vec<EventRecord>) -> Result<(), Vec<EventRecord>> {
            if self.blocked.load(Ordering::Acquire) {
                return Err(batch);
            }
            if lock(&self.failure).is_some() {
                return Err(batch);
            }
            if self.disconnected.load(Ordering::Acquire) {
                return Err(batch);
            }
            if let Some(last) = batch.last() {
                self.accepted_max
                    .fetch_max(last.sequence, Ordering::Release);
                if let Some(watermark) = &self.watermark
                    && !self.watermark_paused.load(Ordering::Acquire)
                {
                    watermark.fetch_max(last.sequence, Ordering::Release);
                }
            }
            lock(&self.batches).push(batch);
            Ok(())
        }

        fn is_disconnected(&self) -> bool {
            self.disconnected.load(Ordering::Acquire)
        }

        fn failure(&self) -> Option<DurableError> {
            lock(&self.failure).clone()
        }
    }

    /// A blocking gate for the test snapshot store.
    #[derive(Clone, Default)]
    struct StoreGate {
        open: Arc<Mutex<bool>>,
        ready: Arc<std::sync::Condvar>,
    }

    impl StoreGate {
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

    /// An inspectable [`EventSnapshotStore`] that can block, fail, and compare each
    /// saved sequence with the live durability watermark.
    #[derive(Clone)]
    struct TestSnapshotStore {
        saved: Arc<Mutex<Vec<u64>>>,
        ahead: Arc<Mutex<Vec<(u64, u64)>>>,
        calls: Arc<AtomicU64>,
        fail: Arc<AtomicBool>,
        durable: Option<Arc<AtomicU64>>,
        gate: Option<StoreGate>,
    }

    impl TestSnapshotStore {
        fn new() -> Self {
            Self {
                saved: Arc::new(Mutex::new(Vec::new())),
                ahead: Arc::new(Mutex::new(Vec::new())),
                calls: Arc::new(AtomicU64::new(0)),
                fail: Arc::new(AtomicBool::new(false)),
                durable: None,
                gate: None,
            }
        }

        fn watching(durable: Arc<AtomicU64>) -> Self {
            Self {
                durable: Some(durable),
                ..Self::new()
            }
        }

        fn failing() -> Self {
            let store = Self::new();
            store.fail.store(true, Ordering::Release);
            store
        }

        fn blocked(gate: StoreGate) -> Self {
            Self {
                gate: Some(gate),
                ..Self::new()
            }
        }

        fn saved(&self) -> Vec<u64> {
            lock(&self.saved).clone()
        }

        fn ahead(&self) -> Vec<(u64, u64)> {
            lock(&self.ahead).clone()
        }

        fn calls(&self) -> u64 {
            self.calls.load(Ordering::Acquire)
        }
    }

    impl EventSnapshotStore for TestSnapshotStore {
        fn save(&mut self, snapshot: EventSnapshot) -> Result<(), DurableError> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            if let Some(gate) = &self.gate {
                gate.wait();
            }
            if let Some(durable) = &self.durable {
                let durable = durable.load(Ordering::Acquire);
                if snapshot.applied_seq > durable {
                    lock(&self.ahead).push((snapshot.applied_seq, durable));
                }
            }
            if self.fail.load(Ordering::Acquire) {
                return Err(DurableError::Storage("snapshot store failure".to_string()));
            }
            lock(&self.saved).push(snapshot.applied_seq);
            Ok(())
        }

        fn load_latest(&self) -> Result<Option<EventSnapshot>, DurableError> {
            Ok(None)
        }

        fn load_previous(&self) -> Result<Option<EventSnapshot>, DurableError> {
            Ok(None)
        }
    }

    #[derive(Clone)]
    struct PausingGate {
        pause_after: u64,
        released: Arc<AtomicBool>,
    }

    impl SnapshotGate for PausingGate {
        fn poll(
            &mut self,
            _engine: &MatchingEngine,
            applied_seq: u64,
            _durable_seq: u64,
            _last_command_id: u64,
        ) -> GateDecision {
            if applied_seq >= self.pause_after && !self.released.load(Ordering::Acquire) {
                GateDecision::Pause
            } else {
                GateDecision::Continue
            }
        }
    }

    #[allow(clippy::type_complexity)]
    fn spawn_matching<G, F>(
        engine: MatchingEngine,
        config: RuntimeConfig,
        queue_capacity: usize,
        pool_capacity: usize,
        journal: TestJournal,
        gate: G,
        preload: F,
    ) -> (
        IngressSubmitter,
        Vec<Ticket>,
        SharedState,
        thread::JoinHandle<MatchingEngine>,
    )
    where
        G: SnapshotGate + 'static,
        F: FnOnce(&IngressSubmitter) -> Vec<Ticket>,
    {
        let (sender, receiver) = sync_channel(queue_capacity);
        let pool = SlotPool::new(pool_capacity);
        let durable_seq = journal
            .watermark
            .clone()
            .unwrap_or_else(|| Arc::new(AtomicU64::new(0)));
        let shared = SharedState::with_durable_seq(config.metrics.clone(), durable_seq);
        let submitter = IngressSubmitter::new(sender, pool, shared.clone());
        let tickets = preload(&submitter);
        let matching = MatchingThread::new(
            engine,
            receiver,
            journal,
            gate,
            shared.clone(),
            config,
            EventCursor {
                applied_seq: 0,
                durable_seq: 0,
                next_sequence: 1,
                next_command_id: 1,
            },
        );
        let handle = thread::spawn(move || matching.run());
        (submitter, tickets, shared, handle)
    }

    fn mixed_commands() -> Vec<EngineCommand> {
        vec![
            submit("BTC-USDT", 1, Side::Ask, 100, 5),
            submit("BTC-USDT", 2, Side::Bid, 100, 7),
            cancel("BTC-USDT", 1),
            submit("BTC-USDT", 3, Side::Bid, 98, 0),
            EngineCommand::ModifyOrder {
                symbol: "BTC-USDT".to_string(),
                order_id: 2,
                request: ModifyRequest {
                    price: 99,
                    quantity: 3,
                    timestamp: 4,
                },
            },
            submit("ETH-USDT", 5, Side::Ask, 200, 1),
            cancel("BTC-USDT", 2),
        ]
    }

    fn executed(count: u64) -> MatchingEngine {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        for timestamp in 1..=count {
            let _ = engine.execute(simple_command(timestamp));
        }
        engine
    }

    #[test]
    fn cadence_zero_disables_automatic_snapshots() {
        let store = InMemoryEventSnapshotStore::new();
        let (sink, worker) = snapshot::spawn(Box::new(store.handle()));
        let mut gate = CadenceSnapshotGate::new(0, sink);
        let engine = executed(5);

        for _ in 0..5 {
            assert_eq!(
                gate.poll(&engine, 5, 5, 5),
                GateDecision::Continue,
                "cadence zero never captures"
            );
        }
        let last = gate.last_snapshot_seq();
        drop(gate);
        worker.join().expect("snapshot worker");

        assert_eq!(last, 0);
        assert!(store.load_latest().unwrap().is_none());
    }

    #[test]
    fn cadence_boundary_is_captured_exactly_once() {
        let store = InMemoryEventSnapshotStore::new();
        let (sink, worker) = snapshot::spawn(Box::new(store.handle()));
        let mut gate = CadenceSnapshotGate::new(2, sink);
        let engine = executed(4);

        assert_eq!(gate.poll(&engine, 2, 2, 1), GateDecision::Continue);
        wait_until(|| {
            store
                .load_latest()
                .unwrap()
                .is_some_and(|stored| stored.applied_seq == 2)
        });
        assert_eq!(gate.last_snapshot_seq(), 2);

        for _ in 0..3 {
            assert_eq!(gate.poll(&engine, 2, 2, 1), GateDecision::Continue);
        }
        assert_eq!(
            store.load_previous().unwrap(),
            None,
            "the same boundary is not captured again"
        );

        assert_eq!(gate.poll(&engine, 4, 4, 2), GateDecision::Continue);
        wait_until(|| {
            store
                .load_latest()
                .unwrap()
                .is_some_and(|stored| stored.applied_seq == 4)
        });
        assert_eq!(
            store
                .load_previous()
                .unwrap()
                .map(|stored| stored.applied_seq),
            Some(2)
        );
        assert_eq!(gate.last_snapshot_seq(), 4);

        drop(gate);
        worker.join().expect("snapshot worker");
    }

    #[test]
    fn gate_pauses_and_refuses_a_snapshot_ahead_of_durability() {
        let store = InMemoryEventSnapshotStore::new();
        let (sink, worker) = snapshot::spawn(Box::new(store.handle()));
        let mut gate = CadenceSnapshotGate::new(2, sink);
        let engine = executed(4);

        assert_eq!(gate.poll(&engine, 4, 0, 2), GateDecision::Pause);
        assert_eq!(gate.poll(&engine, 4, 3, 2), GateDecision::Pause);
        thread::sleep(Duration::from_millis(20));
        assert!(
            store.load_latest().unwrap().is_none(),
            "no snapshot may be saved above the watermark"
        );
        assert_eq!(gate.last_snapshot_seq(), 0);

        assert_eq!(gate.poll(&engine, 4, 4, 2), GateDecision::Continue);
        wait_until(|| {
            store
                .load_latest()
                .unwrap()
                .is_some_and(|stored| stored.applied_seq == 4)
        });
        assert_eq!(gate.last_snapshot_seq(), 4);

        drop(gate);
        worker.join().expect("snapshot worker");
    }

    #[test]
    fn a_snapshot_due_while_durability_lags_pauses_intake_and_resumes() {
        let metrics = Arc::new(RuntimeMetrics::new());
        let config = RuntimeConfig {
            snapshot_cadence: 2,
            apply_batch_max: 1,
            journal_flush_interval: Duration::from_secs(10),
            metrics: Some(Arc::clone(&metrics)),
            ..RuntimeConfig::default()
        };
        let journal = TestJournal::with_watermark();
        journal.pause_watermark();
        let store = TestSnapshotStore::new();
        let (sink, worker) = snapshot::spawn(Box::new(store.clone()));
        let gate = CadenceSnapshotGate::new(2, sink);

        let (submitter, tickets, shared, handle) = spawn_matching(
            MatchingEngine::new(EngineConfig::default()),
            config,
            16,
            64,
            journal.clone(),
            gate,
            |submitter| {
                (1..=4)
                    .map(|timestamp| submitter.submit(simple_command(timestamp)).expect("submit"))
                    .collect()
            },
        );
        drop(submitter);

        wait_until(|| metrics.applied() == 1 && journal.record_count() == 2);
        thread::sleep(Duration::from_millis(50));
        assert_eq!(
            metrics.applied(),
            1,
            "intake pauses at the cadence boundary while durability lags"
        );
        assert_eq!(journal.record_count(), 2);
        assert!(
            store.saved().is_empty(),
            "nothing is captured above the watermark"
        );

        journal.release_watermark();
        for ticket in &tickets {
            ticket
                .wait_applied_timeout(WAIT)
                .expect("applied after resume");
            ticket.wait_durable_timeout(WAIT).expect("durable");
        }
        wait_until(|| store.saved().contains(&4));
        assert!(
            store.saved().starts_with(&[2, 4]),
            "the first two cadence boundaries are captured: {:?}",
            store.saved()
        );

        shared.close();
        handle.join().expect("matching thread");
        worker.join().expect("snapshot worker");
    }

    #[test]
    fn a_slow_snapshot_store_does_not_stall_matching() {
        let metrics = Arc::new(RuntimeMetrics::new());
        let config = RuntimeConfig {
            snapshot_cadence: 2,
            apply_batch_max: 1,
            journal_flush_interval: Duration::from_secs(10),
            metrics: Some(Arc::clone(&metrics)),
            ..RuntimeConfig::default()
        };
        let journal = TestJournal::with_watermark();
        let store_gate = StoreGate::default();
        let store = TestSnapshotStore::blocked(store_gate.clone());
        let (sink, worker) = snapshot::spawn(Box::new(store.clone()));
        let gate = CadenceSnapshotGate::new(2, sink);

        let (submitter, tickets, shared, handle) = spawn_matching(
            MatchingEngine::new(EngineConfig::default()),
            config,
            16,
            64,
            journal.clone(),
            gate,
            |submitter| {
                (1..=6)
                    .map(|timestamp| submitter.submit(simple_command(timestamp)).expect("submit"))
                    .collect()
            },
        );
        drop(submitter);

        wait_until(|| store.calls() >= 1);
        for ticket in &tickets {
            ticket
                .wait_applied_timeout(WAIT)
                .expect("a slow store must not stop matching");
        }
        for ticket in &tickets {
            ticket.wait_durable_timeout(WAIT).expect("durable");
        }
        assert_eq!(metrics.applied(), 6);

        store_gate.open();
        shared.close();
        handle.join().expect("matching thread");
        worker.join().expect("snapshot worker");
        assert!(
            !store.saved().is_empty(),
            "queued snapshots are saved once the store resumes"
        );
    }

    #[test]
    fn a_failing_snapshot_store_does_not_fail_durable_commands() {
        let config = RuntimeConfig {
            snapshot_cadence: 2,
            apply_batch_max: 1,
            journal_flush_interval: Duration::from_secs(10),
            ..RuntimeConfig::default()
        };
        let journal = TestJournal::with_watermark();
        let store = TestSnapshotStore::failing();
        let (sink, worker) = snapshot::spawn(Box::new(store.clone()));
        let probe = sink.clone();
        let gate = CadenceSnapshotGate::new(2, sink);

        let (submitter, tickets, shared, handle) = spawn_matching(
            MatchingEngine::new(EngineConfig::default()),
            config,
            16,
            64,
            journal.clone(),
            gate,
            |submitter| {
                (1..=4)
                    .map(|timestamp| submitter.submit(simple_command(timestamp)).expect("submit"))
                    .collect()
            },
        );
        drop(submitter);

        for ticket in &tickets {
            ticket.wait_applied_timeout(WAIT).expect("applied");
            ticket
                .wait_durable_timeout(WAIT)
                .expect("snapshot failures never fail durable commands");
        }

        shared.close();
        handle.join().expect("matching thread");
        wait_until(|| probe.last_error().is_some());
        assert!(store.saved().is_empty());
        drop(probe);
        worker.join().expect("snapshot worker");
    }

    #[test]
    fn a_due_snapshot_retries_at_the_next_durability_boundary() {
        let engine = executed(4);
        let store_gate = StoreGate::default();
        let store = TestSnapshotStore::blocked(store_gate.clone());
        let (sink, worker) = snapshot::spawn(Box::new(store.clone()));

        // Fill the single-slot worker channel while the worker is blocked in
        // its first save: one job is in the worker, one waits in the channel.
        let filler = sink.clone();
        let filler_job = || EventSnapshot {
            format_version: EVENT_FORMAT_VERSION,
            applied_seq: 0,
            last_command_id: 0,
            snapshot: engine.capture_snapshot(),
        };
        assert_eq!(filler.try_send(filler_job()), SnapshotSend::Sent);
        wait_until(|| store.calls() >= 1);
        assert_eq!(filler.try_send(filler_job()), SnapshotSend::Sent);
        assert_eq!(filler.try_send(filler_job()), SnapshotSend::Full);

        let mut gate = CadenceSnapshotGate::new(2, sink);

        // Boundary 2: the worker cannot take the snapshot, so it stays due.
        assert_eq!(gate.poll(&engine, 2, 2, 1), GateDecision::Continue);
        assert_eq!(gate.last_snapshot_seq(), 0, "nothing was captured");
        assert_eq!(store.calls(), 1, "no save was attempted");

        // Drain the fillers so the worker channel has room again.
        store_gate.open();
        wait_until(|| store.calls() >= 2);

        // With the worker channel open again, the still-due snapshot is
        // captured at the next boundary where durability has caught up.
        assert_eq!(gate.poll(&engine, 3, 3, 1), GateDecision::Continue);
        assert_eq!(gate.last_snapshot_seq(), 3);
        wait_until(|| store.saved().contains(&3));

        // Fewer than `cadence` events have been applied since that capture, so
        // sequence 4 is not due.
        assert_eq!(gate.poll(&engine, 4, 4, 2), GateDecision::Continue);
        assert_eq!(gate.last_snapshot_seq(), 3);
        assert!(!store.saved().contains(&4));

        drop(filler);
        drop(gate);
        worker.join().expect("snapshot worker");
    }

    #[test]
    fn snapshots_are_never_saved_ahead_of_the_durability_watermark() {
        let config = RuntimeConfig {
            snapshot_cadence: 2,
            apply_batch_max: 1,
            journal_flush_interval: Duration::from_secs(10),
            ..RuntimeConfig::default()
        };
        let journal = TestJournal::with_watermark();
        let watermark = journal.watermark.clone().expect("test journal watermark");
        let store = TestSnapshotStore::watching(watermark);
        let (sink, worker) = snapshot::spawn(Box::new(store.clone()));
        let gate = CadenceSnapshotGate::new(2, sink);

        let (submitter, tickets, shared, handle) = spawn_matching(
            MatchingEngine::new(EngineConfig::default()),
            config,
            16,
            64,
            journal.clone(),
            gate,
            |submitter| {
                (1..=5)
                    .map(|timestamp| submitter.submit(simple_command(timestamp)).expect("submit"))
                    .collect()
            },
        );
        drop(submitter);

        for ticket in &tickets {
            ticket.wait_durable_timeout(WAIT).expect("durable");
        }

        shared.close();
        handle.join().expect("matching thread");
        worker.join().expect("snapshot worker");

        assert!(
            store.ahead().is_empty(),
            "every snapshot covers only durable commands: {:?}",
            store.ahead()
        );
        // Automatic snapshots are best effort: cadence 4 can legitimately be
        // dropped when the matching thread exits before the single-slot worker
        // drains cadence 2. The contract under test is "never ahead of
        // durability", so assert the guaranteed first boundary and the shape
        // of every saved snapshot rather than the exact set.
        assert!(
            store.saved().contains(&2),
            "the first cadence boundary is captured: {:?}",
            store.saved()
        );
        assert!(
            store.saved().iter().all(|sequence| sequence % 2 == 0),
            "only cadence boundaries are captured: {:?}",
            store.saved()
        );
    }

    #[test]
    fn final_snapshot_is_present_after_shutdown() {
        let journal = TestJournal::with_watermark();
        let snapshots = InMemoryEventSnapshotStore::new();
        let (sink, worker) = snapshot::spawn(Box::new(snapshots.handle()));

        let (submitter, tickets, shared, handle) = spawn_matching(
            MatchingEngine::new(EngineConfig::default()),
            RuntimeConfig::default(),
            16,
            64,
            journal.clone(),
            NoopSnapshotGate,
            |submitter| {
                (1..=3)
                    .map(|timestamp| submitter.submit(simple_command(timestamp)).expect("submit"))
                    .collect()
            },
        );
        drop(submitter);

        for ticket in &tickets {
            ticket.wait_durable_timeout(WAIT).expect("durable");
        }
        shared.close();
        let engine = handle.join().expect("matching thread");

        // Each of the three submitted orders emits `OrderAccepted` and
        // `OrderRested`, so the applied event boundary is 6.
        sink.capture_final(&engine, 6, 6, 3)
            .expect("the final snapshot is saved");
        let stored = snapshots.load_latest().unwrap().expect("latest snapshot");
        assert_eq!(stored.applied_seq, 6);
        assert_eq!(stored.last_command_id, 3);

        let mut direct = MatchingEngine::new(EngineConfig::default());
        let mut sequence = 1u64;
        for timestamp in 1..=3 {
            for event in direct.decide(&simple_command(timestamp)) {
                direct.apply(&event, sequence);
                sequence += 1;
            }
        }
        assert_eq!(stored.snapshot, direct.capture_snapshot());

        drop(sink);
        worker.join().expect("snapshot worker");
    }

    #[test]
    fn noop_journal_sink_accepts_batches_and_reports_no_failure() {
        let mut sink = NoopJournalSink;

        assert!(sink.try_send_batch(Vec::new()).is_ok());
        assert!(!sink.is_disconnected());
        assert!(sink.failure().is_none());
    }

    #[test]
    fn submitter_maps_pool_exhaustion_to_queue_full() {
        let (sender, _receiver) = sync_channel(8);
        let pool = SlotPool::new(1);
        let metrics = Arc::new(RuntimeMetrics::new());
        let shared = SharedState::new(Some(Arc::clone(&metrics)));
        let submitter = IngressSubmitter::new(sender, pool.clone(), shared);

        let _ticket = submitter.submit(simple_command(1)).expect("first slot");
        let result = submitter.submit(simple_command(2));

        assert!(matches!(result, Err(SubmitError::QueueFull)));
        assert_eq!(metrics.queue_full(), 1);
        assert_eq!(pool.available(), 0);
    }

    #[test]
    fn submitter_maps_a_full_ingress_channel_to_queue_full_and_reclaims_the_slot() {
        let (sender, _receiver) = sync_channel(1);
        let pool = SlotPool::new(4);
        let metrics = Arc::new(RuntimeMetrics::new());
        let shared = SharedState::new(Some(Arc::clone(&metrics)));
        let submitter = IngressSubmitter::new(sender, pool.clone(), shared);

        let _ticket = submitter
            .submit(simple_command(1))
            .expect("channel has room");
        let result = submitter.submit(simple_command(2));

        assert!(matches!(result, Err(SubmitError::QueueFull)));
        assert_eq!(metrics.queue_full(), 1);
        assert_eq!(
            pool.available(),
            3,
            "the refused command's slot is reclaimed"
        );
    }

    #[test]
    fn enqueued_at_is_set_only_when_metrics_are_enabled() {
        let (sender, receiver) = sync_channel(4);
        let submitter = IngressSubmitter::new(sender, SlotPool::new(4), SharedState::new(None));
        let ticket = submitter.submit(simple_command(1)).expect("submit");
        let envelope = receiver.recv().expect("envelope");
        assert!(envelope.enqueued_at.is_none());
        drop(envelope);
        drop(ticket);

        let (sender, receiver) = sync_channel(4);
        let metrics = Arc::new(RuntimeMetrics::new());
        let submitter =
            IngressSubmitter::new(sender, SlotPool::new(4), SharedState::new(Some(metrics)));
        let ticket = submitter.submit(simple_command(1)).expect("submit");
        let envelope = receiver.recv().expect("envelope");
        assert!(envelope.enqueued_at.is_some());
        drop(envelope);
        drop(ticket);
    }

    #[test]
    fn metrics_disabled_submission_and_dequeue_touch_no_depth_counter() {
        let shared = SharedState::new(None);
        let (sender, receiver) = sync_channel(4);
        let submitter = IngressSubmitter::new(sender, SlotPool::new(4), shared.clone());

        let _ticket = submitter.submit(simple_command(1)).expect("submit");
        assert_eq!(
            shared.ingress_depth.load(Ordering::Relaxed),
            0,
            "metrics disabled must not update the ingress depth gauge"
        );

        let envelope = receiver.recv().expect("envelope");
        assert_eq!(
            shared.note_ingress_dequeued(),
            None,
            "metrics disabled must not decrement the ingress depth gauge"
        );
        assert_eq!(shared.ingress_depth.load(Ordering::Relaxed), 0);
        drop(envelope);
    }

    #[test]
    fn high_concurrency_ingress_depth_never_wraps_or_exceeds_the_queue_bound() {
        const PRODUCERS: usize = 8;
        const PER_PRODUCER: usize = 500;
        const QUEUE_CAPACITY: usize = 8;

        let metrics = Arc::new(RuntimeMetrics::new());
        let shared = SharedState::new(Some(Arc::clone(&metrics)));
        let (sender, receiver) = sync_channel(QUEUE_CAPACITY);
        let submitter = IngressSubmitter::new(
            sender,
            SlotPool::new(PRODUCERS * PER_PRODUCER + 16),
            shared.clone(),
        );

        // A consumer that decrements as fast as it can, so the old
        // increment-after-send order would underflow the gauge.
        let consumer_shared = shared.clone();
        let consumer_metrics = Arc::clone(&metrics);
        let consumer = thread::spawn(move || {
            while let Ok(envelope) = receiver.recv() {
                if let Some(previous) = consumer_shared.note_ingress_dequeued() {
                    consumer_metrics
                        .observe_queue_depth(usize::try_from(previous).unwrap_or(usize::MAX));
                }
                drop(envelope);
            }
        });

        let producers = (0..PRODUCERS)
            .map(|producer| {
                let submitter = submitter.clone();
                thread::spawn(move || {
                    let mut accepted = 0usize;
                    let mut attempts = 0u64;
                    while accepted < PER_PRODUCER {
                        let command = simple_command(producer as u64 * 10_000 + attempts + 1);
                        match submitter.submit(command) {
                            Ok(ticket) => {
                                accepted += 1;
                                drop(ticket);
                            }
                            Err(SubmitError::QueueFull) => std::hint::spin_loop(),
                            Err(other) => panic!("unexpected submit error: {other:?}"),
                        }
                        attempts += 1;
                        assert!(attempts < 1_000_000, "producer made no progress");
                    }
                })
            })
            .collect::<Vec<_>>();

        for producer in producers {
            producer.join().expect("producer thread");
        }
        drop(submitter);
        consumer.join().expect("consumer thread");

        let max = metrics.queue_depth_max();
        assert!(max >= 1, "the gauge observed the queue at least once");
        assert!(max < u64::MAX, "the ingress depth gauge wrapped");
        assert!(
            max <= (QUEUE_CAPACITY + PRODUCERS) as u64,
            "queue depth {max} exceeded the bounded ingress plus concurrent submitters"
        );
        assert_eq!(
            shared.ingress_depth.load(Ordering::Relaxed),
            0,
            "every accepted envelope was accounted for"
        );
    }

    #[test]
    fn matching_thread_applies_one_command_and_completes_durability() {
        let journal = TestJournal::with_watermark();
        let (submitter, tickets, shared, handle) = spawn_matching(
            MatchingEngine::new(EngineConfig::default()),
            RuntimeConfig::default(),
            8,
            16,
            journal.clone(),
            NoopSnapshotGate,
            |submitter| vec![submitter.submit(simple_command(1)).expect("submit")],
        );
        drop(submitter);

        let applied = tickets[0]
            .wait_applied_timeout(WAIT)
            .expect("applied receipt");
        assert_eq!(applied.first_event_sequence, 1);
        assert_eq!(applied.last_event_sequence, 2);
        assert_eq!(applied.events.len(), 2, "accepted then rested");

        let durable = tickets[0]
            .wait_durable_timeout(WAIT)
            .expect("durable receipt");
        assert_eq!(durable, applied);

        shared.close();
        let engine = handle.join().expect("matching thread");
        assert!(engine.events().is_empty(), "the runtime retains no events");
        assert_eq!(journal.record_count(), 2);
    }

    #[test]
    fn single_producer_runtime_matches_direct_execution() {
        let commands = mixed_commands();

        let mut direct = MatchingEngine::new(EngineConfig::default());
        let mut next_sequence = 1u64;
        let expected: Vec<CommandReceipt> = commands
            .iter()
            .map(|command| {
                let events = direct.decide(command);
                let first_event_sequence = next_sequence;
                for (offset, event) in events.iter().enumerate() {
                    direct.apply(event, first_event_sequence + offset as u64);
                }
                let last_event_sequence = first_event_sequence + events.len() as u64 - 1;
                next_sequence = last_event_sequence + 1;
                CommandReceipt {
                    events,
                    first_event_sequence,
                    last_event_sequence,
                }
            })
            .collect();

        let metrics = Arc::new(RuntimeMetrics::new());
        let config = RuntimeConfig {
            apply_batch_max: 3,
            metrics: Some(Arc::clone(&metrics)),
            ..RuntimeConfig::default()
        };
        let journal = TestJournal::with_watermark();
        let (submitter, tickets, shared, handle) = spawn_matching(
            MatchingEngine::new(EngineConfig::default()),
            config,
            16,
            64,
            journal.clone(),
            NoopSnapshotGate,
            |submitter| {
                commands
                    .iter()
                    .map(|command| submitter.submit(command.clone()).expect("submit"))
                    .collect()
            },
        );
        drop(submitter);

        for (ticket, expected) in tickets.iter().zip(&expected) {
            assert_eq!(
                &ticket.wait_applied_timeout(WAIT).expect("applied"),
                expected
            );
        }
        for ticket in &tickets {
            ticket.wait_durable_timeout(WAIT).expect("durable");
        }

        shared.close();
        let runtime_engine = handle.join().expect("matching thread");

        assert_eq!(
            runtime_engine.capture_snapshot(),
            direct.capture_snapshot(),
            "runtime and direct execution reach the same decision state"
        );
        assert!(runtime_engine.events().is_empty());

        let expected_events: Vec<DomainEvent> = expected
            .iter()
            .flat_map(|receipt| receipt.events.iter().cloned())
            .collect();
        let logged = journal.records();
        assert_eq!(logged.len(), expected_events.len());
        for (index, (record, event)) in logged.iter().zip(&expected_events).enumerate() {
            assert_eq!(record.format_version, EVENT_FORMAT_VERSION);
            assert_eq!(record.sequence, index as u64 + 1);
            assert_eq!(&record.event, event);
        }
        // Every command's events share one `command_id`, in submission order.
        let mut offset = 0usize;
        for (index, receipt) in expected.iter().enumerate() {
            let group = &logged[offset..offset + receipt.events.len()];
            assert!(
                group
                    .iter()
                    .all(|record| record.command_id == index as u64 + 1),
                "command {} events must share command_id {}",
                index,
                index + 1
            );
            offset += receipt.events.len();
        }

        assert_eq!(metrics.applied(), commands.len() as u64);
        assert_eq!(metrics.queue_depth_max(), commands.len() as u64);
        assert_eq!(
            metrics.batch_records_sum(),
            expected_events.len() as u64,
            "journal batches are counted in event records"
        );
    }

    #[test]
    fn one_producers_commands_reach_the_log_in_submission_order() {
        let commands = vec![
            submit("BTC-USDT", 1, Side::Ask, 100, 5),
            submit("BTC-USDT", 2, Side::Bid, 100, 3),
            cancel("BTC-USDT", 1),
            submit("BTC-USDT", 3, Side::Bid, 100, 1),
        ];

        let journal = TestJournal::with_watermark();
        let (submitter, tickets, shared, handle) = spawn_matching(
            MatchingEngine::new(EngineConfig::default()),
            RuntimeConfig::default(),
            16,
            32,
            journal.clone(),
            NoopSnapshotGate,
            |submitter| {
                commands
                    .iter()
                    .map(|command| submitter.submit(command.clone()).expect("submit"))
                    .collect()
            },
        );
        drop(submitter);

        let receipts: Vec<CommandReceipt> = tickets
            .iter()
            .map(|ticket| ticket.wait_applied_timeout(WAIT).expect("applied"))
            .collect();
        assert_eq!(
            receipts
                .iter()
                .map(|receipt| (receipt.first_event_sequence, receipt.last_event_sequence))
                .collect::<Vec<_>>(),
            vec![(1, 2), (3, 4), (5, 5), (6, 7)]
        );
        assert!(matches!(
            receipts[0].events[0],
            DomainEvent::OrderAccepted { .. }
        ));
        assert!(matches!(
            receipts[1].events[0],
            DomainEvent::OrderAccepted { .. }
        ));
        assert!(matches!(
            receipts[2].events[0],
            DomainEvent::OrderCanceled { .. }
        ));
        assert!(matches!(
            receipts[3].events[0],
            DomainEvent::OrderAccepted { .. }
        ));

        shared.close();
        handle.join().expect("matching thread");
        let logged = journal.records();
        assert_eq!(logged.len(), 7);
        for (index, record) in logged.iter().enumerate() {
            assert_eq!(record.sequence, index as u64 + 1);
        }
        let command_ids: Vec<u64> = logged.iter().map(|record| record.command_id).collect();
        assert_eq!(command_ids, vec![1, 1, 2, 2, 3, 4, 4]);
    }

    #[test]
    fn journal_buffer_flushes_when_it_reaches_apply_batch_max() {
        let config = RuntimeConfig {
            apply_batch_max: 2,
            journal_flush_interval: Duration::from_secs(10),
            ..RuntimeConfig::default()
        };
        let journal = TestJournal::with_watermark();
        let (submitter, tickets, shared, handle) = spawn_matching(
            MatchingEngine::new(EngineConfig::default()),
            config,
            8,
            16,
            journal.clone(),
            NoopSnapshotGate,
            |submitter| {
                vec![
                    submitter.submit(simple_command(1)).expect("submit"),
                    submitter.submit(simple_command(2)).expect("submit"),
                ]
            },
        );
        drop(submitter);

        wait_until(|| journal.record_count() == 4);
        let batches = journal.batches();
        assert_eq!(batches.len(), 2, "each command emits two events");
        assert_eq!(batches[0].len(), 2, "the buffer flushes at apply_batch_max");
        assert_eq!(batches[1].len(), 2);

        for ticket in &tickets {
            ticket.wait_durable_timeout(WAIT).expect("durable");
        }
        shared.close();
        handle.join().expect("matching thread");
    }

    #[test]
    fn journal_buffer_flushes_when_the_ingress_is_empty() {
        let config = RuntimeConfig {
            apply_batch_max: 64,
            journal_flush_interval: Duration::from_secs(10),
            ..RuntimeConfig::default()
        };
        let journal = TestJournal::with_watermark();
        let (submitter, ticket, shared, handle) = {
            let (submitter, mut tickets, shared, handle) = spawn_matching(
                MatchingEngine::new(EngineConfig::default()),
                config,
                8,
                16,
                journal.clone(),
                NoopSnapshotGate,
                |submitter| vec![submitter.submit(simple_command(1)).expect("submit")],
            );
            (
                submitter,
                tickets.pop().expect("one ticket"),
                shared,
                handle,
            )
        };
        drop(submitter);

        ticket.wait_durable_timeout(WAIT).expect("durable");
        wait_until(|| journal.record_count() == 2);

        shared.close();
        let engine = handle.join().expect("matching thread");
        assert!(engine.events().is_empty());
    }

    #[test]
    fn journal_buffer_flushes_when_the_interval_elapses_while_gated() {
        let metrics = Arc::new(RuntimeMetrics::new());
        let config = RuntimeConfig {
            apply_batch_max: 64,
            journal_flush_interval: Duration::from_millis(20),
            metrics: Some(Arc::clone(&metrics)),
            ..RuntimeConfig::default()
        };
        let released = Arc::new(AtomicBool::new(false));
        let gate = PausingGate {
            pause_after: 1,
            released: Arc::clone(&released),
        };
        let journal = TestJournal::with_watermark();
        let (submitter, tickets, shared, handle) = spawn_matching(
            MatchingEngine::new(EngineConfig::default()),
            config,
            8,
            16,
            journal.clone(),
            gate,
            |submitter| {
                (1..=3)
                    .map(|timestamp| submitter.submit(simple_command(timestamp)).expect("submit"))
                    .collect()
            },
        );
        drop(submitter);

        wait_until(|| metrics.applied() == 1);
        // The gate pauses intake after the first command; only the buffered
        // record can flush, and only because the flush interval expires.
        wait_until(|| journal.record_count() == 2);
        assert_eq!(metrics.applied(), 1);
        assert_eq!(journal.batches()[0].len(), 2);

        released.store(true, Ordering::Release);
        for ticket in &tickets {
            ticket.wait_applied_timeout(WAIT).expect("applied");
            ticket.wait_durable_timeout(WAIT).expect("durable");
        }

        shared.close();
        handle.join().expect("matching thread");
        assert_eq!(journal.record_count(), 6);
    }

    #[test]
    fn journal_backpressure_stops_further_application() {
        let metrics = Arc::new(RuntimeMetrics::new());
        let config = RuntimeConfig {
            apply_batch_max: 1,
            journal_flush_interval: Duration::from_secs(10),
            metrics: Some(Arc::clone(&metrics)),
            ..RuntimeConfig::default()
        };
        let journal = TestJournal::with_watermark();
        journal.block();

        let (submitter, tickets, shared, handle) = spawn_matching(
            MatchingEngine::new(EngineConfig::default()),
            config,
            8,
            16,
            journal.clone(),
            NoopSnapshotGate,
            |submitter| {
                (1..=3)
                    .map(|timestamp| submitter.submit(simple_command(timestamp)).expect("submit"))
                    .collect()
            },
        );
        drop(submitter);

        wait_until(|| metrics.applied() >= 1);
        thread::sleep(Duration::from_millis(50));
        assert_eq!(
            metrics.applied(),
            1,
            "the matching thread must not apply while the journal buffer cannot flush"
        );
        assert_eq!(journal.record_count(), 0);

        journal.unblock();
        for ticket in &tickets {
            ticket.wait_durable_timeout(WAIT).expect("durable");
        }

        shared.close();
        handle.join().expect("matching thread");
        assert_eq!(metrics.applied(), 3);
        assert_eq!(journal.record_count(), 6);
    }

    #[test]
    fn journal_failure_fails_pending_and_queued_commands_and_rejects_new_submissions() {
        let metrics = Arc::new(RuntimeMetrics::new());
        let config = RuntimeConfig {
            apply_batch_max: 1,
            journal_flush_interval: Duration::from_secs(10),
            metrics: Some(Arc::clone(&metrics)),
            ..RuntimeConfig::default()
        };
        let journal = TestJournal::with_watermark();
        journal.block();

        let (submitter, tickets, shared, handle) = spawn_matching(
            MatchingEngine::new(EngineConfig::default()),
            config,
            8,
            16,
            journal.clone(),
            NoopSnapshotGate,
            |submitter| {
                (1..=3)
                    .map(|timestamp| submitter.submit(simple_command(timestamp)).expect("submit"))
                    .collect()
            },
        );

        tickets[0]
            .wait_applied_timeout(WAIT)
            .expect("first applied");

        let failure = DurableError::AppendFailed("disk".to_string());
        journal.set_failure(failure.clone());

        assert_eq!(
            tickets[0].wait_durable_timeout(WAIT),
            Err(RuntimeError::EngineFailed(failure.clone()))
        );
        assert_eq!(
            tickets[1].wait_applied_timeout(WAIT),
            Err(RuntimeError::EngineFailed(failure.clone()))
        );
        assert_eq!(
            tickets[2].wait_durable_timeout(WAIT),
            Err(RuntimeError::EngineFailed(failure.clone()))
        );

        let rejected = submitter.submit(simple_command(99));
        assert!(matches!(
            rejected,
            Err(SubmitError::EngineFailed(error)) if error == failure
        ));

        let engine = handle.join().expect("matching thread");
        assert!(engine.events().is_empty());
        assert!(shared.is_failed());
    }

    #[test]
    fn journal_disconnection_fail_stops_the_runtime() {
        let journal = TestJournal::default();
        journal.disconnect();

        let (submitter, tickets, shared, handle) = spawn_matching(
            MatchingEngine::new(EngineConfig::default()),
            RuntimeConfig::default(),
            8,
            16,
            journal,
            NoopSnapshotGate,
            |submitter| vec![submitter.submit(simple_command(1)).expect("submit")],
        );

        let failure = DurableError::Storage("journal thread disconnected".to_string());
        assert_eq!(
            tickets[0].wait_applied_timeout(WAIT),
            Err(RuntimeError::EngineFailed(failure.clone()))
        );
        assert_eq!(
            tickets[0].wait_durable_timeout(WAIT),
            Err(RuntimeError::EngineFailed(failure.clone()))
        );
        assert!(matches!(
            submitter.submit(simple_command(2)),
            Err(SubmitError::EngineFailed(error)) if error == failure
        ));

        handle.join().expect("matching thread");
        assert!(shared.is_failed());
    }

    #[test]
    fn durable_acknowledgements_follow_the_watermark() {
        let journal = TestJournal::default();
        let (submitter, tickets, shared, handle) = spawn_matching(
            MatchingEngine::new(EngineConfig::default()),
            RuntimeConfig::default(),
            8,
            16,
            journal,
            NoopSnapshotGate,
            |submitter| vec![submitter.submit(simple_command(1)).expect("submit")],
        );
        drop(submitter);

        let applied = tickets[0]
            .wait_applied_timeout(WAIT)
            .expect("applied receipt");
        assert_eq!(
            tickets[0].wait_durable_timeout(Duration::from_millis(50)),
            Err(RuntimeError::Timeout),
            "durability must not be confirmed before the watermark advances"
        );

        shared.publish_durable(applied.last_event_sequence);
        assert_eq!(
            tickets[0].wait_durable_timeout(WAIT).expect("durable"),
            applied
        );

        shared.close();
        handle.join().expect("matching thread");
    }

    #[test]
    fn shutdown_drains_the_ingress_and_completes_every_ticket() {
        let metrics = Arc::new(RuntimeMetrics::new());
        let config = RuntimeConfig {
            metrics: Some(Arc::clone(&metrics)),
            ..RuntimeConfig::default()
        };
        let journal = TestJournal::with_watermark();
        let (submitter, tickets, shared, handle) = spawn_matching(
            MatchingEngine::new(EngineConfig::default()),
            config,
            16,
            32,
            journal.clone(),
            NoopSnapshotGate,
            |submitter| {
                (1..=5)
                    .map(|timestamp| submitter.submit(simple_command(timestamp)).expect("submit"))
                    .collect()
            },
        );

        shared.close();
        assert!(matches!(
            submitter.submit(simple_command(6)),
            Err(SubmitError::Shutdown)
        ));

        let engine = handle.join().expect("matching thread");
        for ticket in &tickets {
            ticket
                .wait_durable_timeout(WAIT)
                .expect("every drained command is durable");
        }
        assert_eq!(metrics.applied(), 5);
        assert_eq!(journal.record_count(), 10);
        assert!(engine.events().is_empty());
    }
}
