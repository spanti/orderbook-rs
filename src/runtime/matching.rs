//! The matching thread: an apply-first command pipeline.
//!
//! The thread owns [`MatchingEngine`] state, takes one command at a time from
//! the ingress command queue, publishes an applied receipt, then hands the
//! command record to the journal as a batch. Durable receipts are completed
//! from the shared durability watermark, never from a guess about the log.
//!
//! This module deliberately does not start threads or implement the public
//! `EngineRuntime` lifecycle: [`MatchingThread::run`] is the loop, and the
//! journal and snapshot sides are narrow seams for the later tickets.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, TrySendError};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use crate::durable::{CommandRecord, DurableError, FORMAT_VERSION};
use crate::engine::MatchingEngine;

use super::RuntimeConfig;
use super::envelope::CommandEnvelope;
use super::error::{RuntimeError, SubmitError};
use super::idle::Idle;
use super::metrics::RuntimeMetrics;
use super::ticket::{CommandReceipt, SlotHandle, SlotPool, Ticket};
use crate::types::EngineCommand;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// State shared by the ingress submitters, the matching thread, the journal
/// thread and the runtime lifecycle.
#[derive(Clone)]
pub(crate) struct SharedState {
    closed: Arc<AtomicBool>,
    failed: Arc<AtomicBool>,
    fatal: Arc<Mutex<Option<DurableError>>>,
    durable_seq: Arc<AtomicU64>,
    /// Envelopes currently in the ingress command queue. Used only to report
    /// queue-depth metrics; it is not a correctness signal.
    ingress_depth: Arc<AtomicU64>,
    metrics: Option<Arc<RuntimeMetrics>>,
}

impl SharedState {
    pub(crate) fn new(metrics: Option<Arc<RuntimeMetrics>>) -> Self {
        Self::with_durable_seq(metrics, Arc::new(AtomicU64::new(0)))
    }

    /// Build shared state whose watermark is owned elsewhere, as the journal
    /// thread will own it in ticket 06.
    pub(crate) fn with_durable_seq(
        metrics: Option<Arc<RuntimeMetrics>>,
        durable_seq: Arc<AtomicU64>,
    ) -> Self {
        Self {
            closed: Arc::new(AtomicBool::new(false)),
            failed: Arc::new(AtomicBool::new(false)),
            fatal: Arc::new(Mutex::new(None)),
            durable_seq,
            ingress_depth: Arc::new(AtomicU64::new(0)),
            metrics,
        }
    }

    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Record the first fatal error and fail the runtime closed.
    pub(crate) fn fail(&self, error: DurableError) {
        let mut fatal = lock(&self.fatal);
        if fatal.is_none() {
            *fatal = Some(error);
        }
        drop(fatal);
        self.failed.store(true, Ordering::Release);
    }

    pub(crate) fn is_failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    pub(crate) fn failure(&self) -> Option<DurableError> {
        lock(&self.fatal).clone()
    }

    pub(crate) fn durable_seq(&self) -> u64 {
        self.durable_seq.load(Ordering::Acquire)
    }

    /// Advance the durability watermark to at least `sequence`.
    pub(crate) fn publish_durable(&self, sequence: u64) {
        self.durable_seq.fetch_max(sequence, Ordering::Release);
    }
}

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
    fn try_send_batch(&mut self, batch: Vec<CommandRecord>) -> Result<(), Vec<CommandRecord>>;

    /// Whether the journal is gone. A disconnected journal fail-stops.
    fn is_disconnected(&self) -> bool;

    /// The journal's fatal error, if it failed asynchronously.
    fn failure(&self) -> Option<DurableError>;
}

/// A sink that accepts and drops every batch. For tests and plumbing that do
/// not need journal behavior.
pub(crate) struct NoopJournalSink;

impl JournalSink for NoopJournalSink {
    fn try_send_batch(&mut self, _batch: Vec<CommandRecord>) -> Result<(), Vec<CommandRecord>> {
        Ok(())
    }

    fn is_disconnected(&self) -> bool {
        false
    }

    fn failure(&self) -> Option<DurableError> {
        None
    }
}

/// Whether the matching thread may keep draining the ingress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GateDecision {
    Continue,
    Pause,
}

/// Snapshot-gate seam. Ticket 07 fills this in so a snapshot is only captured
/// when `applied_seq == durable_seq`, pausing intake at a cadence boundary
/// while durability catches up.
pub(crate) trait SnapshotGate: Send + 'static {
    fn poll(&mut self, engine: &MatchingEngine, applied_seq: u64, durable_seq: u64)
    -> GateDecision;
}

/// A gate that never pauses. Correct when snapshots are disabled.
pub(crate) struct NoopSnapshotGate;

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

/// The internal submitter used by [`crate::runtime::EngineHandle`].
///
/// It checks out a pre-allocated response slot, builds a
/// [`CommandEnvelope`], and offers it to the bounded ingress queue without
/// blocking. Pool exhaustion and a full channel both surface as
/// [`SubmitError::QueueFull`]; the command is not applied and consumes no
/// command sequence.
#[derive(Clone)]
pub(crate) struct IngressSubmitter {
    sender: SyncSender<CommandEnvelope>,
    pool: SlotPool,
    shared: SharedState,
    next_request_id: Arc<AtomicU64>,
}

impl IngressSubmitter {
    pub(crate) fn new(
        sender: SyncSender<CommandEnvelope>,
        pool: SlotPool,
        shared: SharedState,
    ) -> Self {
        Self {
            sender,
            pool,
            shared,
            next_request_id: Arc::new(AtomicU64::new(1)),
        }
    }

    pub(crate) fn submit(&self, command: EngineCommand) -> Result<Ticket, SubmitError> {
        if let Some(error) = self.shared.failure() {
            if let Some(metrics) = &self.shared.metrics {
                metrics.record_engine_failed();
            }
            return Err(SubmitError::EngineFailed(error));
        }
        if self.shared.is_closed() {
            if let Some(metrics) = &self.shared.metrics {
                metrics.record_stopped();
            }
            return Err(SubmitError::Shutdown);
        }

        // No slot means the response pool is exhausted; treat it exactly like
        // a full ingress queue so producers shed load instead of allocating.
        let Some((ticket, slot)) = self.pool.checkout() else {
            if let Some(metrics) = &self.shared.metrics {
                metrics.record_queue_full();
            }
            return Err(SubmitError::QueueFull);
        };

        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let enqueued_at = self.shared.metrics.as_ref().map(|_| Instant::now());
        let envelope = CommandEnvelope::new(request_id, enqueued_at, command, slot);

        match self.sender.try_send(envelope) {
            Ok(()) => {
                self.shared.ingress_depth.fetch_add(1, Ordering::Relaxed);
                if let Some(metrics) = &self.shared.metrics {
                    metrics.record_submitted();
                }
                Ok(ticket)
            }
            Err(TrySendError::Full(_envelope)) => {
                // Dropping the returned envelope releases its slot handle; the
                // ticket is dropped on return, so the slot goes back to the
                // pool. The command was never accepted.
                if let Some(metrics) = &self.shared.metrics {
                    metrics.record_queue_full();
                }
                Err(SubmitError::QueueFull)
            }
            Err(TrySendError::Disconnected(_envelope)) => {
                if let Some(metrics) = &self.shared.metrics {
                    metrics.record_stopped();
                }
                Err(SubmitError::Shutdown)
            }
        }
    }
}

struct PendingDurable {
    sequence: u64,
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

    next_command_sequence: u64,
    applied_seq: u64,
    journal_buffer: Vec<CommandRecord>,
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
        next_command_sequence: u64,
        applied_seq: u64,
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
            next_command_sequence,
            applied_seq,
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
            if front.sequence > durable_seq {
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
        self.snapshot_gate
            .poll(&self.engine, self.applied_seq, self.shared.durable_seq())
    }

    fn note_dequeue(&mut self) {
        let previous = self.shared.ingress_depth.fetch_sub(1, Ordering::Relaxed);
        if let Some(metrics) = &self.shared.metrics {
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
        let outcome = self.engine.execute_ref(&command);

        let sequence = self.next_command_sequence;
        self.next_command_sequence = sequence.saturating_add(1);
        self.applied_seq = sequence;

        slot.set_applied(Ok(CommandReceipt {
            outcome,
            command_sequence: sequence,
        }));

        if let (Some(metrics), Some(apply_started)) = (metrics.as_ref(), apply_started) {
            metrics.record_apply(apply_started.elapsed());
            metrics.record_applied();
        }

        if self.journal_buffer.is_empty() {
            self.buffer_started_at = Some(Instant::now());
        }
        self.journal_buffer.push(CommandRecord {
            format_version: FORMAT_VERSION,
            sequence,
            command,
        });
        self.pending_durable.push_back(PendingDurable {
            sequence,
            enqueued_at,
            slot,
        });

        // Events are derived output; the runtime does not retain them.
        self.engine.clear_events();
    }

    /// Fail every command that cannot become durable and every command still
    /// waiting in the ingress.
    fn fail_stop(&mut self) {
        let error = self.shared.failure().unwrap_or(DurableError::EngineFailed);
        let runtime_error = RuntimeError::EngineFailed(error);

        while let Ok(envelope) = self.ingress.try_recv() {
            self.shared.ingress_depth.fetch_sub(1, Ordering::Relaxed);
            envelope.slot.set_applied(Err(runtime_error.clone()));
            envelope.slot.set_durable(Err(runtime_error.clone()));
        }

        while let Some(pending) = self.pending_durable.pop_front() {
            pending.slot.set_durable(Err(runtime_error.clone()));
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
    use crate::durable::DurableError;
    use crate::runtime::metrics::RuntimeMetrics;
    use crate::types::{
        CommandOutcome, EngineConfig, ModifyRequest, OrderRequest, OrderType, Side, TimeInForce,
    };
    use std::sync::mpsc::sync_channel;
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
        batches: Arc<Mutex<Vec<Vec<CommandRecord>>>>,
        watermark: Option<Arc<AtomicU64>>,
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

        fn set_failure(&self, error: DurableError) {
            *lock(&self.failure) = Some(error);
        }

        fn batches(&self) -> Vec<Vec<CommandRecord>> {
            lock(&self.batches).clone()
        }

        fn records(&self) -> Vec<CommandRecord> {
            lock(&self.batches).iter().flatten().cloned().collect()
        }

        fn record_count(&self) -> usize {
            lock(&self.batches).iter().map(Vec::len).sum()
        }
    }

    impl JournalSink for TestJournal {
        fn try_send_batch(&mut self, batch: Vec<CommandRecord>) -> Result<(), Vec<CommandRecord>> {
            if self.blocked.load(Ordering::Acquire) {
                return Err(batch);
            }
            if lock(&self.failure).is_some() {
                return Err(batch);
            }
            if self.disconnected.load(Ordering::Acquire) {
                return Err(batch);
            }
            if let Some(watermark) = &self.watermark
                && let Some(last) = batch.last()
            {
                watermark.fetch_max(last.sequence, Ordering::Release);
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
            1,
            0,
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
        assert_eq!(applied.command_sequence, 1);
        assert!(matches!(applied.outcome, CommandOutcome::Accepted(_)));

        let durable = tickets[0]
            .wait_durable_timeout(WAIT)
            .expect("durable receipt");
        assert_eq!(durable, applied);

        shared.close();
        let engine = handle.join().expect("matching thread");
        assert!(engine.events().is_empty(), "the runtime clears events");
        assert_eq!(journal.record_count(), 1);
    }

    #[test]
    fn single_producer_runtime_matches_direct_execution() {
        let commands = mixed_commands();

        let mut direct = MatchingEngine::new(EngineConfig::default());
        let expected: Vec<CommandReceipt> = commands
            .iter()
            .enumerate()
            .map(|(index, command)| CommandReceipt {
                outcome: direct.execute(command.clone()),
                command_sequence: index as u64 + 1,
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

        let logged = journal.records();
        assert_eq!(logged.len(), commands.len());
        for (index, (record, command)) in logged.iter().zip(&commands).enumerate() {
            assert_eq!(record.format_version, FORMAT_VERSION);
            assert_eq!(record.sequence, index as u64 + 1);
            assert_eq!(&record.command, command);
        }

        assert_eq!(metrics.applied(), commands.len() as u64);
        assert_eq!(metrics.queue_depth_max(), commands.len() as u64);
        assert_eq!(metrics.batch_records_sum(), commands.len() as u64);
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
                .map(|receipt| receipt.command_sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        assert!(matches!(receipts[0].outcome, CommandOutcome::Accepted(_)));
        assert!(matches!(receipts[1].outcome, CommandOutcome::Accepted(_)));
        assert!(matches!(receipts[2].outcome, CommandOutcome::Canceled(_)));
        assert!(matches!(receipts[3].outcome, CommandOutcome::Accepted(_)));

        shared.close();
        handle.join().expect("matching thread");
        let logged = journal.records();
        assert_eq!(logged.len(), commands.len());
        for (record, command) in logged.iter().zip(&commands) {
            assert_eq!(&record.command, command);
        }
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

        wait_until(|| journal.record_count() == 2);
        let batches = journal.batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 2, "the buffer flushes at apply_batch_max");

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
        wait_until(|| journal.record_count() == 1);

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
        wait_until(|| journal.record_count() == 1);
        assert_eq!(metrics.applied(), 1);
        assert_eq!(journal.batches()[0].len(), 1);

        released.store(true, Ordering::Release);
        for ticket in &tickets {
            ticket.wait_applied_timeout(WAIT).expect("applied");
            ticket.wait_durable_timeout(WAIT).expect("durable");
        }

        shared.close();
        handle.join().expect("matching thread");
        assert_eq!(journal.record_count(), 3);
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
        assert_eq!(journal.record_count(), 3);
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

        shared.publish_durable(applied.command_sequence);
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
        assert_eq!(journal.record_count(), 5);
        assert!(engine.events().is_empty());
    }
}
