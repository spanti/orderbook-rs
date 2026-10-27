//! Public runtime lifecycle: create/open, shutdown, and submission handles.
//!
//! One [`EngineRuntime`] owns the matching thread, the journal thread, and the
//! snapshot worker. Producers only hold cheap [`EngineHandle`] clones, so the
//! single-writer invariant is structural rather than a convention.
//!
//! Shutdown has two paths. [`EngineRuntime::shutdown`] drains the ingress,
//! waits for durability (bounded by [`RuntimeConfig::shutdown_timeout`]),
//! writes a final snapshot, and joins every thread. [`Drop`] performs the same
//! stop without the final snapshot and ignores errors; both paths are
//! idempotent, so dropping a runtime that was explicitly shut down is a no-op.

use std::sync::atomic::AtomicU64;
use std::sync::mpsc;
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::Duration;

use crate::bus::{InProcessBus, MessageBus, SubscriberBus};
use crate::durable::{EventCursor, EventLog, FORMAT_VERSION, LogHeader, SnapshotStore, recover};
use crate::engine::MatchingEngine;
use crate::types::{EngineCommand, EngineConfig};

use super::RuntimeConfig;
use super::assembly::RuntimeThreads;
use super::error::{RuntimeError, SubmitError};
use super::journal::{self, SharedEventLog, ThreadWaker};
use super::matching::MatchingThread;
use super::metrics::RuntimeMetrics;
use super::publisher;
use super::shared::SharedState;
use super::snapshot::{self, SnapshotSink};
use super::snapshot_gate::{CadenceSnapshotGate, GateDecision, SnapshotGate};
use super::submitter::IngressSubmitter;
use super::ticket::{CommandReceipt, SlotPool, Ticket};

/// A cloneable, thread-safe submission handle.
///
/// The handle owns no engine state: it reserves a response slot, offers a
/// [`CommandEnvelope`](crate::runtime::CommandEnvelope) to the bounded ingress
/// Command Queue, and observes the
/// result through a [`Ticket`]. Handles may outlive the runtime; after
/// shutdown every submission is refused.
#[derive(Clone)]
pub struct EngineHandle {
    submitter: IngressSubmitter,
    shared: SharedState,
    metrics: Option<Arc<RuntimeMetrics>>,
    bus: SubscriberBus,
}

impl EngineHandle {
    /// A subscriber-only view of the bus durable events are published on.
    ///
    /// Subscribers register on [`crate::bus::EVENTS_TOPIC`] and downcast each
    /// message to [`crate::durable::EventRecord`]. Events that were already
    /// published (for example while this handle was not subscribed) are read
    /// from the event log with `read_from(last_seen + 1)`.
    ///
    /// The view deliberately exposes no publisher: only the Event Publisher
    /// writes to the bus, so holding a handle cannot make the bus a command
    /// ingress.
    pub fn bus(&self) -> SubscriberBus {
        self.bus.clone()
    }

    /// Try to submit without blocking.
    ///
    /// Returns [`SubmitError::QueueFull`] when the ingress Command Queue or the
    /// response-slot pool is exhausted, [`SubmitError::Shutdown`] after
    /// shutdown starts, and [`SubmitError::EngineFailed`] after a fatal journal
    /// failure. The command is not applied and consumes no event sequence on
    /// any error; clone it first to retry.
    pub fn try_submit(&self, command: EngineCommand) -> Result<Ticket, SubmitError> {
        self.submitter.submit(command)
    }

    /// Submit and wait until all of the command's event records are durable.
    pub fn submit_and_wait(&self, command: EngineCommand) -> Result<CommandReceipt, RuntimeError> {
        let ticket = self.try_submit(command)?;
        ticket.wait_durable()
    }

    /// Submit and wait only for the preliminary applied acknowledgement.
    pub fn submit_and_wait_applied(
        &self,
        command: EngineCommand,
    ) -> Result<CommandReceipt, RuntimeError> {
        let ticket = self.try_submit(command)?;
        ticket.wait_applied()
    }

    /// Whether the runtime failed closed after a fatal journal failure.
    pub fn is_failed(&self) -> bool {
        self.shared.run().is_failed()
    }

    /// This runtime's metrics sink, if metrics are enabled.
    pub fn metrics(&self) -> Option<&Arc<RuntimeMetrics>> {
        self.metrics.as_ref()
    }
}

/// The owner of the matching runtime's three threads.
///
/// Dropping the runtime stops it; prefer [`EngineRuntime::shutdown`] when a
/// final snapshot is wanted.
pub struct EngineRuntime {
    shared: SharedState,
    config: RuntimeConfig,
    bus: Arc<dyn MessageBus>,
    threads: RuntimeThreads,
    stopped: bool,
}

impl EngineRuntime {
    /// Start a fresh runtime, writing the log header.
    ///
    /// The engine starts empty: `applied_seq = 0`, `durable_seq = 0`, and the
    /// first event sequence and `command_id` are both `1`.
    pub fn create(
        config: RuntimeConfig,
        engine_config: EngineConfig,
        log: Box<dyn EventLog>,
        snapshots: Box<dyn SnapshotStore>,
    ) -> Result<(EngineHandle, Self), RuntimeError> {
        Self::create_with_bus(
            config,
            engine_config,
            log,
            snapshots,
            Arc::new(InProcessBus::new()),
        )
    }

    /// Start a fresh runtime that publishes durable events on `bus`.
    ///
    /// The caller may keep a clone of `bus` and register subscribers before
    /// submitting commands.
    pub fn create_with_bus(
        config: RuntimeConfig,
        engine_config: EngineConfig,
        mut log: Box<dyn EventLog>,
        snapshots: Box<dyn SnapshotStore>,
        bus: Arc<dyn MessageBus>,
    ) -> Result<(EngineHandle, Self), RuntimeError> {
        log.write_header(LogHeader {
            format_version: FORMAT_VERSION,
            config: engine_config,
        })
        .map_err(|error| RuntimeError::Startup(error.to_string()))?;

        Self::start(
            config,
            MatchingEngine::new(engine_config),
            log,
            snapshots,
            EventCursor::fresh(),
            bus,
        )
    }

    /// Recover from an existing log and snapshot store.
    ///
    /// Uses the same borrowed recovery helper as the synchronous
    /// [`crate::durable::DurableMatchingEngine`], so both write paths apply the
    /// header, version, config, gap, snapshot-ahead, and contiguity checks, and
    /// continue the event sequence and `command_id` from the recovered cursor.
    pub fn open(
        config: RuntimeConfig,
        log: Box<dyn EventLog>,
        snapshots: Box<dyn SnapshotStore>,
    ) -> Result<(EngineHandle, Self), RuntimeError> {
        Self::open_with_bus(config, log, snapshots, Arc::new(InProcessBus::new()))
    }

    /// Recover from `log` and publish only live durable events on `bus`.
    ///
    /// The publisher starts at the recovered durable watermark, so history is
    /// never replayed; consumers read `EventLog::read_from` for that.
    pub fn open_with_bus(
        config: RuntimeConfig,
        log: Box<dyn EventLog>,
        snapshots: Box<dyn SnapshotStore>,
        bus: Arc<dyn MessageBus>,
    ) -> Result<(EngineHandle, Self), RuntimeError> {
        let (engine, cursor) = recover(log.as_ref(), snapshots.as_ref())
            .map_err(|error| RuntimeError::Startup(error.to_string()))?;

        Self::start(config, engine, log, snapshots, cursor, bus)
    }

    fn start(
        config: RuntimeConfig,
        engine: MatchingEngine,
        log: Box<dyn EventLog>,
        snapshots: Box<dyn SnapshotStore>,
        cursor: EventCursor,
        bus: Arc<dyn MessageBus>,
    ) -> Result<(EngineHandle, Self), RuntimeError> {
        Self::start_with_gate(
            config,
            engine,
            log,
            snapshots,
            cursor,
            bus,
            CadenceSnapshotGate::new,
        )
    }

    /// Shared startup path with an injectable snapshot gate.
    ///
    /// The production entry points use [`CadenceSnapshotGate`]; tests inject a
    /// gate that fails on demand so panic propagation can be exercised without
    /// weakening the public API.
    #[allow(clippy::too_many_arguments)]
    fn start_with_gate<G, F>(
        config: RuntimeConfig,
        engine: MatchingEngine,
        log: Box<dyn EventLog>,
        snapshots: Box<dyn SnapshotStore>,
        cursor: EventCursor,
        bus: Arc<dyn MessageBus>,
        make_gate: F,
    ) -> Result<(EngineHandle, Self), RuntimeError>
    where
        G: SnapshotGate + 'static,
        F: FnOnce(u64, SnapshotSink) -> G,
    {
        let shared = SharedState::with_durable_seq(
            config.metrics.clone(),
            Arc::new(AtomicU64::new(cursor.durable_seq)),
        );
        shared.watermark().publish_applied(cursor.applied_seq);
        shared
            .watermark()
            .publish_applied_command_id(cursor.next_command_id.saturating_sub(1));

        let (ingress_sender, ingress_receiver) = mpsc::sync_channel(config.queue_capacity.max(1));
        let pool = SlotPool::from_config(&config);
        let submitter = IngressSubmitter::new(ingress_sender.clone(), pool, shared.clone());
        let handle = EngineHandle {
            submitter,
            shared: shared.clone(),
            metrics: config.metrics.clone(),
            bus: SubscriberBus::new(Arc::clone(&bus)),
        };

        let waker = ThreadWaker::new();
        // The journal writer and the publisher reader share one log handle, so
        // the publisher needs no adapter hook and cannot read a different log.
        let shared_log: SharedEventLog = Arc::new(Mutex::new(log));
        let (journal_sink, journal_handle) = journal::spawn_shared(
            Arc::clone(&shared_log),
            shared.clone(),
            &config,
            waker.clone(),
        );
        let (publisher_handle, publisher_done) = publisher::spawn(
            shared_log,
            shared.clone(),
            Arc::clone(&bus),
            cursor.durable_seq,
        );
        let (snapshot_sink, snapshot_handle) = snapshot::spawn(snapshots);
        let gate = make_gate(config.snapshot_cadence, snapshot_sink.clone());

        let (startup_sender, startup_receiver) = mpsc::sync_channel(1);
        let matching_config = config.clone();
        let matching_waker = waker.clone();
        let matching = MatchingThread::new(
            engine,
            ingress_receiver,
            journal_sink,
            gate,
            shared.clone(),
            matching_config.clone(),
            cursor,
        );

        let matching_handle =
            match thread::Builder::new()
                .name("matching".to_string())
                .spawn(move || {
                    // Affinity is applied on the thread that will run the loop.
                    let startup = bind_matching_core(matching_config.matching_core);
                    let ready = startup.is_ok();
                    let _ = startup_sender.send(startup);
                    if !ready {
                        return None;
                    }
                    matching_waker.attach(thread::current());
                    Some(matching.run())
                }) {
                Ok(handle) => handle,
                Err(error) => {
                    // The closure was dropped with `matching`, disconnecting the
                    // journal sink and snapshot gate; `abort_startup` drops the
                    // remaining owners too so both workers can drain and exit.
                    RuntimeThreads::new(
                        None,
                        journal_handle,
                        publisher_handle,
                        publisher_done,
                        snapshot_handle,
                        snapshot_sink,
                        ingress_sender,
                        waker,
                    )
                    .abort_startup(&shared, &config, handle);
                    return Err(RuntimeError::Startup(format!(
                        "could not start the matching thread: {error}"
                    )));
                }
            };

        let threads = RuntimeThreads::new(
            Some(matching_handle),
            journal_handle,
            publisher_handle,
            publisher_done,
            snapshot_handle,
            snapshot_sink,
            ingress_sender,
            waker,
        );

        let startup = startup_receiver
            .recv_timeout(config.shutdown_timeout.max(Duration::from_millis(100)))
            .unwrap_or_else(|_| Err("the matching thread did not report startup".to_string()));

        if let Err(detail) = startup {
            // The matching thread already returned (or is about to) without
            // running the loop; disconnect both workers and join them.
            threads.abort_startup(&shared, &config, handle);
            return Err(RuntimeError::Startup(detail));
        }

        let runtime = Self {
            shared,
            config,
            bus,
            threads,
            stopped: false,
        };
        Ok((handle, runtime))
    }

    /// Stop intake, drain, wait for durability, write the final snapshot, and
    /// join every thread.
    ///
    /// The wait for durability is bounded by
    /// [`RuntimeConfig::shutdown_timeout`]; if it expires the runtime skips the
    /// final snapshot and returns [`RuntimeError::Timeout`]. A journal thread
    /// already blocked in an fsync cannot be interrupted, so the final journal
    /// join may still take as long as that fsync.
    pub fn shutdown(self) -> Result<(), RuntimeError> {
        // `shutdown` consumes the runtime; the Drop that follows observes
        // `stopped` and does nothing.
        let mut this = self;
        this.stop(true)
    }

    /// This runtime's metrics sink, if metrics are enabled.
    pub fn metrics(&self) -> Option<&Arc<RuntimeMetrics>> {
        self.config.metrics.as_ref()
    }

    /// A subscriber-only view of the bus durable events are published on.
    ///
    /// Subscribers register before submitting commands; events published before
    /// a subscriber exists are recovered from the event log. The view exposes
    /// no publisher, so only the runtime's Event Publisher writes to the bus.
    pub fn bus(&self) -> SubscriberBus {
        SubscriberBus::new(Arc::clone(&self.bus))
    }

    fn stop(&mut self, final_snapshot: bool) -> Result<(), RuntimeError> {
        if self.stopped {
            return Ok(());
        }
        self.stopped = true;
        self.threads
            .shutdown(&self.shared, &self.config, final_snapshot)
    }
}

impl Drop for EngineRuntime {
    fn drop(&mut self) {
        // The same stop without a final snapshot; errors and worker panics are
        // deliberately ignored so dropping never panics.
        let _ = self.stop(false);
    }
}

/// Test-only constructor whose matching thread panics after applying
/// `panic_after` commands.
///
/// A matching-thread panic is not reachable through valid commands, so
/// integration tests cannot produce one from the public surface. The gate
/// waits on `start` before processing anything, which lets a test enqueue its
/// tickets first; every waiter must then be released with
/// [`RuntimeError::EngineStopped`].
///
/// This is hidden test support, not part of the supported API.
#[doc(hidden)]
pub fn start_with_matching_panic_for_test(
    config: RuntimeConfig,
    engine_config: EngineConfig,
    mut log: Box<dyn EventLog>,
    snapshots: Box<dyn SnapshotStore>,
    start: Arc<Barrier>,
    panic_after: u64,
) -> Result<(EngineHandle, EngineRuntime), RuntimeError> {
    log.write_header(LogHeader {
        format_version: FORMAT_VERSION,
        config: engine_config,
    })
    .map_err(|error| RuntimeError::Startup(error.to_string()))?;

    EngineRuntime::start_with_gate(
        config,
        MatchingEngine::new(engine_config),
        log,
        snapshots,
        EventCursor::fresh(),
        Arc::new(InProcessBus::new()),
        move |_cadence, _sink| PanicAfterBarrierGate {
            start,
            started: false,
            panic_after,
        },
    )
}

/// A snapshot gate that blocks until `start` is released, then panics the
/// matching thread once `panic_after` commands have been applied.
struct PanicAfterBarrierGate {
    start: Arc<Barrier>,
    started: bool,
    panic_after: u64,
}

impl SnapshotGate for PanicAfterBarrierGate {
    fn poll(
        &mut self,
        _engine: &MatchingEngine,
        applied_seq: u64,
        _durable_seq: u64,
        _last_command_id: u64,
    ) -> GateDecision {
        if !self.started {
            self.start.wait();
            self.started = true;
        }
        if applied_seq >= self.panic_after {
            panic!("matching thread panic (test hook)");
        }
        GateDecision::Continue
    }
}

/// Bind the current thread when `matching_core` is set.
///
/// `matching_core` is an index into [`core_affinity::get_core_ids`], not a raw
/// platform processor id; the mapping is stable only for the process lifetime.
/// An unsupported platform, an out-of-range index, or a failed bind is a
/// startup error rather than a silent fallback.
fn bind_matching_core(index: Option<usize>) -> Result<(), String> {
    let Some(index) = index else {
        return Ok(());
    };

    let cores = core_affinity::get_core_ids()
        .ok_or_else(|| "core affinity is not supported on this platform".to_string())?;
    let core = cores.get(index).copied().ok_or_else(|| {
        format!(
            "matching_core index {index} is out of range; the process can use {} core(s)",
            cores.len()
        )
    })?;

    if core_affinity::set_for_current(core) {
        Ok(())
    } else {
        Err(format!(
            "could not bind the matching thread to core index {index} (id {})",
            core.id
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durable::InMemorySnapshotStore;
    use crate::runtime::error::Worker;
    use crate::runtime::snapshot_gate::GateDecision;
    use crate::types::{OrderRequest, OrderType, Side, TimeInForce};
    use std::time::Duration;

    const WAIT: Duration = Duration::from_secs(5);

    /// Panics once the engine has applied its first command, so the submitter
    /// and the applied waiter both observe a live worker first.
    struct PanickingGate {
        panic_after: u64,
    }

    impl SnapshotGate for PanickingGate {
        fn poll(
            &mut self,
            _engine: &MatchingEngine,
            applied_seq: u64,
            _durable_seq: u64,
            _last_command_id: u64,
        ) -> GateDecision {
            if applied_seq >= self.panic_after {
                panic!("matching gate panic");
            }
            GateDecision::Continue
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

    #[test]
    fn matching_thread_panic_unblocks_waiters_and_is_reported() {
        let config = RuntimeConfig {
            // Keep the command in the matching thread's journal buffer so the
            // panic, not durability, releases the ticket.
            apply_batch_max: 64,
            journal_flush_interval: Duration::from_secs(10),
            ..RuntimeConfig::default()
        };

        let (handle, runtime) = EngineRuntime::start_with_gate(
            config,
            MatchingEngine::new(EngineConfig::default()),
            Box::new(crate::durable::InMemoryEventLog::new()),
            Box::new(InMemorySnapshotStore::new()),
            EventCursor::fresh(),
            Arc::new(InProcessBus::new()),
            |_cadence, _sink| PanickingGate { panic_after: 1 },
        )
        .expect("start with the test gate");

        let ticket = handle.try_submit(command(1)).expect("submit");
        assert_eq!(
            ticket
                .wait_applied_timeout(WAIT)
                .expect("the command was applied before the panic")
                .last_event_sequence,
            2
        );
        assert_eq!(
            ticket.wait_durable_timeout(WAIT),
            Err(RuntimeError::EngineStopped),
            "a matching-thread panic must not leave waiters hanging"
        );

        let result = runtime.shutdown();
        assert!(
            matches!(result, Err(RuntimeError::WorkerPanicked(Worker::Matching))),
            "shutdown must report the matching panic, got {result:?}"
        );
    }
}
