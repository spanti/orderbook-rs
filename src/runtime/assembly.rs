//! The runtime's worker threads and their teardown.
//!
//! [`RuntimeThreads`] owns the matching, journal, publisher, and snapshot
//! threads plus the sinks and senders whose drop disconnects them. Expressing
//! the teardown once here keeps every startup and shutdown path on the same
//! join order:
//!
//! - [`RuntimeThreads::abort_startup`] tears down a runtime whose matching
//!   thread never started or reported a failed handshake.
//! - [`RuntimeThreads::shutdown`] performs the normal stop, optionally writing
//!   the final snapshot.
//!
//! The order is load-bearing. A stop first closes the shared state and wakes
//! the matching thread, then joins it (so the final snapshot sees the last
//! applied state), drops the snapshot sink, joins the journal (so the
//! durability watermark is final), finishes the publisher, and finally joins
//! the snapshot worker.

use std::sync::mpsc::{self, RecvTimeoutError, SyncSender};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::durable::DurableError;
use crate::engine::MatchingEngine;

use super::RuntimeConfig;
use super::envelope::CommandEnvelope;
use super::error::{RuntimeError, Worker};
use super::journal::{ThreadWaker, Waker};
use super::lifecycle::EngineHandle;
use super::shared::SharedState;
use super::snapshot::SnapshotSink;

/// The worker threads, sinks, and senders owned by one runtime.
pub(crate) struct RuntimeThreads {
    matching: Option<JoinHandle<Option<MatchingEngine>>>,
    journal: Option<JoinHandle<()>>,
    publisher: Option<JoinHandle<()>>,
    publisher_done: Option<mpsc::Receiver<()>>,
    snapshot: Option<JoinHandle<()>>,
    snapshot_sink: Option<SnapshotSink>,
    /// The runtime keeps one ingress sender for its whole life so the Command
    /// Queue does not disconnect while handles still exist. Shutdown is
    /// signalled by the closed flag, not by dropping this sender.
    _ingress_sender: Option<SyncSender<CommandEnvelope>>,
    waker: ThreadWaker,
}

/// How the wait for the Event Publisher ended.
enum PublisherJoin {
    /// The publisher reported completion (or disconnected); its join handle,
    /// when still present, has been joined. `panicked` records a failed join.
    Joined { panicked: bool },
    /// The wait timed out and the publisher thread was left detached.
    TimedOut,
}

impl RuntimeThreads {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        matching: Option<JoinHandle<Option<MatchingEngine>>>,
        journal: JoinHandle<()>,
        publisher: JoinHandle<()>,
        publisher_done: mpsc::Receiver<()>,
        snapshot: JoinHandle<()>,
        snapshot_sink: SnapshotSink,
        ingress_sender: SyncSender<CommandEnvelope>,
        waker: ThreadWaker,
    ) -> Self {
        Self {
            matching,
            journal: Some(journal),
            publisher: Some(publisher),
            publisher_done: Some(publisher_done),
            snapshot: Some(snapshot),
            snapshot_sink: Some(snapshot_sink),
            _ingress_sender: Some(ingress_sender),
            waker,
        }
    }

    /// Tear down a runtime whose matching thread never ran the loop.
    ///
    /// Used for both startup failure paths: a failed thread spawn and a failed
    /// startup handshake (bad affinity). In both cases the matching closure is
    /// already gone, disconnecting the journal sink and the snapshot gate.
    /// Dropping the remaining local owners lets the journal, publisher, and
    /// snapshot worker drain and exit, and then every worker is joined.
    ///
    /// A publisher that misses the configured timeout is detached: a
    /// synchronous handler cannot be interrupted, and every event is already
    /// durable and readable from the log.
    pub(crate) fn abort_startup(
        mut self,
        shared: &SharedState,
        config: &RuntimeConfig,
        handle: EngineHandle,
    ) {
        shared.run().close();
        shared.run().set_shutdown_deadline(Instant::now());

        drop(self.snapshot_sink.take());
        drop(handle);
        drop(self._ingress_sender.take());

        if let Some(handle) = self.matching.take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.journal.take() {
            let _ = handle.join();
        }
        shared.publisher().finish();
        let _ = self.join_publisher(config.shutdown_timeout);
        if let Some(handle) = self.snapshot.take() {
            let _ = handle.join();
        }
    }

    /// Stop intake, drain, wait for durability, optionally write the final
    /// snapshot, and join every thread.
    ///
    /// The wait for durability is bounded by
    /// [`RuntimeConfig::shutdown_timeout`]; if it expires the runtime skips the
    /// final snapshot and returns [`RuntimeError::Timeout`]. A journal thread
    /// already blocked in an fsync cannot be interrupted, so the final journal
    /// join may still take as long as that fsync.
    ///
    /// Error precedence is matching/journal/snapshot panics first, then the
    /// snapshot or timeout failure, then a fail-closed journal error.
    pub(crate) fn shutdown(
        &mut self,
        shared: &SharedState,
        config: &RuntimeConfig,
        final_snapshot: bool,
    ) -> Result<(), RuntimeError> {
        shared.run().close();
        shared
            .run()
            .set_shutdown_deadline(Instant::now() + config.shutdown_timeout);
        self.waker.wake();

        let mut panic_error = None;
        let mut engine = None;
        if let Some(handle) = self.matching.take() {
            match handle.join() {
                Ok(Some(matching_engine)) => engine = Some(matching_engine),
                // Startup failures are reported by `start_with_gate`; a
                // runtime that returned successfully always yields an engine.
                Ok(None) => {}
                Err(_) => panic_error = Some(RuntimeError::WorkerPanicked(Worker::Matching)),
            }
        }

        let timed_out = shared.run().timed_out();
        let mut failure = None;

        if let Some(engine) = engine.as_ref()
            && panic_error.is_none()
            && !timed_out
            && !shared.run().is_failed()
            && final_snapshot
        {
            let applied_seq = shared.watermark().applied_seq();
            let durable_seq = shared.watermark().durable_seq();
            if applied_seq == durable_seq {
                if let Some(sink) = self.snapshot_sink.as_ref()
                    && let Err(error) = sink.capture_final(
                        engine,
                        applied_seq,
                        durable_seq,
                        shared.watermark().applied_command_id(),
                    )
                {
                    failure = Some(RuntimeError::SnapshotFailed(error));
                }
            } else {
                failure = Some(RuntimeError::Timeout);
            }
        }

        if timed_out && failure.is_none() && panic_error.is_none() {
            failure = Some(RuntimeError::Timeout);
        }

        // The matching thread's gate clone is gone; dropping the lifecycle's
        // sink lets the worker drain its queue and exit.
        self.snapshot_sink = None;

        if let Some(handle) = self.journal.take()
            && handle.join().is_err()
            && panic_error.is_none()
        {
            panic_error = Some(RuntimeError::WorkerPanicked(Worker::Journal));
        }

        // The journal has exited, so the durability watermark is final. Let
        // the publisher drain to it and stop. A synchronous handler cannot be
        // interrupted, so the wait is bounded: on timeout the thread is
        // detached and consumers catch up from the event log.
        shared.publisher().finish();
        match self.join_publisher(shared.run().shutdown_remaining()) {
            PublisherJoin::Joined { panicked } => {
                if panicked && panic_error.is_none() {
                    panic_error = Some(RuntimeError::WorkerPanicked(Worker::Publisher));
                }
            }
            PublisherJoin::TimedOut => {
                if failure.is_none() && panic_error.is_none() {
                    failure = Some(RuntimeError::Timeout);
                }
            }
        }

        if let Some(handle) = self.snapshot.take()
            && handle.join().is_err()
            && panic_error.is_none()
        {
            panic_error = Some(RuntimeError::WorkerPanicked(Worker::Snapshot));
        }

        if let Some(error) = panic_error {
            return Err(error);
        }
        if let Some(error) = failure {
            return Err(error);
        }
        if shared.run().is_failed() {
            return Err(RuntimeError::EngineFailed(
                shared.run().failure().unwrap_or(DurableError::EngineFailed),
            ));
        }
        Ok(())
    }

    /// Wait for the Event Publisher's completion signal, bounded by `timeout`.
    ///
    /// On a timeout the join handle is abandoned, which detaches the thread:
    /// a synchronous handler cannot be interrupted, and every event is already
    /// durable and readable from the log.
    fn join_publisher(&mut self, timeout: Duration) -> PublisherJoin {
        let Some(done) = self.publisher_done.take() else {
            return PublisherJoin::Joined { panicked: false };
        };
        match done.recv_timeout(timeout) {
            Ok(()) | Err(RecvTimeoutError::Disconnected) => {
                let panicked = self
                    .publisher
                    .take()
                    .is_some_and(|handle| handle.join().is_err());
                PublisherJoin::Joined { panicked }
            }
            // Abandoning the join handle leaves the thread detached; every
            // event is already durable and readable from the log.
            Err(RecvTimeoutError::Timeout) => {
                self.publisher = None;
                PublisherJoin::TimedOut
            }
        }
    }
}
