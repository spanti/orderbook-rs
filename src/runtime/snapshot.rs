//! The snapshot worker: best-effort engine snapshots off the matching thread.
//!
//! The matching thread captures an [`StoredSnapshot`] and offers it through
//! [`SnapshotSink::try_send`]. The worker owns the [`SnapshotStore`], saves jobs
//! sequentially, records the last failure, and keeps running after failures;
//! only a disconnected channel stops it after draining.
//!
//! Explicit shutdown uses [`SnapshotSink::capture_final`], which waits for the
//! worker to finish saving so a failure can surface. Snapshots are never part
//! of the durable acknowledgement path: an automatic snapshot failure degrades
//! observability but never fails an already-durable command.

use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};

use crate::durable::{DurableError, FORMAT_VERSION, SnapshotStore, StoredSnapshot};
use crate::engine::MatchingEngine;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Outcome of offering a best-effort snapshot to the worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapshotSend {
    /// The worker accepted the job (or queued it in the single slot).
    Sent,
    /// The bounded worker channel is full; keep the snapshot due and retry.
    Full,
    /// The worker is gone; further automatic snapshots cannot be delivered.
    Disconnected,
}

/// One snapshot job. Final snapshots carry an acknowledgement channel so the
/// shutdown path can wait for the save result.
struct SnapshotJob {
    snapshot: StoredSnapshot,
    ack: Option<SyncSender<Result<(), DurableError>>>,
}

/// The matching thread's non-blocking handle to the snapshot worker.
///
/// Cloning the sink is cheap and lets the lifecycle hold one handle after the
/// matching thread owns another.
#[derive(Clone)]
pub(crate) struct SnapshotSink {
    sender: SyncSender<SnapshotJob>,
    last_error: Arc<Mutex<Option<DurableError>>>,
}

impl SnapshotSink {
    /// Offer a snapshot without blocking. A full channel is reported to the
    /// caller so the snapshot can stay due.
    pub(crate) fn try_send(&self, snapshot: StoredSnapshot) -> SnapshotSend {
        match self.sender.try_send(SnapshotJob {
            snapshot,
            ack: None,
        }) {
            Ok(()) => SnapshotSend::Sent,
            Err(TrySendError::Full(_)) => SnapshotSend::Full,
            Err(TrySendError::Disconnected(_)) => SnapshotSend::Disconnected,
        }
    }

    /// Send a snapshot and wait until the worker has attempted to save it.
    pub(crate) fn save_and_wait(&self, snapshot: StoredSnapshot) -> Result<(), DurableError> {
        let (ack_sender, ack_receiver) = mpsc::sync_channel(0);
        self.sender
            .send(SnapshotJob {
                snapshot,
                ack: Some(ack_sender),
            })
            .map_err(|_| DurableError::Storage("snapshot worker is not running".to_string()))?;
        ack_receiver.recv().map_err(|_| {
            DurableError::Storage("snapshot worker stopped before saving".to_string())
        })?
    }

    /// The last snapshot save failure observed by the worker, if any.
    pub(crate) fn last_error(&self) -> Option<DurableError> {
        lock(&self.last_error).clone()
    }

    /// Capture and save the final snapshot on explicit shutdown.
    ///
    /// The caller must have waited for `durable_seq == applied_seq`; refusing
    /// unequal sequences keeps a snapshot from ever covering commands the log
    /// does not contain. Unlike automatic snapshots this waits for the save so
    /// a failure surfaces as [`crate::runtime::RuntimeError::SnapshotFailed`].
    pub(crate) fn capture_final(
        &self,
        engine: &MatchingEngine,
        applied_seq: u64,
        durable_seq: u64,
    ) -> Result<(), DurableError> {
        if applied_seq != durable_seq {
            return Err(DurableError::Storage(format!(
                "cannot snapshot applied sequence {applied_seq} with durable sequence {durable_seq}"
            )));
        }

        self.save_and_wait(StoredSnapshot {
            format_version: FORMAT_VERSION,
            applied_seq,
            snapshot: engine.capture_snapshot(),
        })
    }
}

struct SnapshotWorker {
    store: Box<dyn SnapshotStore>,
    receiver: Receiver<SnapshotJob>,
    last_error: Arc<Mutex<Option<DurableError>>>,
}

impl SnapshotWorker {
    fn run(mut self) {
        // `recv` returns every queued job before reporting disconnect, so this
        // loop drains naturally when the last sink is dropped.
        while let Ok(job) = self.receiver.recv() {
            let result = self.store.save(job.snapshot);
            match &result {
                Ok(()) => *lock(&self.last_error) = None,
                Err(error) => *lock(&self.last_error) = Some(error.clone()),
            }
            if let Some(ack) = job.ack {
                let _ = ack.send(result);
            }
        }
    }
}

/// Start the snapshot worker.
///
/// Returns the sink and the worker's join handle. The runtime lifecycle (ticket
/// 08) drops or joins the sink to disconnect the worker after the final
/// snapshot.
pub(crate) fn spawn(store: Box<dyn SnapshotStore>) -> (SnapshotSink, JoinHandle<()>) {
    let (sender, receiver) = mpsc::sync_channel(1);
    let last_error = Arc::new(Mutex::new(None));
    let sink = SnapshotSink {
        sender,
        last_error: Arc::clone(&last_error),
    };
    let worker = SnapshotWorker {
        store,
        receiver,
        last_error,
    };
    let handle = thread::Builder::new()
        .name("snapshot".to_string())
        .spawn(move || worker.run())
        .expect("start the snapshot thread");
    (sink, handle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durable::InMemorySnapshotStore;
    use crate::snapshot::EngineSnapshot;
    use crate::types::EngineConfig;
    use std::time::{Duration, Instant};

    const WAIT: Duration = Duration::from_secs(3);

    fn stored(applied_seq: u64) -> StoredSnapshot {
        StoredSnapshot {
            format_version: FORMAT_VERSION,
            applied_seq,
            snapshot: EngineSnapshot {
                config: EngineConfig::default(),
                next_order_id: 1,
                next_trade_id: 1,
                next_event_sequence: 1,
                books: Vec::new(),
                ledger: Vec::new(),
                stops: Vec::new(),
                last_trades: Vec::new(),
                trades: Vec::new(),
            },
        }
    }

    fn wait_until(mut condition: impl FnMut() -> bool) {
        let deadline = Instant::now() + WAIT;
        while !condition() {
            assert!(Instant::now() < deadline, "condition was not met in time");
            thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn worker_saves_jobs_and_drains_on_disconnect() {
        let store = InMemorySnapshotStore::new();
        let (sink, handle) = spawn(Box::new(store.handle()));

        assert_eq!(sink.try_send(stored(1)), SnapshotSend::Sent);
        wait_until(|| store.load_latest().unwrap().is_some());
        assert_eq!(store.load_latest().unwrap().unwrap().applied_seq, 1);

        assert_eq!(sink.try_send(stored(2)), SnapshotSend::Sent);
        drop(sink);
        handle.join().expect("snapshot worker drains and exits");

        assert_eq!(
            store.load_latest().unwrap().unwrap().applied_seq,
            2,
            "jobs queued before disconnect are saved"
        );
    }

    #[derive(Clone, Default)]
    struct ProbeStore {
        inner: Arc<Mutex<ProbeInner>>,
    }

    #[derive(Default)]
    struct ProbeInner {
        saved: Vec<u64>,
        fail_on_call: Option<usize>,
        fail_error: Option<DurableError>,
        calls: usize,
        gate: Option<Gate>,
    }

    impl ProbeStore {
        fn handle(&self) -> Self {
            self.clone()
        }

        fn failing_on_first(error: DurableError) -> Self {
            let store = Self::default();
            {
                let mut inner = lock(&store.inner);
                inner.fail_on_call = Some(0);
                inner.fail_error = Some(error);
            }
            store
        }

        fn blocked(gate: Gate) -> Self {
            let store = Self::default();
            lock(&store.inner).gate = Some(gate);
            store
        }

        fn saved(&self) -> Vec<u64> {
            lock(&self.inner).saved.clone()
        }

        fn calls(&self) -> usize {
            lock(&self.inner).calls
        }
    }

    impl SnapshotStore for ProbeStore {
        fn save(&mut self, snapshot: StoredSnapshot) -> Result<(), DurableError> {
            let (gate, fail, error) = {
                let mut inner = lock(&self.inner);
                let call = inner.calls;
                inner.calls += 1;
                (
                    inner.gate.clone(),
                    inner.fail_on_call == Some(call),
                    inner.fail_error.clone(),
                )
            };
            if let Some(gate) = gate {
                gate.wait();
            }
            if fail {
                return Err(error.unwrap_or_else(|| DurableError::Storage("probe".to_string())));
            }
            lock(&self.inner).saved.push(snapshot.applied_seq);
            Ok(())
        }

        fn load_latest(&self) -> Result<Option<StoredSnapshot>, DurableError> {
            Ok(None)
        }

        fn load_previous(&self) -> Result<Option<StoredSnapshot>, DurableError> {
            Ok(None)
        }
    }

    #[derive(Clone, Default)]
    struct Gate {
        open: Arc<Mutex<bool>>,
        ready: Arc<std::sync::Condvar>,
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

    #[test]
    fn worker_records_the_last_error_and_keeps_running() {
        let error = DurableError::Storage("disk full".to_string());
        let store = ProbeStore::failing_on_first(error.clone());
        let (sink, handle) = spawn(Box::new(store.handle()));

        assert_eq!(sink.save_and_wait(stored(1)), Err(error.clone()));
        assert_eq!(sink.last_error(), Some(error));

        assert_eq!(sink.save_and_wait(stored(2)), Ok(()));
        assert_eq!(
            store.saved(),
            vec![2],
            "the worker keeps saving after a failure"
        );
        assert_eq!(
            sink.last_error(),
            None,
            "a successful save clears the status"
        );

        drop(sink);
        handle.join().expect("snapshot worker");
    }

    #[test]
    fn try_send_does_not_block_and_reports_a_full_channel() {
        let gate = Gate::default();
        let store = ProbeStore::blocked(gate.clone());
        let (sink, handle) = spawn(Box::new(store.handle()));

        assert_eq!(sink.try_send(stored(1)), SnapshotSend::Sent);
        wait_until(|| store.calls() == 1);
        assert_eq!(
            sink.try_send(stored(2)),
            SnapshotSend::Sent,
            "the single queue slot accepts one waiting job"
        );
        assert_eq!(
            sink.try_send(stored(3)),
            SnapshotSend::Full,
            "a full channel is reported without blocking"
        );

        gate.open();
        drop(sink);
        handle.join().expect("snapshot worker");
        assert_eq!(store.saved(), vec![1, 2]);
    }

    #[test]
    fn capture_final_refuses_a_sequence_ahead_of_durability() {
        let store = InMemorySnapshotStore::new();
        let (sink, handle) = spawn(Box::new(store.handle()));
        let engine = MatchingEngine::new(EngineConfig::default());

        let error = sink
            .capture_final(&engine, 2, 1)
            .expect_err("a snapshot must not cover a command the log may not hold");
        assert!(matches!(error, DurableError::Storage(_)));
        assert!(
            sink.last_error().is_none(),
            "nothing was handed to the worker"
        );

        sink.capture_final(&engine, 0, 0)
            .expect("an empty engine at sequence zero is snapshotable");
        assert_eq!(store.load_latest().unwrap().unwrap().applied_seq, 0);

        drop(sink);
        handle.join().expect("snapshot worker");
    }

    #[test]
    fn final_snapshot_failures_surface_to_the_caller() {
        let error = DurableError::Storage("final write failed".to_string());
        let store = ProbeStore::failing_on_first(error.clone());
        let (sink, handle) = spawn(Box::new(store.handle()));
        let engine = MatchingEngine::new(EngineConfig::default());

        assert_eq!(sink.capture_final(&engine, 0, 0), Err(error));
        assert!(sink.last_error().is_some());

        drop(sink);
        handle.join().expect("snapshot worker");
    }
}
