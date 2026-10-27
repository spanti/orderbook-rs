//! The runtime's run state: closed, failed closed, and the shutdown deadline.
//!
//! The ingress submitters, the matching thread, the journal thread, and the
//! lifecycle all observe these flags. They are rare control-path state: the
//! fatal error and the shutdown deadline use a mutex, the two flags and the
//! timeout marker are atomics.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::durable::DurableError;

use super::sync::lock;

/// Whether the runtime is running, closed, or failed closed, and how long
/// shutdown may wait for durability.
#[derive(Clone)]
pub(crate) struct RunStatus {
    closed: Arc<AtomicBool>,
    failed: Arc<AtomicBool>,
    fatal: Arc<Mutex<Option<DurableError>>>,
    /// Deadline after which a closed matching thread stops waiting for the
    /// journal and aborts shutdown with a timeout.
    shutdown_deadline: Arc<Mutex<Option<Instant>>>,
    timed_out: Arc<AtomicBool>,
}

impl RunStatus {
    pub(crate) fn new() -> Self {
        Self {
            closed: Arc::new(AtomicBool::new(false)),
            failed: Arc::new(AtomicBool::new(false)),
            fatal: Arc::new(Mutex::new(None)),
            shutdown_deadline: Arc::new(Mutex::new(None)),
            timed_out: Arc::new(AtomicBool::new(false)),
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

    /// Bound how long a closed matching thread waits for the journal.
    pub(crate) fn set_shutdown_deadline(&self, deadline: Instant) {
        *lock(&self.shutdown_deadline) = Some(deadline);
    }

    /// Whether shutdown's durability deadline has elapsed.
    pub(crate) fn shutdown_expired(&self) -> bool {
        lock(&self.shutdown_deadline).is_some_and(|deadline| Instant::now() >= deadline)
    }

    /// How much of the shutdown budget remains; zero when it is unset or
    /// already expired.
    pub(crate) fn shutdown_remaining(&self) -> Duration {
        match *lock(&self.shutdown_deadline) {
            Some(deadline) => deadline.saturating_duration_since(Instant::now()),
            None => Duration::ZERO,
        }
    }

    /// Record that shutdown stopped waiting for durability.
    pub(crate) fn mark_timed_out(&self) {
        self.timed_out.store(true, Ordering::Release);
    }

    /// Whether shutdown aborted because the durability deadline elapsed.
    pub(crate) fn timed_out(&self) -> bool {
        self.timed_out.load(Ordering::Acquire)
    }
}
