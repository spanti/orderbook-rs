//! Errors surfaced by the matching runtime.

use std::fmt;

use crate::durable::DurableError;

/// Why an [`crate::runtime::EngineHandle`]-style submission was refused.
///
/// `try_submit` takes ownership of the command; callers that retry clone it
/// before submitting.
#[derive(Debug, Clone, PartialEq)]
pub enum SubmitError {
    /// The ingress command queue or the response-slot pool is exhausted.
    QueueFull,
    /// The runtime is shutting down and accepts no new commands.
    Shutdown,
    /// The runtime failed closed; no further commands are accepted.
    EngineFailed(DurableError),
}

impl fmt::Display for SubmitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QueueFull => write!(f, "command queue is full"),
            Self::Shutdown => write!(f, "runtime is shut down"),
            Self::EngineFailed(error) => write!(f, "runtime engine failed: {error}"),
        }
    }
}

impl std::error::Error for SubmitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::EngineFailed(error) => Some(error),
            Self::QueueFull | Self::Shutdown => None,
        }
    }
}

/// The runtime thread whose panic a lifecycle call observed.
///
/// The identity is typed so a caller can distinguish workers without matching
/// on strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Worker {
    Matching,
    Journal,
    Publisher,
    Snapshot,
}

impl fmt::Display for Worker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Matching => write!(f, "matching"),
            Self::Journal => write!(f, "journal"),
            Self::Publisher => write!(f, "publisher"),
            Self::Snapshot => write!(f, "snapshot"),
        }
    }
}

/// Why waiting on a ticket, or a runtime lifecycle call, failed.
#[derive(Debug, Clone, PartialEq)]
pub enum RuntimeError {
    /// The journal failed; queued and pending commands cannot become durable.
    EngineFailed(DurableError),
    /// The ingress command queue or the response-slot pool is exhausted.
    QueueFull,
    /// The runtime is shutting down and accepts no new commands.
    Shutdown,
    /// A worker stopped or panicked before completing the command.
    EngineStopped,
    /// The caller's timeout expired before the requested acknowledgement.
    Timeout,
    /// A worker thread panicked. The value names the worker.
    WorkerPanicked(Worker),
    /// The final snapshot could not be saved during shutdown.
    SnapshotFailed(DurableError),
    /// The runtime could not be started (bad affinity, recovery, or storage).
    Startup(String),
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EngineFailed(error) => write!(f, "runtime engine failed: {error}"),
            Self::QueueFull => write!(f, "command queue is full"),
            Self::Shutdown => write!(f, "runtime is shut down"),
            Self::EngineStopped => {
                write!(f, "runtime worker stopped before completing the command")
            }
            Self::Timeout => write!(f, "operation timed out"),
            Self::WorkerPanicked(worker) => write!(f, "runtime worker panicked: {worker}"),
            Self::SnapshotFailed(error) => write!(f, "snapshot failed: {error}"),
            Self::Startup(detail) => write!(f, "runtime startup failed: {detail}"),
        }
    }
}

impl std::error::Error for RuntimeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::EngineFailed(error) | Self::SnapshotFailed(error) => Some(error),
            Self::QueueFull
            | Self::Shutdown
            | Self::EngineStopped
            | Self::Timeout
            | Self::WorkerPanicked(_)
            | Self::Startup(_) => None,
        }
    }
}

/// A convenience wrapper maps a refused submission to the matching runtime
/// error, so `submit_and_wait` does not collapse `QueueFull` into a wait
/// failure.
impl From<SubmitError> for RuntimeError {
    fn from(error: SubmitError) -> Self {
        match error {
            SubmitError::QueueFull => Self::QueueFull,
            SubmitError::Shutdown => Self::Shutdown,
            SubmitError::EngineFailed(error) => Self::EngineFailed(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn submit_error_display_and_source() {
        assert_eq!(SubmitError::QueueFull.to_string(), "command queue is full");
        assert_eq!(SubmitError::Shutdown.to_string(), "runtime is shut down");

        let failed = SubmitError::EngineFailed(DurableError::AppendFailed("disk".into()));
        assert!(failed.to_string().contains("disk"));
        assert!(std::error::Error::source(&failed).is_some());
        assert!(std::error::Error::source(&SubmitError::QueueFull).is_none());
    }

    #[test]
    fn runtime_error_display_and_source() {
        assert_eq!(RuntimeError::Timeout.to_string(), "operation timed out");
        assert_eq!(
            RuntimeError::WorkerPanicked(Worker::Matching).to_string(),
            "runtime worker panicked: matching"
        );
        assert_eq!(
            RuntimeError::WorkerPanicked(Worker::Journal).to_string(),
            "runtime worker panicked: journal"
        );
        assert_eq!(
            RuntimeError::WorkerPanicked(Worker::Snapshot).to_string(),
            "runtime worker panicked: snapshot"
        );
        assert_eq!(RuntimeError::QueueFull.to_string(), "command queue is full");
        assert_eq!(RuntimeError::Shutdown.to_string(), "runtime is shut down");

        let failed = RuntimeError::EngineFailed(DurableError::AppendFailed("disk".into()));
        assert!(failed.to_string().contains("disk"));
        assert!(std::error::Error::source(&failed).is_some());
        assert!(std::error::Error::source(&RuntimeError::EngineStopped).is_none());
        assert!(std::error::Error::source(&RuntimeError::QueueFull).is_none());
        assert!(std::error::Error::source(&RuntimeError::Shutdown).is_none());

        let snapshot = RuntimeError::SnapshotFailed(DurableError::Storage("io".into()));
        assert!(snapshot.to_string().contains("io"));
        assert!(std::error::Error::source(&snapshot).is_some());

        assert!(
            RuntimeError::Startup("no matching core".into())
                .to_string()
                .contains("no matching core")
        );
    }

    #[test]
    fn submit_errors_convert_to_coherent_runtime_errors() {
        assert_eq!(
            RuntimeError::from(SubmitError::QueueFull),
            RuntimeError::QueueFull
        );
        assert_eq!(
            RuntimeError::from(SubmitError::Shutdown),
            RuntimeError::Shutdown
        );

        let error = DurableError::AppendFailed("disk".into());
        assert_eq!(
            RuntimeError::from(SubmitError::EngineFailed(error.clone())),
            RuntimeError::EngineFailed(error)
        );
    }
}
