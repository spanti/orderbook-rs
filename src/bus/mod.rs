//! The in-process synchronous `MessageBus`.
//!
//! Durable events leave the engine through this bus. It is deliberately small:
//!
//! - [`MessageBus::register_any`] registers one or more handlers under an
//!   endpoint name;
//! - [`MessageBus::try_publish_any`] dispatches a borrowed message to every
//!   handler registered under a topic, synchronously, in registration order;
//! - [`MessageBus::send_any`] and [`MessageBus::register_response_handler`]
//!   are declared for the future request/response shape and return
//!   [`BusError::Unsupported`] in this release. There is no command path on the
//!   bus.
//!
//! Messages are `&dyn Any`, so the dispatch is borrowed and cannot outlive the
//! caller: a handler always runs on the caller's thread. The concrete payload
//! published by the runtime is [`EventRecord`](crate::durable::EventRecord);
//! a consumer that casts to the wrong type has a bug the bus cannot detect.
//!
//! A panicking handler is caught with [`std::panic::catch_unwind`]. Its
//! registration is marked failed and is skipped on later publishes, while the
//! remaining handlers in the same publish still run. A bad consumer therefore
//! cannot stop publication for the others. Consumers that miss events must save
//! their last seen sequence and catch up from
//! [`EventLog::read_from`](crate::durable::EventLog::read_from); the bus keeps
//! no history and no per-subscriber queue.

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex, MutexGuard};

/// The endpoint the runtime publishes durable [`EventRecord`]s on.
///
/// [`EventRecord`]: crate::durable::EventRecord
pub const EVENTS_TOPIC: &str = "events";

/// Why a bus operation failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BusError {
    /// The method exists for a future release and is not implemented yet.
    ///
    /// `send_any` and `register_response_handler` return this in every build:
    /// there is no command or request/response path on the bus.
    Unsupported,
    /// A future `send_any` would find more than one handler under the
    /// endpoint. Reserved now so the error shape is stable.
    AmbiguousEndpoint,
}

impl fmt::Display for BusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported => write!(f, "operation is unsupported by this MessageBus"),
            Self::AmbiguousEndpoint => {
                write!(
                    f,
                    "endpoint has more than one handler and cannot receive a request"
                )
            }
        }
    }
}

impl std::error::Error for BusError {}

/// One registered consumer.
struct Registration {
    handler: Box<dyn Fn(&dyn Any) + Send + 'static>,
    /// Set once the handler panics. A failed registration is not invoked again
    /// and is reported by [`InProcessBus::failed_endpoints`].
    failed: bool,
}

/// The bus every runtime publishes through.
pub trait MessageBus: Send + Sync + 'static {
    /// Register a handler for `endpoint`.
    ///
    /// Multiple handlers may share one endpoint; they run in registration
    /// order.
    fn register_any(
        &self,
        endpoint: &str,
        handler: Box<dyn Fn(&dyn Any) + Send + 'static>,
    ) -> Result<(), BusError>;

    /// Register a response handler for `correlation_id`.
    ///
    /// Declared only. Always returns [`BusError::Unsupported`] in this release.
    fn register_response_handler(
        &self,
        correlation_id: u64,
        handler: Box<dyn Fn(&dyn Any) + Send + 'static>,
    ) -> Result<(), BusError>;

    /// Send a request to exactly one handler.
    ///
    /// Declared only. Always returns [`BusError::Unsupported`] in this release;
    /// commands never enter the bus.
    fn send_any(&self, endpoint: &str, message: &dyn Any) -> Result<(), BusError>;

    /// Publish `message` to every handler registered under `topic`.
    ///
    /// Dispatch is synchronous and happens on the caller's thread. Handlers run
    /// in registration order. A handler panic is caught, the endpoint is
    /// recorded as failed, and the remaining handlers still run; the call
    /// still returns `Ok(())` because publication itself succeeded.
    fn try_publish_any(&self, topic: &str, message: &dyn Any) -> Result<(), BusError>;
}

/// A registered bus handler: borrowed message in, synchronous, sendable.
pub type BusHandler = Box<dyn Fn(&dyn Any) + Send + 'static>;

/// A subscriber-only view of a [`MessageBus`].
///
/// [`EngineHandle::bus`](crate::runtime::EngineHandle::bus) and
/// [`EngineRuntime::bus`](crate::runtime::EngineRuntime::bus) return this view
/// instead of the writable bus. It exposes registration only: publishing is
/// reserved for the Event Publisher, so a producer that holds an engine handle
/// cannot inject events or otherwise use the bus as a second command ingress.
///
/// Publishing is not merely discouraged; it is unreachable:
///
/// ```compile_fail
/// use orderbook_rs::bus::{InProcessBus, SubscriberBus};
/// use std::sync::Arc;
///
/// let view = SubscriberBus::new(Arc::new(InProcessBus::new()));
/// view.try_publish_any("events", &());
/// ```
#[derive(Clone)]
pub struct SubscriberBus {
    inner: Arc<dyn MessageBus>,
}

impl SubscriberBus {
    /// Wrap the writable bus, hiding its publisher.
    pub fn new(inner: Arc<dyn MessageBus>) -> Self {
        Self { inner }
    }

    /// Register a handler for `endpoint`; see [`MessageBus::register_any`].
    pub fn register_any(&self, endpoint: &str, handler: BusHandler) -> Result<(), BusError> {
        self.inner.register_any(endpoint, handler)
    }

    /// Register a response handler; always [`BusError::Unsupported`] in this
    /// release, exactly like the underlying bus.
    pub fn register_response_handler(
        &self,
        correlation_id: u64,
        handler: BusHandler,
    ) -> Result<(), BusError> {
        self.inner
            .register_response_handler(correlation_id, handler)
    }
}

impl fmt::Debug for SubscriberBus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubscriberBus").finish_non_exhaustive()
    }
}

/// In-process synchronous [`MessageBus`] implementation.
#[derive(Default)]
pub struct InProcessBus {
    handlers: Mutex<HashMap<String, Vec<Registration>>>,
}

impl InProcessBus {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, Vec<Registration>>> {
        self.handlers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Whether any handler registered under `endpoint` has panicked.
    pub fn is_endpoint_failed(&self, endpoint: &str) -> bool {
        self.lock()
            .get(endpoint)
            .is_some_and(|registrations| registrations.iter().any(|entry| entry.failed))
    }

    /// Every endpoint with at least one failed handler, sorted.
    pub fn failed_endpoints(&self) -> Vec<String> {
        let mut failed = self
            .lock()
            .iter()
            .filter(|(_, registrations)| registrations.iter().any(|entry| entry.failed))
            .map(|(endpoint, _)| endpoint.clone())
            .collect::<Vec<_>>();
        failed.sort();
        failed
    }
}

impl MessageBus for InProcessBus {
    fn register_any(
        &self,
        endpoint: &str,
        handler: Box<dyn Fn(&dyn Any) + Send + 'static>,
    ) -> Result<(), BusError> {
        self.lock()
            .entry(endpoint.to_string())
            .or_default()
            .push(Registration {
                handler,
                failed: false,
            });
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

    fn try_publish_any(&self, topic: &str, message: &dyn Any) -> Result<(), BusError> {
        // Dispatch under the registry lock. Handlers must not re-enter the bus
        // (`register_any` or `try_publish_any` from inside a handler); the
        // runtime's publisher never does, and the lock keeps the registration
        // list stable while it is walked.
        let mut handlers = self.lock();
        let Some(registrations) = handlers.get_mut(topic) else {
            return Ok(());
        };

        for registration in registrations.iter_mut() {
            if registration.failed {
                continue;
            }
            let result = catch_unwind(AssertUnwindSafe(|| (registration.handler)(message)));
            if result.is_err() {
                registration.failed = true;
            }
        }
        Ok(())
    }
}

impl fmt::Debug for InProcessBus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let handlers = self.lock();
        let endpoints = handlers
            .iter()
            .map(|(endpoint, registrations)| (endpoint.clone(), registrations.len()))
            .collect::<Vec<_>>();
        f.debug_struct("InProcessBus")
            .field("endpoints", &endpoints)
            .finish()
    }
}
