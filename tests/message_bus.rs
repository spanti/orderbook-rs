//! Public contracts of the in-process synchronous MessageBus.
//!
//! The bus is the only new seam this ticket adds. These tests exercise it
//! through the crate-root export: registration order, multiple handlers,
//! panic isolation, the deliberately unsupported request/response methods,
//! and the absence of a command ingress.

use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use orderbook_rs::bus::{BusError, EVENTS_TOPIC, InProcessBus, MessageBus};
use orderbook_rs::durable::{EVENT_FORMAT_VERSION, EventRecord};
use orderbook_rs::event::DomainEvent;
use orderbook_rs::types::{EngineCommand, EngineError};

#[derive(Debug, Clone, PartialEq, Eq)]
struct Message {
    value: u64,
}

fn event_record(sequence: u64) -> EventRecord {
    EventRecord {
        format_version: EVENT_FORMAT_VERSION,
        sequence,
        command_id: sequence,
        event: DomainEvent::OrderRejected {
            symbol: "BTC-USDT".to_string(),
            reason: EngineError::UnknownOrder,
            command: EngineCommand::CancelOrder {
                symbol: "BTC-USDT".to_string(),
                order_id: sequence,
            },
        },
    }
}

#[test]
fn handlers_run_in_registration_order_and_only_for_their_endpoint() {
    let bus = InProcessBus::new();
    let order = Arc::new(Mutex::new(Vec::new()));

    let first = Arc::clone(&order);
    bus.register_any(
        EVENTS_TOPIC,
        Box::new(move |message: &dyn Any| {
            let message = message.downcast_ref::<Message>().expect("Message");
            first
                .lock()
                .unwrap()
                .push(format!("first:{}", message.value));
        }),
    )
    .expect("register the first handler");

    let second = Arc::clone(&order);
    bus.register_any(
        EVENTS_TOPIC,
        Box::new(move |message: &dyn Any| {
            let message = message.downcast_ref::<Message>().expect("Message");
            second
                .lock()
                .unwrap()
                .push(format!("second:{}", message.value));
        }),
    )
    .expect("register the second handler");

    let other = Arc::clone(&order);
    bus.register_any(
        "other",
        Box::new(move |message: &dyn Any| {
            let message = message.downcast_ref::<Message>().expect("Message");
            other
                .lock()
                .unwrap()
                .push(format!("other:{}", message.value));
        }),
    )
    .expect("register a handler on another endpoint");

    bus.try_publish_any(EVENTS_TOPIC, &Message { value: 7 })
        .expect("publish to the events endpoint");

    assert_eq!(
        *order.lock().unwrap(),
        vec!["first:7", "second:7"],
        "multiple handlers per endpoint run in registration order and other endpoints are isolated"
    );
}

#[test]
fn publishing_to_an_endpoint_without_handlers_is_a_noop() {
    let bus = InProcessBus::new();

    bus.try_publish_any("nobody-listens", &Message { value: 1 })
        .expect("publishing without subscribers is not an error");
}

#[test]
fn a_panicking_handler_is_isolated_and_its_endpoint_is_marked_failed() {
    let bus = InProcessBus::new();
    let delivered = Arc::new(AtomicUsize::new(0));

    bus.register_any("events", Box::new(|_: &dyn Any| panic!("bad consumer")))
        .expect("register the panicking handler");
    let counter = Arc::clone(&delivered);
    bus.register_any(
        "events",
        Box::new(move |_: &dyn Any| {
            counter.fetch_add(1, Ordering::SeqCst);
        }),
    )
    .expect("register a healthy handler");

    bus.try_publish_any("events", &Message { value: 1 })
        .expect("a handler panic does not fail publication");

    assert_eq!(
        delivered.load(Ordering::SeqCst),
        1,
        "handlers after a panicking one still run"
    );
    assert!(
        bus.is_endpoint_failed("events"),
        "the panicking endpoint is marked failed"
    );
    assert_eq!(bus.failed_endpoints(), vec!["events".to_string()]);

    // A failed registration is not retried; the healthy handlers keep
    // receiving, so one bad consumer cannot break publication.
    bus.try_publish_any("events", &Message { value: 2 })
        .expect("publication continues after a handler failure");
    assert_eq!(delivered.load(Ordering::SeqCst), 2);
}

#[test]
fn send_any_and_register_response_handler_are_unsupported() {
    let bus = InProcessBus::new();

    assert_eq!(
        bus.send_any("events", &Message { value: 1 }),
        Err(BusError::Unsupported),
        "the bus carries events only; commands have no request path"
    );
    assert_eq!(
        bus.register_response_handler(1, Box::new(|_: &dyn Any| {})),
        Err(BusError::Unsupported),
        "request/response is declared but not implemented in this release"
    );
}

#[test]
fn commands_have_no_ingress_on_the_bus() {
    let bus = InProcessBus::new();
    let received = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&received);
    bus.register_any(
        EVENTS_TOPIC,
        Box::new(move |message: &dyn Any| {
            assert!(
                message.downcast_ref::<EventRecord>().is_some(),
                "only durable EventRecords are published on the events endpoint"
            );
            counter.fetch_add(1, Ordering::SeqCst);
        }),
    )
    .expect("register a subscriber");

    let command = EngineCommand::CancelOrder {
        symbol: "BTC-USDT".to_string(),
        order_id: 1,
    };
    assert_eq!(
        bus.send_any(EVENTS_TOPIC, &command),
        Err(BusError::Unsupported),
        "there is no command path on the bus"
    );
    assert_eq!(
        received.load(Ordering::SeqCst),
        0,
        "a command was never delivered to subscribers"
    );
}

#[test]
fn the_bus_is_object_safe_and_shareable_across_threads() {
    fn assert_send_sync<T: Send + Sync + 'static>() {}
    assert_send_sync::<InProcessBus>();

    let bus: Arc<dyn MessageBus> = Arc::new(InProcessBus::new());
    let received = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&received);
    bus.register_any(
        EVENTS_TOPIC,
        Box::new(move |message: &dyn Any| {
            assert!(message.downcast_ref::<EventRecord>().is_some());
            counter.fetch_add(1, Ordering::SeqCst);
        }),
    )
    .expect("register through the trait object");

    let producer = Arc::clone(&bus);
    std::thread::spawn(move || {
        producer
            .try_publish_any(EVENTS_TOPIC, &event_record(1))
            .expect("publish from another thread");
    })
    .join()
    .expect("publisher thread");

    assert_eq!(received.load(Ordering::SeqCst), 1);
}
