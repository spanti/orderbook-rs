# 08: MessageBus and Event Publisher

**What to build:** Durable events reach consumers through an in-process MessageBus. A separate publisher reads the event log from its last published sequence up to the durability watermark and publishes in sequence order. Subscribers register handlers synchronously; a bad handler cannot kill publication; a consumer that falls behind catches up from the event log. The bus never carries commands.

**Blocked by:** 05, 06

**Status:** done

- [x] `register_any` accepts multiple handlers per endpoint; `try_publish_any` synchronously calls every handler registered under the topic in registration order.
- [x] A panicking handler is caught, its endpoint is marked failed, and remaining handlers still run.
- [x] `send_any` and `register_response_handler` return an unsupported error; there is no command message type or command path.
- [x] The Event Publisher reads `EventLog::read_from(last_published + 1)` up to the durable watermark and publishes records in sequence order.
- [x] The publisher starts at the recovered durable watermark and does not republish history.
- [x] A subscriber that missed events can catch up from `EventLog::read_from` using its last seen sequence.
- [x] A slow handler delays publication for all subscribers but does not block the matching thread.
- [x] A test subscriber receives durable events in order; the bus is not on the matching-thread path.

## Comments

Merged in `merge: in-process MessageBus and Event Publisher (ticket 08)`.

- The publisher shares the event log behind an `Arc<Mutex<Box<dyn EventLog>>>` with the journal, so both threads use one handle and the publisher cannot read a different log. The mutex covers one `append_batch` or one `read_from`; the publisher reads only after the durability watermark advances, and it dispatches handlers after releasing the log lock, so a slow subscriber never blocks the journal or the matching thread.
- `try_publish_any` wraps each handler in `catch_unwind`; a panicking handler marks its registration failed, is skipped on later publishes, and the remaining handlers in the same publish still run. The concrete `InProcessBus` exposes the failed endpoints via `is_endpoint_failed` / `failed_endpoints`.
- `send_any` and `register_response_handler` return `BusError::Unsupported`; there is no command or request/response path on the bus.
- Shutdown joins the journal first, making the watermark final; the publisher then drains to it and exits within `shutdown_timeout`, detaching only if a synchronous handler cannot be interrupted (consumers catch up from the log).
