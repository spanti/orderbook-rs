# 08: MessageBus and Event Publisher

**What to build:** Durable events reach consumers through an in-process MessageBus. A separate publisher reads the event log from its last published sequence up to the durability watermark and publishes in sequence order. Subscribers register handlers synchronously; a bad handler cannot kill publication; a consumer that falls behind catches up from the event log. The bus never carries commands.

**Blocked by:** 05, 06

**Status:** ready-for-agent

- [ ] `register_any` accepts multiple handlers per endpoint; `try_publish_any` synchronously calls every handler registered under the topic in registration order.
- [ ] A panicking handler is caught, its endpoint is marked failed, and remaining handlers still run.
- [ ] `send_any` and `register_response_handler` return an unsupported error; there is no command message type or command path.
- [ ] The Event Publisher reads `EventLog::read_from(last_published + 1)` up to the durable watermark and publishes records in sequence order.
- [ ] The publisher starts at the recovered durable watermark and does not republish history.
- [ ] A subscriber that missed events can catch up from `EventLog::read_from` using its last seen sequence.
- [ ] A slow handler delays publication for all subscribers but does not block the matching thread.
- [ ] A test subscriber receives durable events in order; the bus is not on the matching-thread path.
