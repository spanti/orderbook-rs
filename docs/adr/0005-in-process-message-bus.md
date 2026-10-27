# 进程内同步 MessageBus 与日志追赶

## Status

accepted

## Context

运行时目前在每个命令后清空事件，没有任何输出流。将来的行情模块与（盘后）风控模块需要消费事件，因此事件必须在落盘后发布到一个 MessageBus。

约束与取舍：

- 事件只有落盘后才能发布，否则下游会看到崩溃后不存在的"幽灵事件"。
- 发布不能阻塞撮合线程；消费者可以落后，并在之后从事件日志追赶。
- 消息参数定为 `&dyn Any`。`&dyn Any` 是借用，dispatcher 线程无法持有它，因此总线只能是**同步分发**：handler 在调用者线程上执行。
- 本轮先做进程内实现；外部 broker（Kafka/NATS）以后作为 adapter。

## Decision

- 接口为四个方法：

```
register_any(endpoint: &str, handler: Box<dyn Fn(&dyn Any) + Send + 'static>) -> Result<(), BusError>
register_response_handler(correlation_id: u64, handler: Box<dyn Fn(&dyn Any) + Send + 'static>) -> Result<(), BusError>
send_any(endpoint: &str, message: &dyn Any) -> Result<(), BusError>
try_publish_any(topic: &str, message: &dyn Any) -> Result<(), BusError>
```

- 本轮实现 `register_any` 与 `try_publish_any`。`send_any` 与 `register_response_handler` 只保留 trait 方法与文档，返回 `BusError::Unsupported`；没有真实请求/响应用例之前不实现。
- `register_any` 允许同名多个 handler；`try_publish_any` 同步调用该 topic 下所有 handler。`send_any` 未来只投递给一个 handler；同名多个时返回 `BusError::AmbiguousEndpoint`。
- handler 在调用者线程同步执行；publisher 对每个 handler 捕获 panic，把该 endpoint 标记为 failed 并继续发布其他 handler。
- 只有 Event Publisher 线程可以对 `"events"` topic 调用 `try_publish_any`，消息的具体类型是 `EventRecord`。命令不允许进入 bus；`&dyn Any` 无法在类型上强制这一点，因此这是约定 + review 约束。
- **发布路径**：Journal Thread 在 `append_batch` 落盘后只发一个"水位/唤醒"信号；Event Publisher 用 `EventLog::read_from(last_published + 1)` 读到 `durable_seq`，逐条 `try_publish_any`。这样唤醒丢失也无害，publisher 永远从日志补齐，不漏事件。
- **读句柄**：Journal 与 Publisher 共享同一个 `EventLog` 句柄（`Arc<Mutex<..>>`）。互斥锁只覆盖一次 `append_batch` 或一次 `read_from` 的拷贝；handler 在锁外同步执行。因此慢订阅者只拖慢后续发布，不会阻塞 Journal 写入或撮合线程，也不需要为每个 adapter 增加读句柄接口。
- publisher 启动时从恢复出的 durable 水位开始，只发实时事件；历史回放由消费方自己读 `EventLog`，避免每次重启把全量历史重发给所有进程内订阅者。
- 发布顺序 = 事件序号顺序；publisher 是唯一发布者。bus 不做过滤，消费方自己 downcast/filter。
- shutdown：publisher 追到 `durable_seq` 后退出，受 `shutdown_timeout` 约束；未发布部分由消费方从日志追赶。

## Considered Options

- **异步队列 + 每订阅者有界队列**：拒绝。`&dyn Any` 不能跨线程持有；要异步必须把 `send_any`/`try_publish_any` 的参数改成 `Box<dyn Any + Send>` 或 `Arc<dyn Any + Send + Sync>`。
- **以事件日志本身作为 bus**：拒绝。用户要求独立的 MessageBus；日志仍作为事实源与追赶来源。
- **落盘前发布（apply 后立即发布）**：拒绝。下游可能看到崩溃后不存在的事件。
- **Journal 把整批记录发给 publisher**：拒绝。队列满时仍要回退到读日志；唤醒/水位更简单，且天然可补齐。
- **阻塞 publisher 以施加背压**：拒绝。会间接拖住 Journal/撮合；消费方读日志追赶即可。

## Consequences

- 一个慢 handler 会拖住整个发布流（但不拖住撮合）；所有订阅者共享这一延迟。
- handler panic 被隔离，endpoint 标记失败；不会杀死 publisher。
- `&dyn Any` 失去类型安全；downcast 失败是消费方 bug，bus 不检测。
- 消费方必须自己保存 `last_seen` 并从 `EventLog` 追赶；bus 不保存历史、不做 lag 检测。
- `send_any`/`register_response_handler` 未实现；在有人给出真实请求/响应用例前，不应把命令或控制消息接到 bus。
- 外部 broker 以后作为 `MessageBus` adapter；事件日志仍是事实源。
