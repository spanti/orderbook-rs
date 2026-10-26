# 以事件日志而非命令日志作为恢复事实源

## Status

accepted；取代 [ADR 0001](0001-command-log-as-source-of-truth.md)

## Context

ADR 0001 选择命令日志，是因为当时的 `EngineEvent` 只是结果通知：`OrderAccepted` 不带 side/price，`OrderRejected` 连 symbol 都没有，无法把订单放回簿里；而且当时没有下游消费者需要事件历史。

现在的目标变了：

- 订单的事件结构必须携带产生状态变化所需的完整信息；重放直接应用已经产生的事件，而不是重新执行命令。
- 事件日志是恢复引擎状态的唯一事实来源；命令只用于驱动撮合引擎产生事件，不持久化。
- 事件要发布到 MessageBus，供将来的行情模块与（盘后）风控模块消费。

这意味着不是把现有通知事件塞进日志，而是重新定义事件，使每个事件都是可折叠的状态增量。

## Decision

- **事件目录**：`OrderAccepted`、`OrderRested`、`TradeExecuted`、`OrderCanceled`、`StopPlaced`、`StopTriggered`、`StopCanceled`、`OrderRejected`。`Order` 携带 id/side/price/quantity/timestamp/type/TIF；symbol 由事件携带。`OrderRested` 与 `StopTriggered` 中的 `Order.quantity` 是该事件发生时的剩余量。
- **每类命令的事件序列**：
  - 新提交：`OrderAccepted` → `TradeExecuted`* → `OrderRested`（有剩余且可挂）或 `OrderCanceled{UnfilledResidual}`（有剩余但不可挂）；全成交则没有后两者。
  - 撤单：`OrderCanceled{UserCancel}`；撤待触发止损：`StopCanceled`。
  - 改单：保持现有语义（撤旧 + 新 id）→ `OrderCanceled{Modified}` → `OrderAccepted`（新 id）→ 成交* → `OrderRested` 或 `OrderCanceled{UnfilledResidual}`。
  - 提交止损：`StopPlaced`（不再用 `OrderAccepted` 表示止损登记）。
  - 触发止损：`StopTriggered`（携带转换后的完整 `Order`）→ 成交* → `OrderRested` 或 `OrderCanceled{UnfilledResidual}`。
- **前缀合法**：事件流的任意前缀都是合法引擎状态。因此 `OrderAccepted` 只创建台账、不入簿；入簿由 `OrderRested` 表达；`StopTriggered` 自己完成"移出止损簿 + 台账转为活动"，不依赖后续 `OrderAccepted` 补齐。
- **拒绝是事件**：被拒命令产生 `OrderRejected { symbol, reason, command }`，写入事件日志并发布到 bus。它是唯一不改变状态的事件，重放时 no-op。被拒命令消耗一个事件序号，但不分配 order id。
- **decide/apply 拆分**：`decide(&self, &EngineCommand) -> Vec<DomainEvent>` 只读；`apply(&mut self, &DomainEvent, sequence)` 是唯一写入口。`MatchingEngine::execute` 只是 decide + apply 的便利组合。内部不变量破坏仍 panic，不转成 `OrderRejected`。
- **事件记录**：`EventRecord { format_version, sequence, command_id, event }`。`sequence` 由唯一写者分配，是恢复与快照边界；`command_id` 是每条命令一个全局递增关联标识，盖在该命令的所有事件上，仅供下游分组与回执，重放忽略。`command_id` 必须跨重启连续：`StoredSnapshot` 记录 `last_command_id`，恢复取快照与日志尾部的最大值 +1。
- **事件序号归日志层**：`DomainEvent` 不带 `sequence_id`；`MatchingEngine` 不保留事件计数器；`EngineSnapshot` 删除 `next_event_sequence`；`BookSnapshot.sequence` 取最后已应用事件序号。
- **计数器**：`next_order_id` / `next_trade_id` 由已应用事件中的 id 推导（`max(id)+1`），不单独持久化；`EngineConfig` 仍在日志头。
- **快照**：`EngineSnapshot` 是事件日志的派生缓存；`StoredSnapshot.applied_seq` 是事件序号。快照只在命令边界、且 `applied_event_seq == durable_event_seq` 时捕获。
- **API**：删除 `ExecutionReport`、`CommandOutcome`、`events()`、`clear_events()`、`last_sequence()`；`execute` 返回 `Vec<DomainEvent>`；回执返回该命令的事件列表与事件序号区间。
- **格式**：`FORMAT_VERSION = 2`；v1 命令日志直接拒绝；不提供迁移工具；本轮不截断事件日志。
- **schema 演进**：只允许加可选字段（serde 默认值）；破坏性变更升版本并拒绝旧日志；暂不做 per-event 版本号。

## Considered Options

- **只富化现有 5 种通知事件**：拒绝。仍无法表达"IOC/市价剩余被丢弃"，也无法让入簿与成交各自成为原子增量；重放会重新撮合。
- **命令日志 + 事件日志双写**：拒绝。两个事实源，需要额外的顺序、去重与一致性协议。
- **拒绝只发 bus、不写日志**：拒绝（用户选择统一为事件）。代价是日志包含被拒流量，换来完整审计与单一事件流。
- **由引擎分配事件序号**：拒绝。apply-first 下引擎先分配、日志后追加，两个分配器容易漂移。
- **保留 `ExecutionReport` 作为并行返回值**：拒绝。它会变成第二套结果视图；调用方应从事件派生。
- **提供 v1→v2 迁移工具**：拒绝。当前没有生产数据，迁移工具的成本大于收益。

## Consequences

- 事件日志比命令日志大：一条挂单至少 2 个事件，扫描成交会产生大量事件；被拒流量也占日志与序号。
- 重放不再撮合，只做状态折叠；确定性由"事件完整 + 单写者 + 连续序号"保证。
- 被接受命令的原始命令不再持久化；下游只能看到事件，需要分组时使用 `command_id`。
- `OrderRejected` 是唯一无状态效果的事件，重放 no-op；`Exact Recovery` 不因它改变状态。
- 所有依赖 `CommandOutcome`/`ExecutionReport` 的测试、bench、CLI 必须迁移。
- v1 日志不可读；没有迁移工具，格式升级需要显式重建。
- 事件日志无截断；快照只是缓存，不能单独作为事实源。
- 行情/风控消费者必须自己按 `command_id` 分组、按 `sequence` 去重/续读。
