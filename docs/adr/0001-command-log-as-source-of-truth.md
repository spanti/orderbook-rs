# 以命令日志而非事件日志作为恢复事实源

## Status

accepted（已被 [ADR 0004](0004-event-sourced-order-log.md) 取代）

> 本 ADR 记录当时的决策。事件模型已在 ADR 0004 中重设计，命令日志不再作为事实源。

## Context

恢复要求把引擎重建到与中断前逐位一致的状态。引擎对外发出 `EngineEvent`（OrderAccepted、TradeExecuted 等），但事件并不携带重建订单簿所需的全部信息：`OrderAccepted` 里的 `ExecutionReport` 没有 side/price，`OrderRejected` 连 symbol 都没有。命令（Submit/Modify/Cancel/SubmitStop）则是完整输入，且重放确定性已被现有测试验证。行业实践也把命令视为 input facts、事件视为 engine decisions（pitboss、Clobber、LMAX 风格的 state machine replication）。

## Decision

持久化日志只保存 `EngineCommand`。`EngineEvent` 保持为命令驱动引擎后产生的派生输出，不进入命令日志，也不进入快照。恢复边界使用命令序号（command sequence），不使用事件序号。

## Considered Options

- **以 `EngineEvent` 为事实源**：拒绝。事件缺少 side/price/symbol 等字段，无法把挂单放回簿里；补齐这些字段等于把事件变成命令的副本。
- **命令日志 + 事件日志双写**：拒绝。出现两个事实源，需要额外的顺序、去重与一致性协议；本次没有下游消费者需要回放事件历史。
- **只存快照、不存日志**：拒绝。快照之间的命令会丢失，无法做到精确恢复，也没有审计能力。

## Consequences

- 重放会重新生成事件；事件序号由 `next_sequence_id` 恢复，事件历史本身不恢复。
- 需要事件历史的消费者以后应消费独立的输出流，而不是命令日志。
- 引擎内的命令缓冲被移除，历史查询改由 `DurableMatchingEngine::history_from` 从命令日志读取。
