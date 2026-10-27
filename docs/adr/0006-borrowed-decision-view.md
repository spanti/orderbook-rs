# 决策阶段使用借用视图与写时复制覆盖层

## Status

accepted

## Context

`MatchingEngine::decide` 是事件化路径的纯决策步骤：命令进、事件出，不修改状态。为了既能撮合又不写真实簿，`DecisionScratch` 目前会在每次 `SubmitOrder` / `ModifyOrder` 前 `clone` 当前 symbol 的整个 `OrderBook` 与 `StopBook`（`src/engine.rs`），随后在副本上撮合，并由 stop cascade 继续使用同一份副本。

这带来两个问题：

- 复杂度错误：`decide` 的成本变成 O(resting orders)，与本次命令实际触碰的价位/订单无关。簿越大，每条命令越慢。
- 运行时代价：Matching Runtime 的撮合线程对每条命令都调用 `decide`（`src/runtime/matching.rs`），因此每条命令都在为整本簿的拷贝付费；`--path decide` 的 bench 只在开始时 seed 一张挂单，clone 成本被基线掩盖，无法暴露问题。

事件目录、每命令事件顺序、`apply` 是唯一写入口、以及“事件流任意前缀都是合法引擎状态”都必须保持不变。`docs/durable-storage.md` 规定：改变决策策略需要显式 ADR。

## Decision

- 新增 `pub(crate) BookView<'a>`，借用 `Option<&'a OrderBook>`（`None` 表示空 symbol）。它是 `OrderBook::submit` 的非改写等价物：
  - `plan(&mut self, order: Order) -> SubmitResult`：遍历 base 价位与 overlay，返回 `fills` / `remaining_quantity` / `rested`，并把本次效果记入 overlay，供同一 cascade 的后续 plan 使用；
  - `cancel_limit_order(&mut self, order_id) -> Option<Order>`：modify 用的非改写等价物，返回当前剩余量。
- `BookView` 的 overlay 只记录被触碰的订单与价位：
  - 每个订单的 remaining quantity override（`0` 表示已移除）；
  - 之前 rest 新增的订单，按 `(side, price)` 以 FIFO 保存；
  - base 价位与 added 价位按价格优先惰性合并，只在需要时前进游标。
- overlay 精确复刻 `OrderBook::submit`：同一价位的 base 订单按 FIFO 在前，之前 rest 的订单追加在后；部分成交的 maker 保持位置；全成交的 maker 消失；taker 剩余量追加到自己的价位。
- 新增 `pub(crate) StopView<'a>`，借用 `Option<&'a StopBook>`。`next_triggered(last_price)` 与 `remove(order_id)` 通过 removed 集合在 overlay 中记录移除，跳过已移除的止损；买卖两侧的触发方向与“同价按 order id 升序、跨侧取较小 id”的规则不变。
- `DecisionScratch` 改为持有/借用这些视图；`push_taker_match` 与 `run_stop_cascade` 接收 `&mut BookView` / `&mut StopView`。remove 掉 `scratch_book()` / `scratch_stops()`。
- 决策路径不再 `clone` 整个 `OrderBook` 或 `StopBook`。`decide` 仍为 `&self` 纯函数；`apply` 仍是唯一状态写入口。
- 事件目录、每命令事件顺序、`OrderAccepted` 与 `OrderRested` 的分工、`apply`、恢复、事件日志、快照与 durable format 全部不变。
- `OrderBook::submit`、`OrderBook::cancel_limit_order`、`rest_order`、`reduce_order` 等保留给 apply 路径与既有测试。

## Considered Options

- **保留整簿 clone**：实现最简单，但 `decide` 仍是 O(resting orders)，运行时每条命令都付费；放弃。
- **只 clone 会被穿越的价位**：仍需可变副本，而且 cascade 的后续 plan 需要看到前面 plan 的成交与新挂单，可穿越价位集合会变化，等价于逐步重建整本簿；放弃。
- **直接在真实簿上撮合再回滚**：违反 `decide` 纯函数与“apply 是唯一写入口”，回滚失败还会污染状态；放弃。
- **为视图复制一套撮合算法**：两份价格优先/FIFO 语义容易漂移；改为复用 `MatchWalk` 的价格优先定义，只实现负责叠加 overlay 的 plan。
- **不合并 base 与 added 价位**：例如把新 rest 的订单直接排在所有 base 价位之后，会破坏价格优先（rest 到更优价位时尤其明显）；放弃。

## Consequences

- `decide` 的成本降为 O(touched levels/orders)，与未触碰的挂单总量无关；运行时与 decide bench 都受益。
- 正确性负担转移到 overlay：它必须精确复刻 FIFO 与部分成交语义，并由等价测试锁定——`BookView::plan` vs `OrderBook::submit`、连续 plan vs 连续 submit、cancel、以及一个混合命令序列的 `DomainEvent` 流。
- 事件目录、`apply`、恢复、快照与 durable format 不变；ADR 0003（apply-first 运行时）与 ADR 0004（事件源日志）不受影响。
- 视图只在决策阶段使用；apply 仍通过 `rest_order` / `reduce_order` / `cancel_limit_order` 折叠已决定的事件。
- 这是决策策略变更，后续若再改（包括替换本视图机制）仍需显式 ADR。
