# Order Book Matching Engine

撮合引擎的领域词汇。本文件只定义术语，不记录实现决策或设计方案。

## Matching

**Order** / 订单:
一张带有 side、price、quantity 的委托。订单进入引擎后由引擎分配全局唯一的 order id。
_Avoid_: request（request 专指命令内携带的参数载体）

**Maker** / 挂单方:
先挂在簿上、被动等待成交的订单。

**Taker** / 吃单方:
主动进入引擎并立即与簿上订单撮合的订单。

**Resting Order** / 挂单:
成交后仍有剩余、并按价格-时间优先级留在簿上的限价 GTC 订单。

**Stop Order** / 止损单:
在触发前不参与撮合的订单；当 last trade price 达到其 stop price 时转为市价单或限价单。

**Last Trade Price** / 最新成交价:
某 symbol 最近一笔成交的价格，是止损单触发的判据。

**Order Ledger** / 订单台账:
每条已接受订单的状态与数量记录的唯一所有者，不是资金账本。
_Avoid_: ledger（有歧义）

## Commands and Events

**Command** / 命令:
一次进入引擎的状态变更请求：提交订单、改单、撤单、提交止损单。
_Avoid_: order event、request

**Event** / 事件:
引擎处理命令后发出的结果通知（OrderAccepted、TradeExecuted、OrderCanceled、OrderRejected、StopTriggered）。事件由命令派生，不用于恢复状态。
_Avoid_: 用 "event" 指代命令

**Command Log** / 命令日志:
按全局顺序持久化的命令序列，是重建引擎状态的唯一事实来源，等价于 WAL。
_Avoid_: event log、order event log

**Sequencer** / 定序器:
为每条命令分配全局递增命令序号，并在应用前写入命令日志的组件；本项目中由 DurableMatchingEngine 承担。
_Avoid_: dispatcher、gateway

**Recovery Gap** / 恢复空洞:
命令日志中缺失的序号区间，使快照与剩余日志无法衔接。出现时必须拒绝启动，而不是带病运行。

## Recovery

**Durable Matching Engine** / 持久化撮合引擎:
组合在纯内存撮合引擎之外的持久化层，负责先写命令日志再执行，并按阈值生成引擎快照。撮合引擎本身不感知 IO。
_Avoid_: persistent engine、matching service

**Replay** / 重放:
从全新引擎开始，按顺序应用命令日志中的命令，重建状态。

**Exact Recovery** / 精确恢复:
恢复后的引擎决策状态与中断前逐位一致：簿内订单及优先级、订单台账、止损单、各计数器、成交记录、last trade price、配置。输出/审计缓冲（事件历史、命令缓冲）不属于恢复状态。

**Engine Snapshot** / 引擎快照:
某个命令边界上的完整引擎状态，内含 applied command sequence，用于缩短恢复时的重放长度。
_Avoid_: book snapshot

**Book Snapshot** / 行情快照:
某 symbol 的聚合价格档位视图，只用于行情展示，不能用于恢复状态。
_Avoid_: snapshot

**Command Sequence** / 命令序号:
引擎为每条已持久化命令分配的全局递增序号，是快照的恢复边界。
_Avoid_: event sequence、sequence_id

**Snapshot Floor** / 快照地板:
快照存储中已持久化的最大 applied command sequence；新快照不得低于它，命令日志也不得在它之前被清理。
_Avoid_: checkpoint offset

**Event Sequence** / 事件序号:
引擎为每个输出事件分配的全局递增序号（`EngineEvent::sequence_id`）。一条命令可能产生多个事件，因此事件序号不是恢复边界。
_Avoid_: command sequence
