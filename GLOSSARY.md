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
一次进入引擎的状态变更请求：提交订单、改单、撤单、提交止损单。命令只用于驱动撮合引擎产生事件，不被持久化，也不能用于恢复状态。
_Avoid_: order event、request

**Event** / 事件:
引擎对一条命令作出决策后产生的、可重放的事实。除 `OrderRejected` 外，事件是状态变更增量并携带重建状态所需的完整信息；`OrderRejected` 是唯一不改变状态的审计事件，重放时无效果。事件日志是重建引擎状态的唯一事实来源。
_Avoid_: 用 "event" 指代命令、notification

**Event Log** / 事件日志:
按全局顺序持久化的事件序列，是重建引擎状态的唯一事实来源，等价于 WAL。命令不进入事件日志。
_Avoid_: command log、order event log

**Event Record** / 事件记录:
Event Log 中的一条记录：format version、全局事件序号、command id 与事件本身。日志单位是事件记录，不是命令。
_Avoid_: command record、log line

**Command Id** / 命令关联标识:
写者为每条命令分配的全局递增标识，盖在该命令产生的所有事件记录上，用于下游分组与回执；不是恢复边界，重放忽略。
_Avoid_: command sequence、event sequence

**Decide** / 决策:
纯函数步骤：读取引擎状态与一条命令，产出事件列表，不修改状态。
_Avoid_: execute（决策阶段）

**Apply** / 应用:
唯一的状态写入口：把一个事件增量折叠进引擎状态。重放只调用 apply，不重新撮合。
_Avoid_: execute、replay command

**Sequencer** / 定序器:
为每个事件分配全局递增事件序号，并保证 Event Log 按该顺序追加的组件。分配序号不等于落盘。
_Avoid_: dispatcher、gateway

**Recovery Gap** / 恢复空洞:
事件日志中缺失的序号区间，使快照与剩余日志无法衔接。出现时必须拒绝启动，而不是带病运行。

## Ingestion

**Command Queue** / 入站命令队列:
Command 从提交方到达撮合线程所经过的瞬态通道。命令离开队列后不留下任何可恢复的痕迹。
_Avoid_: 队列、buffer、command buffer

**Command Envelope** / 命令信封:
Command 在 Command Queue 中的载体，同时携带 Request Id 与回执凭据。
_Avoid_: request、message

**Request Id** / 请求标识:
提交方为一次 Command 附加的、用于观测与追踪的标识。它不是幂等键：引擎不据此去重，也不把它纳入恢复状态。
_Avoid_: request（request 专指命令内携带的参数载体）、client order id

**Ticket** / 回执凭据:
命令入队后交给提交方的凭据，用于取得该命令的处理结果。
_Avoid_: future、promise

**Matching Thread** / 撮合线程:
独占引擎状态、从 Command Queue 取出命令、决策并应用其事件的唯一线程。
_Avoid_: worker、dispatcher

**Engine Handle** / 引擎句柄:
提交方持有的投递入口，自身不拥有撮合状态。
_Avoid_: client、proxy、gateway

## Runtime

**Matching Runtime** / 撮合运行时:
由一个 Engine Handle、一个 Command Queue、一个 Matching Thread、一个 Journal Thread 和一个 Snapshot Worker 组成的运行时；提交方只通过 Engine Handle 与它交互。
_Avoid_: server、engine service

**Journal Thread** / 日志线程:
独占 Event Log、按批次追加事件记录并 fsync 的线程。它不拥有撮合状态。
_Avoid_: writer、logger

**Snapshot Worker** / 快照工作线程:
独占 Snapshot Store、把 Engine Snapshot 写入存储的线程。它不拥有撮合状态。
_Avoid_: checkpoint thread

**Applied Acknowledgement** / 应用回执:
命令产生的全部事件已应用到引擎状态、但尚未确认落盘的回执。它是 preliminary 的：崩溃后该命令的事件可能不在 Event Log 中。
_Avoid_: immediate ack、soft ack

**Durable Acknowledgement** / 持久化回执:
命令产生的全部事件已落盘、可从 Event Log 恢复的回执。它同时蕴含 Applied Acknowledgement。
_Avoid_: commit ack、hard ack

**Durability Watermark** / 持久化水位:
已确认落盘的最大 Event Sequence（`durable_seq`）。Engine Snapshot 不得越过它。
_Avoid_: commit index、checkpoint

## Distribution

**MessageBus** / 消息总线:
进程内同步分发的事件出口。消费者按 endpoint 注册 handler；事件在落盘后由 Event Publisher 发布。它不持久化，也不承载命令。
_Avoid_: broker、queue

**Event Publisher** / 事件发布者:
独立线程，按事件序号从 Event Log 读取已落盘事件并发布到 MessageBus；启动时从恢复水位开始，不重发历史。
_Avoid_: journal thread、dispatcher

## Recovery

**Durable Matching Engine** / 持久化撮合引擎:
组合在纯内存撮合引擎之外的同步持久化门面（`DurableMatchingEngine`）；先决策、追加事件、再应用事件（append-before-apply），并可按需生成引擎快照。自动、按事件阈值触发的快照属于 Matching Runtime 的 Snapshot Gate，不在本门面内。它不同于 apply-first 的 Matching Runtime；两者共享 `decide`/`apply`、恢复算法与存储 trait。撮合引擎本身不感知 IO。
_Avoid_: persistent engine、matching service

**Replay** / 重放:
从全新引擎开始，按顺序应用事件日志中的事件，重建状态。重放不重新撮合、不重新判定拒绝。

**Exact Recovery** / 精确恢复:
恢复后的引擎决策状态与按顺序折叠已落盘 Event Log 的结果逐位一致：簿内订单及优先级、订单台账、止损单、各计数器、成交记录、last trade price、配置。只收到 Applied Acknowledgement 而未被 Durable Acknowledgement 确认的事件从未写入日志，因而也不在恢复范围内。

**Engine Snapshot** / 引擎快照:
某个事件边界上的完整引擎状态，内含 applied event sequence，用于缩短恢复时的重放长度。它是事件日志的派生缓存，不是事实来源。
_Avoid_: book snapshot

**Book Snapshot** / 行情快照:
某 symbol 的聚合价格档位视图，只用于行情展示，不能用于恢复状态。
_Avoid_: snapshot

**Snapshot Floor** / 快照地板:
快照存储中已持久化的最大 applied event sequence；新快照不得低于它，事件日志也不得在它之前被清理。
_Avoid_: checkpoint offset

**Event Sequence** / 事件序号:
事件在 Event Log 中的全局递增序号，是恢复与快照边界。一条命令可能产生多个事件，它们共享同一个命令关联标识但各自占用一个事件序号。
