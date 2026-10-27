# 运行时采用 apply-first 与有界队列

## Status

accepted

## Context

撮合引擎要从"调用方直接持有 `&mut` 引擎"变成"命令经队列进入单一撮合线程"，并且要提供低延迟路径。原有的同步门面是 append-before-apply：每条命令都必须等 fsync 才返回。这样 applied 回执同样被 fsync 拖到几十–几百微秒，双档回执就没有意义，`submit → applied` 的 10µs 目标也无法达成。

事件化之后，持久化的单位从命令变成事件：一条命令先被决策为若干事件，事件才是日志记录与重放的单位。同步门面仍然可以"先决策、追加事件、再应用事件"；运行时则必须"先应用事件、后写日志"，否则 applied 回执要等日志。

研究（LMAX、CME Falcon、Eurex T7、TigerBeetle）显示两种真实做法：日志先于业务（LMAX），或业务先于持久化、回执标注 preliminary（Eurex）。本项目选择后者作为运行时路径，同时保留前者的同步实现。

## Decision

- 同步门面 `DurableMatchingEngine` 保持 **append-before-apply**：`decide` 产出事件 → 分配事件序号与 `command_id` → 追加全部 `EventRecord` 并 fsync → 逐条 `apply(event, sequence)`。追加失败则不应用，实例进入 failed 状态。
- 运行时（Matching Runtime）采用 **apply-first**：撮合线程 `decide` 后分配事件序号区间，逐条 `apply`，发送 Applied Acknowledgement（携带该命令的事件列表与首/末事件序号），再把全部 `EventRecord` 交给 Journal Thread；Journal Thread 批量追加并 fsync 后推进 Durability Watermark，撮合线程据此发送 Durable Acknowledgement。
- 提交方显式选择等待级别：`wait_applied`（preliminary）或 `wait_durable`（默认）。Durable 回执只在**该命令的最后一个事件序号**落盘后发出；崩溃可能使已发 Applied 回执的命令丢失。
- Ingress Command Queue 与 Journal 批次队列都有界：入站满时 `try_submit` 返回 `QueueFull`；日志队列满时撮合线程停止继续应用，让压力回到入站队列。禁止无界缓冲。
- `StoredSnapshot` 只在命令边界、且 `applied_seq == durable_seq` 时捕获，且以 `durable_seq`（事件序号）为边界；快照由独立 Snapshot Worker 写入 `SnapshotStore`，快照失败不失败已应用命令。`snapshot_cadence` 按事件序号计，在越过阈值的下一个命令边界捕获。
- 事件落盘后由独立的 Event Publisher 按 `sequence` 读取并发布到 MessageBus；匹配线程与 Journal Thread 都不等待消费者。见 ADR 0005。
- 原有低延迟配套保留：Journal Thread + group commit、预分配回执槽（热路径不分配）、逐条应用而按批写日志、可选 `core_affinity` 绑核、可配置 spin/backoff。
- 两条写路径共享 `decide`/`apply`、恢复算法与存储 trait，避免语义漂移。

## Considered Options

- **运行时继续 append-before-apply**：拒绝。applied 回执会被 fsync 拖到几十–几百微秒，双档失去意义，10µs 的 applied SLO 无法达成。
- **单档 durable-first + group commit**：保留"内存永不领先于日志"的强不变量，但 `submit → applied` 仍受 fsync 约束；作为同步门面保留，不作为运行时的低延迟路径。
- **无界队列**：拒绝。过载时变成 OOM 定时器，排队延迟不可控。
- **自研 Disruptor 式 ring**：本次拒绝。std mpsc 的有界 array flavor 已满足 µs–ms 目标；ring 留作 sub-µs 目标的独立特性。
- **WAL 留在撮合线程**：拒绝。逐命令 fsync 直接决定延迟下限。
- **快照节奏按命令计**：拒绝。一条命令可能产生大量事件（扫描成交），按命令计无法直接约束重放长度；按事件序号计更贴近恢复成本。

## Consequences

- "内存永不领先于日志"不再对运行时成立；新的不变量是"绝不向 Durable 等待者确认未落盘的状态"，且 Durable 回执必须等到该命令的**全部**事件落盘。
- Applied 回执是 preliminary：只等 applied 的客户端必须接受崩溃丢失整条命令的事件。
- 回执与 `PendingDurable` 以"命令的最后一个事件序号"为完成条件，不再使用命令序号。
- 快照会滞后于应用进度，直到 Journal Thread 追上；快照节流与重试必须可观测。
- 运行时与同步门面是两条写路径，恢复算法必须共享，避免语义漂移。
- 双 SLO 下，低延迟指撮合路径（`submit → applied` p99 ≤ 10µs，包含该命令全部事件的 apply）；durable ack 的 p99 ≤ 500µs 仍由 fsync 主导。
- 事件量放大：Journal 的批次上限、队列容量与 metrics 需要按事件记录而不是命令计量。
- 在没有 CPU 隔离与自研 ring 之前，不应宣称 HFT 级低延迟。
- 事件日志、成交历史与台账仍会持续增长；保留策略不在本 ADR 范围，本轮不截断。
