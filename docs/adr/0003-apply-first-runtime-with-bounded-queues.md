# 运行时采用 apply-first 与有界队列

## Status

accepted

## Context

撮合引擎要从"调用方直接持有 `&mut` 引擎"变成"命令经队列进入单一撮合线程"，并且要提供低延迟路径。原有的 `DurableMatchingEngine` 是 append-before-apply：每条命令都必须等 fsync 才返回。这样 applied 回执同样被 fsync 拖到几十–几百微秒，双档回执就没有意义，`submit → applied` 的 10µs 目标也无法达成。

研究（LMAX、CME Falcon、Eurex T7、TigerBeetle）显示两种真实做法：日志先于业务（LMAX），或业务先于持久化、回执标注 preliminary（Eurex）。本项目选择后者作为运行时路径，同时保留前者的同步实现。

## Decision

- 运行时（Matching Runtime）采用 **apply-first**：撮合线程先应用命令并发送 Applied Acknowledgement，再把 Command Record 交给 Journal Thread；Journal Thread 批量追加并 fsync 后推进 Durability Watermark，撮合线程据此发送 Durable Acknowledgement。
- 提交方显式选择等待级别：`wait_applied`（preliminary）或 `wait_durable`（默认）。Durable 回执只在记录落盘后发出；崩溃可能使已发 Applied 回执的命令丢失。
- Ingress Command Queue 与 Journal 批次队列都有界：入站满时 `try_submit` 返回 `QueueFull`；日志队列满时撮合线程停止继续应用，让压力回到入站队列。禁止无界缓冲。
- Engine Snapshot 只在 `applied_seq == durable_seq` 时捕获，且以 `durable_seq` 为边界；快照由独立 Snapshot Worker 写入，快照失败不失败已应用命令。
- 原有 `DurableMatchingEngine` 保留为同步 append-before-apply 门面（直接调用与测试）；运行时使用独立的 apply-first 管线，共享恢复算法与存储 trait。
- 低延迟配套：Journal Thread + group commit、预分配回执槽（热路径不分配）、逐条应用而按批写日志、可选 `core_affinity` 绑核、可配置 spin/backoff。

## Considered Options

- **运行时继续 append-before-apply**：拒绝。applied 回执会被 fsync 拖到几十–几百微秒，双档失去意义，10µs 的 applied SLO 无法达成。
- **单档 durable-first + group commit**：保留"内存永不领先于日志"的强不变量，但 `submit → applied` 仍受 fsync 约束；作为同步门面保留，不作为运行时的低延迟路径。
- **无界队列**：拒绝。过载时变成 OOM 定时器，排队延迟不可控。
- **自研 Disruptor 式 ring**：本次拒绝。std mpsc 的有界 array flavor 已满足 µs–ms 目标；ring 留作 sub-µs 目标的独立特性。
- **WAL 留在撮合线程**：拒绝。逐命令 fsync 直接决定延迟下限。

## Consequences

- "内存永不领先于日志"不再对运行时成立；新的不变量是"绝不向 Durable 等待者确认未落盘的状态"。
- Applied 回执是 preliminary：只等 applied 的客户端必须接受崩溃丢失。
- 快照会滞后于应用进度，直到 Journal Thread 追上；快照节流与重试必须可观测。
- 运行时与同步门面是两条写路径，恢复算法必须共享，避免语义漂移。
- 双 SLO 下，低延迟指撮合路径（`submit → applied` p99 ≤ 10µs）；durable ack 的 p99 ≤ 500µs 仍由 fsync 主导。
- 在没有 CPU 隔离与自研 ring 之前，不应宣称 HFT 级低延迟。
- 成交历史、台账与命令日志仍会持续增长；运行时会清空事件向量，但保留策略不在本 ADR 范围。
