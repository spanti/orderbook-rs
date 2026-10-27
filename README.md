# orderbook-rs

事件溯源（event-sourced）的 Rust 撮合引擎：价格-时间优先订单簿、单写者低延迟运行时、可恢复的事件日志，以及面向上层模块的进程内 `MessageBus`。

这是一个教学/工程实践项目，重点在于把“命令 → 事件 → 状态折叠 → 持久化 → 运行时 → 下游发布”这条链路做完整、可测试；它不是可直接上生产的 HFT 引擎。已知边界见文末。

需要支持 edition 2024 的 Rust 工具链（建议 stable）。

## 特性

- **订单簿与撮合**：限价/市价、GTC/IOC/FOK、post-only、改单（撤旧 + 新 id）、撤单、止损单及触发级联；价格-时间优先（FIFO）。
- **事件溯源**：`DomainEvent` 是完整的状态增量；事件日志是唯一持久事实源；重放只做 `apply`，不重新撮合。
- **纯决策 + 唯一写入口**：`MatchingEngine::decide` 只读地产生事件，`apply` 是唯一状态写入口；任意事件前缀都是合法状态。
- **持久化与恢复**：v2 事件日志（CRC-32 + fsync）、事件序号快照、快照 + 连续尾部恢复、`command_id` 跨重启连续。
- **低延迟运行时**：单写者撮合线程、独立 Journal 线程与 group commit、有界队列、applied/durable 双档回执、命令边界快照、可选绑核。
- **MessageBus**：同步、进程内、`&dyn Any`；publisher 在事件落盘后按事件序号发布；消费方可从事件日志追赶；引擎句柄只暴露订阅视图，不能发布。
- **可验证性**：`--path decide` 纯决策基线、runtime applied/durable 双 SLO、事件量与 journal 批次 metrics。

## 架构

```text
producers
   │  EngineCommand（只作为输入，不落盘）
   ▼
EngineHandle ──► bounded Command Queue ──► Matching Thread
                                             │ decide → apply（逐事件）
                                             ├─ applied ack：events + first/last event sequence
                                             ├─ Vec<EventRecord> ──► Journal Thread ──► EventLog (fsync)
                                             │                              │
                                             │                              └─ watermark ──► durable ack
                                             └─ snapshot at command boundary ──► Snapshot Worker ──► SnapshotStore

EventLog ──► Event Publisher ──► MessageBus ──► subscribers（行情 / 盘后风控 / …）
```

- **Matching Thread** 独占引擎状态，是唯一写者。
- **Journal Thread** 独占事件日志，按批次 `append_batch` + fsync，推进 durability watermark。
- **Snapshot Worker** 独占快照存储；快照只是事件日志的派生缓存。
- **Event Publisher** 是独立线程，从 durable watermark 读日志并发布；慢订阅者只拖发布，不拖撮合。

## 快速开始

### 纯内存引擎

```rust
use orderbook_rs::types::{
    EngineCommand, EngineConfig, OrderRequest, OrderType, Side, TimeInForce,
};
use orderbook_rs::MatchingEngine;

fn main() {
    let mut engine = MatchingEngine::new(EngineConfig::default());

    let events = engine.execute(EngineCommand::SubmitOrder {
        symbol: "BTC-USDT".to_string(),
        request: OrderRequest {
            side: Side::Bid,
            price: 10_000,
            quantity: 5,
            timestamp: 1,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::GTC,
            post_only: false,
        },
    });

    for event in events {
        println!("{event:?}");
    }
}
```

一张不交叉的限价买单会产生 `OrderAccepted` + `OrderRested`。交叉提交会产生 `OrderAccepted` → `TradeExecuted`* → `OrderRested` 或 `OrderCanceled{UnfilledResidual}`。

### 同步持久化门面

```rust
use orderbook_rs::types::{EngineCommand, EngineConfig};
use orderbook_rs::{DurableMatchingEngine, InMemoryEventLog, InMemorySnapshotStore};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = DurableMatchingEngine::create(
        EngineConfig::default(),
        Box::new(InMemoryEventLog::new()),
        Box::new(InMemorySnapshotStore::new()),
    )?;

    // command 见上一节；execute 先追加全部 EventRecord，再逐事件 apply。
    let events = engine.execute(command)?;
    println!(
        "{} events, applied through event sequence {}",
        events.len(),
        engine.applied_seq()
    );
    Ok(())
}
```

`DurableMatchingEngine::open(log, snapshots)` 从“最新快照 + 连续事件尾部”恢复；v1 日志会被拒绝，没有迁移工具。

### 运行时 + MessageBus

```rust
use std::any::Any;
use std::sync::Arc;

use orderbook_rs::runtime::{EngineRuntime, RuntimeConfig};
use orderbook_rs::types::EngineConfig;
use orderbook_rs::{
    EventRecord, InMemoryEventLog, InMemorySnapshotStore, InProcessBus,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bus = Arc::new(InProcessBus::new());
    let (handle, _runtime) = EngineRuntime::create_with_bus(
        RuntimeConfig::default(),
        EngineConfig::default(),
        Box::new(InMemoryEventLog::new()),
        Box::new(InMemorySnapshotStore::new()),
        bus.clone(),
    )?;

    handle.bus().register_any(
        "market-data",
        Box::new(|message: &dyn Any| {
            if let Some(record) = message.downcast_ref::<EventRecord>() {
                println!(
                    "seq={} command={} event={:?}",
                    record.sequence, record.command_id, record.event
                );
            }
        }),
    )?;

    // command 见上；submit_and_wait 默认等 durable。
    let receipt = handle.submit_and_wait(command)?;
    println!(
        "{} events, seq {}..={}",
        receipt.events.len(),
        receipt.first_event_sequence,
        receipt.last_event_sequence
    );
    Ok(())
}
```

- `submit_and_wait_applied` 只等 applied（preliminary）；`submit_and_wait` 等该命令最后一个事件落盘。
- `send_any` / `register_response_handler` 本轮返回 `BusError::Unsupported`；bus 不承载命令。
- 订阅者掉队时，用 `EventLog::read_from(last_seen + 1)` 自行追赶。

## 事件模型

`EventRecord { format_version, sequence, command_id, event }` 是日志单位；`DomainEvent` 本身不带序号。`sequence` 是全局事件序号，也是恢复/快照边界；`command_id` 把一条命令产生的事件归组，重放忽略它。

| 事件 | 状态效果 |
| --- | --- |
| `OrderAccepted { symbol, order, post_only }` | 创建台账记录；**不改簿**（保证前缀合法） |
| `OrderRested { symbol, order }` | 把剩余量按价格-时间优先级插入簿 |
| `TradeExecuted { trade }` | maker 减簿/减台账、taker 加成交、记成交 tape、更新 last price |
| `OrderCanceled { symbol, order_id, canceled_quantity, reason }` | 从簿移除（若在簿）并标记台账取消；`reason` 为 `UserCancel` / `UnfilledResidual` / `Modified` |
| `StopPlaced { symbol, stop }` | 插入止损簿 + `PendingTrigger` 台账 |
| `StopTriggered { symbol, order, last_price }` | 移出止损簿并激活台账；后续成交/入簿/剩余丢弃照常 |
| `StopCanceled { symbol, order_id }` | 移出止损簿并标记台账取消 |
| `OrderRejected { symbol, reason, command }` | **唯一无状态效果**的审计事件；重放 no-op |

每类命令的事件序列：

- 提交：`OrderAccepted` → `TradeExecuted`* → `OrderRested` 或 `OrderCanceled{UnfilledResidual}`；全成交则没有后两者。
- 撤单：`OrderCanceled{UserCancel}`；撤待触发止损：`StopCanceled`。
- 改单：`OrderCanceled{Modified}` → `OrderAccepted`（新 id）→ 成交* → `OrderRested` 或 `OrderCanceled{UnfilledResidual}`。
- 提交止损：`StopPlaced`。
- 触发止损：`StopTriggered` → 成交* → `OrderRested` 或 `OrderCanceled{UnfilledResidual}`；触发级联按触发顺序继续。
- 拒绝：只有 `OrderRejected`；不分配 order id，但消耗一个事件序号和 `command_id`。

**核心不变量**：事件流的任意前缀都是合法引擎状态。这是 `OrderAccepted` 不入簿、`OrderRested` 独立存在、`StopTriggered` 自带转换后订单的原因。

## 持久化与恢复

- **线上格式**：每行 `{crc32:08x} {json}\n`；header 与 `EventRecord` 都带 `FORMAT_VERSION = 2`。append 在返回前完成 write + flush + fsync；`append_batch` 整批落盘后才返回。
- **恢复**：读取 header → 加载最新 `StoredSnapshot`（校验版本/config，拒绝快照领先于日志）→ 检查首事件序号无空洞 → 从 `applied_seq + 1` 读取尾部 → 跳过重复序号、拒绝非连续、逐条 `apply(event, sequence)`。
- **快照**：`StoredSnapshot { format_version, applied_seq, last_command_id, snapshot }`；`applied_seq` 是事件序号。快照只在命令边界、且 `applied_seq == durable_seq` 时捕获。
- **`command_id` 连续性**：恢复取 `max(snapshot.last_command_id, 最后一条日志记录的 command_id) + 1`。
- **兼容性**：v1 命令日志/快照直接拒绝；不提供迁移工具。本轮不截断事件日志，快照不能单独作为事实源。

存储契约、Kafka/Redis adapter 要求与格式升级流程见 [docs/durable-storage.md](docs/durable-storage.md)。

## 运行时与 SLO

运行时是 **apply-first**：

1. 撮合线程 `decide` 出事件，分配连续事件序号区间和一个 `command_id`；
2. 逐条 `apply`，发送 applied 回执（携带事件列表与首/末事件序号）；
3. 把 `EventRecord` 交给 Journal；
4. Journal 批量 fsync 后推进事件水位，撮合线程再完成 durable 回执。

目标（详见 [docs/durable-storage.md](docs/durable-storage.md) 的 SLO 测量）：

- `submit -> applied` p99 ≤ 10µs：`--log memory`、单生产者、`--in-flight 1`、`--idle spin`、撮合线程绑核、`queue_full == 0`。
- `submit -> durable` p99 ≤ 500µs（100k commands/s）：`--log file` + NVMe，同样 spin/绑核、`queue_full == 0`。

Windows 的 QPC/NTFS 只做功能验证；numeric SLO 需要在原生 Linux + NVMe 上测量。

## MessageBus

- 同步、进程内、handler 参数为 `&dyn Any`；事件的具体类型是 `EventRecord`。
- `register_any(endpoint, handler)` 可注册多个 handler；`try_publish_any(topic, &dyn Any)` 同步调用该 topic 下所有 handler。
- handler panic 会被捕获，endpoint 标记 failed，其余 handler 继续。
- `EngineHandle::bus()` / `EngineRuntime::bus()` 返回只读 `SubscriberBus`，**不暴露 publish**；只有 Event Publisher 能发布。
- `send_any` 与 `register_response_handler` 返回 `Unsupported`。bus 不保存历史；消费方从 `EventLog` 追赶。
- 慢 handler 只拖后续发布，不阻塞 Journal 写入或撮合线程。

## Benchmark

构建：

```bash
cargo build --release
```

Linux + NVMe（完整 SLO 认证）：

```bash
# submit -> applied
taskset -c 0-15 ./target/release/bench \
  --path runtime --log memory \
  --events 200000 --warmup 20000 \
  --rate 0 --in-flight 1 \
  --idle spin --affinity 15

# submit -> durable（把 data-dir 放在 NVMe 挂载点）
mkdir -p /mnt/nvme/orderbook-bench
taskset -c 0-15 ./target/release/bench \
  --path runtime --log file --data-dir /mnt/nvme/orderbook-bench \
  --events 200000 --warmup 20000 \
  --rate 100000 --in-flight 128 \
  --idle spin --affinity 15

# 纯 decide 基线
taskset -c 0-15 ./target/release/bench \
  --path decide \
  --events 200000 --warmup 20000 \
  --rate 0 --affinity 15
```

- `--affinity <CORE>` 是 `core_affinity::get_core_ids` 的索引，不是裸 CPU 号。
- `PASS` 同时要求 `p99 <= target` 与 `queue_full == 0`；`--log memory` 时 durable 为 `NOT APPLICABLE`。
- 若 `queue_full > 0` 或 achieved rate 明显低于 100k，先增大 `--in-flight`，不要用有拒绝的运行宣称 PASS。
- Windows 功能 smoke：`cargo run --release --bin bench -- --path runtime --log memory --events 20000 --warmup 2000 --rate 0 --in-flight 1 --idle spin`。

## 测试

```bash
cargo test --all-targets
cargo test --doc
cargo fmt --check
cargo clippy --all-targets   # 仓库有既有 warning；不要用 -D warnings 判定
```

当前主线：`cargo test --all-targets` 413 passed，`cargo test --doc` 1 passed（`SubscriberBus` 的 `compile_fail`），`cargo fmt --check` clean。

## 文档

- [ADR 0002：全局单一有序事件日志](docs/adr/0002-single-global-ordered-log.md)
- [ADR 0003：apply-first 运行时与有界队列](docs/adr/0003-apply-first-runtime-with-bounded-queues.md)
- [ADR 0004：以事件日志作为恢复事实源](docs/adr/0004-event-sourced-order-log.md)
- [ADR 0005：进程内 MessageBus 与日志追赶](docs/adr/0005-in-process-message-bus.md)
- [ADR 0006：决策阶段使用借用视图](docs/adr/0006-borrowed-decision-view.md)
- [存储契约与格式](docs/durable-storage.md)
- [领域词汇表](GLOSSARY.md)
- Event-sourcing 设计材料： [spec](.scratch/event-sourcing/spec.md)、[plan](.scratch/event-sourcing/plan.md)、[tickets](.scratch/event-sourcing/issues/)

ADR 0001（命令日志作为事实源）已被 ADR 0004 取代，保留作历史记录。

## 已知限制

- 单引擎实例只有一条全局事件日志，没有 per-symbol 分片；扩展时按“每 symbol 一个引擎实例 + 独立日志”拆，而不是在簿内加线程。
- 没有外部 broker adapter；Kafka 的单分区/`acks=all`/RF≥3/不压缩要求写在 `docs/durable-storage.md`。Redis 只能做快照存储，不能做事件日志。
- `send_any` / `register_response_handler` 未实现；bus 没有命令或请求/响应入口。
- 事件日志无截断/归档；被拒命令也占日志与事件序号，事件日志增长快于旧命令日志。
- v1 日志不可读、无迁移工具；破坏性 schema 变更需要升版本并显式重建。
- 不做客户端幂等/去重；调用方必须按自己的 client order id 去重，或不要重试结果未知的请求。
- 未实现行情模块、盘前风控或盘后风控业务；bus 只是交付 seam。盘前风控应放在网关，而不是引擎 bus。
- 不是 HFT 级：ingress 是 `std::sync::mpsc`，单 Journal 线程，无 `io_uring`、复制、NUMA/实时优先级；Windows 数字 SLO 不适用。
- 快照包含台账与成交历史，会随它们增长；事件日志与快照的 retention 不在本版本范围。

## 目录结构

```text
src/engine.rs        撮合引擎：decide / apply、订单簿交互、止损级联
src/event.rs         DomainEvent 与取消原因
src/durable/         EventRecord / EventLog / 快照 / 恢复 / 文件与内存 adapter
src/runtime/         单写者运行时、Journal、Publisher、Ticket、metrics
src/bus/             进程内同步 MessageBus
src/bench/           benchmark、workload、SLO 报告
tests/               事件流、恢复、运行时、bus、bench CLI 测试
docs/adr/            架构决策记录
docs/durable-storage.md
GLOSSARY.md
```
