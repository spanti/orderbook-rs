//! Both write paths must produce the same event stream and the same state.
//!
//! `DurableMatchingEngine` appends before applying; the `EngineRuntime` applies
//! before the journal makes the records durable. They share `decide`, `apply`,
//! and the `Sequencer`, so the only acceptable difference is acknowledgement
//! timing — never the records.

use orderbook_rs::durable::{
    DurableMatchingEngine, EventLog, InMemoryEventLog, InMemorySnapshotStore,
};
use orderbook_rs::runtime::{EngineRuntime, RuntimeConfig};
use orderbook_rs::types::{
    EngineCommand, EngineConfig, ModifyRequest, OrderRequest, OrderType, Side, StopKind,
    StopOrderRequest, TimeInForce,
};

const BTC: &str = "BTC-USDT";

fn limit(side: Side, price: u64, quantity: u64, timestamp: u64) -> EngineCommand {
    EngineCommand::SubmitOrder {
        symbol: BTC.to_string(),
        request: OrderRequest {
            side,
            price,
            quantity,
            timestamp,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::GTC,
            post_only: false,
        },
    }
}

/// A deterministic stream that exercises rest, trade, modify, cancel, a stop
/// trigger cascade, and a rejection.
fn workflow() -> Vec<EngineCommand> {
    vec![
        limit(Side::Ask, 100, 5, 1), // id 1 rests
        limit(Side::Bid, 100, 3, 2), // id 2 trades against id 1
        limit(Side::Bid, 99, 4, 3),  // id 3 rests
        EngineCommand::ModifyOrder {
            symbol: BTC.to_string(),
            order_id: 3,
            request: ModifyRequest {
                price: 100,
                quantity: 4,
                timestamp: 4,
            },
        }, // id 4 trades 2, rests 2
        EngineCommand::CancelOrder {
            symbol: BTC.to_string(),
            order_id: 4,
        },
        EngineCommand::SubmitStopOrder {
            symbol: BTC.to_string(),
            request: StopOrderRequest {
                side: Side::Bid,
                stop_price: 105,
                order_type: StopKind::Market,
                quantity: 1,
                timestamp: 6,
            },
        }, // id 5 pending
        limit(Side::Ask, 105, 1, 7), // id 6 rests
        limit(Side::Bid, 105, 1, 8), // id 7 trades at 105, triggers id 5
        EngineCommand::CancelOrder {
            symbol: BTC.to_string(),
            order_id: 99,
        }, // rejected
    ]
}

#[test]
fn both_write_paths_produce_identical_records_and_state() {
    let workflow = workflow();

    let sync_log = InMemoryEventLog::new();
    let mut sync = DurableMatchingEngine::create(
        EngineConfig::default(),
        Box::new(sync_log.handle()),
        Box::new(InMemorySnapshotStore::new()),
    )
    .expect("sync log header");
    for command in &workflow {
        sync.execute(command.clone()).expect("sync execute");
    }
    let sync_records = sync.history_from(1).expect("sync history");
    let sync_snapshot = sync.engine().capture_snapshot();

    let runtime_log = InMemoryEventLog::new();
    let (handle, runtime) = EngineRuntime::create(
        RuntimeConfig::default(),
        EngineConfig::default(),
        Box::new(runtime_log.handle()),
        Box::new(InMemorySnapshotStore::new()),
    )
    .expect("runtime start");
    for command in &workflow {
        handle
            .submit_and_wait(command.clone())
            .expect("runtime durable acknowledgement");
    }
    runtime.shutdown().expect("runtime shutdown");
    let runtime_records = runtime_log.handle().read_from(1).expect("runtime history");

    assert_eq!(sync_records, runtime_records);

    let sync_recovered = DurableMatchingEngine::open(
        Box::new(sync_log.handle()),
        Box::new(InMemorySnapshotStore::new()),
    )
    .expect("sync recovery");
    let runtime_recovered = DurableMatchingEngine::open(
        Box::new(runtime_log.handle()),
        Box::new(InMemorySnapshotStore::new()),
    )
    .expect("runtime recovery");

    assert_eq!(
        sync_recovered.applied_seq(),
        runtime_recovered.applied_seq()
    );
    assert_eq!(
        sync_recovered.engine().capture_snapshot(),
        runtime_recovered.engine().capture_snapshot()
    );
    assert_eq!(sync_recovered.engine().capture_snapshot(), sync_snapshot);
}
