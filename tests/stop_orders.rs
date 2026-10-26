//! Integration tests for stop-market and stop-limit orders.
//!
//! Stop orders live outside the book, trigger off the last trade price, and
//! convert into ordinary market/limit orders. These tests exercise the public
//! engine API only.

use orderbook_rs::durable::DurableMatchingEngine;
use orderbook_rs::durable::memory::{InMemoryCommandLog, InMemorySnapshotStore};
use orderbook_rs::engine::MatchingEngine;
use orderbook_rs::types::{
    CommandOutcome, EngineCommand, EngineConfig, EngineError, EngineEvent, ExecutionReport,
    ModifyRequest, OrderRecord, OrderRequest, OrderStatus, OrderType, Side, StopKind,
    StopOrderRequest, TimeInForce,
};

const SYMBOL: &str = "BTC-USDT";

fn limit(side: Side, price: u64, quantity: u64, timestamp: u64) -> OrderRequest {
    OrderRequest {
        side,
        price,
        quantity,
        timestamp,
        order_type: OrderType::Limit,
        time_in_force: TimeInForce::GTC,
        post_only: false,
    }
}

fn stop_market(side: Side, stop_price: u64, quantity: u64, timestamp: u64) -> StopOrderRequest {
    StopOrderRequest {
        side,
        stop_price,
        order_type: StopKind::Market,
        quantity,
        timestamp,
    }
}

fn stop_limit(
    side: Side,
    stop_price: u64,
    price: u64,
    quantity: u64,
    timestamp: u64,
) -> StopOrderRequest {
    StopOrderRequest {
        side,
        stop_price,
        order_type: StopKind::Limit { price },
        quantity,
        timestamp,
    }
}

fn submit_command(request: OrderRequest) -> EngineCommand {
    EngineCommand::SubmitOrder {
        symbol: SYMBOL.to_string(),
        request,
    }
}

fn submit_stop_command(request: StopOrderRequest) -> EngineCommand {
    EngineCommand::SubmitStopOrder {
        symbol: SYMBOL.to_string(),
        request,
    }
}

fn cancel_command(order_id: u64) -> EngineCommand {
    EngineCommand::CancelOrder {
        symbol: SYMBOL.to_string(),
        order_id,
    }
}

fn modify_command(order_id: u64, price: u64, quantity: u64, timestamp: u64) -> EngineCommand {
    EngineCommand::ModifyOrder {
        symbol: SYMBOL.to_string(),
        order_id,
        request: ModifyRequest {
            price,
            quantity,
            timestamp,
        },
    }
}

/// Execute a command while collecting the stream locally, so replay does not
/// depend on the engine's in-memory command buffer.
fn execute_recorded(
    engine: &mut MatchingEngine,
    commands: &mut Vec<EngineCommand>,
    command: EngineCommand,
) -> CommandOutcome {
    commands.push(command.clone());
    engine.execute(command)
}

fn submit_recorded(
    engine: &mut MatchingEngine,
    commands: &mut Vec<EngineCommand>,
    request: OrderRequest,
) -> ExecutionReport {
    match execute_recorded(engine, commands, submit_command(request)) {
        CommandOutcome::Accepted(report) => report,
        other => panic!("submit failed: {other:?}"),
    }
}

fn submit_stop_recorded(
    engine: &mut MatchingEngine,
    commands: &mut Vec<EngineCommand>,
    request: StopOrderRequest,
) -> ExecutionReport {
    match execute_recorded(engine, commands, submit_stop_command(request)) {
        CommandOutcome::Accepted(report) => report,
        other => panic!("stop submit failed: {other:?}"),
    }
}

fn submit_on(engine: &mut MatchingEngine, symbol: &str, request: OrderRequest) -> ExecutionReport {
    match engine.execute(EngineCommand::SubmitOrder {
        symbol: symbol.to_string(),
        request,
    }) {
        CommandOutcome::Accepted(report) => report,
        other => panic!("submit failed: {other:?}"),
    }
}

fn submit(engine: &mut MatchingEngine, request: OrderRequest) -> ExecutionReport {
    submit_on(engine, SYMBOL, request)
}

fn submit_stop_on(
    engine: &mut MatchingEngine,
    symbol: &str,
    request: StopOrderRequest,
) -> ExecutionReport {
    match engine.execute(EngineCommand::SubmitStopOrder {
        symbol: symbol.to_string(),
        request,
    }) {
        CommandOutcome::Accepted(report) => report,
        other => panic!("stop submit failed: {other:?}"),
    }
}

fn submit_stop(engine: &mut MatchingEngine, request: StopOrderRequest) -> ExecutionReport {
    submit_stop_on(engine, SYMBOL, request)
}

/// Seed a last trade at `price` and leave the book empty.
fn seed_last_price_on(engine: &mut MatchingEngine, symbol: &str, price: u64) {
    submit_on(engine, symbol, limit(Side::Ask, price, 1, 1));
    submit_on(engine, symbol, limit(Side::Bid, price, 1, 2));
    assert_eq!(engine.last_trade_price(symbol), Some(price));
    assert!(engine.book(symbol).unwrap().best_bid().is_none());
    assert!(engine.book(symbol).unwrap().best_ask().is_none());
}

fn seed_last_price(engine: &mut MatchingEngine, price: u64) {
    seed_last_price_on(engine, SYMBOL, price);
}

fn triggered_on(engine: &MatchingEngine, symbol: &str) -> Vec<(u64, u64, u64)> {
    engine
        .events()
        .iter()
        .filter_map(|event| match event {
            EngineEvent::StopTriggered {
                symbol: event_symbol,
                order_id,
                stop_price,
                last_price,
                ..
            } if event_symbol == symbol => Some((*order_id, *stop_price, *last_price)),
            _ => None,
        })
        .collect()
}

fn triggered(engine: &MatchingEngine) -> Vec<(u64, u64, u64)> {
    triggered_on(engine, SYMBOL)
}

fn records(engine: &MatchingEngine) -> Vec<(u64, OrderRecord)> {
    let mut records: Vec<_> = engine
        .order_records()
        .map(|(id, record)| (id, record.clone()))
        .collect();
    records.sort_by_key(|(id, _)| *id);
    records
}

#[test]
fn stop_submission_does_not_create_a_book() {
    let mut engine = MatchingEngine::new(EngineConfig::default());

    let stop = submit_stop(&mut engine, stop_market(Side::Bid, 90, 1, 1));

    assert_eq!(stop.status, OrderStatus::PendingTrigger);
    assert!(engine.book(SYMBOL).is_none());
    assert!(engine.snapshot(SYMBOL, 10).is_none());
    assert!(engine.stop_order(stop.order_id).is_some());
    assert_eq!(engine.order_count(), 1);
}

#[test]
fn pending_stop_is_not_in_the_book_and_triggers_on_falling_price() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    seed_last_price(&mut engine, 100);

    let stop = submit_stop(&mut engine, stop_market(Side::Ask, 95, 2, 3));
    assert_eq!(stop.status, OrderStatus::PendingTrigger);
    assert_eq!(stop.remaining_quantity, 2);
    assert!(stop.fills.is_empty());
    assert_eq!(
        engine.order_record(stop.order_id).unwrap().status,
        OrderStatus::PendingTrigger
    );
    assert!(engine.is_open(stop.order_id));
    assert!(engine.stop_order(stop.order_id).is_some());
    assert_eq!(engine.stop_orders(SYMBOL, Side::Ask).len(), 1);

    // A trade at 95 triggers the stop; the converted market sell hits the
    // remaining bid at 95.
    submit(&mut engine, limit(Side::Bid, 95, 5, 4));
    submit(&mut engine, limit(Side::Ask, 95, 1, 5));

    assert_eq!(triggered(&engine), vec![(stop.order_id, 95, 95)]);
    assert_eq!(engine.last_trade_price(SYMBOL), Some(95));
    assert!(engine.stop_order(stop.order_id).is_none());
    assert_eq!(
        engine.order_record(stop.order_id).unwrap().status,
        OrderStatus::Filled
    );
    // The converted taker reuses the stop's order id.
    assert!(
        engine
            .trades()
            .iter()
            .any(|trade| trade.taker_order_id == stop.order_id && trade.quantity == 2)
    );
}

#[test]
fn buy_stop_triggers_on_rising_price_and_uses_trigger_timestamp() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    seed_last_price(&mut engine, 100);

    let stop = submit_stop(&mut engine, stop_market(Side::Bid, 105, 1, 3));

    submit(&mut engine, limit(Side::Ask, 105, 3, 4));
    // Triggering trade at 105.
    submit(&mut engine, limit(Side::Bid, 105, 1, 5));

    assert_eq!(triggered(&engine), vec![(stop.order_id, 105, 105)]);
    assert_eq!(engine.last_trade_price(SYMBOL), Some(105));

    let converted: Vec<_> = engine
        .trades()
        .iter()
        .filter(|trade| trade.taker_order_id == stop.order_id)
        .collect();
    assert_eq!(converted.len(), 1);
    assert_eq!(converted[0].price, 105);
    // The converted order is stamped with the triggering trade's timestamp.
    assert_eq!(converted[0].timestamp, 5);
}

#[test]
fn cascade_processes_every_triggered_stop_in_one_command() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    seed_last_price(&mut engine, 100);

    let first = submit_stop(&mut engine, stop_market(Side::Ask, 95, 1, 3));
    let second = submit_stop(&mut engine, stop_market(Side::Ask, 93, 1, 4));

    submit(&mut engine, limit(Side::Bid, 95, 1, 5));
    submit(&mut engine, limit(Side::Bid, 93, 5, 6));

    // Trade at 95 triggers `first`; its market sell prints 93, which triggers
    // `second`; that prints 93 again.
    submit(&mut engine, limit(Side::Ask, 95, 1, 7));

    assert_eq!(
        triggered(&engine),
        vec![(first.order_id, 95, 95), (second.order_id, 93, 93)]
    );
    assert_eq!(engine.last_trade_price(SYMBOL), Some(93));
    assert!(engine.stop_orders(SYMBOL, Side::Ask).is_empty());
}

#[test]
fn trigger_batch_uses_price_priority_then_fifo() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    seed_last_price(&mut engine, 100);

    let first = submit_stop(&mut engine, stop_market(Side::Ask, 97, 1, 3));
    let second = submit_stop(&mut engine, stop_market(Side::Ask, 95, 1, 4));
    let third = submit_stop(&mut engine, stop_market(Side::Ask, 95, 1, 5));

    submit(&mut engine, limit(Side::Bid, 94, 10, 6));
    submit(&mut engine, limit(Side::Ask, 94, 1, 7));

    assert_eq!(
        triggered(&engine),
        vec![
            (first.order_id, 97, 94),
            (second.order_id, 95, 94),
            (third.order_id, 95, 94),
        ]
    );
}

#[test]
fn cross_side_triggers_use_global_order_id_order() {
    let mut engine = MatchingEngine::new(EngineConfig::default());

    // No last price yet, so direction validation does not reject either side.
    let buy = submit_stop(&mut engine, stop_market(Side::Bid, 90, 1, 1));
    let sell = submit_stop(&mut engine, stop_market(Side::Ask, 110, 1, 2));

    // First trade ever at 100 makes both sides eligible.
    submit(&mut engine, limit(Side::Ask, 100, 1, 3));
    submit(&mut engine, limit(Side::Bid, 100, 1, 4));

    assert_eq!(
        triggered(&engine),
        vec![(buy.order_id, 90, 100), (sell.order_id, 110, 100)]
    );
}

#[test]
fn stop_limit_that_does_not_cross_rests_as_a_gtc_limit() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    seed_last_price(&mut engine, 100);

    let stop = submit_stop(&mut engine, stop_limit(Side::Ask, 95, 94, 2, 3));

    submit(&mut engine, limit(Side::Bid, 95, 1, 4));
    submit(&mut engine, limit(Side::Bid, 93, 5, 5));
    // Triggering trade: best bid becomes 93, so the converted sell at 94 rests.
    submit(&mut engine, limit(Side::Ask, 95, 1, 6));

    assert!(engine.stop_order(stop.order_id).is_none());
    let book = engine.book(SYMBOL).unwrap();
    assert_eq!(book.best_ask(), Some(94));
    assert_eq!(book.depth(Side::Ask, 0), Some((94, 2)));
    let record = engine.order_record(stop.order_id).unwrap();
    assert_eq!(record.status, OrderStatus::New);
    assert_eq!(record.filled_quantity, 0);
}

#[test]
fn stop_limit_that_crosses_fills_at_maker_price() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    seed_last_price(&mut engine, 100);

    let stop = submit_stop(&mut engine, stop_limit(Side::Ask, 95, 90, 1, 3));

    submit(&mut engine, limit(Side::Bid, 95, 1, 4));
    submit(&mut engine, limit(Side::Bid, 93, 5, 5));
    submit(&mut engine, limit(Side::Ask, 95, 1, 6));

    let record = engine.order_record(stop.order_id).unwrap();
    assert_eq!(record.status, OrderStatus::Filled);
    let converted = engine
        .trades()
        .iter()
        .find(|trade| trade.taker_order_id == stop.order_id)
        .unwrap();
    assert_eq!(converted.price, 93);
    assert_eq!(converted.timestamp, 6);
}

#[test]
fn stop_limit_partial_fill_rests_the_remainder() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    seed_last_price(&mut engine, 100);

    let stop = submit_stop(&mut engine, stop_limit(Side::Ask, 95, 94, 5, 3));

    submit(&mut engine, limit(Side::Bid, 95, 1, 4));
    submit(&mut engine, limit(Side::Bid, 94, 2, 5));
    // Trigger at 95; the converted sell at 94 fills 2 and rests 3.
    submit(&mut engine, limit(Side::Ask, 95, 1, 6));

    let record = engine.order_record(stop.order_id).unwrap();
    assert_eq!(record.status, OrderStatus::PartiallyFilled);
    assert_eq!(record.filled_quantity, 2);
    assert_eq!(record.remaining_quantity, 3);
    let book = engine.book(SYMBOL).unwrap();
    assert_eq!(book.best_ask(), Some(94));
    assert_eq!(book.depth(Side::Ask, 0), Some((94, 3)));
}

#[test]
fn stop_market_with_no_liquidity_after_trigger_is_canceled() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    seed_last_price(&mut engine, 100);

    let stop = submit_stop(&mut engine, stop_market(Side::Ask, 95, 2, 3));

    // The triggering trade consumes the only bid; the converted market order
    // has nothing left to hit.
    submit(&mut engine, limit(Side::Bid, 95, 1, 4));
    submit(&mut engine, limit(Side::Ask, 95, 1, 5));

    let record = engine.order_record(stop.order_id).unwrap();
    assert_eq!(record.status, OrderStatus::Canceled);
    assert_eq!(record.filled_quantity, 0);
    assert_eq!(record.remaining_quantity, 2);
}

#[test]
fn direction_validation_rejects_already_triggered_stops() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    seed_last_price(&mut engine, 100);

    for request in [
        stop_market(Side::Bid, 100, 1, 3),
        stop_market(Side::Bid, 99, 1, 3),
        stop_market(Side::Ask, 100, 1, 3),
        stop_market(Side::Ask, 101, 1, 3),
    ] {
        let outcome = engine.execute(EngineCommand::SubmitStopOrder {
            symbol: SYMBOL.to_string(),
            request,
        });
        assert_eq!(
            outcome,
            CommandOutcome::Rejected(EngineError::StopPriceWouldTriggerImmediately)
        );
    }

    // The correct side is accepted and stays pending.
    submit_stop(&mut engine, stop_market(Side::Bid, 101, 1, 3));
    submit_stop(&mut engine, stop_market(Side::Ask, 99, 1, 3));
    assert_eq!(engine.stop_orders(SYMBOL, Side::Bid).len(), 1);
    assert_eq!(engine.stop_orders(SYMBOL, Side::Ask).len(), 1);
}

#[test]
fn stop_order_prices_must_be_on_tick() {
    let config = EngineConfig {
        min_order_quantity: 1,
        max_order_quantity: u64::MAX,
        tick_size: 5,
    };
    let mut engine = MatchingEngine::new(config);

    let outcome = engine.execute(EngineCommand::SubmitStopOrder {
        symbol: SYMBOL.to_string(),
        request: stop_market(Side::Ask, 96, 1, 1),
    });
    assert_eq!(
        outcome,
        CommandOutcome::Rejected(EngineError::PriceNotOnTick)
    );

    let outcome = engine.execute(EngineCommand::SubmitStopOrder {
        symbol: SYMBOL.to_string(),
        request: stop_limit(Side::Ask, 95, 96, 1, 2),
    });
    assert_eq!(
        outcome,
        CommandOutcome::Rejected(EngineError::PriceNotOnTick)
    );
}

#[test]
fn pending_stop_can_be_canceled_but_not_modified() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    seed_last_price(&mut engine, 100);
    let stop = submit_stop(&mut engine, stop_market(Side::Ask, 95, 1, 3));

    let modify = engine.execute(EngineCommand::ModifyOrder {
        symbol: SYMBOL.to_string(),
        order_id: stop.order_id,
        request: ModifyRequest {
            price: 94,
            quantity: 1,
            timestamp: 4,
        },
    });
    assert_eq!(
        modify,
        CommandOutcome::Rejected(EngineError::StopOrderNotModifiable)
    );
    assert!(engine.stop_order(stop.order_id).is_some());

    let cancel = engine.execute(EngineCommand::CancelOrder {
        symbol: SYMBOL.to_string(),
        order_id: stop.order_id,
    });
    match cancel {
        CommandOutcome::Canceled(report) => {
            assert_eq!(report.status, OrderStatus::Canceled);
            assert_eq!(report.remaining_quantity, 1);
        }
        other => panic!("cancel failed: {other:?}"),
    }
    assert!(engine.stop_order(stop.order_id).is_none());
    assert!(!engine.is_open(stop.order_id));

    // A later trade through the trigger price does not resurrect it.
    submit(&mut engine, limit(Side::Bid, 95, 5, 5));
    submit(&mut engine, limit(Side::Ask, 95, 1, 6));
    assert!(triggered(&engine).is_empty());
}

#[test]
fn rejected_stop_does_not_consume_order_id() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    seed_last_price(&mut engine, 100);

    let rejected = engine.execute(EngineCommand::SubmitStopOrder {
        symbol: SYMBOL.to_string(),
        request: stop_market(Side::Bid, 100, 1, 3),
    });
    assert_eq!(
        rejected,
        CommandOutcome::Rejected(EngineError::StopPriceWouldTriggerImmediately)
    );

    let report = submit(&mut engine, limit(Side::Bid, 99, 1, 4));
    assert_eq!(report.order_id, 3);
    assert_eq!(engine.order_count(), 3);
}

#[test]
fn order_count_and_is_open_include_pending_stops() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    let stop = submit_stop(&mut engine, stop_market(Side::Bid, 90, 1, 1));

    assert_eq!(engine.order_count(), 1);
    assert!(engine.is_open(stop.order_id));
    assert_eq!(
        engine.order_record(stop.order_id).unwrap().status,
        OrderStatus::PendingTrigger
    );

    let cancel = engine.execute(EngineCommand::CancelOrder {
        symbol: SYMBOL.to_string(),
        order_id: stop.order_id,
    });
    assert!(matches!(cancel, CommandOutcome::Canceled(_)));
    assert!(!engine.is_open(stop.order_id));
    // The ledger keeps the record after cancellation.
    assert_eq!(engine.order_count(), 1);
}

#[test]
fn stop_orders_are_isolated_per_symbol() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    let aaa = submit_stop_on(&mut engine, "AAA-USDT", stop_market(Side::Bid, 90, 1, 1));
    let bbb = submit_stop_on(&mut engine, "BBB-USDT", stop_market(Side::Ask, 110, 1, 2));

    assert_eq!(engine.stop_orders("AAA-USDT", Side::Bid).len(), 1);
    assert_eq!(engine.stop_orders("BBB-USDT", Side::Ask).len(), 1);

    // Trigger AAA only.
    seed_last_price_on(&mut engine, "AAA-USDT", 100);
    assert_eq!(triggered_on(&engine, "AAA-USDT").len(), 1);
    assert!(engine.stop_order(aaa.order_id).is_none());
    assert!(engine.stop_order(bbb.order_id).is_some());
    assert_eq!(engine.last_trade_price("BBB-USDT"), None);
}

#[test]
fn last_trade_price_tracks_latest_fill_per_symbol() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    seed_last_price_on(&mut engine, "AAA-USDT", 100);
    seed_last_price_on(&mut engine, "BBB-USDT", 200);

    submit_on(&mut engine, "AAA-USDT", limit(Side::Ask, 101, 1, 3));
    submit_on(&mut engine, "AAA-USDT", limit(Side::Bid, 101, 1, 4));

    assert_eq!(engine.last_trade_price("AAA-USDT"), Some(101));
    assert_eq!(engine.last_trade_price("BBB-USDT"), Some(200));
    assert_eq!(engine.last_trade_price("CCC-USDT"), None);
}

#[test]
fn stop_orders_query_is_ordered_by_price_then_fifo() {
    let mut engine = MatchingEngine::new(EngineConfig::default());

    // No last price yet: every trigger price is accepted.
    submit_stop(&mut engine, stop_market(Side::Bid, 105, 1, 1));
    submit_stop(&mut engine, stop_market(Side::Bid, 101, 1, 2));
    submit_stop(&mut engine, stop_market(Side::Bid, 101, 1, 3));
    submit_stop(&mut engine, stop_market(Side::Ask, 95, 1, 4));
    submit_stop(&mut engine, stop_market(Side::Ask, 99, 1, 5));
    submit_stop(&mut engine, stop_market(Side::Ask, 99, 1, 6));

    let bid_ids: Vec<u64> = engine
        .stop_orders(SYMBOL, Side::Bid)
        .iter()
        .map(|stop| stop.id)
        .collect();
    let ask_ids: Vec<u64> = engine
        .stop_orders(SYMBOL, Side::Ask)
        .iter()
        .map(|stop| stop.id)
        .collect();

    // Bids ascend by trigger price, then by order id.
    assert_eq!(bid_ids, vec![2, 3, 1]);
    // Asks descend by trigger price, then by order id.
    assert_eq!(ask_ids, vec![5, 6, 4]);
}

#[test]
fn triggered_stop_that_rests_is_a_normal_limit_order() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    seed_last_price(&mut engine, 100);
    let stop = submit_stop(&mut engine, stop_limit(Side::Ask, 95, 94, 2, 3));

    submit(&mut engine, limit(Side::Bid, 95, 1, 4));
    submit(&mut engine, limit(Side::Bid, 93, 5, 5));
    submit(&mut engine, limit(Side::Ask, 95, 1, 6));
    assert!(engine.is_open(stop.order_id));

    // The converted order is now an ordinary resting limit: modify works.
    let modified = engine.execute(EngineCommand::ModifyOrder {
        symbol: SYMBOL.to_string(),
        order_id: stop.order_id,
        request: ModifyRequest {
            price: 95,
            quantity: 2,
            timestamp: 7,
        },
    });
    let new_id = match modified {
        CommandOutcome::Accepted(report) => {
            assert_eq!(report.status, OrderStatus::New);
            report.order_id
        }
        other => panic!("modify failed: {other:?}"),
    };
    assert_eq!(
        engine.order_record(stop.order_id).unwrap().status,
        OrderStatus::Canceled
    );
    assert!(engine.is_open(new_id));

    // And so does cancel.
    let cancel = engine.execute(EngineCommand::CancelOrder {
        symbol: SYMBOL.to_string(),
        order_id: new_id,
    });
    match cancel {
        CommandOutcome::Canceled(report) => {
            assert_eq!(report.status, OrderStatus::Canceled);
            assert_eq!(report.remaining_quantity, 2);
        }
        other => panic!("cancel failed: {other:?}"),
    }
}

#[test]
fn replay_reproduces_stop_lifecycle() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    // Collect the stream locally rather than reading the engine's buffer.
    let mut commands: Vec<EngineCommand> = Vec::new();

    // Seed a last trade at 100 and leave the book empty.
    submit_recorded(&mut engine, &mut commands, limit(Side::Ask, 100, 1, 1));
    submit_recorded(&mut engine, &mut commands, limit(Side::Bid, 100, 1, 2));
    assert_eq!(engine.last_trade_price(SYMBOL), Some(100));
    assert!(engine.book(SYMBOL).unwrap().best_bid().is_none());
    assert!(engine.book(SYMBOL).unwrap().best_ask().is_none());

    // One pending stop that never triggers, so replay compares a non-empty
    // stop store as well.
    let pending = submit_stop_recorded(&mut engine, &mut commands, stop_market(Side::Bid, 105, 1, 3));

    let first = submit_stop_recorded(&mut engine, &mut commands, stop_market(Side::Ask, 95, 1, 4));
    let second = submit_stop_recorded(&mut engine, &mut commands, stop_market(Side::Ask, 93, 1, 5));
    let canceled = submit_stop_recorded(&mut engine, &mut commands, stop_market(Side::Ask, 99, 1, 6));

    let canceled_command = execute_recorded(
        &mut engine,
        &mut commands,
        cancel_command(canceled.order_id),
    );
    assert!(matches!(canceled_command, CommandOutcome::Canceled(_)));
    let rejected_modify = execute_recorded(
        &mut engine,
        &mut commands,
        modify_command(first.order_id, 94, 1, 7),
    );
    assert_eq!(
        rejected_modify,
        CommandOutcome::Rejected(EngineError::StopOrderNotModifiable)
    );
    let rejected_stop = execute_recorded(
        &mut engine,
        &mut commands,
        submit_stop_command(stop_market(Side::Bid, 99, 1, 8)),
    );
    assert_eq!(
        rejected_stop,
        CommandOutcome::Rejected(EngineError::StopPriceWouldTriggerImmediately)
    );

    submit_recorded(&mut engine, &mut commands, limit(Side::Bid, 95, 1, 9));
    submit_recorded(&mut engine, &mut commands, limit(Side::Bid, 93, 5, 10));
    submit_recorded(&mut engine, &mut commands, limit(Side::Ask, 95, 1, 11));

    assert_eq!(
        triggered(&engine),
        vec![(first.order_id, 95, 95), (second.order_id, 93, 93)]
    );
    assert!(engine.stop_order(pending.order_id).is_some());

    let log = InMemoryCommandLog::new();
    let snapshots = InMemorySnapshotStore::new();
    let replayed = DurableMatchingEngine::from_commands(
        EngineConfig::default(),
        &commands,
        Box::new(log.handle()),
        Box::new(snapshots.handle()),
        0,
    )
    .expect("stop lifecycle replay succeeds");

    // Events belong to live execution; recovery deliberately does not restore
    // them. Everything else must match, including the pending stop store.
    assert_eq!(replayed.applied_seq(), commands.len() as u64);
    assert_eq!(
        replayed.engine().capture_snapshot(),
        engine.capture_snapshot()
    );
    assert_eq!(replayed.engine().trades(), engine.trades());
    assert_eq!(records(replayed.engine()), records(&engine));
    assert_eq!(replayed.engine().book(SYMBOL), engine.book(SYMBOL));
    assert_eq!(
        replayed.engine().last_trade_price(SYMBOL),
        engine.last_trade_price(SYMBOL)
    );
    assert_eq!(
        replayed.engine().stop_orders(SYMBOL, Side::Bid),
        engine.stop_orders(SYMBOL, Side::Bid)
    );
    assert_eq!(
        replayed.engine().stop_orders(SYMBOL, Side::Ask),
        engine.stop_orders(SYMBOL, Side::Ask)
    );
    assert_eq!(replayed.engine().order_count(), engine.order_count());
    assert_eq!(
        replayed.engine().snapshot(SYMBOL, 10),
        engine.snapshot(SYMBOL, 10)
    );
}
