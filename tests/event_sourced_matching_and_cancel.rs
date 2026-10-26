//! Event-sourced matching and cancel (ticket 02).
//!
//! These tests exercise the event-sourced path only through public seams: the
//! pure `decide`, the sole state-write entry `apply`, and the append-before-
//! apply `EventSourcedEngine`. Expected event streams and state are literals
//! from the ticket and the event catalog, never recomputed by the code under
//! test.

use std::collections::HashMap;

use orderbook_rs::durable::{DurableError, EventSourcedEngine, InMemoryEventLog};
use orderbook_rs::engine::MatchingEngine;
use orderbook_rs::event::{DomainEvent, OrderCancelReason};
use orderbook_rs::snapshot::EngineSnapshot;
use orderbook_rs::types::{
    EngineCommand, EngineConfig, EngineError, OrderBookId, OrderRecord, OrderRequest, OrderStatus,
    OrderType, Side, StopKind, StopOrderRequest, TimeInForce, Trade,
};

const BTC: &str = "BTC-USDT";
const ETH: &str = "ETH-USDT";

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

fn ioc_limit(side: Side, price: u64, quantity: u64, timestamp: u64) -> OrderRequest {
    OrderRequest {
        time_in_force: TimeInForce::IOC,
        ..limit(side, price, quantity, timestamp)
    }
}

fn market(side: Side, quantity: u64, timestamp: u64) -> OrderRequest {
    OrderRequest {
        side,
        price: 0,
        quantity,
        timestamp,
        order_type: OrderType::Market,
        time_in_force: TimeInForce::IOC,
        post_only: false,
    }
}

fn submit(symbol: &str, request: OrderRequest) -> EngineCommand {
    EngineCommand::SubmitOrder {
        symbol: symbol.to_string(),
        request,
    }
}

fn cancel(symbol: &str, order_id: OrderBookId) -> EngineCommand {
    EngineCommand::CancelOrder {
        symbol: symbol.to_string(),
        order_id,
    }
}

fn accepted(order_id: OrderBookId, request: &OrderRequest) -> DomainEvent {
    DomainEvent::OrderAccepted {
        symbol: BTC.to_string(),
        order: request.to_order(order_id),
        post_only: request.post_only,
    }
}

fn trade(
    id: u64,
    maker: OrderBookId,
    taker: OrderBookId,
    price: u64,
    quantity: u64,
    taker_side: Side,
    timestamp: u64,
) -> Trade {
    Trade {
        id,
        symbol: BTC.to_string(),
        maker_order_id: maker,
        taker_order_id: taker,
        price,
        quantity,
        taker_side,
        timestamp,
    }
}

/// Apply a decided event list with the next contiguous sequences.
fn apply_all(engine: &mut MatchingEngine, events: &[DomainEvent]) {
    for event in events {
        let sequence = engine.last_applied_sequence() + 1;
        engine.apply(event, sequence);
    }
}

/// Decide and apply one command through the pure engine seam.
fn run(engine: &mut MatchingEngine, command: &EngineCommand) -> Vec<DomainEvent> {
    let events = engine
        .decide(command)
        .expect("command is supported by the event-sourced path");
    apply_all(engine, &events);
    events
}

/// Seed a resting order through the event-sourced path, not the old path.
fn rest(engine: &mut MatchingEngine, symbol: &str, request: OrderRequest) -> OrderBookId {
    let events = run(engine, &submit(symbol, request));
    match events.first() {
        Some(DomainEvent::OrderAccepted { order, .. }) => order.id,
        other => panic!("resting submit must start with OrderAccepted, got {other:?}"),
    }
}

fn records(engine: &MatchingEngine) -> Vec<(OrderBookId, OrderRecord)> {
    let mut records: Vec<(OrderBookId, OrderRecord)> = engine
        .order_records()
        .map(|(order_id, record)| (order_id, record.clone()))
        .collect();
    records.sort_by_key(|(order_id, _)| *order_id);
    records
}

fn ledger_map(snapshot: &EngineSnapshot) -> HashMap<OrderBookId, OrderRecord> {
    snapshot
        .ledger
        .iter()
        .map(|entry| (entry.order_id, entry.record.clone()))
        .collect()
}

/// Compare decision state, deliberately ignoring `next_event_sequence`.
///
/// The old command path emits one output event per state transition while the
/// event-sourced path emits `OrderRested`/residual-cancel events too, so the
/// derived event counter is not part of the shared decision state contract.
fn assert_decision_state_eq(event_sourced: &MatchingEngine, command_sourced: &MatchingEngine) {
    let event_sourced = event_sourced.capture_snapshot();
    let command_sourced = command_sourced.capture_snapshot();
    assert_eq!(event_sourced.books, command_sourced.books);
    assert_eq!(event_sourced.ledger, command_sourced.ledger);
    assert_eq!(event_sourced.stops, command_sourced.stops);
    assert_eq!(event_sourced.last_trades, command_sourced.last_trades);
    assert_eq!(event_sourced.trades, command_sourced.trades);
    assert_eq!(event_sourced.next_order_id, command_sourced.next_order_id);
    assert_eq!(event_sourced.next_trade_id, command_sourced.next_trade_id);
    assert_eq!(event_sourced.config, command_sourced.config);
}

/// Every prefix of the stream must leave a structurally valid engine state.
fn assert_prefix_invariants(engine: &MatchingEngine) {
    let snapshot = engine.capture_snapshot();
    let ledger = ledger_map(&snapshot);

    // filled + remaining == original for every lifecycle record.
    for entry in &snapshot.ledger {
        assert_eq!(
            entry.record.filled_quantity + entry.record.remaining_quantity,
            entry.record.original_quantity,
            "order {} violated filled + remaining == original",
            entry.order_id
        );
    }

    // Every book order is backed by an open ledger record with the same
    // remaining quantity, and book order ids are unique across both sides.
    let mut seen = HashMap::new();
    for book in &snapshot.books {
        for level in book.bids.iter().chain(book.asks.iter()) {
            for order in &level.orders {
                assert!(
                    seen.insert(order.id, ()).is_none(),
                    "order {} appears twice in the books",
                    order.id
                );
                let record = ledger
                    .get(&order.id)
                    .unwrap_or_else(|| panic!("book order {} has no ledger record", order.id));
                assert_eq!(record.symbol, book.symbol);
                assert!(
                    record.is_open(),
                    "book order {} has non-open ledger status {:?}",
                    order.id,
                    record.status
                );
                assert_eq!(
                    record.remaining_quantity, order.quantity,
                    "book order {} quantity disagrees with the ledger",
                    order.id
                );
            }
        }
    }

    // Trade tape is in trade-id order with no gaps.
    for (index, trade) in snapshot.trades.iter().enumerate() {
        assert_eq!(trade.id, index as u64 + 1, "trade ids must be contiguous");
    }

    // The last trade price is the price of the symbol's most recent trade.
    for symbol in [BTC, ETH] {
        let expected = snapshot
            .trades
            .iter()
            .rev()
            .find(|trade| trade.symbol == symbol)
            .map(|trade| trade.price);
        assert_eq!(engine.last_trade_price(symbol), expected);
    }
}

// --------------------------------------------------------------------------
// Exact crossing, partial, full, market, IOC and residual event streams
// --------------------------------------------------------------------------

#[test]
fn fully_filled_crossing_limit_emits_accepted_and_trade_only() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    rest(&mut engine, BTC, limit(Side::Ask, 100, 5, 1));

    let request = limit(Side::Bid, 100, 5, 2);
    let events = engine
        .decide(&submit(BTC, request))
        .expect("crossing limit submit is supported");

    assert_eq!(
        events.len(),
        2,
        "a full fill has no trailing rest or cancel"
    );
    assert_eq!(events[0], accepted(2, &request));
    assert_eq!(
        events[1],
        DomainEvent::TradeExecuted {
            trade: trade(1, 1, 2, 100, 5, Side::Bid, 2),
        }
    );

    apply_all(&mut engine, &events);
    assert_eq!(engine.trades(), &vec![trade(1, 1, 2, 100, 5, Side::Bid, 2)]);
    assert_eq!(engine.order_record(1).unwrap().status, OrderStatus::Filled);
    assert_eq!(engine.order_record(2).unwrap().status, OrderStatus::Filled);
    assert_eq!(engine.last_trade_price(BTC), Some(100));
    assert_eq!(engine.book(BTC).unwrap().best_bid(), None);
    assert_eq!(engine.book(BTC).unwrap().best_ask(), None);
}

#[test]
fn partially_filled_crossing_limit_rests_its_remainder() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    rest(&mut engine, BTC, limit(Side::Ask, 100, 3, 1));

    let request = limit(Side::Bid, 100, 5, 2);
    let events = engine.decide(&submit(BTC, request)).unwrap();

    assert_eq!(events.len(), 3);
    assert_eq!(events[0], accepted(2, &request));
    assert_eq!(
        events[1],
        DomainEvent::TradeExecuted {
            trade: trade(1, 1, 2, 100, 3, Side::Bid, 2),
        }
    );
    match &events[2] {
        DomainEvent::OrderRested { symbol, order } => {
            assert_eq!(symbol, BTC);
            assert_eq!(order.id, 2);
            assert_eq!(order.quantity, 2, "the unfilled remainder rests");
            assert_eq!(order.side, Side::Bid);
            assert_eq!(order.price, 100);
        }
        other => panic!("expected OrderRested, got {other:?}"),
    }

    apply_all(&mut engine, &events);
    assert_eq!(
        engine.book(BTC).unwrap().depth(Side::Bid, 0),
        Some((100, 2))
    );
    assert_eq!(engine.book(BTC).unwrap().best_ask(), None);

    let maker = engine.order_record(1).unwrap();
    assert_eq!(maker.status, OrderStatus::Filled);
    assert_eq!(maker.filled_quantity, 3);
    assert_eq!(maker.remaining_quantity, 0);

    let taker = engine.order_record(2).unwrap();
    assert_eq!(taker.status, OrderStatus::PartiallyFilled);
    assert_eq!(taker.original_quantity, 5);
    assert_eq!(taker.filled_quantity, 3);
    assert_eq!(taker.remaining_quantity, 2);
}

#[test]
fn crossing_limit_sweeps_levels_in_price_time_priority() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    rest(&mut engine, BTC, limit(Side::Ask, 100, 2, 1)); // maker 1
    rest(&mut engine, BTC, limit(Side::Ask, 100, 3, 2)); // maker 2, same level
    rest(&mut engine, BTC, limit(Side::Ask, 101, 2, 3)); // maker 3, worse price

    let request = limit(Side::Bid, 101, 8, 4);
    let events = engine.decide(&submit(BTC, request)).unwrap();

    assert_eq!(events.len(), 5, "accepted + three fills + rest");
    assert_eq!(events[0], accepted(4, &request));
    assert_eq!(
        events[1],
        DomainEvent::TradeExecuted {
            trade: trade(1, 1, 4, 100, 2, Side::Bid, 4),
        }
    );
    assert_eq!(
        events[2],
        DomainEvent::TradeExecuted {
            trade: trade(2, 2, 4, 100, 3, Side::Bid, 4),
        }
    );
    assert_eq!(
        events[3],
        DomainEvent::TradeExecuted {
            trade: trade(3, 3, 4, 101, 2, Side::Bid, 4),
        }
    );
    match &events[4] {
        DomainEvent::OrderRested { order, .. } => {
            assert_eq!(order.id, 4);
            assert_eq!(order.quantity, 1);
            assert_eq!(order.price, 101);
        }
        other => panic!("expected OrderRested, got {other:?}"),
    }

    apply_all(&mut engine, &events);
    assert_eq!(engine.trades().len(), 3);
    assert_eq!(engine.order_record(1).unwrap().status, OrderStatus::Filled);
    assert_eq!(engine.order_record(2).unwrap().status, OrderStatus::Filled);
    assert_eq!(engine.order_record(3).unwrap().status, OrderStatus::Filled);
    assert_eq!(engine.book(BTC).unwrap().best_ask(), None);
    assert_eq!(
        engine.book(BTC).unwrap().depth(Side::Bid, 0),
        Some((101, 1))
    );
    assert_eq!(engine.last_trade_price(BTC), Some(101));
}

#[test]
fn fully_filled_market_order_has_no_trailing_event() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    rest(&mut engine, BTC, limit(Side::Ask, 100, 2, 1));

    let request = market(Side::Bid, 2, 2);
    let events = engine.decide(&submit(BTC, request)).unwrap();

    assert_eq!(events.len(), 2);
    assert_eq!(events[0], accepted(2, &request));
    assert_eq!(
        events[1],
        DomainEvent::TradeExecuted {
            trade: trade(1, 1, 2, 100, 2, Side::Bid, 2),
        }
    );
}

#[test]
fn market_order_with_insufficient_liquidity_cancels_the_residual() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    rest(&mut engine, BTC, limit(Side::Ask, 100, 2, 1));

    let request = market(Side::Bid, 5, 2);
    let events = engine.decide(&submit(BTC, request)).unwrap();

    assert_eq!(events.len(), 3);
    assert_eq!(events[0], accepted(2, &request));
    assert_eq!(
        events[1],
        DomainEvent::TradeExecuted {
            trade: trade(1, 1, 2, 100, 2, Side::Bid, 2),
        }
    );
    assert_eq!(
        events[2],
        DomainEvent::OrderCanceled {
            symbol: BTC.to_string(),
            order_id: 2,
            canceled_quantity: 3,
            reason: OrderCancelReason::UnfilledResidual,
        }
    );

    apply_all(&mut engine, &events);
    let record = engine.order_record(2).unwrap();
    assert_eq!(record.status, OrderStatus::Canceled);
    assert_eq!(record.filled_quantity, 2);
    assert_eq!(record.remaining_quantity, 3);
    assert_eq!(engine.book(BTC).unwrap().best_bid(), None);
    assert_eq!(engine.last_trade_price(BTC), Some(100));
}

#[test]
fn ioc_limit_without_liquidity_is_accepted_then_canceled() {
    let mut engine = MatchingEngine::new(EngineConfig::default());

    let request = ioc_limit(Side::Bid, 100, 5, 1);
    let events = engine.decide(&submit(BTC, request)).unwrap();

    assert_eq!(
        events,
        vec![
            accepted(1, &request),
            DomainEvent::OrderCanceled {
                symbol: BTC.to_string(),
                order_id: 1,
                canceled_quantity: 5,
                reason: OrderCancelReason::UnfilledResidual,
            },
        ]
    );

    apply_all(&mut engine, &events);
    assert!(engine.trades().is_empty());
    assert_eq!(
        engine.order_record(1).unwrap().status,
        OrderStatus::Canceled
    );
    assert_eq!(engine.order_record(1).unwrap().remaining_quantity, 5);
    assert!(engine.book(BTC).is_none(), "nothing was ever inserted");
}

#[test]
fn ioc_limit_partial_fill_discards_only_the_residual() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    rest(&mut engine, BTC, limit(Side::Ask, 100, 2, 1));

    let request = ioc_limit(Side::Bid, 100, 5, 2);
    let events = engine.decide(&submit(BTC, request)).unwrap();

    assert_eq!(events.len(), 3);
    assert_eq!(events[0], accepted(2, &request));
    assert_eq!(
        events[1],
        DomainEvent::TradeExecuted {
            trade: trade(1, 1, 2, 100, 2, Side::Bid, 2),
        }
    );
    assert_eq!(
        events[2],
        DomainEvent::OrderCanceled {
            symbol: BTC.to_string(),
            order_id: 2,
            canceled_quantity: 3,
            reason: OrderCancelReason::UnfilledResidual,
        }
    );

    apply_all(&mut engine, &events);
    assert_eq!(engine.book(BTC).unwrap().best_bid(), None);
    assert_eq!(
        engine.order_record(2).unwrap().status,
        OrderStatus::Canceled
    );
    assert_eq!(engine.order_record(2).unwrap().filled_quantity, 2);
    assert_eq!(engine.order_record(2).unwrap().remaining_quantity, 3);
}

// --------------------------------------------------------------------------
// Apply: maker/taker ledger, book reduction, tape and last price
// --------------------------------------------------------------------------

#[test]
fn trade_executed_reduces_the_maker_and_increases_the_taker_ledger() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    rest(&mut engine, BTC, limit(Side::Ask, 100, 5, 1));

    let events = engine
        .decide(&submit(BTC, limit(Side::Bid, 100, 3, 2)))
        .unwrap();

    // Apply OrderAccepted only: the ledger exists, the trade has not applied.
    engine.apply(&events[0], 3);
    assert_eq!(engine.order_record(2).unwrap().filled_quantity, 0);
    assert_eq!(
        engine.book(BTC).unwrap().depth(Side::Ask, 0),
        Some((100, 5))
    );

    // Apply the trade: maker book and ledger both drop by the traded quantity.
    engine.apply(&events[1], 4);
    let maker = engine.order_record(1).unwrap();
    assert_eq!(maker.status, OrderStatus::PartiallyFilled);
    assert_eq!(maker.filled_quantity, 3);
    assert_eq!(maker.remaining_quantity, 2);
    assert_eq!(
        engine.book(BTC).unwrap().depth(Side::Ask, 0),
        Some((100, 2)),
        "the maker's book quantity follows the ledger"
    );

    let taker = engine.order_record(2).unwrap();
    assert_eq!(taker.status, OrderStatus::Filled);
    assert_eq!(taker.filled_quantity, 3);
    assert_eq!(engine.last_trade_price(BTC), Some(100));
}

#[test]
fn trade_tape_and_last_price_match_the_trade_executed_events() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    rest(&mut engine, BTC, limit(Side::Ask, 100, 2, 1));
    rest(&mut engine, BTC, limit(Side::Ask, 101, 3, 2));

    let events = engine
        .decide(&submit(BTC, limit(Side::Bid, 101, 4, 3)))
        .unwrap();
    apply_all(&mut engine, &events);

    let traded: Vec<Trade> = events
        .iter()
        .filter_map(|event| match event {
            DomainEvent::TradeExecuted { trade } => Some(trade.clone()),
            _ => None,
        })
        .collect();

    assert_eq!(engine.trades(), &traded);
    assert_eq!(engine.last_trade_price(BTC), Some(101));
    let snapshot = engine.capture_snapshot();
    let last = snapshot
        .last_trades
        .iter()
        .find(|last| last.symbol == BTC)
        .expect("a last trade is recorded");
    assert_eq!(last.price, 101);
    assert_eq!(
        last.timestamp, 3,
        "the last trade carries the taker timestamp"
    );
}

// --------------------------------------------------------------------------
// User cancel
// --------------------------------------------------------------------------

#[test]
fn user_cancel_of_an_open_order_emits_order_canceled_for_the_resting_quantity() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    rest(&mut engine, BTC, limit(Side::Ask, 100, 5, 1));

    let events = engine.decide(&cancel(BTC, 1)).unwrap();

    assert_eq!(
        events,
        vec![DomainEvent::OrderCanceled {
            symbol: BTC.to_string(),
            order_id: 1,
            canceled_quantity: 5,
            reason: OrderCancelReason::UserCancel,
        }]
    );

    apply_all(&mut engine, &events);
    assert_eq!(engine.book(BTC).unwrap().best_ask(), None);
    assert_eq!(
        engine.order_record(1).unwrap().status,
        OrderStatus::Canceled
    );
    assert_eq!(engine.order_record(1).unwrap().remaining_quantity, 5);
}

#[test]
fn user_cancel_after_a_partial_fill_carries_the_book_remainder() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    rest(&mut engine, BTC, limit(Side::Ask, 100, 5, 1));
    run(&mut engine, &submit(BTC, limit(Side::Bid, 100, 2, 2)));

    let events = engine.decide(&cancel(BTC, 1)).unwrap();

    assert_eq!(
        events,
        vec![DomainEvent::OrderCanceled {
            symbol: BTC.to_string(),
            order_id: 1,
            canceled_quantity: 3,
            reason: OrderCancelReason::UserCancel,
        }]
    );

    apply_all(&mut engine, &events);
    let record = engine.order_record(1).unwrap();
    assert_eq!(record.status, OrderStatus::Canceled);
    assert_eq!(record.filled_quantity, 2);
    assert_eq!(record.remaining_quantity, 3);
    assert_eq!(engine.book(BTC).unwrap().best_ask(), None);
}

#[test]
fn unknown_cancel_is_rejected_without_appending_anything() {
    let engine = MatchingEngine::new(EngineConfig::default());
    assert_eq!(
        engine.decide(&cancel(BTC, 42)).unwrap_err(),
        EngineError::UnknownOrder
    );

    let log = InMemoryEventLog::new();
    let mut durable = EventSourcedEngine::create(EngineConfig::default(), Box::new(log.handle()))
        .expect("fresh event log");
    let error = durable
        .execute(cancel(BTC, 42))
        .expect_err("unknown cancel must not be appended");
    assert_eq!(
        error,
        DurableError::DecisionRejected(EngineError::UnknownOrder)
    );
    assert!(!durable.is_failed());
    assert_eq!(durable.applied_seq(), 0);
    assert!(durable.history_from(1).unwrap().is_empty());
}

#[test]
fn cancel_is_scoped_to_the_symbol() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    rest(&mut engine, BTC, limit(Side::Ask, 100, 5, 1));

    assert_eq!(
        engine.decide(&cancel(ETH, 1)).unwrap_err(),
        EngineError::UnknownOrder
    );
    assert!(engine.decide(&cancel(BTC, 1)).is_ok());
}

// --------------------------------------------------------------------------
// Purity, prefix validity, and replay equality
// --------------------------------------------------------------------------

#[test]
fn decide_is_pure_for_a_crossing_submit() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    rest(&mut engine, BTC, limit(Side::Ask, 100, 5, 1));
    let before = engine.capture_snapshot();
    let sequence_before = engine.last_sequence();

    let command = submit(BTC, limit(Side::Bid, 100, 3, 2));
    let first = engine.decide(&command).unwrap();
    let second = engine.decide(&command).unwrap();

    assert_eq!(first, second, "decide allocates ids deterministically");
    assert_eq!(engine.capture_snapshot(), before, "decide mutates nothing");
    assert_eq!(engine.last_sequence(), sequence_before);
    assert!(engine.trades().is_empty());
    assert_eq!(engine.last_trade_price(BTC), None);
}

#[test]
fn every_prefix_is_a_valid_engine_state_and_replay_equals_live_execution() {
    let commands = vec![
        submit(BTC, limit(Side::Ask, 100, 5, 1)),
        // Partial fill: the taker exhausts itself, the maker keeps 3.
        submit(BTC, limit(Side::Bid, 100, 2, 2)),
        // Cancel the maker's remainder.
        cancel(BTC, 1),
        submit(BTC, limit(Side::Ask, 101, 4, 3)),
        // Market residual is discarded.
        submit(BTC, market(Side::Bid, 7, 4)),
        // Rest and cancel a fresh order.
        submit(BTC, limit(Side::Bid, 99, 2, 5)),
        cancel(BTC, 5),
        // Cross another symbol to prove isolation survives replay.
        submit(ETH, limit(Side::Ask, 200, 2, 6)),
    ];

    let mut live = MatchingEngine::new(EngineConfig::default());
    let mut commands_events = Vec::new();
    for command in &commands {
        let events = live.decide(command).expect("scenario is supported");
        for event in &events {
            let sequence = live.last_applied_sequence() + 1;
            live.apply(event, sequence);
            assert_prefix_invariants(&live);
        }
        commands_events.push(events);
    }

    // The live path must match the old command path's decision state.
    let mut old = MatchingEngine::new(EngineConfig::default());
    for command in &commands {
        let _ = old.execute(command.clone());
    }
    assert_decision_state_eq(&live, &old);

    // Replay the same events from an empty engine through the durable seam.
    let log = InMemoryEventLog::new();
    {
        let mut durable =
            EventSourcedEngine::create(EngineConfig::default(), Box::new(log.handle()))
                .expect("fresh event log");
        for command in &commands {
            durable.execute(command.clone()).expect("supported command");
        }
    }
    let reopened = EventSourcedEngine::open(Box::new(log.handle())).expect("replay");

    assert_eq!(
        reopened.engine().capture_snapshot(),
        live.capture_snapshot()
    );
    assert_eq!(reopened.engine().trades(), live.trades());
    assert_eq!(records(reopened.engine()), records(&live));
    assert_eq!(
        reopened.engine().last_trade_price(BTC),
        live.last_trade_price(BTC)
    );
    assert_eq!(
        reopened.engine().last_trade_price(ETH),
        live.last_trade_price(ETH)
    );

    let total_events: usize = commands_events.iter().map(Vec::len).sum();
    assert_eq!(reopened.applied_seq(), total_events as u64);
    assert_eq!(
        reopened.engine().last_applied_sequence(),
        total_events as u64
    );
}

#[test]
fn replay_preserves_fifo_priority_across_fills_and_rests() {
    let commands = vec![
        submit(BTC, limit(Side::Ask, 100, 2, 1)), // id 1
        submit(BTC, limit(Side::Ask, 100, 3, 2)), // id 2, behind 1
        submit(BTC, limit(Side::Bid, 100, 1, 3)), // id 3, takes 1 from id 1
        cancel(BTC, 1),                           // id 1 has 1 left, canceled
        submit(BTC, limit(Side::Bid, 100, 3, 4)), // id 4, takes 3 from id 2
    ];

    let log = InMemoryEventLog::new();
    let mut live = Mint::new();
    {
        let mut durable =
            EventSourcedEngine::create(EngineConfig::default(), Box::new(log.handle()))
                .expect("fresh event log");
        for command in &commands {
            live.observe(&mut durable, command);
        }
    }

    let reopened = EventSourcedEngine::open(Box::new(log.handle())).expect("replay");
    let makers: Vec<(OrderBookId, u64)> = reopened
        .engine()
        .trades()
        .iter()
        .map(|trade| (trade.maker_order_id, trade.quantity))
        .collect();
    assert_eq!(
        makers,
        vec![(1, 1), (2, 3)],
        "price-time priority survives replay"
    );
    assert_eq!(records(reopened.engine()), live.records);
    assert_eq!(reopened.engine().trades(), &live.trades);
}

/// Small helper that records the live engine's observed tape/state as the
/// durable engine executes, so the replay comparison is against live values.
struct Mint {
    trades: Vec<Trade>,
    records: Vec<(OrderBookId, OrderRecord)>,
}

impl Mint {
    fn new() -> Self {
        Self {
            trades: Vec::new(),
            records: Vec::new(),
        }
    }

    fn observe(&mut self, durable: &mut EventSourcedEngine, command: &EngineCommand) {
        let _events = durable.execute(command.clone()).expect("supported command");
        self.trades = durable.engine().trades().clone();
        self.records = records(durable.engine());
    }
}

// --------------------------------------------------------------------------
// Ticket boundary: pending stop triggers belong to ticket 03
// --------------------------------------------------------------------------

#[test]
fn a_command_that_would_trigger_a_pending_stop_is_rejected_without_appending() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    // Establish a last trade price of 100 on the old path.
    let _ = engine.execute(submit(BTC, limit(Side::Ask, 100, 1, 1)));
    let _ = engine.execute(submit(BTC, limit(Side::Bid, 100, 1, 2)));
    // Pending buy stop at 105 (valid while the last price is 100).
    let _ = engine.execute(EngineCommand::SubmitStopOrder {
        symbol: BTC.to_string(),
        request: StopOrderRequest {
            side: Side::Bid,
            stop_price: 105,
            order_type: StopKind::Market,
            quantity: 1,
            timestamp: 3,
        },
    });
    // Rest a bid that the next ask can trade against at 106.
    let _ = engine.execute(submit(BTC, limit(Side::Bid, 106, 1, 4)));

    let before = engine.capture_snapshot();
    let error = engine
        .decide(&submit(BTC, limit(Side::Ask, 106, 1, 5)))
        .expect_err("triggering a pending stop is ticket 03's scope");
    assert_eq!(error, EngineError::UnsupportedCommand);
    assert_eq!(
        engine.capture_snapshot(),
        before,
        "rejecting must not mutate the engine"
    );
}

#[test]
fn a_command_that_does_not_trigger_a_pending_stop_is_still_decided() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    let _ = engine.execute(submit(BTC, limit(Side::Ask, 100, 1, 1)));
    let _ = engine.execute(submit(BTC, limit(Side::Bid, 100, 1, 2)));
    // A stop at 110 is not triggered by a trade at 104.
    let _ = engine.execute(EngineCommand::SubmitStopOrder {
        symbol: BTC.to_string(),
        request: StopOrderRequest {
            side: Side::Bid,
            stop_price: 110,
            order_type: StopKind::Market,
            quantity: 1,
            timestamp: 3,
        },
    });
    let _ = engine.execute(submit(BTC, limit(Side::Bid, 104, 1, 4)));

    let events = engine
        .decide(&submit(BTC, limit(Side::Ask, 104, 1, 5)))
        .expect("the pending stop stays dormant");
    assert!(matches!(events[0], DomainEvent::OrderAccepted { .. }));
    assert!(matches!(events[1], DomainEvent::TradeExecuted { .. }));
}

#[test]
fn pending_stop_cancel_is_unsupported_in_ticket_02() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    let _ = engine.execute(EngineCommand::SubmitStopOrder {
        symbol: BTC.to_string(),
        request: StopOrderRequest {
            side: Side::Bid,
            stop_price: 105,
            order_type: StopKind::Market,
            quantity: 1,
            timestamp: 1,
        },
    });

    assert_eq!(
        engine.decide(&cancel(BTC, 1)).unwrap_err(),
        EngineError::UnsupportedCommand,
        "StopCanceled belongs to ticket 03"
    );
}
