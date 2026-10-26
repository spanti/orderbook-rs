//! Turn an engine's complete decision state into a deterministic snapshot and
//! rebuild an equivalent engine from it, asserted only through the public
//! `EngineSnapshot` and `MatchingEngine` surfaces.

use std::collections::BTreeMap;

use orderbook_rs::engine::MatchingEngine;
use orderbook_rs::snapshot::{BookState, EngineSnapshot, PriceLevelState};
use orderbook_rs::types::{
    CommandOutcome, EngineCommand, EngineConfig, EngineError, ExecutionReport, ModifyRequest,
    OrderBookId, OrderRecord, OrderRequest, OrderStatus, OrderType, Side, StopKind, StopOrder,
    StopOrderRequest, TimeInForce, Trade,
};

const BTC: &str = "BTC-USDT";
const ETH: &str = "ETH-USDT";

fn limit(timestamp: u64, side: Side, price: u64, quantity: u64) -> OrderRequest {
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

fn stop(side: Side, stop_price: u64, quantity: u64, timestamp: u64) -> StopOrderRequest {
    StopOrderRequest {
        side,
        stop_price,
        order_type: StopKind::Market,
        quantity,
        timestamp,
    }
}

fn submit(engine: &mut MatchingEngine, symbol: &str, request: OrderRequest) -> CommandOutcome {
    engine.execute(EngineCommand::SubmitOrder {
        symbol: symbol.to_string(),
        request,
    })
}

fn submit_stop(
    engine: &mut MatchingEngine,
    symbol: &str,
    request: StopOrderRequest,
) -> CommandOutcome {
    engine.execute(EngineCommand::SubmitStopOrder {
        symbol: symbol.to_string(),
        request,
    })
}

fn cancel(engine: &mut MatchingEngine, symbol: &str, order_id: OrderBookId) -> CommandOutcome {
    engine.execute(EngineCommand::CancelOrder {
        symbol: symbol.to_string(),
        order_id,
    })
}

fn accepted(outcome: CommandOutcome) -> ExecutionReport {
    match outcome {
        CommandOutcome::Accepted(report) => report,
        other => panic!("expected an accepted outcome, got {other:?}"),
    }
}

fn book_state<'a>(snapshot: &'a EngineSnapshot, symbol: &str) -> &'a BookState {
    snapshot
        .books
        .iter()
        .find(|book| book.symbol == symbol)
        .unwrap_or_else(|| panic!("snapshot has no book for {symbol}"))
}

fn level_orders(levels: &[PriceLevelState], price: u64) -> Vec<(OrderBookId, u64)> {
    levels
        .iter()
        .find(|level| level.price == price)
        .unwrap_or_else(|| panic!("missing price level {price}"))
        .orders
        .iter()
        .map(|order| (order.id, order.quantity))
        .collect()
}

fn ledger_map(snapshot: &EngineSnapshot) -> BTreeMap<OrderBookId, &OrderRecord> {
    snapshot
        .ledger
        .iter()
        .map(|entry| (entry.order_id, &entry.record))
        .collect()
}

fn trade_makers(trades: &[Trade]) -> Vec<OrderBookId> {
    trades.iter().map(|trade| trade.maker_order_id).collect()
}

/// Two symbols, two orders sharing a price level (FIFO), one partial maker
/// fill, one filled taker and a resting bid.
fn two_symbol_scenario() -> MatchingEngine {
    let mut engine = MatchingEngine::new(EngineConfig::default());

    // BTC: id 1 and 2 share the ask level at 100; id 3 partially fills id 1.
    submit(&mut engine, BTC, limit(1, Side::Ask, 100, 5));
    submit(&mut engine, BTC, limit(2, Side::Ask, 100, 3));
    submit(&mut engine, BTC, limit(3, Side::Bid, 100, 4));
    submit(&mut engine, BTC, limit(4, Side::Bid, 99, 2));

    // ETH: id 5 partially fills against id 6.
    submit(&mut engine, ETH, limit(5, Side::Bid, 2000, 5));
    submit(&mut engine, ETH, limit(6, Side::Ask, 2000, 2));

    engine
}

#[test]
fn round_trip_preserves_multi_symbol_books_fifo_priority_and_partial_fills() {
    let engine = two_symbol_scenario();
    let snapshot = engine.capture_snapshot();

    assert_eq!(snapshot.next_order_id, 7);
    assert_eq!(snapshot.next_trade_id, 3);
    assert_eq!(
        snapshot.next_event_sequence,
        engine.last_sequence() + 1,
        "the next event sequence continues the original stream"
    );

    // Books are sorted by symbol; both symbols exist.
    assert_eq!(snapshot.books.len(), 2);
    assert_eq!(snapshot.books[0].symbol, BTC);
    assert_eq!(snapshot.books[1].symbol, ETH);

    // FIFO order inside one price level survives: id 1 (1 left) before id 2.
    assert_eq!(
        level_orders(&book_state(&snapshot, BTC).asks, 100),
        vec![(1, 1), (2, 3)]
    );
    assert_eq!(
        level_orders(&book_state(&snapshot, BTC).bids, 99),
        vec![(4, 2)]
    );
    assert_eq!(
        level_orders(&book_state(&snapshot, ETH).bids, 2000),
        vec![(5, 3)]
    );
    assert!(book_state(&snapshot, ETH).asks.is_empty());

    let records = ledger_map(&snapshot);
    assert_eq!(records[&1].status, OrderStatus::PartiallyFilled);
    assert_eq!(
        (records[&1].filled_quantity, records[&1].remaining_quantity),
        (4, 1)
    );
    assert_eq!(records[&2].status, OrderStatus::New);
    assert_eq!(records[&3].status, OrderStatus::Filled);
    assert_eq!(records[&4].status, OrderStatus::New);
    assert_eq!(records[&5].status, OrderStatus::PartiallyFilled);
    assert_eq!(records[&6].status, OrderStatus::Filled);

    assert_eq!(snapshot.trades.len(), 2);
    assert_eq!(snapshot.trades[0].id, 1);
    assert_eq!(snapshot.trades[1].id, 2);

    let restored = MatchingEngine::restore_snapshot(snapshot.clone());
    assert_eq!(restored.capture_snapshot(), snapshot);
    assert_eq!(restored.trades(), engine.trades());
    assert_eq!(restored.last_trade_price(BTC), Some(100));
    assert_eq!(restored.last_trade_price(ETH), Some(2000));
    assert_eq!(restored.snapshot(BTC, 10), engine.snapshot(BTC, 10));
    assert_eq!(restored.snapshot(ETH, 10), engine.snapshot(ETH, 10));
}

#[test]
fn restored_fifo_priority_drives_the_next_match() {
    let mut original = two_symbol_scenario();
    let snapshot = original.capture_snapshot();
    let mut restored = MatchingEngine::restore_snapshot(snapshot);

    let command = EngineCommand::SubmitOrder {
        symbol: BTC.to_string(),
        request: limit(7, Side::Bid, 100, 2),
    };
    let original_outcome = original.execute(command.clone());
    let restored_outcome = restored.execute(command);

    assert_eq!(original_outcome, restored_outcome);
    let report = accepted(restored_outcome);
    // The partially filled id 1 is still first in line, then id 2.
    assert_eq!(
        trade_makers(&report.fills),
        vec![1, 2],
        "restored FIFO priority must match the original"
    );
    assert_eq!(
        restored.capture_snapshot(),
        original.capture_snapshot(),
        "both engines must converge to the same state"
    );
}

#[test]
fn restored_engine_makes_the_same_decisions_for_subsequent_commands() {
    let mut original = two_symbol_scenario();
    // Pending stops: a buy stop above the last price and a sell stop below it.
    submit_stop(&mut original, BTC, stop(Side::Bid, 101, 2, 7));
    submit_stop(&mut original, ETH, stop(Side::Ask, 1999, 1, 8));

    let snapshot = original.capture_snapshot();
    let mut restored = MatchingEngine::restore_snapshot(snapshot);
    let original_event_len = original.events().len();

    let commands = vec![
        EngineCommand::SubmitOrder {
            symbol: BTC.to_string(),
            request: limit(9, Side::Bid, 99, 2),
        },
        EngineCommand::SubmitOrder {
            symbol: BTC.to_string(),
            request: limit(10, Side::Bid, 100, 2),
        },
        EngineCommand::CancelOrder {
            symbol: BTC.to_string(),
            order_id: 4,
        },
        EngineCommand::CancelOrder {
            symbol: BTC.to_string(),
            order_id: 9,
        },
        EngineCommand::ModifyOrder {
            symbol: BTC.to_string(),
            order_id: 2,
            request: ModifyRequest {
                price: 110,
                quantity: 5,
                timestamp: 11,
            },
        },
        EngineCommand::SubmitStopOrder {
            symbol: BTC.to_string(),
            request: stop(Side::Bid, 112, 1, 12),
        },
        EngineCommand::SubmitOrder {
            symbol: ETH.to_string(),
            request: limit(13, Side::Ask, 2000, 1),
        },
        // A rejected command must consume the same event sequence in both.
        EngineCommand::SubmitOrder {
            symbol: BTC.to_string(),
            request: limit(14, Side::Bid, 100, 0),
        },
        EngineCommand::CancelOrder {
            symbol: ETH.to_string(),
            order_id: 999,
        },
    ];

    for command in commands {
        let original_outcome = original.execute(command.clone());
        let restored_outcome = restored.execute(command);
        assert_eq!(original_outcome, restored_outcome);
    }

    assert_eq!(
        restored.events(),
        &original.events()[original_event_len..],
        "the restored engine re-derives exactly the same events"
    );
    assert_eq!(restored.last_sequence(), original.last_sequence());
    assert_eq!(restored.capture_snapshot(), original.capture_snapshot());
}

#[test]
fn ledger_round_trips_filled_canceled_and_open_records() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    submit(&mut engine, BTC, limit(1, Side::Ask, 100, 5)); // filled maker
    submit(&mut engine, BTC, limit(2, Side::Bid, 100, 5)); // filled taker
    submit(&mut engine, BTC, limit(3, Side::Ask, 100, 5)); // canceled
    submit(&mut engine, BTC, limit(4, Side::Ask, 101, 4)); // still open

    let canceled = cancel(&mut engine, BTC, 3);
    assert!(matches!(canceled, CommandOutcome::Canceled(_)));

    let snapshot = engine.capture_snapshot();
    let records = ledger_map(&snapshot);
    assert_eq!(records.len(), 4);
    assert_eq!(records[&1].status, OrderStatus::Filled);
    assert_eq!(records[&2].status, OrderStatus::Filled);
    assert_eq!(records[&3].status, OrderStatus::Canceled);
    assert_eq!(
        (records[&3].filled_quantity, records[&3].remaining_quantity),
        (0, 5)
    );
    assert_eq!(records[&4].status, OrderStatus::New);

    let restored = MatchingEngine::restore_snapshot(snapshot.clone());
    assert_eq!(restored.capture_snapshot(), snapshot);
    assert_eq!(restored.order_count(), 4);
    assert_eq!(
        restored.order_record(1).map(|record| record.status),
        Some(OrderStatus::Filled)
    );
    assert_eq!(
        restored.order_record(3).map(|record| record.status),
        Some(OrderStatus::Canceled)
    );
    assert!(restored.is_open(4));
    assert!(!restored.is_open(3));
}

#[test]
fn pending_stop_orders_survive_the_round_trip() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    submit_stop(&mut engine, BTC, stop(Side::Bid, 95, 1, 10)); // id 1
    submit_stop(&mut engine, BTC, stop(Side::Ask, 105, 2, 11)); // id 2
    submit_stop(&mut engine, BTC, stop(Side::Bid, 95, 3, 12)); // id 3, same price
    submit_stop(&mut engine, ETH, stop(Side::Ask, 2100, 4, 13)); // id 4

    let snapshot = engine.capture_snapshot();
    assert_eq!(snapshot.stops.len(), 2);
    assert_eq!(snapshot.stops[0].symbol, BTC);
    assert_eq!(snapshot.stops[1].symbol, ETH);
    let btc_stop_ids: Vec<OrderBookId> =
        snapshot.stops[0].stops.iter().map(|stop| stop.id).collect();
    // (stop_price, side_rank, id): bid 95 id 1, bid 95 id 3, ask 105 id 2.
    assert_eq!(btc_stop_ids, vec![1, 3, 2]);
    assert_eq!(
        snapshot.stops[1]
            .stops
            .iter()
            .map(|stop| stop.id)
            .collect::<Vec<_>>(),
        vec![4]
    );
    for entry in &snapshot.ledger {
        assert_eq!(entry.record.status, OrderStatus::PendingTrigger);
    }

    let restored = MatchingEngine::restore_snapshot(snapshot.clone());
    assert_eq!(restored.capture_snapshot(), snapshot);
    assert_eq!(
        restored
            .stop_orders(BTC, Side::Bid)
            .iter()
            .map(|stop| stop.id)
            .collect::<Vec<_>>(),
        vec![1, 3]
    );
    assert_eq!(
        restored
            .stop_orders(BTC, Side::Ask)
            .iter()
            .map(|stop| stop.id)
            .collect::<Vec<_>>(),
        vec![2]
    );
    assert_eq!(
        restored
            .stop_orders(ETH, Side::Ask)
            .iter()
            .map(|stop| stop.id)
            .collect::<Vec<_>>(),
        vec![4]
    );
    assert_eq!(
        restored.stop_order(2).map(|stop| stop.stop_price),
        Some(105)
    );
}

#[test]
fn last_trade_price_and_timestamp_survive_the_round_trip() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    submit(&mut engine, BTC, limit(3, Side::Ask, 100, 5));
    submit(&mut engine, BTC, limit(3, Side::Bid, 100, 2));

    let snapshot = engine.capture_snapshot();
    assert_eq!(snapshot.last_trades.len(), 1);
    assert_eq!(snapshot.last_trades[0].symbol, BTC);
    assert_eq!(snapshot.last_trades[0].price, 100);
    assert_eq!(snapshot.last_trades[0].timestamp, 3);

    let restored = MatchingEngine::restore_snapshot(snapshot.clone());
    assert_eq!(restored.last_trade_price(BTC), Some(100));
    assert_eq!(restored.capture_snapshot(), snapshot);
}

#[test]
fn empty_engine_round_trips() {
    let engine = MatchingEngine::new(EngineConfig::default());
    let snapshot = engine.capture_snapshot();

    assert_eq!(
        snapshot,
        EngineSnapshot {
            config: EngineConfig::default(),
            next_order_id: 1,
            next_trade_id: 1,
            next_event_sequence: 1,
            books: Vec::new(),
            ledger: Vec::new(),
            stops: Vec::new(),
            last_trades: Vec::new(),
            trades: Vec::new(),
        }
    );
    assert_eq!(engine.last_sequence(), 0);

    let restored = MatchingEngine::restore_snapshot(snapshot.clone());
    assert_eq!(restored.capture_snapshot(), snapshot);
    assert!(restored.events().is_empty());
    assert_eq!(restored.last_sequence(), 0);
}

#[test]
fn empty_books_are_preserved() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    // The book entry is created by the first submit and remains after both
    // orders fully fill.
    submit(&mut engine, BTC, limit(1, Side::Ask, 100, 5));
    submit(&mut engine, BTC, limit(2, Side::Bid, 100, 5));
    assert!(engine.book(BTC).is_some());
    assert_eq!(engine.book(BTC).unwrap().depth(Side::Ask, 0), None);

    let snapshot = engine.capture_snapshot();
    assert_eq!(snapshot.books.len(), 1);
    assert_eq!(snapshot.books[0].symbol, BTC);
    assert!(snapshot.books[0].bids.is_empty());
    assert!(snapshot.books[0].asks.is_empty());

    let restored = MatchingEngine::restore_snapshot(snapshot.clone());
    assert!(restored.book(BTC).is_some());
    assert_eq!(restored.book(BTC).unwrap().depth(Side::Bid, 0), None);
    assert_eq!(restored.snapshot(BTC, 10), engine.snapshot(BTC, 10));
    assert_eq!(restored.capture_snapshot(), snapshot);
}

#[test]
fn repeated_capture_restore_cycles_are_stable() {
    let original = two_symbol_scenario();
    let mut snapshot = original.capture_snapshot();

    for _ in 0..5 {
        let restored = MatchingEngine::restore_snapshot(snapshot.clone());
        let recaptured = restored.capture_snapshot();
        assert_eq!(recaptured, snapshot);
        snapshot = recaptured;
    }
}

#[test]
fn equal_state_serializes_to_identical_bytes() {
    let first = two_symbol_scenario();
    let second = two_symbol_scenario();
    let snapshot = first.capture_snapshot();

    assert_eq!(second.capture_snapshot(), snapshot);
    assert_eq!(
        serde_json::to_string(&second.capture_snapshot()).unwrap(),
        serde_json::to_string(&snapshot).unwrap(),
        "the same command stream must produce identical snapshot bytes"
    );

    // Restore the same logical state through a snapshot whose collections are
    // in a different order. The engine's hash maps are then populated in that
    // order; capture must sort back to the canonical bytes.
    let mut reordered = snapshot.clone();
    reordered.books.reverse();
    reordered.ledger.reverse();
    reordered.stops.reverse();
    reordered.last_trades.reverse();
    let restored = MatchingEngine::restore_snapshot(reordered);

    assert_eq!(
        serde_json::to_string(&restored.capture_snapshot()).unwrap(),
        serde_json::to_string(&snapshot).unwrap(),
        "insertion order into the engine's maps must not change snapshot bytes"
    );
}

#[test]
fn snapshot_collections_have_a_deterministic_order() {
    let mut engine = two_symbol_scenario();
    submit_stop(&mut engine, BTC, stop(Side::Ask, 105, 1, 7));
    submit_stop(&mut engine, BTC, stop(Side::Bid, 95, 1, 8));
    let snapshot = engine.capture_snapshot();

    for pair in snapshot.books.windows(2) {
        assert!(pair[0].symbol < pair[1].symbol, "books sort by symbol");
    }
    for book in &snapshot.books {
        for pair in book.bids.windows(2) {
            assert!(
                pair[0].price < pair[1].price,
                "bids sort by ascending price"
            );
        }
        for pair in book.asks.windows(2) {
            assert!(
                pair[0].price < pair[1].price,
                "asks sort by ascending price"
            );
        }
    }

    for pair in snapshot.ledger.windows(2) {
        assert!(
            pair[0].order_id < pair[1].order_id,
            "ledger sorts by ascending order id"
        );
    }

    for pair in snapshot.stops.windows(2) {
        assert!(pair[0].symbol < pair[1].symbol, "stop books sort by symbol");
    }
    for stops in &snapshot.stops {
        for pair in stops.stops.windows(2) {
            let key = |stop: &StopOrder| {
                let side_rank = match stop.side {
                    Side::Bid => 0u8,
                    Side::Ask => 1u8,
                };
                (stop.stop_price, side_rank, stop.id)
            };
            assert!(
                key(&pair[0]) < key(&pair[1]),
                "stops sort by (stop_price, side_rank, id)"
            );
        }
    }

    for pair in snapshot.last_trades.windows(2) {
        assert!(
            pair[0].symbol < pair[1].symbol,
            "last trades sort by symbol"
        );
    }
    for pair in snapshot.trades.windows(2) {
        assert!(pair[0].id < pair[1].id, "trades stay in id order");
    }
}

#[test]
fn restore_rebuilds_price_level_totals_and_lookup_without_matching() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    submit(&mut engine, BTC, limit(1, Side::Ask, 100, 2));
    submit(&mut engine, BTC, limit(2, Side::Ask, 100, 3));
    submit(&mut engine, BTC, limit(3, Side::Bid, 100, 4)); // fills 1 fully, 2 partially
    submit(&mut engine, BTC, limit(4, Side::Bid, 99, 5));

    let snapshot = engine.capture_snapshot();
    let mut restored = MatchingEngine::restore_snapshot(snapshot.clone());

    // Restoring must not run matching: the trade history and counter are intact.
    assert_eq!(restored.trades().len(), engine.trades().len());
    assert_eq!(restored.capture_snapshot(), snapshot);
    assert_eq!(
        restored.book(BTC).unwrap().depth(Side::Ask, 0),
        Some((100, 1)),
        "the price-level total is rebuilt from the stored orders"
    );

    // Canceling a restored order proves the lookup index was rebuilt.
    let canceled = cancel(&mut restored, BTC, 2);
    match canceled {
        CommandOutcome::Canceled(report) => assert_eq!(report.remaining_quantity, 1),
        other => panic!("expected a canceled outcome, got {other:?}"),
    }
    assert_eq!(restored.book(BTC).unwrap().depth(Side::Ask, 0), None);

    // Matching still works against the rebuilt book.
    let report = accepted(submit(&mut restored, BTC, limit(20, Side::Ask, 99, 3)));
    assert_eq!(report.remaining_quantity, 0);
    assert_eq!(trade_makers(&report.fills), vec![4]);
    assert_eq!(report.fills[0].quantity, 3);
    assert_eq!(
        restored.book(BTC).unwrap().depth(Side::Bid, 0),
        Some((99, 2))
    );
}

#[test]
fn events_are_absent_and_the_event_sequence_continues_after_restore() {
    let mut engine = MatchingEngine::new(EngineConfig::default());
    submit(&mut engine, BTC, limit(1, Side::Ask, 100, 2)); // accepted: seq 1
    submit(&mut engine, BTC, limit(2, Side::Bid, 100, 2)); // accepted + trade: seq 2, 3
    let rejected = submit(&mut engine, BTC, limit(3, Side::Bid, 100, 0)); // rejected: seq 4
    assert_eq!(
        rejected,
        CommandOutcome::Rejected(EngineError::QuantityOutOfRange)
    );
    assert_eq!(engine.events().len(), 4);
    assert_eq!(engine.last_sequence(), 4);

    let snapshot = engine.capture_snapshot();
    assert_eq!(snapshot.next_event_sequence, 5);

    let mut restored = MatchingEngine::restore_snapshot(snapshot.clone());
    assert!(
        restored.events().is_empty(),
        "events are derived outputs and are not part of a snapshot"
    );
    assert_eq!(restored.last_sequence(), 4);

    let report = accepted(submit(&mut restored, BTC, limit(5, Side::Ask, 101, 1)));
    assert_eq!(report.order_id, 3, "order ids continue without collision");
    assert_eq!(restored.events().len(), 1);
    assert_eq!(restored.events()[0].sequence_id(), 5);
    assert_eq!(restored.last_sequence(), 5);
    assert_eq!(restored.capture_snapshot().next_event_sequence, 6);
}
