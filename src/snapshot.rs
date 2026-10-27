//! Deterministic, serializable form of a matching engine's decision state.
//!
//! An [`EngineSnapshot`] is a data-transfer object: it contains everything the
//! engine needs to make the same decisions after a restart, and nothing that is
//! derived from commands (events are deliberately absent). Collections are
//! captured in a canonical order so that equal engine state always serializes
//! to identical bytes.

use crate::engine::{EngineParts, LastTrade, MatchingEngine};
use crate::ledger::OrderLedger;
use crate::orderbook::OrderBook;
use crate::stop_book::StopBook;
use crate::types::{EngineConfig, Order, OrderBookId, OrderRecord, StopOrder, Trade};

fn price_level_states(levels: Vec<(u64, Vec<Order>)>) -> Vec<PriceLevelState> {
    levels
        .into_iter()
        .map(|(price, orders)| PriceLevelState { price, orders })
        .collect()
}

/// Complete decision state of a [`crate::engine::MatchingEngine`].
///
/// This is the state contract used by durable recovery: config, counters,
/// books with FIFO priority, the order ledger, pending stop orders, last trade
/// prices and the trade history. Event history is not included.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct EngineSnapshot {
    pub config: EngineConfig,
    pub next_order_id: u64,
    pub next_trade_id: u64,
    /// Every book, including empty ones, sorted by symbol.
    pub books: Vec<BookState>,
    /// Every ledger record, sorted by ascending order id.
    pub ledger: Vec<LedgerEntry>,
    /// Every symbol's pending stops, sorted by symbol.
    pub stops: Vec<StopBookState>,
    /// Last trade price and timestamp per symbol, sorted by symbol.
    pub last_trades: Vec<LastTradeState>,
    /// Full trade history in trade-id order.
    pub trades: Vec<Trade>,
}

/// One symbol's order book: both sides with their price levels.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BookState {
    pub symbol: String,
    /// Bid price levels sorted by ascending price.
    pub bids: Vec<PriceLevelState>,
    /// Ask price levels sorted by ascending price.
    pub asks: Vec<PriceLevelState>,
}

/// Orders resting at one price, in FIFO priority order.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PriceLevelState {
    pub price: u64,
    pub orders: Vec<Order>,
}

/// One order-ledger record, keyed by its order id.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LedgerEntry {
    pub order_id: OrderBookId,
    pub record: OrderRecord,
}

/// Pending stop orders for one symbol.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StopBookState {
    pub symbol: String,
    /// Stops sorted by `(stop_price, side_rank, id)`, where a bid ranks before
    /// an ask at the same stop price.
    pub stops: Vec<StopOrder>,
}

/// Last trade price and the timestamp of that trade for one symbol.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LastTradeState {
    pub symbol: String,
    pub price: u64,
    pub timestamp: u64,
}

impl EngineSnapshot {
    /// Capture the complete decision state of `engine` as a deterministic
    /// snapshot.
    ///
    /// Events are derived outputs and are not included. Books (including empty
    /// ones), ledger records, stop books and last trades are emitted in a
    /// canonical order so equal state has equal bytes.
    pub(crate) fn capture(engine: &MatchingEngine) -> Self {
        let parts = engine.parts();

        let mut book_symbols: Vec<&String> = parts.books.keys().collect();
        book_symbols.sort();
        let books = book_symbols
            .into_iter()
            .map(|symbol| {
                let (bids, asks) = parts.books[symbol].export_levels();
                BookState {
                    symbol: symbol.clone(),
                    bids: price_level_states(bids),
                    asks: price_level_states(asks),
                }
            })
            .collect();

        let ledger = parts
            .ledger
            .export_records()
            .into_iter()
            .map(|(order_id, record)| LedgerEntry { order_id, record })
            .collect();

        let mut stop_symbols: Vec<&String> = parts.stops.keys().collect();
        stop_symbols.sort();
        let stops = stop_symbols
            .into_iter()
            .map(|symbol| StopBookState {
                symbol: symbol.clone(),
                stops: parts.stops[symbol].export_stops(),
            })
            .collect();

        let mut last_trades: Vec<LastTradeState> = parts
            .last_trades
            .iter()
            .map(|(symbol, last)| LastTradeState {
                symbol: symbol.clone(),
                price: last.price,
                timestamp: last.timestamp,
            })
            .collect();
        last_trades.sort_by(|left, right| left.symbol.cmp(&right.symbol));

        EngineSnapshot {
            config: parts.config,
            next_order_id: parts.next_order_id,
            next_trade_id: parts.next_trade_id,
            books,
            ledger,
            stops,
            last_trades,
            trades: parts.trades.to_vec(),
        }
    }

    /// Rebuild an engine from a snapshot without running any matching.
    ///
    /// Books recompute their price-level totals and order lookup indexes from
    /// the stored orders; the ledger and both stop indexes are reinserted
    /// directly. Events start empty and the applied event boundary is left at
    /// zero for the durable layer to set.
    pub(crate) fn restore(self) -> MatchingEngine {
        let EngineSnapshot {
            config,
            next_order_id,
            next_trade_id,
            books,
            ledger,
            stops,
            last_trades,
            trades,
        } = self;

        let books = books
            .into_iter()
            .map(|state| {
                let bids = state
                    .bids
                    .into_iter()
                    .map(|level| (level.price, level.orders))
                    .collect();
                let asks = state
                    .asks
                    .into_iter()
                    .map(|level| (level.price, level.orders))
                    .collect();
                (state.symbol, OrderBook::from_levels(bids, asks))
            })
            .collect();

        let ledger = OrderLedger::from_records(
            ledger
                .into_iter()
                .map(|entry| (entry.order_id, entry.record))
                .collect(),
        );

        let stops = stops
            .into_iter()
            .map(|state| (state.symbol, StopBook::from_stops(state.stops)))
            .collect();

        let last_trades = last_trades
            .into_iter()
            .map(|last| {
                (
                    last.symbol,
                    LastTrade {
                        price: last.price,
                        timestamp: last.timestamp,
                    },
                )
            })
            .collect();

        MatchingEngine::from_parts(EngineParts {
            config,
            next_order_id,
            next_trade_id,
            books,
            ledger,
            stops,
            last_trades,
            trades,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{OrderStatus, Side};

    #[test]
    fn engine_snapshot_round_trips_through_json() {
        let snapshot = EngineSnapshot {
            config: EngineConfig::default(),
            next_order_id: 3,
            next_trade_id: 2,
            books: vec![BookState {
                symbol: "BTC-USDT".to_string(),
                bids: vec![PriceLevelState {
                    price: 100,
                    orders: Vec::new(),
                }],
                asks: Vec::new(),
            }],
            ledger: vec![LedgerEntry {
                order_id: 1,
                record: OrderRecord {
                    symbol: "BTC-USDT".to_string(),
                    status: OrderStatus::New,
                    original_quantity: 5,
                    filled_quantity: 0,
                    remaining_quantity: 5,
                },
            }],
            stops: vec![StopBookState {
                symbol: "BTC-USDT".to_string(),
                stops: vec![StopOrder {
                    id: 2,
                    side: Side::Bid,
                    stop_price: 95,
                    order_type: crate::types::StopKind::Market,
                    quantity: 1,
                    timestamp: 7,
                }],
            }],
            last_trades: vec![LastTradeState {
                symbol: "BTC-USDT".to_string(),
                price: 100,
                timestamp: 7,
            }],
            trades: Vec::new(),
        };

        let json = serde_json::to_string(&snapshot).expect("snapshot must serialize");
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(value.get("events").is_none());
        let decoded: EngineSnapshot =
            serde_json::from_str(&json).expect("snapshot JSON must round-trip");
        assert_eq!(decoded, snapshot);
    }

    #[test]
    fn empty_snapshot_has_no_event_field() {
        let snapshot = EngineSnapshot {
            config: EngineConfig::default(),
            next_order_id: 1,
            next_trade_id: 1,
            books: Vec::new(),
            ledger: Vec::new(),
            stops: Vec::new(),
            last_trades: Vec::new(),
            trades: Vec::new(),
        };
        let value = serde_json::to_value(&snapshot).unwrap();
        assert!(value.get("events").is_none());
    }
}
