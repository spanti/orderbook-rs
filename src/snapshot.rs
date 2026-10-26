//! Deterministic, serializable form of a matching engine's decision state.
//!
//! An [`EngineSnapshot`] is a data-transfer object: it contains everything the
//! engine needs to make the same decisions after a restart, and nothing that is
//! derived from commands (events are deliberately absent). Collections are
//! captured in a canonical order so that equal engine state always serializes
//! to identical bytes.

use crate::types::{EngineConfig, Order, OrderBookId, OrderRecord, StopOrder, Trade};

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
