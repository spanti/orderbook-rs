use std::collections::HashMap;

use colored::Colorize;
use rust_decimal::Decimal;
use uuid::{Uuid, timestamp};

use crate::orderbook::{Fill, Order, OrderBook, OrderBookId, OrderType, Side, Trade};

#[derive(Debug, Clone, Copy)]
pub struct OrderRequest {
    // TODO: add symbol
    pub side: Side,
    pub price: u64,
    pub quantity: u64,
    pub timestamp: u64,
    pub order_type: OrderType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderStatus {
    New,             // 未成交，挂在簿上
    PartiallyFilled, // 部分成交，剩余挂在簿上
    Filled,          // 全部成交
    Canceled,        // 未成交或剩余部分被丢弃（市价单/IOC）
}

pub struct OrderRecord {
    pub symbol: String,
    pub status: OrderStatus,
    pub original_quantity: u64,
    pub filled_quantity: u64,
    pub remaining_quantity: u64,
}

#[derive(Debug, Clone)]
pub struct ExecutionReport {
    pub order_id: u64,
    pub symbol: String,
    pub status: OrderStatus,
    pub original_quantity: u64,
    pub filled_quantity: u64,
    pub remaining_quantity: u64,
    pub fills: Vec<Trade>,
}

#[derive(Debug, Clone)]
pub struct CancelReport {
    pub order_id: OrderBookId,
    pub symbol: String,
    pub canceled_quantity: u64, // 撤单时的剩余量
}

#[derive(Debug, Clone, Copy)]
pub struct EngineConfig {
    pub min_order_quantity: u64,
    pub max_order_quantity: u64,
    pub tick_size: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            min_order_quantity: 1,
            max_order_quantity: u64::MAX,
            tick_size: 1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineError {
    QuantityOutOfRange,
    PriceNotOnTick,
    WrongOrderType,
}

// I want to implement a matching engine for the order book system
// for educational purposes......
// MatchingEngine has a hashmap <Symbol, OrderBook> ?
pub struct MatchingEngine {
    order_books: HashMap<String, OrderBook>,
    trades: Vec<Trade>,
    next_trade_id: u64,
    config: EngineConfig,
    orders: HashMap<OrderBookId, OrderRecord>,
    next_order_id: u64,
}

impl Default for MatchingEngine {
    fn default() -> Self {
        Self {
            order_books: HashMap::new(),
            trades: Vec::new(),
            next_trade_id: 1,
            config: EngineConfig::default(),
            orders: HashMap::new(),
            next_order_id: 1,
        }
    }
}

impl MatchingEngine {
    pub fn new(config: EngineConfig) -> Self {
        Self {
            order_books: HashMap::new(),
            trades: Vec::new(),
            next_trade_id: 1,
            config,
            orders: HashMap::new(),
            next_order_id: 1,
        }
    }

    pub fn submit_limit_order(
        &mut self,
        symbol: &str,
        request: OrderRequest,
    ) -> Result<ExecutionReport, EngineError> {
        self.validate(&request, OrderType::Limit)?;

        let order_id = self.next_order_id;
        self.next_order_id += 1;
        let order = Order {
            id: order_id,
            side: request.side,
            price: request.price,
            quantity: request.quantity,
            timestamp: request.timestamp,
            order_type: request.order_type,
        };
        let original_quantity = request.quantity;
        let timestamp = request.timestamp;
        let symbol_owned = symbol.to_string();

        let order_book = self
            .order_books
            .entry(symbol.to_string())
            .or_insert_with(OrderBook::new);

        let result = order_book.submit_limit_order(order);

        let fills = self.record_fills(symbol, timestamp, result.fills);

        let filled_quantity = original_quantity - result.remaining_quantity;
        let status = if result.remaining_quantity == 0 {
            OrderStatus::Filled
        } else if filled_quantity > 0 {
            OrderStatus::PartiallyFilled
        } else {
            OrderStatus::New
        };

        self.orders.insert(
            order_id,
            OrderRecord {
                symbol: symbol_owned.clone(),
                status,
                original_quantity,
                filled_quantity,
                remaining_quantity: result.remaining_quantity,
            },
        );

        Ok(ExecutionReport {
            order_id,
            symbol: symbol_owned,
            status: status,
            original_quantity,
            filled_quantity: filled_quantity,
            remaining_quantity: result.remaining_quantity,
            fills: fills,
        })
    }

    pub fn submit_market_order(
        &mut self,
        symbol: &str,
        request: OrderRequest,
    ) -> Result<ExecutionReport, EngineError> {
        self.validate(&request, OrderType::Market)?;

        let order_id = self.next_order_id;
        self.next_order_id += 1;
        let order = Order {
            id: order_id,
            side: request.side,
            price: request.price,
            quantity: request.quantity,
            timestamp: request.timestamp,
            order_type: request.order_type,
        };
        let original_quantity = request.quantity;
        let timestamp = request.timestamp;
        let symbol_owned = symbol.to_string();

        let order_book = self
            .order_books
            .entry(symbol.to_string())
            .or_insert_with(OrderBook::new);

        let result = order_book.submit_market_order(order);

        let fills = self.record_fills(symbol, timestamp, result.fills);

        let filled_quantity = original_quantity - result.remaining_quantity;
        // 市价单不回挂，剩余量视为取消
        let status = if result.remaining_quantity == 0 {
            OrderStatus::Filled
        } else {
            OrderStatus::Canceled
        };

        self.orders.insert(
            order_id,
            OrderRecord {
                symbol: symbol_owned.clone(),
                status,
                original_quantity,
                filled_quantity,
                remaining_quantity: result.remaining_quantity,
            },
        );

        Ok(ExecutionReport {
            order_id,
            symbol: symbol_owned,
            status,
            original_quantity,
            filled_quantity,
            remaining_quantity: result.remaining_quantity,
            fills,
        })
    }

    pub fn cancel_order(&mut self, symbol: &str, order_id: u64) -> Option<ExecutionReport> {
        let record = self.orders.get_mut(&order_id)?;
        if record.symbol != symbol
            || !matches!(
                record.status,
                OrderStatus::New | OrderStatus::PartiallyFilled
            )
        {
            return None;
        }

        let order = self.order_books.get_mut(symbol)?.cancel_limit_order(order_id)?;

        let canceled_quantity = order.quantity;
        let record = self.orders.get_mut(&order_id)?;
        record.status = OrderStatus::Canceled;
        record.remaining_quantity = canceled_quantity;
        
        Some(ExecutionReport {
            order_id,
            symbol: symbol.to_string(),
            status: OrderStatus::Canceled,
            original_quantity: record.original_quantity,
            filled_quantity: record.filled_quantity,
            remaining_quantity: record.remaining_quantity,
            fills: Vec::new(),
        })
    }

    pub fn trades(&self) -> &Vec<Trade> {
        &self.trades
    }

    pub fn book(&self, symbol: &str) -> Option<&OrderBook> {
        self.order_books.get(symbol)
    }

    pub fn print_trades(&self) {
        for trade in &self.trades {
            let side = match trade.taker_side {
                Side::Bid => "BUY".green(),
                Side::Ask => "SELL".red(),
            };
            println!(
                "#{} {} {} {} @ {}",
                trade.id, trade.symbol, side, trade.quantity, trade.price
            );
        }
    }

    fn validate(&self, request: &OrderRequest, expected: OrderType) -> Result<(), EngineError> {
        if request.order_type != expected {
            return Err(EngineError::WrongOrderType);
        }
        if request.quantity < self.config.min_order_quantity
            || request.quantity > self.config.max_order_quantity
        {
            return Err(EngineError::QuantityOutOfRange);
        }
        if request.order_type == OrderType::Limit && request.price % self.config.tick_size != 0 {
            return Err(EngineError::PriceNotOnTick);
        }
        Ok(())
    }

    fn record_fills(&mut self, symbol: &str, timestamp: u64, fills: Vec<Fill>) -> Vec<Trade> {
        let mut record_trades = Vec::with_capacity(fills.len());
        fills.iter().for_each(|fill| {
            if let Some(record) = self.orders.get_mut(&fill.maker_order_id) {
                record.filled_quantity += fill.quantity;
                record.remaining_quantity -= fill.quantity;
                record.status = if record.remaining_quantity == 0 {
                    OrderStatus::Filled
                } else {
                    OrderStatus::PartiallyFilled
                };
            }

            let trade = Trade {
                id: self.next_trade_id,
                symbol: symbol.to_string(),
                timestamp,
                price: fill.price,
                quantity: fill.quantity,
                taker_side: fill.taker_side,
                maker_order_id: fill.maker_order_id,
                taker_order_id: fill.taker_order_id,
            };
            self.next_trade_id += 1;

            self.trades.push(trade.clone());
            record_trades.push(trade);
        });
        record_trades
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limit(timestamp: u64, side: Side, price: u64, quantity: u64) -> OrderRequest {
        OrderRequest {
            side,
            price,
            quantity,
            timestamp,
            order_type: OrderType::Limit,
        }
    }

    fn market(timestamp: u64, side: Side, quantity: u64) -> OrderRequest {
        OrderRequest {
            side,
            price: 0,
            quantity,
            timestamp,
            order_type: OrderType::Market,
        }
    }

    #[test]
    fn crossing_orders_produce_a_trade() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        engine
            .submit_limit_order("BTC-USDT", limit(1, Side::Ask, 100, 5))
            .unwrap();
        engine
            .submit_limit_order("BTC-USDT", limit(2, Side::Bid, 100, 5))
            .unwrap();

        assert_eq!(engine.trades().len(), 1);
        let trade = &engine.trades()[0];
        assert_eq!(trade.id, 1);
        assert_eq!(trade.symbol, "BTC-USDT");
        assert_eq!(trade.maker_order_id, 1);
        assert_eq!(trade.taker_order_id, 2);
        assert_eq!(trade.price, 100);
        assert_eq!(trade.quantity, 5);
        assert_eq!(trade.taker_side, Side::Bid);
        assert_eq!(trade.timestamp, 2); // taker 的 timestamp
    }

    #[test]
    fn trade_ids_increment_across_price_levels() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        engine
            .submit_limit_order("BTC-USDT", limit(1, Side::Ask, 100, 2))
            .unwrap();
        engine
            .submit_limit_order("BTC-USDT", limit(2, Side::Ask, 101, 3))
            .unwrap();
        engine
            .submit_limit_order("BTC-USDT", limit(3, Side::Bid, 101, 4))
            .unwrap();

        let trades = engine.trades();
        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].id, 1);
        assert_eq!(trades[0].maker_order_id, 1);
        assert_eq!(trades[0].price, 100);
        assert_eq!(trades[0].quantity, 2);
        assert_eq!(trades[1].id, 2);
        assert_eq!(trades[1].maker_order_id, 2);
        assert_eq!(trades[1].price, 101);
        assert_eq!(trades[1].quantity, 2);
        assert!(
            trades
                .iter()
                .all(|t| t.taker_order_id == 3 && t.taker_side == Side::Bid)
        );
    }

    #[test]
    fn market_order_records_fill_and_does_not_rest() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        engine
            .submit_limit_order("BTC-USDT", limit(1, Side::Ask, 100, 2))
            .unwrap();

        engine
            .submit_market_order("BTC-USDT", market(2, Side::Bid, 5))
            .unwrap();

        assert_eq!(engine.trades().len(), 1);
        let trade = &engine.trades()[0];
        assert_eq!(trade.maker_order_id, 1);
        assert_eq!(trade.taker_order_id, 2);
        assert_eq!(trade.price, 100);
        assert_eq!(trade.quantity, 2);
        assert_eq!(trade.taker_side, Side::Bid);
    }

    #[test]
    fn books_are_isolated_per_symbol() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        engine
            .submit_limit_order("BTC-USDT", limit(1, Side::Ask, 100, 5))
            .unwrap();
        engine
            .submit_limit_order("ETH-USDT", limit(2, Side::Bid, 100, 5))
            .unwrap();

        assert!(engine.trades().is_empty());
        assert!(engine.book("BTC-USDT").is_some());
        assert!(engine.book("ETH-USDT").is_some());
        assert!(engine.book("SOL-USDT").is_none());
    }

    #[test]
    fn config_rejects_invalid_orders() {
        let config = EngineConfig {
            min_order_quantity: 1,
            max_order_quantity: 10,
            tick_size: 5,
        };
        let mut engine = MatchingEngine::new(config);

        assert_eq!(
            engine
                .submit_limit_order("X", limit(1, Side::Bid, 100, 0))
                .unwrap_err(),
            EngineError::QuantityOutOfRange
        );
        assert_eq!(
            engine
                .submit_limit_order("X", limit(2, Side::Bid, 100, 11))
                .unwrap_err(),
            EngineError::QuantityOutOfRange
        );
        assert_eq!(
            engine
                .submit_limit_order("X", limit(3, Side::Bid, 101, 5))
                .unwrap_err(),
            EngineError::PriceNotOnTick
        );
        assert_eq!(
            engine
                .submit_limit_order("X", market(4, Side::Bid, 5))
                .unwrap_err(),
            EngineError::WrongOrderType
        );
        assert_eq!(
            engine
                .submit_market_order("X", limit(5, Side::Bid, 100, 5))
                .unwrap_err(),
            EngineError::WrongOrderType
        );
        assert!(engine.trades().is_empty());
        assert!(engine.book("X").is_none()); // 校验失败不应创建簿
    }

    #[test]
    fn cancel_order_is_scoped_to_symbol() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        engine
            .submit_limit_order("BTC-USDT", limit(1, Side::Bid, 100, 5))
            .unwrap();

        let report = engine.cancel_order("BTC-USDT", 1).unwrap();
        assert_eq!(report.order_id, 1);
        assert_eq!(report.symbol, "BTC-USDT");
        assert_eq!(report.status, OrderStatus::Canceled);
        assert_eq!(report.original_quantity, 5);
        assert_eq!(report.filled_quantity, 0);
        assert_eq!(report.remaining_quantity, 5);
        assert!(report.fills.is_empty());

        assert_eq!(engine.orders.get(&1).unwrap().status, OrderStatus::Canceled);
        assert!(engine.cancel_order("BTC-USDT", 1).is_none());
        assert!(engine.cancel_order("ETH-USDT", 1).is_none());
    }

    #[test]
    fn resting_limit_order_reports_new() {
        let mut engine = MatchingEngine::new(EngineConfig::default());

        let report = engine
            .submit_limit_order("BTC-USDT", limit(1, Side::Bid, 100, 5))
            .unwrap();

        assert_eq!(report.order_id, 1);
        assert_eq!(report.symbol, "BTC-USDT");
        assert_eq!(report.status, OrderStatus::New);
        assert_eq!(report.original_quantity, 5);
        assert_eq!(report.filled_quantity, 0);
        assert_eq!(report.remaining_quantity, 5);
        assert!(report.fills.is_empty());
        assert!(engine.trades().is_empty());
    }

    #[test]
    fn fully_filled_limit_order_reports_filled() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        engine
            .submit_limit_order("BTC-USDT", limit(1, Side::Ask, 100, 5))
            .unwrap();

        let report = engine
            .submit_limit_order("BTC-USDT", limit(2, Side::Bid, 100, 5))
            .unwrap();

        assert_eq!(report.status, OrderStatus::Filled);
        assert_eq!(report.original_quantity, 5);
        assert_eq!(report.filled_quantity, 5);
        assert_eq!(report.remaining_quantity, 0);
        assert_eq!(report.fills.len(), 1);
        assert_eq!(report.fills[0].id, 1);
        assert_eq!(report.fills[0].maker_order_id, 1);
        assert_eq!(report.fills[0].taker_order_id, 2);
        assert_eq!(report.fills[0].price, 100);
        assert_eq!(report.fills[0].quantity, 5);
    }

    #[test]
    fn partially_filled_limit_order_reports_partially_filled() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        engine
            .submit_limit_order("BTC-USDT", limit(1, Side::Ask, 100, 3))
            .unwrap();

        let report = engine
            .submit_limit_order("BTC-USDT", limit(2, Side::Bid, 105, 10))
            .unwrap();

        assert_eq!(report.status, OrderStatus::PartiallyFilled);
        assert_eq!(report.original_quantity, 10);
        assert_eq!(report.filled_quantity, 3);
        assert_eq!(report.remaining_quantity, 7);
        assert_eq!(report.fills.len(), 1);
        assert_eq!(report.fills[0].price, 100);
        assert_eq!(report.fills[0].quantity, 3);
    }

    #[test]
    fn filled_market_order_reports_filled() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        engine
            .submit_limit_order("BTC-USDT", limit(1, Side::Ask, 100, 2))
            .unwrap();

        let report = engine
            .submit_market_order("BTC-USDT", market(2, Side::Bid, 2))
            .unwrap();

        assert_eq!(report.status, OrderStatus::Filled);
        assert_eq!(report.filled_quantity, 2);
        assert_eq!(report.remaining_quantity, 0);
        assert_eq!(report.fills.len(), 1);
    }

    #[test]
    fn partially_filled_market_order_reports_canceled() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        engine
            .submit_limit_order("BTC-USDT", limit(1, Side::Ask, 100, 2))
            .unwrap();

        let report = engine
            .submit_market_order("BTC-USDT", market(2, Side::Bid, 5))
            .unwrap();

        assert_eq!(report.status, OrderStatus::Canceled);
        assert_eq!(report.original_quantity, 5);
        assert_eq!(report.filled_quantity, 2);
        assert_eq!(report.remaining_quantity, 3);
        assert_eq!(report.fills.len(), 1);
        assert_eq!(report.fills[0].quantity, 2);
    }

    #[test]
    fn market_order_without_liquidity_reports_canceled() {
        let mut engine = MatchingEngine::new(EngineConfig::default());

        let report = engine
            .submit_market_order("BTC-USDT", market(1, Side::Bid, 5))
            .unwrap();

        assert_eq!(report.status, OrderStatus::Canceled);
        assert_eq!(report.original_quantity, 5);
        assert_eq!(report.filled_quantity, 0);
        assert_eq!(report.remaining_quantity, 5);
        assert!(report.fills.is_empty());
        assert!(engine.trades().is_empty());
    }

    #[test]
    fn cancel_after_partial_fill_reports_remaining_quantity() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        engine
            .submit_limit_order("BTC-USDT", limit(1, Side::Ask, 100, 2))
            .unwrap();
        engine
            .submit_limit_order("BTC-USDT", limit(2, Side::Bid, 105, 5))
            .unwrap();

        let report = engine.cancel_order("BTC-USDT", 2).unwrap();

        assert_eq!(report.order_id, 2);
        assert_eq!(report.symbol, "BTC-USDT");
        assert_eq!(report.status, OrderStatus::Canceled);
        assert_eq!(report.original_quantity, 5);
        assert_eq!(report.filled_quantity, 2);
        assert_eq!(report.remaining_quantity, 3);
        assert!(report.fills.is_empty());

        let record = engine.orders.get(&2).unwrap();
        assert_eq!(record.status, OrderStatus::Canceled);
        assert_eq!(record.filled_quantity, 2);
        assert_eq!(record.remaining_quantity, 3);
    }

    #[test]
    fn order_ids_are_assigned_by_engine_across_symbols() {
        let mut engine = MatchingEngine::new(EngineConfig::default());

        let first = engine
            .submit_limit_order("BTC-USDT", limit(10, Side::Bid, 100, 1))
            .unwrap();
        let second = engine
            .submit_limit_order("ETH-USDT", limit(20, Side::Bid, 100, 1))
            .unwrap();

        assert_eq!(first.order_id, 1);
        assert_eq!(second.order_id, 2);
    }

    #[test]
    fn rejected_order_does_not_consume_order_id() {
        let mut engine = MatchingEngine::new(EngineConfig::default());

        assert!(engine
            .submit_limit_order("BTC-USDT", limit(1, Side::Bid, 100, 0))
            .is_err());

        let report = engine
            .submit_limit_order("BTC-USDT", limit(2, Side::Bid, 100, 1))
            .unwrap();
        assert_eq!(report.order_id, 1);
    }

    #[test]
    fn order_records_track_fills_for_maker_and_taker() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        engine
            .submit_limit_order("BTC-USDT", limit(1, Side::Ask, 100, 3))
            .unwrap();

        // taker 成交 3，剩余 2 挂在 100
        engine
            .submit_limit_order("BTC-USDT", limit(2, Side::Bid, 100, 5))
            .unwrap();

        let maker = engine.orders.get(&1).unwrap();
        assert_eq!(maker.status, OrderStatus::Filled);
        assert_eq!(maker.original_quantity, 3);
        assert_eq!(maker.filled_quantity, 3);
        assert_eq!(maker.remaining_quantity, 0);

        let taker = engine.orders.get(&2).unwrap();
        assert_eq!(taker.status, OrderStatus::PartiallyFilled);
        assert_eq!(taker.original_quantity, 5);
        assert_eq!(taker.filled_quantity, 3);
        assert_eq!(taker.remaining_quantity, 2);
    }

    #[test]
    fn maker_record_updates_across_multiple_fills() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        engine
            .submit_limit_order("BTC-USDT", limit(1, Side::Ask, 100, 5))
            .unwrap();

        engine
            .submit_limit_order("BTC-USDT", limit(2, Side::Bid, 100, 2))
            .unwrap();
        let maker = engine.orders.get(&1).unwrap();
        assert_eq!(maker.status, OrderStatus::PartiallyFilled);
        assert_eq!(maker.filled_quantity, 2);
        assert_eq!(maker.remaining_quantity, 3);

        engine
            .submit_limit_order("BTC-USDT", limit(3, Side::Bid, 100, 3))
            .unwrap();
        let maker = engine.orders.get(&1).unwrap();
        assert_eq!(maker.status, OrderStatus::Filled);
        assert_eq!(maker.filled_quantity, 5);
        assert_eq!(maker.remaining_quantity, 0);
    }

    #[test]
    fn order_record_is_created_for_market_orders() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        engine
            .submit_limit_order("BTC-USDT", limit(1, Side::Ask, 100, 2))
            .unwrap();

        let report = engine
            .submit_market_order("BTC-USDT", market(2, Side::Bid, 5))
            .unwrap();

        let record = engine.orders.get(&report.order_id).unwrap();
        assert_eq!(record.symbol, "BTC-USDT");
        assert_eq!(record.status, OrderStatus::Canceled);
        assert_eq!(record.original_quantity, 5);
        assert_eq!(record.filled_quantity, 2);
        assert_eq!(record.remaining_quantity, 3);
    }

    #[test]
    fn cancel_filled_or_already_canceled_order_returns_none() {
        let mut engine = MatchingEngine::new(EngineConfig::default());
        engine
            .submit_limit_order("BTC-USDT", limit(1, Side::Ask, 100, 2))
            .unwrap();
        engine
            .submit_limit_order("BTC-USDT", limit(2, Side::Bid, 100, 2))
            .unwrap();

        // 双方都已完全成交
        assert!(engine.cancel_order("BTC-USDT", 1).is_none());
        assert!(engine.cancel_order("BTC-USDT", 2).is_none());

        engine
            .submit_limit_order("BTC-USDT", limit(3, Side::Bid, 99, 1))
            .unwrap();
        assert!(engine.cancel_order("BTC-USDT", 3).is_some());
        assert!(engine.cancel_order("BTC-USDT", 3).is_none());
    }
}
