//! Shared data types for the order book and matching engine.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TimeInForce {
    #[default]
    GTC,
    IOC,
    FOK,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderType {
    Limit,
    Market,
}

// Unique identifier for the OrderBook system
pub type OrderBookId = u64;

// Side of an order in the OrderBook
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Bid,
    Ask,
}

impl Side {
    pub fn opposite(&self) -> Side {
        match self {
            Side::Bid => Side::Ask,
            Side::Ask => Side::Bid,
        }
    }
}

// Structure representing an order in the OrderBook
#[derive(Debug, Clone, PartialEq)]
pub struct Order {
    pub id: OrderBookId,
    pub side: Side,
    pub price: u64,     // Price in ticks (smallest price increment)
    pub quantity: u64,  // Remaining quantity of the order
    pub timestamp: u64, // Timestamp of the order in milliseconds since epoch
    pub order_type: OrderType,
}

impl Order {
    // Check if the order crosses the given best price on the opposite side of the book
    pub fn crosses(&self, best_price: u64) -> bool {
        match self.side {
            Side::Bid => self.price >= best_price,
            Side::Ask => self.price <= best_price,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Fill {
    pub maker_order_id: OrderBookId,
    pub taker_order_id: OrderBookId,
    pub price: u64, // 成交价 = 挂单价（maker price）
    pub quantity: u64,
    pub taker_side: Side,
}

#[derive(Debug, Default)]
pub struct MatchOutcome {
    pub fills: Vec<Fill>,
    pub filled_order_ids: Vec<OrderBookId>, // 完全成交的，需要从 order_map 移除
}

#[derive(Debug, Default)]
pub struct SubmitResult {
    pub remaining_quantity: u64,
    pub fills: Vec<Fill>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Trade {
    pub id: u64,
    pub symbol: String,
    pub maker_order_id: OrderBookId,
    pub taker_order_id: OrderBookId,
    pub price: u64,
    pub quantity: u64,
    pub taker_side: Side,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OrderRequest {
    // TODO: add symbol
    pub side: Side,
    pub price: u64,
    pub quantity: u64,
    pub timestamp: u64,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    pub post_only: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderStatus {
    New,             // 未成交，挂在簿上
    PartiallyFilled, // 部分成交，剩余挂在簿上
    Filled,          // 全部成交
    Canceled,        // 未成交或剩余部分被丢弃（市价单/IOC）
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderRecord {
    pub symbol: String,
    pub status: OrderStatus,
    pub original_quantity: u64,
    pub filled_quantity: u64,
    pub remaining_quantity: u64,
}

#[derive(Debug, Clone, PartialEq)]
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
    InvalidTimeInForce,
    PostOnlyWouldCross,
    InsufficientLiquidity,
}

#[derive(Debug, Clone, PartialEq)]
pub enum EngineCommand {
    SubmitOrder {
        symbol: String,
        request: OrderRequest,
        order_type: OrderType,
    },
    CancelOrder {
        symbol: String,
        order_id: OrderBookId,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum EngineEvent {
    OrderAccepted {
        sequence_id: u64,
        report: ExecutionReport,
    },
    TradeExecuted {
        sequence_id: u64,
        trade: Trade,
    },
    OrderCanceled {
        sequence_id: u64,
        report: ExecutionReport,
    },
    OrderRejected {
        sequence_id: u64,
        reason: EngineError,
    },
}

impl EngineEvent {
    pub fn sequence_id(&self) -> u64 {
        match self {
            EngineEvent::OrderAccepted { sequence_id, .. } => *sequence_id,
            EngineEvent::TradeExecuted { sequence_id, .. } => *sequence_id,
            EngineEvent::OrderCanceled { sequence_id, .. } => *sequence_id,
            EngineEvent::OrderRejected { sequence_id, .. } => *sequence_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookSnapshot {
    pub symbol: String,
    pub sequence: u64,
    pub bids: Vec<(u64, u64)>, // (price, quantity)，从高到低
    pub asks: Vec<(u64, u64)>, // (price, quantity)，从低到高
}