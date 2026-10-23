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
    pub time_in_force: TimeInForce,
}

impl Order {
    // Check if the order crosses the given best price on the opposite side of the book
    pub fn crosses(&self, best_price: u64) -> bool {
        match self.side {
            Side::Bid => self.price >= best_price,
            Side::Ask => self.price <= best_price,
        }
    }

    // 市价单接受任意价格；限价单必须穿过该价格
    pub fn matches_price(&self, price: u64) -> bool {
        self.order_type == OrderType::Market || self.crosses(price)
    }

    // Only a GTC limit order keeps its unfilled remainder on the book.
    pub fn rests(&self) -> bool {
        self.order_type == OrderType::Limit && self.time_in_force == TimeInForce::GTC
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
pub struct SubmitResult {
    pub remaining_quantity: u64,
    pub fills: Vec<Fill>,
    pub rested: bool,
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

impl OrderRequest {
    // 由引擎分配 id 后转成簿内订单
    pub fn to_order(&self, id: OrderBookId) -> Order {
        Order {
            id,
            side: self.side,
            price: self.price,
            quantity: self.quantity,
            timestamp: self.timestamp,
            order_type: self.order_type,
            time_in_force: self.time_in_force,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderStatus {
    New,             // 未成交，挂在簿上
    PartiallyFilled, // 部分成交，剩余挂在簿上
    Filled,          // 全部成交
    Canceled,        // 未成交或剩余部分被丢弃（市价单/IOC）
    PendingTrigger,   // 等待触发的止损单
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderRecord {
    pub symbol: String,
    pub status: OrderStatus,
    pub original_quantity: u64,
    pub filled_quantity: u64,
    pub remaining_quantity: u64,
}

impl OrderRecord {
    /// Fresh record for a taker order, before its match result is applied.
    pub(crate) fn new(symbol: String, original_quantity: u64) -> Self {
        let record = Self {
            symbol,
            status: OrderStatus::New,
            original_quantity,
            filled_quantity: 0,
            remaining_quantity: original_quantity,
        };
        record.debug_assert_invariant();
        record
    }

    /// Whether the order is still working on the book.
    pub fn is_open(&self) -> bool {
        matches!(self.status, OrderStatus::New | OrderStatus::PartiallyFilled | OrderStatus::PendingTrigger)
    }

    /// Record a maker fill delta reported by the book.
    pub(crate) fn apply_maker_fill(&mut self, quantity: u64) {
        debug_assert!(
            quantity <= self.remaining_quantity,
            "maker fill {quantity} exceeds remaining {}",
            self.remaining_quantity
        );
        self.filled_quantity = self.filled_quantity.saturating_add(quantity);
        self.remaining_quantity = self.original_quantity.saturating_sub(self.filled_quantity);
        self.status = if self.remaining_quantity == 0 {
            OrderStatus::Filled
        } else {
            OrderStatus::PartiallyFilled
        };
        self.debug_assert_invariant();
    }

    /// Record the taker's final match result. `rests` is true when the
    /// leftover was placed on the book, as opposed to discarded (IOC/market).
    pub(crate) fn complete_taker(&mut self, remaining_quantity: u64, rests: bool) {
        debug_assert!(
            remaining_quantity <= self.original_quantity,
            "remaining {remaining_quantity} exceeds original {}",
            self.original_quantity
        );
        self.filled_quantity = self.original_quantity.saturating_sub(remaining_quantity);
        self.remaining_quantity = remaining_quantity;
        self.status = if remaining_quantity == 0 {
            OrderStatus::Filled
        } else if rests {
            if self.filled_quantity > 0 {
                OrderStatus::PartiallyFilled
            } else {
                OrderStatus::New
            }
        } else {
            OrderStatus::Canceled
        };
        self.debug_assert_invariant();
    }

    /// Record a cancel. The remaining quantity comes from the book.
    pub(crate) fn mark_canceled(&mut self, canceled_quantity: u64) {
        debug_assert!(self.is_open(), "only open orders can be canceled");
        debug_assert_eq!(
            self.remaining_quantity, canceled_quantity,
            "book canceled quantity differs from the ledger's remaining quantity"
        );
        self.status = OrderStatus::Canceled;
        self.remaining_quantity = canceled_quantity;
        self.debug_assert_invariant();
    }

    /// The single projection from a record to an execution report.
    pub(crate) fn to_report(&self, order_id: OrderBookId, fills: Vec<Trade>) -> ExecutionReport {
        ExecutionReport {
            order_id,
            symbol: self.symbol.clone(),
            status: self.status,
            original_quantity: self.original_quantity,
            filled_quantity: self.filled_quantity,
            remaining_quantity: self.remaining_quantity,
            fills,
        }
    }

    /// Every mutation must preserve `filled + remaining == original`.
    fn debug_assert_invariant(&self) {
        debug_assert_eq!(
            self.filled_quantity + self.remaining_quantity,
            self.original_quantity,
            "order invariant violated: filled {} + remaining {} != original {} (status {:?})",
            self.filled_quantity,
            self.remaining_quantity,
            self.original_quantity,
            self.status
        );
    }
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
    UnknownOrder,
    InvalidTimeInForce,
    PostOnlyWouldCross,
    InsufficientLiquidity,
    StopPriceWouldTriggerImmediately,
    StopOrderNotModifiable,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ModifyRequest {
    pub price: u64,
    pub quantity: u64,
    pub timestamp: u64,
}

/// The engine's command vocabulary: every state change enters through a
/// command. For submissions, order type and time-in-force travel inside
/// `request`, so a command cannot disagree with the engine method it is sent to.
#[derive(Debug, Clone, PartialEq)]
pub enum EngineCommand {
    SubmitOrder {
        symbol: String,
        request: OrderRequest,
    },
    ModifyOrder {
        symbol: String,
        order_id: OrderBookId,
        request: ModifyRequest,
    },
    CancelOrder {
        symbol: String,
        order_id: OrderBookId,
    },
    SubmitStopOrder {
        symbol: String,
        request: StopOrderRequest,
    },
}

/// What [`crate::engine::MatchingEngine::execute`] did with a command.
#[derive(Debug, Clone, PartialEq)]
pub enum CommandOutcome {
    Accepted(ExecutionReport),
    Canceled(ExecutionReport),
    Rejected(EngineError),
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
    StopTriggered {
        symbol: String,
        sequence_id: u64,
        order_id: OrderBookId,
        stop_price: u64,
        last_price: u64,   // 触发瞬间的成交价
    },
}

impl EngineEvent {
    pub fn sequence_id(&self) -> u64 {
        match self {
            EngineEvent::OrderAccepted { sequence_id, .. } => *sequence_id,
            EngineEvent::TradeExecuted { sequence_id, .. } => *sequence_id,
            EngineEvent::OrderCanceled { sequence_id, .. } => *sequence_id,
            EngineEvent::OrderRejected { sequence_id, .. } => *sequence_id,
            EngineEvent::StopTriggered { sequence_id, .. } => *sequence_id,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopKind {
    Market,
    Limit { price: u64 },   // 触发后转换成的限价
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StopOrderRequest {
    pub side: Side,
    pub stop_price: u64,
    pub order_type: StopKind,
    pub quantity: u64,
    pub timestamp: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StopOrder {
    pub id: OrderBookId,
    pub side: Side,
    pub stop_price: u64,
    pub order_type: StopKind,
    pub quantity: u64,
    pub timestamp: u64,
}
