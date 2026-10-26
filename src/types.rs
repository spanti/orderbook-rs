//! Shared data types for the order book and matching engine.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum TimeInForce {
    #[default]
    GTC,
    IOC,
    FOK,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum OrderType {
    Limit,
    Market,
}

// Unique identifier for the OrderBook system
pub type OrderBookId = u64;

// Side of an order in the OrderBook
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
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

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
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

#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum OrderStatus {
    New,             // 未成交，挂在簿上
    PartiallyFilled, // 部分成交，剩余挂在簿上
    Filled,          // 全部成交
    Canceled,        // 未成交或剩余部分被丢弃（市价单/IOC）
    PendingTrigger,  // 等待触发的止损单
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
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
        matches!(
            self.status,
            OrderStatus::New | OrderStatus::PartiallyFilled | OrderStatus::PendingTrigger
        )
    }

    /// Record a fill delta for either the maker or the taker.
    ///
    /// The ledger only tracks filled and remaining quantities, so both sides
    /// apply the same delta.
    pub(crate) fn apply_fill(&mut self, quantity: u64) {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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

#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ModifyRequest {
    pub price: u64,
    pub quantity: u64,
    pub timestamp: u64,
}

/// The engine's command vocabulary: every state change enters through a
/// command. For submissions, order type and time-in-force travel inside
/// `request`, so a command cannot disagree with the engine method it is sent to.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookSnapshot {
    pub symbol: String,
    pub sequence: u64,
    pub bids: Vec<(u64, u64)>, // (price, quantity)，从高到低
    pub asks: Vec<(u64, u64)>, // (price, quantity)，从低到高
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum StopKind {
    Market,
    Limit { price: u64 }, // 触发后转换成的限价
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StopOrderRequest {
    pub side: Side,
    pub stop_price: u64,
    pub order_type: StopKind,
    pub quantity: u64,
    pub timestamp: u64,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StopOrder {
    pub id: OrderBookId,
    pub side: Side,
    pub stop_price: u64,
    pub order_type: StopKind,
    pub quantity: u64,
    pub timestamp: u64,
}

#[cfg(test)]
mod serialization_tests {
    use super::*;
    use serde::Serialize;
    use serde::de::DeserializeOwned;

    fn assert_round_trip<T>(value: T)
    where
        T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        let json = serde_json::to_string(&value).expect("value should serialize to JSON");
        let decoded: T = serde_json::from_str(&json).expect("serialized value should decode");
        assert_eq!(value, decoded, "JSON round trip must preserve equality");
    }

    fn sample_order_request() -> OrderRequest {
        OrderRequest {
            side: Side::Bid,
            price: 10_000,
            quantity: 25,
            timestamp: 1_700_000_000_000,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::GTC,
            post_only: true,
        }
    }

    #[test]
    fn side_round_trips() {
        for side in [Side::Bid, Side::Ask] {
            assert_round_trip(side);
        }
    }

    #[test]
    fn order_type_round_trips() {
        for order_type in [OrderType::Limit, OrderType::Market] {
            assert_round_trip(order_type);
        }
    }

    #[test]
    fn time_in_force_round_trips() {
        for time_in_force in [TimeInForce::GTC, TimeInForce::IOC, TimeInForce::FOK] {
            assert_round_trip(time_in_force);
        }
    }

    #[test]
    fn stop_kind_round_trips() {
        for stop_kind in [StopKind::Market, StopKind::Limit { price: 10_500 }] {
            assert_round_trip(stop_kind);
        }
    }

    #[test]
    fn order_round_trips() {
        assert_round_trip(Order {
            id: 42,
            side: Side::Ask,
            price: 10_500,
            quantity: 7,
            timestamp: 1_700_000_000_123,
            order_type: OrderType::Market,
            time_in_force: TimeInForce::IOC,
        });
    }

    #[test]
    fn order_status_round_trips() {
        for status in [
            OrderStatus::New,
            OrderStatus::PartiallyFilled,
            OrderStatus::Filled,
            OrderStatus::Canceled,
            OrderStatus::PendingTrigger,
        ] {
            assert_round_trip(status);
        }
    }

    #[test]
    fn order_record_round_trips() {
        assert_round_trip(OrderRecord {
            symbol: "BTCUSDT".to_string(),
            status: OrderStatus::PartiallyFilled,
            original_quantity: 10,
            filled_quantity: 4,
            remaining_quantity: 6,
        });
    }

    #[test]
    fn trade_round_trips() {
        assert_round_trip(Trade {
            id: 9,
            symbol: "ETHUSDT".to_string(),
            maker_order_id: 3,
            taker_order_id: 8,
            price: 2_000,
            quantity: 5,
            taker_side: Side::Bid,
            timestamp: 1_700_000_000_456,
        });
    }

    #[test]
    fn stop_order_round_trips() {
        assert_round_trip(StopOrder {
            id: 11,
            side: Side::Ask,
            stop_price: 1_900,
            order_type: StopKind::Limit { price: 1_890 },
            quantity: 15,
            timestamp: 1_700_000_000_789,
        });
    }

    #[test]
    fn engine_config_round_trips_and_compares_equal() {
        let config = EngineConfig {
            min_order_quantity: 2,
            max_order_quantity: 1_000,
            tick_size: 5,
        };
        assert_round_trip(config);
        assert_eq!(config, config);
    }

    #[test]
    fn request_payloads_round_trip() {
        assert_round_trip(sample_order_request());
        assert_round_trip(ModifyRequest {
            price: 10_100,
            quantity: 12,
            timestamp: 1_700_000_000_111,
        });
        assert_round_trip(StopOrderRequest {
            side: Side::Ask,
            stop_price: 9_800,
            order_type: StopKind::Market,
            quantity: 30,
            timestamp: 1_700_000_000_222,
        });
    }

    #[test]
    fn engine_command_round_trips() {
        assert_round_trip(EngineCommand::SubmitOrder {
            symbol: "BTCUSDT".to_string(),
            request: sample_order_request(),
        });
        assert_round_trip(EngineCommand::ModifyOrder {
            symbol: "BTCUSDT".to_string(),
            order_id: 42,
            request: ModifyRequest {
                price: 10_200,
                quantity: 20,
                timestamp: 1_700_000_000_333,
            },
        });
        assert_round_trip(EngineCommand::CancelOrder {
            symbol: "ETHUSDT".to_string(),
            order_id: 7,
        });
        assert_round_trip(EngineCommand::SubmitStopOrder {
            symbol: "ETHUSDT".to_string(),
            request: StopOrderRequest {
                side: Side::Bid,
                stop_price: 2_100,
                order_type: StopKind::Limit { price: 2_110 },
                quantity: 5,
                timestamp: 1_700_000_000_444,
            },
        });
    }

    #[test]
    fn rejected_command_payload_round_trips_and_keeps_its_shape() {
        // The engine rejects this command (quantity below the configured
        // minimum), but rejected commands are still appended to the log, so
        // their payload must round-trip like any accepted one.
        let command = EngineCommand::SubmitOrder {
            symbol: "BTCUSDT".to_string(),
            request: OrderRequest {
                side: Side::Bid,
                price: 10_000,
                quantity: 0,
                timestamp: 1_700_000_000_000,
                order_type: OrderType::Limit,
                time_in_force: TimeInForce::GTC,
                post_only: false,
            },
        };

        let mut engine = crate::engine::MatchingEngine::default();
        assert_eq!(
            engine.execute(command.clone()),
            vec![crate::event::DomainEvent::OrderRejected {
                symbol: "BTCUSDT".to_string(),
                reason: EngineError::QuantityOutOfRange,
                command: command.clone(),
            }]
        );

        let json = serde_json::to_value(&command).expect("rejected command must serialize");
        assert_eq!(json["SubmitOrder"]["symbol"], "BTCUSDT");
        assert_eq!(json["SubmitOrder"]["request"]["quantity"], 0);
        assert_eq!(json["SubmitOrder"]["request"]["side"], "Bid");
        assert_eq!(json["SubmitOrder"]["request"]["order_type"], "Limit");

        assert_round_trip(command);
    }

    #[test]
    fn malformed_json_returns_an_error_instead_of_panicking() {
        assert!(serde_json::from_str::<EngineCommand>(r#"{"SubmitOrder":"#).is_err());
        assert!(serde_json::from_str::<Trade>(r#"{"id":"not-a-number"}"#).is_err());
        assert!(serde_json::from_str::<EngineConfig>("null").is_err());
    }

    #[test]
    fn unknown_enum_values_are_errors() {
        assert!(serde_json::from_str::<Side>(r#""Buy""#).is_err());
        assert!(serde_json::from_str::<OrderType>(r#""Stop""#).is_err());
        assert!(serde_json::from_str::<TimeInForce>(r#""Day""#).is_err());
        assert!(serde_json::from_str::<StopKind>(r#""TrailingStop""#).is_err());
        assert!(serde_json::from_str::<OrderStatus>(r#""Expired""#).is_err());
        assert!(
            serde_json::from_str::<EngineCommand>(r#"{"CancelAll":{"symbol":"BTCUSDT"}}"#).is_err()
        );
    }
}
