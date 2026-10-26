//! Replayable domain events.
//!
//! A [`DomainEvent`] is a state delta produced by a command decision. It
//! carries every attribute needed to reproduce the change during replay, so
//! recovery never has to re-run matching or re-read the command. The durable
//! envelope (format version, sequence and command id) lives in
//! [`crate::durable::EventRecord`], not in the event itself.
//!
//! Ticket 02 adds matching and cancel: `TradeExecuted` and `OrderCanceled`.
//! Ticket 03 adds stops: `StopPlaced`, `StopTriggered` and `StopCanceled`.
//! Ticket 04 adds modify (`OrderCanceled{Modified}`) and `OrderRejected`.

use crate::types::{EngineCommand, EngineError, Order, OrderBookId, StopOrder, Trade};

/// Why an order's remaining quantity left the book.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum OrderCancelReason {
    /// The order's time-in-force or market type discarded the leftover.
    UnfilledResidual,
    /// The client canceled the open order.
    UserCancel,
    /// The order was canceled as the first half of a modify.
    Modified,
}

/// A replayable fact emitted by the matching engine's decision step.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum DomainEvent {
    /// The engine accepted an order and created its ledger record.
    ///
    /// This event deliberately has no book effect. That keeps every prefix of
    /// an event stream a valid engine state: a durable `OrderAccepted` without
    /// its following `OrderRested` describes an accepted order that is not yet
    /// resting.
    OrderAccepted {
        symbol: String,
        /// The full original order, before any matching.
        order: Order,
        post_only: bool,
    },
    /// The order's remaining quantity was inserted into the book with
    /// price-time priority.
    ///
    /// `order.quantity` is the remaining quantity after any fills.
    OrderRested { symbol: String, order: Order },
    /// One fill between a resting maker and the incoming taker.
    ///
    /// Applying this event reduces the maker's book and ledger, increases the
    /// taker's ledger, appends the trade and updates the last trade price. It
    /// never re-runs matching.
    TradeExecuted { trade: Trade },
    /// The order's remaining quantity left the book.
    ///
    /// `canceled_quantity` is the quantity that was removed: the resting
    /// remainder for a user cancel, or the leftover an IOC/market order could
    /// not fill.
    OrderCanceled {
        symbol: String,
        order_id: OrderBookId,
        canceled_quantity: u64,
        reason: OrderCancelReason,
    },
    /// A stop order was accepted and placed in the pending-stop book.
    ///
    /// Applying it inserts the stop and creates a `PendingTrigger` ledger
    /// record; it never touches the order book.
    StopPlaced { symbol: String, stop: StopOrder },
    /// A pending stop triggered and was converted into a working order.
    ///
    /// `order` is the fully converted order (original quantity, market+IOC or
    /// limit+GTC) and `last_price` is the trade price that triggered it.
    /// Applying this event removes the stop and activates the existing ledger
    /// record; it deliberately does not re-run matching. The following
    /// `TradeExecuted`*, `OrderRested` or `OrderCanceled` events describe the
    /// activated order's match result.
    StopTriggered {
        symbol: String,
        order: Order,
        last_price: u64,
    },
    /// A pending stop was canceled before it triggered.
    ///
    /// Applying it removes the stop and marks its ledger record canceled.
    StopCanceled {
        symbol: String,
        order_id: OrderBookId,
    },
    /// A command was rejected by business validation.
    ///
    /// This is the only event with no state effect: applying it changes
    /// nothing. It exists so rejected attempts are auditable and replayable
    /// facts. `command` is the full original command, preserved as audit
    /// payload; the event allocates no order id. Internal invariant violations
    /// never become this event; they panic.
    OrderRejected {
        symbol: String,
        reason: EngineError,
        command: EngineCommand,
    },
}

impl DomainEvent {
    /// The single order id this event refers to, if it has one.
    ///
    /// `TradeExecuted` carries both a maker and a taker id, so it deliberately
    /// returns `None` rather than picking one.
    pub fn order_id(&self) -> Option<OrderBookId> {
        match self {
            Self::OrderAccepted { order, .. }
            | Self::OrderRested { order, .. }
            | Self::StopTriggered { order, .. } => Some(order.id),
            Self::OrderCanceled { order_id, .. } | Self::StopCanceled { order_id, .. } => {
                Some(*order_id)
            }
            Self::StopPlaced { stop, .. } => Some(stop.id),
            // A rejection is audit-only: even when the rejected command names
            // an order, the event allocates and changes no order id.
            Self::TradeExecuted { .. } | Self::OrderRejected { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{EngineCommand, EngineError, OrderType, Side, StopKind, TimeInForce};

    fn order() -> Order {
        Order {
            id: 7,
            side: Side::Bid,
            price: 100,
            quantity: 5,
            timestamp: 11,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::GTC,
        }
    }

    fn stop() -> StopOrder {
        StopOrder {
            id: 9,
            side: Side::Ask,
            stop_price: 95,
            order_type: StopKind::Limit { price: 94 },
            quantity: 3,
            timestamp: 12,
        }
    }

    #[test]
    fn domain_events_round_trip_through_json() {
        for event in [
            DomainEvent::OrderAccepted {
                symbol: "BTC-USDT".to_string(),
                order: order(),
                post_only: true,
            },
            DomainEvent::OrderRested {
                symbol: "BTC-USDT".to_string(),
                order: order(),
            },
            DomainEvent::TradeExecuted {
                trade: Trade {
                    id: 3,
                    symbol: "BTC-USDT".to_string(),
                    maker_order_id: 7,
                    taker_order_id: 8,
                    price: 100,
                    quantity: 2,
                    taker_side: Side::Ask,
                    timestamp: 12,
                },
            },
            DomainEvent::OrderCanceled {
                symbol: "BTC-USDT".to_string(),
                order_id: 7,
                canceled_quantity: 3,
                reason: OrderCancelReason::UnfilledResidual,
            },
            DomainEvent::OrderCanceled {
                symbol: "BTC-USDT".to_string(),
                order_id: 7,
                canceled_quantity: 3,
                reason: OrderCancelReason::UserCancel,
            },
            DomainEvent::OrderCanceled {
                symbol: "BTC-USDT".to_string(),
                order_id: 7,
                canceled_quantity: 3,
                reason: OrderCancelReason::Modified,
            },
            DomainEvent::StopPlaced {
                symbol: "BTC-USDT".to_string(),
                stop: stop(),
            },
            DomainEvent::StopTriggered {
                symbol: "BTC-USDT".to_string(),
                order: order(),
                last_price: 95,
            },
            DomainEvent::StopCanceled {
                symbol: "BTC-USDT".to_string(),
                order_id: 7,
            },
            DomainEvent::OrderRejected {
                symbol: "BTC-USDT".to_string(),
                reason: EngineError::UnknownOrder,
                command: EngineCommand::CancelOrder {
                    symbol: "BTC-USDT".to_string(),
                    order_id: 7,
                },
            },
        ] {
            let json = serde_json::to_string(&event).expect("event must serialize");
            let decoded: DomainEvent =
                serde_json::from_str(&json).expect("event JSON must round-trip");
            assert_eq!(decoded, event);
        }
    }
}
