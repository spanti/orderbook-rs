//! Replayable domain events.
//!
//! A [`DomainEvent`] is a state delta produced by a command decision. It
//! carries every attribute needed to reproduce the change during replay, so
//! recovery never has to re-run matching or re-read the command. The durable
//! envelope (format version, sequence and command id) lives in
//! [`crate::durable::EventRecord`], not in the event itself.
//!
//! Ticket 01 implements only the resting-submit tracer: `OrderAccepted` then
//! `OrderRested`. Later tickets add trades, cancels, stops and rejections.

use crate::types::{Order, OrderBookId};

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
}

impl DomainEvent {
    /// The order id this event refers to, if it carries one.
    pub fn order_id(&self) -> Option<OrderBookId> {
        match self {
            Self::OrderAccepted { order, .. } | Self::OrderRested { order, .. } => Some(order.id),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{OrderType, Side, TimeInForce};

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
        ] {
            let json = serde_json::to_string(&event).expect("event must serialize");
            let decoded: DomainEvent =
                serde_json::from_str(&json).expect("event JSON must round-trip");
            assert_eq!(decoded, event);
        }
    }
}
