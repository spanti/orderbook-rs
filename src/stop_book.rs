//! Pending stop orders for one symbol.
//!
//! The stop book owns the trigger-price indexes and the order map. Every
//! mutation keeps the two in sync, so the engine only ever sees whole stop
//! orders.

use std::cmp::Reverse;
use std::collections::{BTreeSet, HashMap};

use crate::types::{OrderBookId, Side, StopOrder};

/// Pending stop orders for one symbol, indexed by trigger price.
///
/// A buy stop triggers when the last price reaches its stop price or lower, so
/// buys are visited by ascending stop price. A sell stop triggers when the
/// last price reaches its stop price or higher, so sells are visited by
/// descending stop price. Orders at the same stop price break by ascending
/// order id. Storing the sell side under `Reverse(stop_price)` lets both
/// indexes share one ascending scan.
#[derive(Debug, Clone, Default)]
pub(crate) struct StopBook {
    buy: BTreeSet<(u64, OrderBookId)>, // (stop_price, order_id)
    sell: BTreeSet<(Reverse<u64>, OrderBookId)>, // (Reverse(stop_price), order_id)
    orders: HashMap<OrderBookId, StopOrder>,
}

impl StopBook {
    pub(crate) fn insert(&mut self, stop: StopOrder) {
        match stop.side {
            Side::Bid => {
                self.buy.insert((stop.stop_price, stop.id));
            }
            Side::Ask => {
                self.sell.insert((Reverse(stop.stop_price), stop.id));
            }
        }
        self.orders.insert(stop.id, stop);
    }

    /// Remove a pending stop, keeping both indexes and the order map in sync.
    pub(crate) fn remove(&mut self, order_id: OrderBookId) -> Option<StopOrder> {
        let stop = self.orders.remove(&order_id)?;
        let removed = match stop.side {
            Side::Bid => self.buy.remove(&(stop.stop_price, order_id)),
            Side::Ask => self.sell.remove(&(Reverse(stop.stop_price), order_id)),
        };
        debug_assert!(removed, "stop index missing entry for {order_id}");
        Some(stop)
    }

    pub(crate) fn get(&self, order_id: OrderBookId) -> Option<&StopOrder> {
        self.orders.get(&order_id)
    }

    /// The next stop that a trade at `last_price` should trigger.
    ///
    /// Buy stops are eligible at or below `last_price`, sell stops at or above
    /// it. When both sides have a candidate, the smaller order id wins.
    pub(crate) fn next_triggered(&self, last_price: u64) -> Option<OrderBookId> {
        // 买止损：stop_price <= last，价格升序 + id 升序
        let buy = self
            .buy
            .range(..=(last_price, OrderBookId::MAX))
            .next()
            .map(|&(_, id)| id);

        // 卖止损：stop_price >= last，即 Reverse(stop_price) <= Reverse(last)
        // 升序遍历 = 价格降序 + id 升序
        let sell = self
            .sell
            .range(..=(Reverse(last_price), OrderBookId::MAX))
            .next()
            .map(|&(_, id)| id);

        match (buy, sell) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    /// Pending stops on `side`: bids by ascending stop price, asks by
    /// descending stop price; ties break by ascending order id.
    pub(crate) fn iter(&self, side: Side) -> impl Iterator<Item = &StopOrder> + '_ {
        let ids: Box<dyn Iterator<Item = OrderBookId> + '_> = match side {
            Side::Bid => Box::new(self.buy.iter().map(|&(_, id)| id)),
            Side::Ask => Box::new(self.sell.iter().map(|&(_, id)| id)),
        };
        ids.filter_map(|id| self.orders.get(&id))
    }

    /// All pending stops sorted by `(stop_price, side_rank, id)`, where a bid
    /// ranks before an ask at the same stop price. Deterministic snapshot order.
    pub(crate) fn export_stops(&self) -> Vec<StopOrder> {
        let mut stops: Vec<StopOrder> = self.orders.values().cloned().collect();
        stops.sort_by_key(|stop| (stop.stop_price, side_rank(stop.side), stop.id));
        stops
    }

    /// Rebuild a stop book from exported stops, reinserting both price indexes
    /// and the order map without running any trigger logic.
    pub(crate) fn from_stops(stops: Vec<StopOrder>) -> Self {
        let mut book = Self::default();
        for stop in stops {
            book.insert(stop);
        }
        book
    }
}

/// Deterministic rank for snapshot ordering: bids before asks.
fn side_rank(side: Side) -> u8 {
    match side {
        Side::Bid => 0,
        Side::Ask => 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::StopKind;

    fn stop(id: OrderBookId, side: Side, stop_price: u64) -> StopOrder {
        StopOrder {
            id,
            side,
            stop_price,
            order_type: StopKind::Market,
            quantity: 1,
            timestamp: id,
        }
    }

    fn ids(book: &StopBook, side: Side) -> Vec<OrderBookId> {
        book.iter(side).map(|stop| stop.id).collect()
    }

    #[test]
    fn buy_side_is_ascending_stop_price_then_ascending_id() {
        let mut book = StopBook::default();
        book.insert(stop(9, Side::Bid, 105));
        book.insert(stop(7, Side::Bid, 100));
        book.insert(stop(3, Side::Bid, 100));

        assert_eq!(ids(&book, Side::Bid), vec![3, 7, 9]);
    }

    #[test]
    fn sell_side_is_descending_stop_price_then_ascending_id() {
        let mut book = StopBook::default();
        book.insert(stop(4, Side::Ask, 100));
        book.insert(stop(2, Side::Ask, 110));
        book.insert(stop(8, Side::Ask, 100));

        assert_eq!(ids(&book, Side::Ask), vec![2, 4, 8]);
    }

    #[test]
    fn next_triggered_uses_inclusive_price_boundaries() {
        let mut book = StopBook::default();
        book.insert(stop(1, Side::Bid, 100));
        assert_eq!(book.next_triggered(99), None);
        assert_eq!(book.next_triggered(100), Some(1));
        assert_eq!(book.next_triggered(101), Some(1));

        let mut book = StopBook::default();
        book.insert(stop(2, Side::Ask, 100));
        assert_eq!(book.next_triggered(101), None);
        assert_eq!(book.next_triggered(100), Some(2));
        assert_eq!(book.next_triggered(99), Some(2));
    }

    #[test]
    fn next_triggered_picks_the_smaller_id_across_sides() {
        let mut book = StopBook::default();
        book.insert(stop(5, Side::Bid, 100));
        book.insert(stop(2, Side::Ask, 100));

        assert_eq!(book.next_triggered(100), Some(2));
    }

    #[test]
    fn remove_keeps_indexes_and_order_map_in_sync() {
        let mut book = StopBook::default();
        book.insert(stop(1, Side::Bid, 100));
        book.insert(stop(2, Side::Ask, 110));
        book.insert(stop(3, Side::Bid, 101));

        let removed = book.remove(2).expect("sell stop is present");
        assert_eq!(removed.id, 2);
        assert!(book.get(2).is_none());
        assert!(book.iter(Side::Ask).next().is_none());
        assert_eq!(ids(&book, Side::Bid), vec![1, 3]);

        let removed = book.remove(1).expect("buy stop is present");
        assert_eq!(removed.id, 1);
        assert_eq!(ids(&book, Side::Bid), vec![3]);
        assert_eq!(book.next_triggered(200), Some(3));

        assert!(book.remove(1).is_none());
    }
}
