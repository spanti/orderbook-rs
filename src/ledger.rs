//! Single owner of order lifecycle records.
//!
//! The book owns placement and resting quantity. The ledger consumes the
//! fill/cancel deltas the engine reports and is the only writer of
//! [`OrderRecord`] status and remaining quantity.

use std::collections::HashMap;

use crate::types::{OrderBookId, OrderRecord};

/// Status and quantity record for every order the engine has accepted.
#[derive(Debug, Default)]
pub(crate) struct OrderLedger {
    records: HashMap<OrderBookId, OrderRecord>,
}

impl OrderLedger {
    /// Insert the record of a freshly matched taker order and derive its status.
    pub(crate) fn insert_taker(
        &mut self,
        order_id: OrderBookId,
        symbol: String,
        original_quantity: u64,
        remaining_quantity: u64,
        rests: bool,
    ) -> &OrderRecord {
        let mut record = OrderRecord::new(symbol, original_quantity);
        record.complete_taker(remaining_quantity, rests);
        self.insert(order_id, record);
        self.records
            .get(&order_id)
            .expect("taker record was just inserted")
    }

    fn insert(&mut self, order_id: OrderBookId, record: OrderRecord) {
        debug_assert!(
            !self.records.contains_key(&order_id),
            "duplicate order id {order_id} in the ledger"
        );
        self.records.insert(order_id, record);
    }

    /// Apply a maker fill delta reported by the book.
    pub(crate) fn apply_maker_fill(&mut self, order_id: OrderBookId, quantity: u64) {
        if let Some(record) = self.records.get_mut(&order_id) {
            record.apply_maker_fill(quantity);
        }
    }

    /// Mark an open order canceled with the remaining quantity taken from the book.
    pub(crate) fn mark_canceled(
        &mut self,
        order_id: OrderBookId,
        canceled_quantity: u64,
    ) -> Option<&OrderRecord> {
        let record = self.records.get_mut(&order_id)?;
        record.mark_canceled(canceled_quantity);
        Some(record)
    }

    pub(crate) fn get(&self, order_id: OrderBookId) -> Option<&OrderRecord> {
        self.records.get(&order_id)
    }

    pub(crate) fn is_open(&self, order_id: OrderBookId) -> bool {
        self.get(order_id).is_some_and(OrderRecord::is_open)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (OrderBookId, &OrderRecord)> {
        self.records
            .iter()
            .map(|(&order_id, record)| (order_id, record))
    }

    pub(crate) fn len(&self) -> usize {
        self.records.len()
    }
}
