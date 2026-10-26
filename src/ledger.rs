//! Single owner of order lifecycle records.
//!
//! The book owns placement and resting quantity. The ledger consumes the
//! fill/cancel deltas the engine reports and is the only writer of
//! [`OrderRecord`] status and remaining quantity.

use std::collections::HashMap;

use crate::types::{OrderBookId, OrderRecord, OrderStatus};

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

    pub(crate) fn insert_pending_stop(
        &mut self,
        order_id: OrderBookId,
        symbol: String,
        quantity: u64,
    ) -> &OrderRecord {
        let mut record = OrderRecord::new(symbol, quantity);
        record.status = OrderStatus::PendingTrigger;
        self.insert(order_id, record);
        self.records
            .get(&order_id)
            .expect("pending stop record was just inserted")
    }

    /// Resume a triggered stop's pending record and record its match result in
    /// one step. Returns `None` if the record is missing.
    pub(crate) fn resume_stop(
        &mut self,
        order_id: OrderBookId,
        remaining_quantity: u64,
        rests: bool,
    ) -> Option<&OrderRecord> {
        let record = self.records.get_mut(&order_id)?;
        debug_assert_eq!(record.status, OrderStatus::PendingTrigger);
        debug_assert_eq!(record.filled_quantity, 0);
        record.complete_taker(remaining_quantity, rests);
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

    /// Every record in ascending order-id order, for deterministic snapshots.
    pub(crate) fn export_records(&self) -> Vec<(OrderBookId, OrderRecord)> {
        let mut records: Vec<(OrderBookId, OrderRecord)> = self
            .records
            .iter()
            .map(|(&order_id, record)| (order_id, record.clone()))
            .collect();
        records.sort_by_key(|(order_id, _)| *order_id);
        records
    }

    /// Rebuild the ledger from exported records without touching the book.
    pub(crate) fn from_records(records: Vec<(OrderBookId, OrderRecord)>) -> Self {
        Self {
            records: records.into_iter().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pending_ledger(order_id: OrderBookId, quantity: u64) -> OrderLedger {
        let mut ledger = OrderLedger::default();
        ledger.insert_pending_stop(order_id, "BTC-USDT".to_string(), quantity);
        ledger
    }

    fn resume(
        ledger: &mut OrderLedger,
        order_id: OrderBookId,
        remaining_quantity: u64,
        rests: bool,
    ) -> OrderRecord {
        ledger
            .resume_stop(order_id, remaining_quantity, rests)
            .expect("pending stop record is present")
            .clone()
    }

    #[test]
    fn resume_stop_marks_a_full_fill_filled() {
        let mut ledger = pending_ledger(1, 5);

        let record = resume(&mut ledger, 1, 0, false);

        assert_eq!(record.status, OrderStatus::Filled);
        assert_eq!(record.filled_quantity, 5);
        assert_eq!(record.remaining_quantity, 0);
    }

    #[test]
    fn resume_stop_with_resting_remainder_is_new() {
        let mut ledger = pending_ledger(1, 5);

        let record = resume(&mut ledger, 1, 5, true);

        assert_eq!(record.status, OrderStatus::New);
        assert_eq!(record.filled_quantity, 0);
        assert_eq!(record.remaining_quantity, 5);
    }

    #[test]
    fn resume_stop_with_discarded_remainder_is_canceled() {
        let mut ledger = pending_ledger(1, 5);

        let record = resume(&mut ledger, 1, 3, false);

        assert_eq!(record.status, OrderStatus::Canceled);
        assert_eq!(record.filled_quantity, 2);
        assert_eq!(record.remaining_quantity, 3);
    }

    #[test]
    fn resume_stop_returns_none_for_an_unknown_record() {
        let mut ledger = OrderLedger::default();

        assert!(ledger.resume_stop(42, 0, false).is_none());
    }
}
