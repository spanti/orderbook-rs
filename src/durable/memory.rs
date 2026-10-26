//! In-memory reference adapters for the durable storage traits.
//!
//! Both types are cheap `Clone` handles over `Arc<Mutex<..>>`, so tests can
//! simulate a process crash by dropping a [`DurableMatchingEngine`] and opening
//! a new one from a second handle to the same state.
//!
//! [`DurableMatchingEngine`]: crate::durable::DurableMatchingEngine

use std::sync::{Arc, Mutex, MutexGuard};

use crate::durable::{
    CommandLog, CommandRecord, DurableError, EventLog, EventRecord, LogHeader, SnapshotStore,
    StoredSnapshot,
};

/// Lock a mutex, recovering the guard if a previous holder panicked.
///
/// A poisoned in-memory adapter must not panic public trait methods: its state
/// is still structurally valid, so callers keep seeing normal `Result` values.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Debug, Default)]
struct LogState {
    header: Option<LogHeader>,
    records: Vec<CommandRecord>,
}

/// In-memory [`CommandLog`] shared by every clone of a handle.
#[derive(Debug, Clone, Default)]
pub struct InMemoryCommandLog {
    state: Arc<Mutex<LogState>>,
}

impl InMemoryCommandLog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Another handle to the same log, used to reopen after a simulated crash.
    pub fn handle(&self) -> Self {
        self.clone()
    }
}

impl CommandLog for InMemoryCommandLog {
    fn read_header(&self) -> Result<Option<LogHeader>, DurableError> {
        Ok(lock(&self.state).header.clone())
    }

    fn write_header(&mut self, header: LogHeader) -> Result<(), DurableError> {
        let mut state = lock(&self.state);
        if state.header.is_some() {
            return Err(DurableError::HeaderAlreadyPresent);
        }
        state.header = Some(header);
        Ok(())
    }

    fn append(&mut self, record: CommandRecord) -> Result<(), DurableError> {
        let mut state = lock(&self.state);
        if state.header.is_none() {
            return Err(DurableError::MissingHeader);
        }

        let expected = state
            .records
            .last()
            .map(|last| last.sequence.saturating_add(1))
            .unwrap_or(1);
        if record.sequence != expected {
            return Err(DurableError::NonContiguousSequence {
                expected,
                found: record.sequence,
            });
        }

        state.records.push(record);
        Ok(())
    }

    fn read_from(&self, from: u64) -> Result<Vec<CommandRecord>, DurableError> {
        let state = lock(&self.state);
        Ok(state
            .records
            .iter()
            .filter(|record| record.sequence >= from)
            .cloned()
            .collect())
    }

    fn first_sequence(&self) -> Option<u64> {
        lock(&self.state)
            .records
            .first()
            .map(|record| record.sequence)
    }

    fn last_sequence(&self) -> Option<u64> {
        lock(&self.state)
            .records
            .last()
            .map(|record| record.sequence)
    }
}

#[derive(Debug, Default)]
struct EventLogState {
    header: Option<LogHeader>,
    records: Vec<EventRecord>,
}

/// In-memory [`EventLog`] shared by every clone of a handle.
///
/// This is the ticket-01 event-log adapter: fast recovery tests can drop an
/// [`EventSourcedEngine`](crate::durable::EventSourcedEngine) and reopen from a
/// second handle to the same log.
#[derive(Debug, Clone, Default)]
pub struct InMemoryEventLog {
    state: Arc<Mutex<EventLogState>>,
}

impl InMemoryEventLog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Another handle to the same log, used to reopen after a simulated crash.
    pub fn handle(&self) -> Self {
        self.clone()
    }
}

impl EventLog for InMemoryEventLog {
    fn read_header(&self) -> Result<Option<LogHeader>, DurableError> {
        Ok(lock(&self.state).header.clone())
    }

    fn write_header(&mut self, header: LogHeader) -> Result<(), DurableError> {
        let mut state = lock(&self.state);
        if state.header.is_some() {
            return Err(DurableError::HeaderAlreadyPresent);
        }
        state.header = Some(header);
        Ok(())
    }

    fn append(&mut self, record: EventRecord) -> Result<(), DurableError> {
        let mut state = lock(&self.state);
        if state.header.is_none() {
            return Err(DurableError::MissingHeader);
        }

        let expected = state
            .records
            .last()
            .map(|last| last.sequence.saturating_add(1))
            .unwrap_or(1);
        if record.sequence != expected {
            return Err(DurableError::NonContiguousSequence {
                expected,
                found: record.sequence,
            });
        }

        state.records.push(record);
        Ok(())
    }

    fn append_batch(&mut self, records: Vec<EventRecord>) -> Result<(), DurableError> {
        let mut state = lock(&self.state);
        if state.header.is_none() {
            return Err(DurableError::MissingHeader);
        }

        // Validate the whole batch before mutating, so a gap cannot leave a
        // partial batch visible.
        let mut expected = state
            .records
            .last()
            .map(|last| last.sequence.saturating_add(1))
            .unwrap_or(1);
        for record in &records {
            if record.sequence != expected {
                return Err(DurableError::NonContiguousSequence {
                    expected,
                    found: record.sequence,
                });
            }
            expected = expected.saturating_add(1);
        }

        state.records.extend(records);
        Ok(())
    }

    fn read_from(&self, from: u64) -> Result<Vec<EventRecord>, DurableError> {
        let state = lock(&self.state);
        Ok(state
            .records
            .iter()
            .filter(|record| record.sequence >= from)
            .cloned()
            .collect())
    }

    fn first_sequence(&self) -> Option<u64> {
        lock(&self.state)
            .records
            .first()
            .map(|record| record.sequence)
    }

    fn last_sequence(&self) -> Option<u64> {
        lock(&self.state)
            .records
            .last()
            .map(|record| record.sequence)
    }

    fn last_command_id(&self) -> Option<u64> {
        lock(&self.state)
            .records
            .last()
            .map(|record| record.command_id)
    }
}

/// In-memory [`SnapshotStore`] shared by every clone of a handle.
#[derive(Debug, Clone, Default)]
pub struct InMemorySnapshotStore {
    snapshots: Arc<Mutex<Vec<StoredSnapshot>>>,
}

impl InMemorySnapshotStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Another handle to the same store, used to reopen after a simulated crash.
    pub fn handle(&self) -> Self {
        self.clone()
    }
}

impl SnapshotStore for InMemorySnapshotStore {
    fn save(&mut self, snapshot: StoredSnapshot) -> Result<(), DurableError> {
        let mut snapshots = lock(&self.snapshots);
        let floor = snapshots
            .iter()
            .map(|stored| stored.applied_seq)
            .max()
            .unwrap_or(0);
        if snapshot.applied_seq < floor {
            return Err(DurableError::SnapshotBelowFloor {
                applied_seq: snapshot.applied_seq,
                floor,
            });
        }

        snapshots.push(snapshot);
        // Retain the two newest complete snapshots.
        while snapshots.len() > 2 {
            snapshots.remove(0);
        }
        Ok(())
    }

    fn load_latest(&self) -> Result<Option<StoredSnapshot>, DurableError> {
        Ok(lock(&self.snapshots).last().cloned())
    }

    fn load_previous(&self) -> Result<Option<StoredSnapshot>, DurableError> {
        let snapshots = lock(&self.snapshots);
        let len = snapshots.len();
        if len < 2 {
            return Ok(None);
        }
        Ok(Some(snapshots[len - 2].clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    /// Poison `mutex` by panicking while a thread holds its guard.
    fn poison<T: Send + 'static>(mutex: &Arc<Mutex<T>>) {
        let handle = Arc::clone(mutex);
        thread::spawn(move || {
            let _guard = handle.lock().unwrap();
            panic!("poison the mutex");
        })
        .join()
        .expect_err("the poisoning thread must panic");
        assert!(mutex.is_poisoned());
    }

    #[test]
    fn a_poisoned_log_lock_does_not_panic_trait_methods() {
        let log = InMemoryCommandLog::new();
        poison(&log.state);

        assert_eq!(log.read_header().unwrap(), None);
        assert_eq!(log.first_sequence(), None);
        assert_eq!(log.last_sequence(), None);
        assert!(log.read_from(1).unwrap().is_empty());
    }

    #[test]
    fn a_poisoned_snapshot_lock_does_not_panic_trait_methods() {
        let snapshots = InMemorySnapshotStore::new();
        poison(&snapshots.snapshots);

        assert_eq!(snapshots.load_latest().unwrap(), None);
        assert_eq!(snapshots.load_previous().unwrap(), None);
    }
}
