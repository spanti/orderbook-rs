//! In-memory reference adapters for the durable storage traits.
//!
//! Both types are cheap `Clone` handles over `Arc<Mutex<..>>`, so tests can
//! simulate a process crash by dropping a [`DurableMatchingEngine`] and opening
//! a new one from a second handle to the same state.
//!
//! [`DurableMatchingEngine`]: crate::durable::DurableMatchingEngine

use std::sync::{Arc, Mutex};

use crate::durable::{
    CommandLog, CommandRecord, DurableError, LogHeader, SnapshotStore, StoredSnapshot,
};

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
        Ok(self.state.lock().unwrap().header.clone())
    }

    fn write_header(&mut self, header: LogHeader) -> Result<(), DurableError> {
        let mut state = self.state.lock().unwrap();
        if state.header.is_some() {
            return Err(DurableError::HeaderAlreadyPresent);
        }
        state.header = Some(header);
        Ok(())
    }

    fn append(&mut self, record: CommandRecord) -> Result<(), DurableError> {
        let mut state = self.state.lock().unwrap();
        if state.header.is_none() {
            return Err(DurableError::MissingHeader);
        }

        let expected = state
            .records
            .last()
            .map(|last| last.sequence + 1)
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
        let state = self.state.lock().unwrap();
        Ok(state
            .records
            .iter()
            .filter(|record| record.sequence >= from)
            .cloned()
            .collect())
    }

    fn first_sequence(&self) -> Option<u64> {
        self.state
            .lock()
            .unwrap()
            .records
            .first()
            .map(|record| record.sequence)
    }

    fn last_sequence(&self) -> Option<u64> {
        self.state
            .lock()
            .unwrap()
            .records
            .last()
            .map(|record| record.sequence)
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
        let mut snapshots = self.snapshots.lock().unwrap();
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
        Ok(self.snapshots.lock().unwrap().last().cloned())
    }

    fn load_previous(&self) -> Result<Option<StoredSnapshot>, DurableError> {
        let snapshots = self.snapshots.lock().unwrap();
        let len = snapshots.len();
        if len < 2 {
            return Ok(None);
        }
        Ok(Some(snapshots[len - 2].clone()))
    }
}
