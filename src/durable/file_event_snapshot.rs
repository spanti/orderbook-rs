//! A file-backed [`EventSnapshotStore`] using the shared v2 snapshot layout.
//!
//! Snapshots are stored as `snapshot-{applied_seq:020}.json`, one
//! `{crc32:08x} {json}\n` line each. Saves are atomic (temporary file, fsync,
//! rename, best-effort directory sync) and only the two newest files are
//! retained. Loading falls back to the previous complete snapshot when the
//! newest file is corrupt.

use std::path::Path;

use crate::durable::DurableError;
use crate::durable::event_snapshot::{EventSnapshot, EventSnapshotStore};
use crate::durable::snapshot_files::SnapshotDirectory;

/// An [`EventSnapshotStore`] persisted as one checksummed JSON file per
/// snapshot.
#[derive(Debug)]
pub struct FileEventSnapshotStore {
    inner: SnapshotDirectory<EventSnapshot>,
}

impl FileEventSnapshotStore {
    /// Open the snapshot directory, creating it (and any parents) when absent.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, DurableError> {
        Ok(Self {
            inner: SnapshotDirectory::open(dir)?,
        })
    }

    /// The directory snapshots are stored in.
    pub fn dir(&self) -> &Path {
        self.inner.dir()
    }
}

impl EventSnapshotStore for FileEventSnapshotStore {
    fn save(&mut self, snapshot: EventSnapshot) -> Result<(), DurableError> {
        self.inner.save(snapshot.applied_seq, &snapshot)
    }

    fn load_latest(&self) -> Result<Option<EventSnapshot>, DurableError> {
        self.inner.load_index(0)
    }

    fn load_previous(&self) -> Result<Option<EventSnapshot>, DurableError> {
        self.inner.load_index(1)
    }
}
