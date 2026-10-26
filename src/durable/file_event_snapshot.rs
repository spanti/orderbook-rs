//! A file-backed [`SnapshotStore`] using the shared v2 snapshot layout.
//!
//! Snapshots are stored as `snapshot-{applied_seq:020}.json`, one
//! `{crc32:08x} {json}\n` line each. Saves are atomic (temporary file, fsync,
//! rename, best-effort directory sync) and only the two newest files are
//! retained. Loading falls back to the previous complete snapshot when the
//! newest file is corrupt.

use std::path::Path;

use crate::durable::DurableError;
use crate::durable::event_snapshot::{SnapshotStore, StoredSnapshot};
use crate::durable::snapshot_files::SnapshotDirectory;

/// An [`SnapshotStore`] persisted as one checksummed JSON file per
/// snapshot.
#[derive(Debug)]
pub struct FileSnapshotStore {
    inner: SnapshotDirectory<StoredSnapshot>,
}

impl FileSnapshotStore {
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

impl SnapshotStore for FileSnapshotStore {
    fn save(&mut self, snapshot: StoredSnapshot) -> Result<(), DurableError> {
        self.inner.save(snapshot.applied_seq, &snapshot)
    }

    fn load_latest(&self) -> Result<Option<StoredSnapshot>, DurableError> {
        self.inner.load_index(0)
    }

    fn load_previous(&self) -> Result<Option<StoredSnapshot>, DurableError> {
        self.inner.load_index(1)
    }
}
