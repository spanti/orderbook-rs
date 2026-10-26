//! A file-backed [`SnapshotStore`] that writes snapshots atomically and keeps
//! the two most recent for fallback.
//!
//! Each snapshot lives in its own file named `snapshot-{applied_seq:020}.json`.
//! The file holds a single line, `{crc32:08x} {json}\n`, where the CRC-32 covers
//! the JSON bytes of a [`StoredSnapshot`] (the same envelope convention as
//! [`FileCommandLog`](crate::durable::FileCommandLog)).
//!
//! The directory mechanics are shared with the event-sourced
//! [`FileEventSnapshotStore`](crate::durable::FileEventSnapshotStore) so the two
//! adapters cannot drift. Saving writes a temporary file, fsyncs it, renames it
//! over the target, best-effort fsyncs the directory, and only then prunes
//! older snapshots. Loading sorts candidates newest-first and skips a file that
//! fails its checksum or JSON decoding; a format-version mismatch is
//! deliberately *not* skipped, because the durable layer validates the version.

use std::path::Path;

use crate::durable::snapshot_files::SnapshotDirectory;
use crate::durable::{DurableError, SnapshotStore, StoredSnapshot};

/// A [`SnapshotStore`] persisted as one checksummed JSON file per snapshot.
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
