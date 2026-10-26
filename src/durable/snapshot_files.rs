//! Shared implementation of the file-backed snapshot store.
//!
//! [`StoredSnapshot`](crate::durable::StoredSnapshot) is written with one file
//! per snapshot named `snapshot-{applied_seq:020}.json`, containing a single
//! `{crc32:08x} {json}\n` line. This module owns the directory scanning,
//! atomic replace, retention and corruption fallback.
//!
//! Saving writes a temporary file in the same directory, flushes and fsyncs it,
//! renames it over the target, best-effort fsyncs the directory, and only then
//! prunes older snapshots. Loading sorts candidate files newest-first and skips
//! any file that fails its checksum or JSON decoding; a format-version mismatch
//! is deliberately *not* skipped, because the durable layer validates the
//! version and must fail loudly on incompatible data.

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::durable::DurableError;
use crate::durable::framing::{decode_line, encode_line};

/// How many snapshot files are retained after a successful save.
const RETAINED_SNAPSHOTS: usize = 2;

const FILE_PREFIX: &str = "snapshot-";
const FILE_SUFFIX: &str = ".json";
/// Width of the zero-padded applied sequence in file names.
const SEQUENCE_WIDTH: usize = 20;

/// One directory of checksummed snapshot files holding records of type `T`.
pub(crate) struct SnapshotDirectory<T> {
    dir: PathBuf,
    // `fn() -> T` keeps the marker independent of `T`'s ownership traits.
    marker: PhantomData<fn() -> T>,
}

impl<T> fmt::Debug for SnapshotDirectory<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SnapshotDirectory")
            .field("dir", &self.dir)
            .finish()
    }
}

impl<T: Serialize + DeserializeOwned> SnapshotDirectory<T> {
    /// Open the snapshot directory, creating it (and any parents) when absent.
    pub(crate) fn open(dir: impl AsRef<Path>) -> Result<Self, DurableError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir).map_err(|error| {
            DurableError::Storage(format!(
                "cannot create snapshot directory {}: {error}",
                dir.display()
            ))
        })?;
        Ok(Self {
            dir,
            marker: PhantomData,
        })
    }

    /// The directory snapshots are stored in.
    pub(crate) fn dir(&self) -> &Path {
        &self.dir
    }

    /// Store one snapshot under `applied_seq`, refusing a sequence below the
    /// current floor. An equal sequence is allowed.
    pub(crate) fn save(&self, applied_seq: u64, snapshot: &T) -> Result<(), DurableError> {
        let floor = self.floor()?;
        if applied_seq < floor {
            return Err(DurableError::SnapshotBelowFloor { applied_seq, floor });
        }

        let line = encode_line(snapshot).map_err(|error| match error {
            DurableError::Storage(detail) => {
                DurableError::Storage(format!("cannot encode snapshot: {detail}"))
            }
            other => other,
        })?;

        let target = self.snapshot_path(applied_seq);
        let temp = self.temp_path(applied_seq);

        let written = (|| -> std::io::Result<()> {
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&temp)?;
            file.write_all(line.as_bytes())?;
            file.flush()?;
            file.sync_all()?;
            Ok(())
        })();
        if let Err(error) = written {
            let _ = fs::remove_file(&temp);
            return Err(DurableError::Storage(format!(
                "cannot write temporary snapshot {}: {error}",
                temp.display()
            )));
        }

        if let Err(error) = fs::rename(&temp, &target) {
            let _ = fs::remove_file(&temp);
            return Err(DurableError::Storage(format!(
                "cannot rename {} to {}: {error}",
                temp.display(),
                target.display()
            )));
        }

        // Best effort: syncing a directory is unsupported on Windows (the
        // handle cannot even be opened there), so never fail an otherwise
        // durable save because of it.
        let _ = sync_directory(&self.dir);

        self.prune()
    }

    /// The `index`th complete snapshot, newest-first, skipping corrupt files.
    pub(crate) fn load_index(&self, index: usize) -> Result<Option<T>, DurableError> {
        let mut valid = 0usize;
        for candidate in self.candidates()? {
            let Some(stored) = read_snapshot(&candidate.path)? else {
                // Checksum or decode failure: treat the file as corrupt and
                // fall back to the next newest candidate.
                continue;
            };
            if valid == index {
                return Ok(Some(stored));
            }
            valid += 1;
        }
        Ok(None)
    }

    /// Path of the snapshot file for one applied sequence.
    fn snapshot_path(&self, applied_seq: u64) -> PathBuf {
        self.dir.join(format!(
            "{FILE_PREFIX}{applied_seq:0width$}{FILE_SUFFIX}",
            width = SEQUENCE_WIDTH
        ))
    }

    /// Temporary file used for the atomic replace, unique per process and save.
    fn temp_path(&self, applied_seq: u64) -> PathBuf {
        static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);
        let unique = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        self.dir.join(format!(
            "{FILE_PREFIX}{applied_seq:0width$}{FILE_SUFFIX}.tmp-{}-{unique}",
            std::process::id(),
            width = SEQUENCE_WIDTH
        ))
    }

    /// Snapshot files, sorted newest applied sequence first.
    fn candidates(&self) -> Result<Vec<Candidate>, DurableError> {
        let entries = fs::read_dir(&self.dir).map_err(|error| {
            DurableError::Storage(format!(
                "cannot read snapshot directory {}: {error}",
                self.dir.display()
            ))
        })?;

        let mut candidates = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|error| {
                DurableError::Storage(format!(
                    "cannot read snapshot directory {}: {error}",
                    self.dir.display()
                ))
            })?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if let Some(sequence) = sequence_from_name(name) {
                candidates.push(Candidate {
                    sequence,
                    path: entry.path(),
                });
            }
        }

        candidates.sort_by(|left, right| {
            right
                .sequence
                .cmp(&left.sequence)
                .then_with(|| right.path.cmp(&left.path))
        });
        Ok(candidates)
    }

    /// Current snapshot floor: the highest applied sequence on disk.
    fn floor(&self) -> Result<u64, DurableError> {
        Ok(self
            .candidates()?
            .first()
            .map(|candidate| candidate.sequence)
            .unwrap_or(0))
    }

    /// Delete every snapshot beyond the two newest. Only called after a
    /// successful save, so a failed save never loses a fallback candidate.
    fn prune(&self) -> Result<(), DurableError> {
        for candidate in self.candidates()?.into_iter().skip(RETAINED_SNAPSHOTS) {
            fs::remove_file(&candidate.path).map_err(|error| {
                DurableError::Storage(format!(
                    "cannot remove old snapshot {}: {error}",
                    candidate.path.display()
                ))
            })?;
        }
        Ok(())
    }
}

struct Candidate {
    sequence: u64,
    path: PathBuf,
}

/// Parse `snapshot-{20 digits}.json` into its applied sequence.
fn sequence_from_name(name: &str) -> Option<u64> {
    let digits = name.strip_prefix(FILE_PREFIX)?.strip_suffix(FILE_SUFFIX)?;
    if digits.len() != SEQUENCE_WIDTH || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

/// Read one snapshot file. `Ok(None)` means the file exists but is corrupt
/// (bad checksum or undecodable JSON) and should be skipped.
fn read_snapshot<T: DeserializeOwned>(path: &Path) -> Result<Option<T>, DurableError> {
    let bytes = fs::read(path).map_err(|error| {
        DurableError::Storage(format!("cannot read snapshot {}: {error}", path.display()))
    })?;
    Ok(decode_snapshot(&bytes).ok())
}

fn decode_snapshot<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, DurableError> {
    let text = std::str::from_utf8(bytes).map_err(|_| DurableError::CorruptRecord {
        detail: "snapshot is not valid UTF-8".to_string(),
    })?;
    decode_line(text)
}

fn sync_directory(dir: &Path) -> std::io::Result<()> {
    File::open(dir)?.sync_all()
}
