//! A file-backed [`EventLog`] for the v2 event-sourced path.
//!
//! The file starts with a v2 header line and continues with one [`EventRecord`]
//! per line. Every line is `{crc32:08x} {json}\n`, where the CRC-32 covers the
//! JSON bytes. An append writes, flushes and fsyncs before returning, so an
//! acknowledged record is on disk. A batch append validates and encodes the
//! whole batch first, then writes all lines with one flush and one fsync; the
//! cached records change only after that fsync succeeds.
//!
//! On open only a final line without a terminating newline is truncated: that
//! append was never acknowledged. Any newline-terminated line that fails its
//! checksum or decoding aborts open with [`DurableError::CorruptRecord`],
//! including the last one.
//!
//! Version 1 is the old command-log format. A v1 header or record is refused
//! with [`DurableError::UnsupportedVersion`] before it can be misread as an
//! event log; there is no migration path.
//!
//! Parsed records are cached, so [`EventLog::read_from`],
//! [`EventLog::first_sequence`], [`EventLog::last_stored_sequence`] and
//! [`EventLog::last_command_id`] need no IO.

use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;

use crate::durable::event_log::{EventLog, EventRecord, FORMAT_VERSION};
use crate::durable::framing::{decode_line, encode_line};
use crate::durable::{DurableError, LogHeader};

/// A v2 [`EventLog`] persisted as checksummed JSON Lines.
pub struct FileEventLog {
    path: PathBuf,
    file: File,
    header: Option<LogHeader>,
    records: Vec<EventRecord>,
}

impl fmt::Debug for FileEventLog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileEventLog")
            .field("path", &self.path)
            .field("header", &self.header)
            .field("records", &self.records.len())
            .finish()
    }
}

impl FileEventLog {
    /// Open the log at `path`, creating an empty file when it does not exist.
    ///
    /// A newly created or empty file has no header until
    /// [`EventLog::write_header`] is called. An unterminated final fragment is
    /// removed from the file; any newline-terminated line that fails its
    /// checksum or decoding, including the last one, makes this call fail.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DurableError> {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|error| storage_error(&path, "open", &error))?;

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(|error| storage_error(&path, "read", &error))?;

        let lines = split_lines(&bytes);
        let mut header = None;
        let mut records = Vec::new();
        // Offset just past the last line we accept; anything after it is a torn
        // append and is truncated away.
        let mut kept_end = 0usize;

        for (index, line) in lines.iter().enumerate() {
            if !line.terminated {
                // A final line without its newline is an append that was never
                // acknowledged, so it is truncated rather than parsed.
                break;
            }

            match parse_line(line.content, index == 0) {
                Ok(ParsedLine::Header(parsed)) => {
                    header = Some(parsed);
                    kept_end = line.end;
                }
                Ok(ParsedLine::Record(parsed)) => {
                    records.push(parsed);
                    kept_end = line.end;
                }
                Err(error) => {
                    // A newline-terminated line was written completely; a
                    // checksum, decode or version failure must not be silently
                    // dropped.
                    return Err(line_context(
                        error,
                        &format!("{} line {}", path.display(), index + 1),
                    ));
                }
            }
        }

        if kept_end != bytes.len() {
            file.set_len(kept_end as u64)
                .map_err(|error| storage_error(&path, "truncate", &error))?;
            file.sync_all()
                .map_err(|error| storage_error(&path, "sync truncated file", &error))?;
        }
        file.seek(SeekFrom::End(0))
            .map_err(|error| storage_error(&path, "seek", &error))?;

        Ok(Self {
            path,
            file,
            header,
            records,
        })
    }

    /// The path this log is stored at.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Write one already-encoded line and make it durable.
    fn append_bytes(&mut self, line: &[u8]) -> std::io::Result<()> {
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(line)?;
        self.file.flush()?;
        self.file.sync_all()?;
        Ok(())
    }
}

impl EventLog for FileEventLog {
    fn read_header(&self) -> Result<Option<LogHeader>, DurableError> {
        Ok(self.header.clone())
    }

    fn write_header(&mut self, header: LogHeader) -> Result<(), DurableError> {
        if header.format_version != FORMAT_VERSION {
            return Err(DurableError::UnsupportedVersion {
                found: header.format_version,
                supported: FORMAT_VERSION,
            });
        }
        if self.header.is_some() {
            return Err(DurableError::HeaderAlreadyPresent);
        }

        let line = encode_line(&header)
            .map_err(|error| encode_context(error, "cannot encode event-log header"))?;
        let result = self.append_bytes(line.as_bytes());
        result.map_err(|error| storage_error(&self.path, "write header to", &error))?;

        self.header = Some(header);
        Ok(())
    }

    fn append(&mut self, record: EventRecord) -> Result<(), DurableError> {
        if self.header.is_none() {
            return Err(DurableError::MissingHeader);
        }
        check_record_version(&record)?;

        let expected = self
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

        let line = encode_line(&record)
            .map_err(|error| encode_context(error, "cannot encode event record"))?;
        let result = self.append_bytes(line.as_bytes());
        result.map_err(|error| {
            DurableError::AppendFailed(format!("cannot append to {}: {error}", self.path.display()))
        })?;

        self.records.push(record);
        Ok(())
    }

    fn append_batch(&mut self, records: Vec<EventRecord>) -> Result<(), DurableError> {
        if self.header.is_none() {
            return Err(DurableError::MissingHeader);
        }
        if records.is_empty() {
            return Ok(());
        }

        // Validate the whole batch before touching the file, so an invalid
        // batch cannot leave a prefix on disk.
        for record in &records {
            check_record_version(record)?;
        }
        let mut expected = self
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

        // Encode everything up front for the same reason: an encoding failure
        // must not write part of the batch.
        let mut bytes = Vec::new();
        for record in &records {
            let line = encode_line(record)
                .map_err(|error| encode_context(error, "cannot encode event record"))?;
            bytes.extend_from_slice(line.as_bytes());
        }

        // One write, one flush, one fsync for the whole group.
        self.file
            .seek(SeekFrom::End(0))
            .and_then(|_| self.file.write_all(&bytes))
            .and_then(|_| self.file.flush())
            .and_then(|_| self.file.sync_all())
            .map_err(|error| {
                DurableError::AppendFailed(format!(
                    "cannot append to {}: {error}",
                    self.path.display()
                ))
            })?;

        // Cache only after durability: `last_stored_sequence` must never advertise a
        // record a crash could lose.
        self.records.extend(records);
        Ok(())
    }

    fn read_from(&self, from: u64) -> Result<Vec<EventRecord>, DurableError> {
        Ok(self
            .records
            .iter()
            .filter(|record| record.sequence >= from)
            .cloned()
            .collect())
    }

    fn first_sequence(&self) -> Option<u64> {
        self.records.first().map(|record| record.sequence)
    }

    fn last_stored_sequence(&self) -> Option<u64> {
        self.records.last().map(|record| record.sequence)
    }

    fn last_command_id(&self) -> Option<u64> {
        self.records.last().map(|record| record.command_id)
    }
}

fn check_record_version(record: &EventRecord) -> Result<(), DurableError> {
    if record.format_version != FORMAT_VERSION {
        return Err(DurableError::UnsupportedVersion {
            found: record.format_version,
            supported: FORMAT_VERSION,
        });
    }
    Ok(())
}

/// One raw line from the file, without its terminating newline.
struct RawLine<'a> {
    content: &'a [u8],
    /// Offset just past the line, including its newline when present.
    end: usize,
    terminated: bool,
}

fn split_lines(bytes: &[u8]) -> Vec<RawLine<'_>> {
    let mut lines = Vec::new();
    let mut start = 0usize;
    for (index, &byte) in bytes.iter().enumerate() {
        if byte == b'\n' {
            lines.push(RawLine {
                content: &bytes[start..index],
                end: index + 1,
                terminated: true,
            });
            start = index + 1;
        }
    }
    if start < bytes.len() {
        lines.push(RawLine {
            content: &bytes[start..],
            end: bytes.len(),
            terminated: false,
        });
    }
    lines
}

enum ParsedLine {
    Header(LogHeader),
    Record(EventRecord),
}

fn parse_line(line: &[u8], is_header: bool) -> Result<ParsedLine, DurableError> {
    let text = std::str::from_utf8(line).map_err(|_| DurableError::CorruptRecord {
        detail: "record is not valid UTF-8".to_string(),
    })?;
    if is_header {
        decode_versioned(text, "invalid event-log header").map(ParsedLine::Header)
    } else {
        decode_versioned(text, "invalid event record").map(ParsedLine::Record)
    }
}

/// Decode one checksummed line, refusing a format version this build cannot
/// read before attempting the typed decode.
///
/// The version probe is what makes a v1 command-log header or record fail with
/// [`DurableError::UnsupportedVersion`] rather than a generic JSON error.
fn decode_versioned<T: DeserializeOwned>(line: &str, context: &str) -> Result<T, DurableError> {
    let value: serde_json::Value =
        decode_line(line).map_err(|error| corrupt_context(error, context))?;
    let found = value
        .get("format_version")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| DurableError::CorruptRecord {
            detail: format!("{context}: missing numeric format_version"),
        })?;
    let found = u32::try_from(found).map_err(|_| DurableError::CorruptRecord {
        detail: format!("{context}: format_version is out of range"),
    })?;
    if found != FORMAT_VERSION {
        return Err(DurableError::UnsupportedVersion {
            found,
            supported: FORMAT_VERSION,
        });
    }

    serde_json::from_value(value).map_err(|error| DurableError::CorruptRecord {
        detail: format!("{context}: invalid JSON: {error}"),
    })
}

/// Add context to a corruption detail, leaving other variants untouched.
fn corrupt_context(error: DurableError, context: &str) -> DurableError {
    match error {
        DurableError::CorruptRecord { detail } => DurableError::CorruptRecord {
            detail: format!("{context}: {detail}"),
        },
        other => other,
    }
}

/// Add line context while preserving a version refusal as-is.
fn line_context(error: DurableError, context: &str) -> DurableError {
    corrupt_context(error, context)
}

/// Add context to an encoding detail, leaving other variants untouched.
fn encode_context(error: DurableError, context: &str) -> DurableError {
    match error {
        DurableError::Storage(detail) => DurableError::Storage(format!("{context}: {detail}")),
        other => other,
    }
}

fn storage_error(path: &Path, action: &str, error: &std::io::Error) -> DurableError {
    DurableError::Storage(format!("cannot {action} {}: {error}", path.display()))
}
