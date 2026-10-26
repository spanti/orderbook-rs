//! A file-backed [`CommandLog`] that survives a real process restart.
//!
//! The file starts with a header line and continues with one command record per
//! line. Every line is `{crc32:08x} {json}\n`, where the CRC-32 covers the JSON
//! bytes. An append writes, flushes and fsyncs before returning, so an
//! acknowledged record is on disk.
//!
//! On open only a final line without a terminating newline is truncated: that
//! append was never acknowledged. Any newline-terminated line that fails its
//! checksum or decoding aborts open with [`DurableError::CorruptRecord`],
//! including the last one. A completed, fsynced append cannot legitimately lose
//! its checksum, and treating it as a torn write would silently drop applied
//! history.
//!
//! Parsed records are cached, so [`CommandLog::read_from`],
//! [`CommandLog::first_sequence`] and [`CommandLog::last_sequence`] need no IO.

use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::durable::framing::{decode_line, encode_line};
use crate::durable::{CommandLog, CommandRecord, DurableError, LogHeader};

/// A [`CommandLog`] persisted as checksummed JSON Lines.
pub struct FileCommandLog {
    path: PathBuf,
    file: File,
    header: Option<LogHeader>,
    records: Vec<CommandRecord>,
}

impl fmt::Debug for FileCommandLog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileCommandLog")
            .field("path", &self.path)
            .field("header", &self.header)
            .field("records", &self.records.len())
            .finish()
    }
}

impl FileCommandLog {
    /// Open the log at `path`, creating an empty file when it does not exist.
    ///
    /// A newly created or empty file has no header until
    /// [`CommandLog::write_header`] is called. An unterminated final fragment
    /// is removed from the file; any newline-terminated line that fails its
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
                    // checksum or decode failure means real corruption, even on
                    // the last line, and must not be silently dropped.
                    return Err(corrupt_context(
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

impl CommandLog for FileCommandLog {
    fn read_header(&self) -> Result<Option<LogHeader>, DurableError> {
        Ok(self.header.clone())
    }

    fn write_header(&mut self, header: LogHeader) -> Result<(), DurableError> {
        if self.header.is_some() {
            return Err(DurableError::HeaderAlreadyPresent);
        }

        let line = encode_line(&header)
            .map_err(|error| encode_context(error, "cannot encode log header"))?;
        let result = self.append_bytes(line.as_bytes());
        result.map_err(|error| storage_error(&self.path, "write header to", &error))?;

        self.header = Some(header);
        Ok(())
    }

    fn append(&mut self, record: CommandRecord) -> Result<(), DurableError> {
        if self.header.is_none() {
            return Err(DurableError::MissingHeader);
        }

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

        let line = encode_line(&record).map_err(|error| match error {
            DurableError::Storage(detail) => {
                DurableError::AppendFailed(format!("cannot encode command: {detail}"))
            }
            other => other,
        })?;
        let result = self.append_bytes(line.as_bytes());
        result.map_err(|error| {
            DurableError::AppendFailed(format!("cannot append to {}: {error}", self.path.display()))
        })?;

        self.records.push(record);
        Ok(())
    }

    fn read_from(&self, from: u64) -> Result<Vec<CommandRecord>, DurableError> {
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

    fn last_sequence(&self) -> Option<u64> {
        self.records.last().map(|record| record.sequence)
    }
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
    Record(CommandRecord),
}

fn parse_line(line: &[u8], is_header: bool) -> Result<ParsedLine, DurableError> {
    let text = std::str::from_utf8(line).map_err(|_| DurableError::CorruptRecord {
        detail: "record is not valid UTF-8".to_string(),
    })?;
    if is_header {
        decode_line(text)
            .map(ParsedLine::Header)
            .map_err(|error| corrupt_context(error, "invalid header"))
    } else {
        decode_line(text)
            .map(ParsedLine::Record)
            .map_err(|error| corrupt_context(error, "invalid command record"))
    }
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
