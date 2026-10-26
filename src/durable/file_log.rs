//! A file-backed [`CommandLog`] that survives a real process restart.
//!
//! The file starts with a header line and continues with one command record per
//! line. Every line is `{crc32:08x} {json}\n`, where the CRC-32 covers the JSON
//! bytes. An append writes, flushes and fsyncs before returning, so an
//! acknowledged record is on disk.
//!
//! On open a partial trailing line is truncated: a final line without a
//! terminating newline, or one that fails its checksum or decoding, represents
//! an append that was never acknowledged. Any corrupt line before the last one
//! aborts open with [`DurableError::CorruptRecord`], because silently skipping
//! history would create a gap.
//!
//! Parsed records are cached, so [`CommandLog::read_from`],
//! [`CommandLog::first_sequence`] and [`CommandLog::last_sequence`] need no IO.

use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

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
    /// [`CommandLog::write_header`] is called. A torn final line is removed
    /// from the file; a corrupt line before it makes this call fail.
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
        let line_count = lines.len();
        let mut header = None;
        let mut records = Vec::new();
        // Offset just past the last line we accept; anything after it is a torn
        // append and is truncated away.
        let mut kept_end = 0usize;

        for (index, line) in lines.iter().enumerate() {
            let is_last = index + 1 == line_count;
            if is_last && !line.terminated {
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
                Err(detail) if is_last => {
                    // The last complete line can still be a half-finished
                    // append; it was never acknowledged, so discard it.
                    let _ = detail;
                    break;
                }
                Err(detail) => {
                    return Err(DurableError::CorruptRecord {
                        detail: format!("{} line {}: {detail}", path.display(), index + 1),
                    });
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

        let json = serde_json::to_vec(&header)
            .map_err(|error| DurableError::Storage(format!("cannot encode log header: {error}")))?;
        let line = encode_line(&json);
        let result = self.append_bytes(&line);
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

        let json = serde_json::to_vec(&record).map_err(|error| {
            DurableError::AppendFailed(format!("cannot encode command: {error}"))
        })?;
        let line = encode_line(&json);
        let result = self.append_bytes(&line);
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

fn parse_line(line: &[u8], is_header: bool) -> Result<ParsedLine, String> {
    let json = checked_json(line)?;
    let text = std::str::from_utf8(json).map_err(|_| "record is not valid UTF-8".to_string())?;
    if is_header {
        serde_json::from_str(text)
            .map(ParsedLine::Header)
            .map_err(|error| format!("invalid header JSON: {error}"))
    } else {
        serde_json::from_str(text)
            .map(ParsedLine::Record)
            .map_err(|error| format!("invalid command JSON: {error}"))
    }
}

/// Verify the `{crc32:08x} ` prefix and return the JSON bytes it covers.
fn checked_json(line: &[u8]) -> Result<&[u8], String> {
    if line.len() < 9 {
        return Err("line is shorter than a checksummed record".to_string());
    }

    let (crc_bytes, rest) = line.split_at(8);
    if rest.first() != Some(&b' ') {
        return Err("checksum is not followed by a space".to_string());
    }
    let json = &rest[1..];

    let crc_text =
        std::str::from_utf8(crc_bytes).map_err(|_| "checksum is not ASCII".to_string())?;
    let expected =
        u32::from_str_radix(crc_text, 16).map_err(|_| "checksum is not hexadecimal".to_string())?;
    let actual = crc32fast::hash(json);
    if expected != actual {
        return Err(format!(
            "crc32 mismatch: expected {expected:08x}, computed {actual:08x}"
        ));
    }

    Ok(json)
}

fn encode_line(json: &[u8]) -> Vec<u8> {
    let crc = crc32fast::hash(json);
    let mut line = Vec::with_capacity(9 + json.len() + 1);
    line.extend_from_slice(format!("{crc:08x}").as_bytes());
    line.push(b' ');
    line.extend_from_slice(json);
    line.push(b'\n');
    line
}

fn storage_error(path: &Path, action: &str, error: &std::io::Error) -> DurableError {
    DurableError::Storage(format!("cannot {action} {}: {error}", path.display()))
}
