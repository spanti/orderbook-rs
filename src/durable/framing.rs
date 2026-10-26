//! Shared `{crc32:08x} {json}\n` framing for the file-backed durable adapters.
//!
//! One line holds one JSON value prefixed by a CRC-32 of exactly the JSON
//! bytes. The event log and the snapshot store use this same convention, so
//! encoding and verification live here instead of being duplicated.

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::durable::DurableError;

/// Encode `value` as one newline-terminated, checksummed JSON line.
pub(crate) fn encode_line<T: Serialize>(value: &T) -> Result<String, DurableError> {
    let json =
        serde_json::to_string(value).map_err(|error| DurableError::Storage(error.to_string()))?;
    let crc = crc32fast::hash(json.as_bytes());
    Ok(format!("{crc:08x} {json}\n"))
}

/// Decode one checksummed JSON line, verifying its CRC-32 first.
///
/// A single trailing newline is ignored. A checksum mismatch, malformed prefix
/// or JSON decode failure is returned as [`DurableError::CorruptRecord`];
/// callers may add the source path and line number.
pub(crate) fn decode_line<T: DeserializeOwned>(line: &str) -> Result<T, DurableError> {
    let line = line.strip_suffix('\n').unwrap_or(line);
    let bytes = line.as_bytes();
    if bytes.len() < 9 {
        return Err(corrupt("line is shorter than a checksummed record"));
    }

    let (crc_bytes, rest) = bytes.split_at(8);
    if rest.first() != Some(&b' ') {
        return Err(corrupt("checksum is not followed by a space"));
    }
    let json = &rest[1..];

    let crc_text = std::str::from_utf8(crc_bytes).map_err(|_| corrupt("checksum is not ASCII"))?;
    let expected =
        u32::from_str_radix(crc_text, 16).map_err(|_| corrupt("checksum is not hexadecimal"))?;
    let actual = crc32fast::hash(json);
    if expected != actual {
        return Err(corrupt(format!(
            "crc32 mismatch: expected {expected:08x}, computed {actual:08x}"
        )));
    }

    serde_json::from_slice(json).map_err(|error| corrupt(format!("invalid JSON: {error}")))
}

fn corrupt(detail: impl Into<String>) -> DurableError {
    DurableError::CorruptRecord {
        detail: detail.into(),
    }
}
