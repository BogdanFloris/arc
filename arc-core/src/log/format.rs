//! On-disk record framing for the event log.
//!
//! This module is the single definition of the segment file format. Writer and
//! reader both go through it; neither re-derives offsets or checksum rules.
//!
//! A segment file is a bare concatenation of records — no file header, no
//! trailer, no padding, no alignment. Every record is:
//!
//! ```text
//! ┌──────────────┬──────────────┬─────────────────────┐
//! │ len: u32 LE  │ crc: u32 LE  │ payload: `len` bytes│
//! │  (4 bytes)   │  (4 bytes)   │                     │
//! └──────────────┴──────────────┴─────────────────────┘
//!  offset 0       offset 4       offset 8
//! ```
//!
//! - `len` counts the payload only; the 8 framing bytes are excluded.
//! - `crc` is CRC32 (`crc32fast`) over the payload bytes only, not over `len`.
//! - `payload` is a prost-encoded [`arc_proto::v1::Event`].
//!
//! The two integrity mechanisms are split:
//!
//! - **Truncation** is detected by the length prefix. A tail with fewer than
//!   [`HEADER_SIZE`] bytes, or fewer than `len` payload bytes after the header,
//!   is a torn record.
//! - **Corruption** is detected by the CRC.
//!
//! Decode failure detects neither. proto3 decodes empty and partially-truncated
//! byte strings "successfully" into a default message, so a reader that leans on
//! `Event::decode` to spot damage will silently accept garbage.

use super::Error;

/// Byte width of the length prefix.
pub const LEN_SIZE: usize = 4;

/// Byte width of the CRC32 field.
pub const CRC_SIZE: usize = 4;

/// Byte width of the full record header (`len` + `crc`).
pub const HEADER_SIZE: usize = LEN_SIZE + CRC_SIZE;

/// Largest payload the framing can describe, imposed by the u32 length prefix.
pub const MAX_PAYLOAD_LEN: usize = u32::MAX as usize;

/// The framing fields that precede a payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    /// Payload length in bytes, framing excluded.
    pub len: u32,
    /// CRC32 of the payload bytes.
    pub crc: u32,
}

/// CRC32 of a record payload.
#[must_use]
pub fn checksum(payload: &[u8]) -> u32 {
    crc32fast::hash(payload)
}

/// Frames one payload into a complete record, ready to be appended.
///
/// The returned buffer is written as a single unit; records are never assembled
/// from separate writes.
///
/// # Errors
///
/// [`Error::RecordTooLarge`] if the payload does not fit the u32 length prefix.
pub fn encode_record(payload: &[u8]) -> Result<Vec<u8>, Error> {
    let len =
        u32::try_from(payload.len()).map_err(|_| Error::RecordTooLarge { len: payload.len() })?;

    let mut record = Vec::with_capacity(HEADER_SIZE + payload.len());
    record.extend_from_slice(&len.to_le_bytes());
    record.extend_from_slice(&checksum(payload).to_le_bytes());
    record.extend_from_slice(payload);
    Ok(record)
}

/// Reads the framing fields from the first [`HEADER_SIZE`] bytes of a record.
#[must_use]
pub fn decode_header(bytes: &[u8; HEADER_SIZE]) -> Header {
    // Constant indices into a fixed-size array: no panic path, no bounds check.
    Header {
        len: u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
        crc: u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
    }
}

/// Whether `payload` matches its header: right length, right checksum.
///
/// Returns a bool rather than an error so the reader decides how a mismatch is
/// reported (torn tail vs. hard corruption is a reader policy, not a format one).
#[must_use]
pub fn verify(header: &Header, payload: &[u8]) -> bool {
    u32::try_from(payload.len()).is_ok_and(|len| len == header.len)
        && checksum(payload) == header.crc
}

#[cfg(test)]
mod tests {
    use super::{HEADER_SIZE, Header, checksum, decode_header, encode_record, verify};

    #[test]
    fn encode_record_lays_bytes_out_per_spec() {
        let payload = b"\x01\x02\x03\x04\x05";
        let record = encode_record(payload).expect("encode");

        assert_eq!(record.len(), HEADER_SIZE + payload.len());
        assert_eq!(&record[0..4], &5u32.to_le_bytes());
        assert_eq!(&record[4..8], &crc32fast::hash(payload).to_le_bytes());
        assert_eq!(&record[8..], payload);
    }

    #[test]
    fn empty_payload_frames_to_header_only() {
        let record = encode_record(&[]).expect("encode");

        assert_eq!(record.len(), HEADER_SIZE);
        assert_eq!(&record[0..4], &0u32.to_le_bytes());
        assert_eq!(&record[4..8], &checksum(&[]).to_le_bytes());
    }

    #[test]
    fn header_round_trips_and_verifies() {
        let payload = b"arc";
        let record = encode_record(payload).expect("encode");
        let header = decode_header(record[..HEADER_SIZE].try_into().expect("header"));

        assert_eq!(
            header,
            Header {
                len: 3,
                crc: checksum(payload)
            }
        );
        assert!(verify(&header, &record[HEADER_SIZE..]));
    }

    #[test]
    fn verify_rejects_flipped_bits_and_wrong_length() {
        let payload = b"arc";
        let header = Header {
            len: 3,
            crc: checksum(payload),
        };

        assert!(!verify(&header, b"orc"));
        assert!(!verify(&header, b"arch"));
    }
}
