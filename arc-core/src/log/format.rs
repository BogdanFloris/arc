use super::Error;

pub const LEN_SIZE: usize = 4;

pub const CRC_SIZE: usize = 4;

pub const HEADER_SIZE: usize = LEN_SIZE + CRC_SIZE;

pub const MAX_RECORD_LEN: u32 = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub len: u32,
    pub crc: u32,
}

#[must_use]
pub fn checksum(payload: &[u8]) -> u32 {
    crc32fast::hash(payload)
}

pub fn encode_record(payload: &[u8]) -> Result<Vec<u8>, Error> {
    let len = u32::try_from(payload.len())
        .ok()
        .filter(|len| *len <= MAX_RECORD_LEN)
        .ok_or(Error::RecordTooLarge { len: payload.len() })?;

    let mut record = Vec::with_capacity(HEADER_SIZE + payload.len());
    record.extend_from_slice(&len.to_le_bytes());
    record.extend_from_slice(&checksum(payload).to_le_bytes());
    record.extend_from_slice(payload);
    Ok(record)
}

#[must_use]
pub fn decode_header(bytes: &[u8; HEADER_SIZE]) -> Header {
    Header {
        len: u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
        crc: u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
    }
}

#[must_use]
pub fn verify(header: &Header, payload: &[u8]) -> bool {
    u32::try_from(payload.len()).is_ok_and(|len| len == header.len)
        && checksum(payload) == header.crc
}

#[cfg(test)]
mod tests {
    use super::{
        HEADER_SIZE, Header, MAX_RECORD_LEN, checksum, decode_header, encode_record, verify,
    };
    use crate::log::Error;

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
    fn payload_at_the_cap_frames_and_one_byte_over_is_refused() {
        let at_cap = vec![0u8; MAX_RECORD_LEN as usize];
        let record = encode_record(&at_cap).expect("a payload at the cap is legal");
        assert_eq!(record.len(), HEADER_SIZE + at_cap.len());
        assert_eq!(&record[0..4], &MAX_RECORD_LEN.to_le_bytes());

        let over_cap = vec![0u8; MAX_RECORD_LEN as usize + 1];
        let err = encode_record(&over_cap).expect_err("one byte over the cap must be refused");
        assert!(
            matches!(err, Error::RecordTooLarge { len } if len == over_cap.len()),
            "got: {err:?}"
        );
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
