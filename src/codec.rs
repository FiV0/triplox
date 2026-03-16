#![allow(dead_code)]
use anyhow::Error;

use crate::clock::Instant;
use crate::index::IndexType;

// TODO should this become 2 bytes?
pub const INDEX_VERSION: u8 = 1;

// TODO use these throughout the codebase
pub const CODEC_LENGTH: usize = 1;
// TODO: Entity IDs are encoded as DataType::Long (4-byte variant tag + 8-byte i64).
// Revisit with schema/custom encoding — could go back to raw i64 (8 bytes).
pub const ENTITY_LENGTH: usize = 12;
pub const ATTRIBUTE_LENGTH: usize = 8;
pub const OP_LENGTH: usize = 1;
pub const TIMESTAMP_LENGTH: usize = 8;

pub const EAV: u8 = 0;
pub const AVE: u8 = 1;
pub const AEV: u8 = 2;
pub const AE: u8 = 3;
pub const AV: u8 = 4;

pub const TX_TO_SEQ: u8 = 6;

pub const STATS_INDEX: u8 = 128;
pub const META_INDEX: u8 = 129;

pub const ADD: u8 = 0;
pub const DELETE: u8 = 1;
pub const RETRACT: u8 = 2;


/// Suffix length: timestamp (8 bytes) + op (1 byte), used for end-of-key extraction.
pub const TIMESTAMP_OP_SUFFIX: usize = TIMESTAMP_LENGTH + OP_LENGTH;

// TODO move this to lazy_static
pub fn index_type_to_prefix(index_type: IndexType) -> Result<u8, Error> {
    match index_type {
        IndexType::EAV => Ok(EAV),
        IndexType::AVE => Ok(AVE),
        IndexType::AEV => Ok(AEV),
        IndexType::AE => Ok(AE),
        IndexType::AV => Ok(AV),
    }
}

/// Encode a timestamp as inverted big-endian i64 microseconds.
/// Inverted so that newer timestamps sort first in ascending byte order.
pub fn encode_timestamp(t: Instant) -> [u8; 8] {
    let micros = t.timestamp_micros();
    (!micros).to_be_bytes()
}

/// Decode an inverted big-endian timestamp back to an Instant.
pub fn decode_timestamp(bytes: &[u8]) -> Instant {
    let inverted = i64::from_be_bytes(bytes.try_into().expect("timestamp must be 8 bytes"));
    let micros = !inverted;
    chrono::TimeZone::timestamp_micros(&chrono::Utc, micros).unwrap()
}
