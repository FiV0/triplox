#![allow(dead_code)]
use anyhow::Error;

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
