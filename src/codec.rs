#![allow(dead_code)]
use anyhow::Error;

use crate::index::IndexType;

// TODO should this become 2 bytes?
pub const INDEX_VERSION: u8 = 1;
pub const CODEC_LENGTH: usize = 1;

pub const EAV: u8 = 0;
pub const AVE: u8 = 1;
pub const AEV: u8 = 2;
// TODO move this to normal entities
// {:db/id ... :db/ident ...}
pub const ATTRIBUTE_TO_ID: u8 = 3;

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
        _ => Err(anyhow::anyhow!("VAE index not (yet) supported"))
    }
}
