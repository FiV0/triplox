use anyhow::{anyhow, Error};
use bytes::Bytes;
use rand::Rng;

use crate::{codec, index::IndexType};

pub fn random_string(length: usize) -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::rng();

    (0..length)
        .map(|_| {
            let idx = rng.random_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

pub fn concat_bytes(parts: &[&[u8]]) -> Vec<u8> {
    let mut result = Vec::new();
    for part in parts {
        result.extend(*part);
    }
    result
}

pub trait GetSlice {
    fn get_slice(&self, start: usize, end: usize) -> Self;
}

impl GetSlice for &[u8] {
    fn get_slice(&self, start: usize, end: usize) -> Self {
        &self[start..end]
    }
}

impl GetSlice for Vec<u8> {
    fn get_slice(&self, start: usize, end: usize) -> Self {
        self[start..end].to_vec()
    }
}

impl GetSlice for Bytes {
    fn get_slice(&self, start: usize, end: usize) -> Self {
        self.slice(start..end)
    }
}

pub fn get_slice<T: GetSlice>(bytes: T, start: usize, end: usize) -> T {
    bytes.get_slice(start, end)
}

pub trait StripPrefix {
    fn strip_prefix(&self, prefix_len: usize) -> Self;
}

impl<T: GetSlice + AsRef<[u8]>> StripPrefix for T {
    fn strip_prefix(&self, prefix_len: usize) -> Self {
        self.get_slice(prefix_len, self.as_ref().len())
    }
}

pub fn strip_prefix<T: StripPrefix>(bytes: T, prefix_len: usize) -> T {
    bytes.strip_prefix(prefix_len)
}

pub trait StripSuffix {
    fn strip_suffix(&self, suffix_len: usize) -> Self;
}

impl<T: GetSlice + AsRef<[u8]>> StripSuffix for T {
    fn strip_suffix(&self, suffix_len: usize) -> Self {
        self.get_slice(0, self.as_ref().len() - suffix_len)
    }
}

pub fn strip_suffix<T: StripSuffix>(bytes: T, suffix_len: usize) -> T {
    bytes.strip_suffix(suffix_len)
}

fn check_position(position: usize, index: IndexType) {
    match index {
        IndexType::EAV | IndexType::AVE | IndexType::AEV | IndexType::VAE => {
            assert!(position < 3, "Position must be less than 3");
        }
        IndexType::AE | IndexType::AV => {
            assert!(position < 2, "Position must be less than 2");
        }
    }
}

/// Subtract `amount` from a key length, erroring on short/corrupt keys instead
/// of underflowing.
fn key_len_sub(total_length: usize, amount: usize) -> Result<usize, Error> {
    total_length.checked_sub(amount).ok_or_else(|| {
        anyhow!(
            "key too short: {} bytes, need at least {}",
            total_length,
            amount
        )
    })
}

// TODO  refactor this and prefix_len
pub fn make_extractor<T: GetSlice + AsRef<[u8]>>(
    position: usize,
    index: IndexType,
) -> impl Fn(T) -> Result<T, Error> {
    move |bytes: T| {
        check_position(position, index);
        let total_length = bytes.as_ref().len();
        let sub = |amount| key_len_sub(total_length, amount);

        let (start, end) = match index {
            IndexType::EAV => match position {
                0 => (
                    codec::CODEC_LENGTH,
                    codec::CODEC_LENGTH + codec::ENTITY_LENGTH,
                ),
                1 => (
                    codec::CODEC_LENGTH + codec::ENTITY_LENGTH,
                    codec::CODEC_LENGTH + codec::ENTITY_LENGTH + codec::ATTRIBUTE_LENGTH,
                ),
                2.. => (
                    codec::CODEC_LENGTH + codec::ENTITY_LENGTH + codec::ATTRIBUTE_LENGTH,
                    sub(codec::TX_EID_OP_SUFFIX)?,
                ),
            },
            IndexType::AVE => match position {
                0 => (
                    codec::CODEC_LENGTH,
                    codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH,
                ),
                1 => (
                    codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH,
                    sub(codec::ENTITY_LENGTH + codec::TX_EID_OP_SUFFIX)?,
                ),
                2.. => (
                    sub(codec::ENTITY_LENGTH + codec::TX_EID_OP_SUFFIX)?,
                    sub(codec::TX_EID_OP_SUFFIX)?,
                ),
            },
            IndexType::AEV => match position {
                0 => (
                    codec::CODEC_LENGTH,
                    codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH,
                ),
                1 => (
                    codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH,
                    codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH + codec::ENTITY_LENGTH,
                ),
                2.. => (
                    codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH + codec::ENTITY_LENGTH,
                    sub(codec::TX_EID_OP_SUFFIX)?,
                ),
            },
            IndexType::VAE => match position {
                0 => (
                    codec::CODEC_LENGTH,
                    sub(codec::ATTRIBUTE_LENGTH + codec::ENTITY_LENGTH + codec::TX_EID_OP_SUFFIX)?,
                ),
                1 => (
                    sub(codec::ATTRIBUTE_LENGTH + codec::ENTITY_LENGTH + codec::TX_EID_OP_SUFFIX)?,
                    sub(codec::ENTITY_LENGTH + codec::TX_EID_OP_SUFFIX)?,
                ),
                2.. => (
                    sub(codec::ENTITY_LENGTH + codec::TX_EID_OP_SUFFIX)?,
                    sub(codec::TX_EID_OP_SUFFIX)?,
                ),
            },
            // AE/AV are atemporal — no T+op suffix
            IndexType::AE => match position {
                0 => (
                    codec::CODEC_LENGTH,
                    codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH,
                ),
                1.. => (codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH, total_length),
            },
            IndexType::AV => match position {
                0 => (
                    codec::CODEC_LENGTH,
                    codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH,
                ),
                1.. => (codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH, total_length),
            },
        };
        if start > end || end > total_length {
            return Err(anyhow!(
                "invalid key: extractor range {}..{} out of bounds for {} bytes",
                start,
                end,
                total_length
            ));
        }
        Ok(bytes.get_slice(start, end))
    }
}

pub fn extract_value<T: GetSlice + AsRef<[u8]>>(
    bytes: T,
    position: usize,
    index: IndexType,
) -> Result<T, Error> {
    make_extractor(position, index)(bytes)
}

pub fn prefix_extractor<T: GetSlice + AsRef<[u8]>>(
    position: usize,
    index: IndexType,
) -> impl Fn(T) -> Result<T, Error> {
    move |bytes: T| {
        check_position(position, index);
        let total_length = bytes.as_ref().len();
        let sub = |amount| key_len_sub(total_length, amount);

        let end = match index {
            IndexType::EAV => match position {
                0 => codec::CODEC_LENGTH,
                1 => codec::CODEC_LENGTH + codec::ENTITY_LENGTH,
                2.. => codec::CODEC_LENGTH + codec::ENTITY_LENGTH + codec::ATTRIBUTE_LENGTH,
            },
            IndexType::AVE => match position {
                0 => codec::CODEC_LENGTH,
                1 => codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH,
                2.. => sub(codec::ENTITY_LENGTH + codec::TX_EID_OP_SUFFIX)?,
            },
            IndexType::AEV => match position {
                0 => codec::CODEC_LENGTH,
                1 => codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH,
                2.. => codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH + codec::ENTITY_LENGTH,
            },
            IndexType::VAE => match position {
                0 => codec::CODEC_LENGTH,
                1 => sub(codec::ATTRIBUTE_LENGTH + codec::ENTITY_LENGTH + codec::TX_EID_OP_SUFFIX)?,
                2.. => sub(codec::ENTITY_LENGTH + codec::TX_EID_OP_SUFFIX)?,
            },
            IndexType::AE => match position {
                0 => codec::CODEC_LENGTH,
                1.. => codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH,
            },
            IndexType::AV => match position {
                0 => codec::CODEC_LENGTH,
                1.. => codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH,
            },
        };
        if end > total_length {
            return Err(anyhow!(
                "invalid key: prefix end {} out of bounds for {} bytes",
                end,
                total_length
            ));
        }
        Ok(bytes.get_slice(0, end))
    }
}

pub fn extract_prefix<T: GetSlice + AsRef<[u8]>>(
    bytes: T,
    position: usize,
    index: IndexType,
) -> Result<T, Error> {
    prefix_extractor(position, index)(bytes)
}

/// Compute the lexicographic successor of a byte string.
/// Returns None if all bytes are 0xFF (no successor exists).
///
/// Note: this duplicates `BytesRange::increment_prefix` in SlateDB
/// (`slatedb/src/bytes_range.rs`), which is not publicly exposed.
pub fn next_prefix(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut next = prefix.to_vec();
    for i in (0..next.len()).rev() {
        if next[i] < 0xFF {
            next[i] += 1;
            next.truncate(i + 1);
            return Some(next);
        }
    }
    None
}
