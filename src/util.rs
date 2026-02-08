use bytes::Bytes;
use rand::Rng;

use crate::{codec, index::IndexType};

pub fn random_string(length: usize) -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::thread_rng();

    (0..length)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
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
        IndexType::EAV | IndexType::AVE | IndexType::AEV => {
            assert!(position < 3, "Position must be less than 3");
        }
        IndexType::AE | IndexType::AV => {
            assert!(position < 2, "Position must be less than 2");
        }
        IndexType::VAE => {
            todo!("VAE index not (yet) supported")
        }
    }
}

// TODO  refactor this and prefix_len
pub fn make_extractor<T: GetSlice + AsRef<[u8]>>(
    position: usize,
    index: IndexType,
) -> impl Fn(T) -> T {
    move |bytes: T| {
        check_position(position, index);
        let total_length = bytes.as_ref().len();

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
                    total_length - codec::OP_LENGTH,
                ),
            },
            IndexType::AVE => match position {
                0 => (
                    codec::CODEC_LENGTH,
                    codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH,
                ),
                1 => (
                    codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH,
                    total_length - codec::ENTITY_LENGTH - codec::OP_LENGTH,
                ),
                2.. => (
                    total_length - codec::ENTITY_LENGTH - codec::OP_LENGTH,
                    total_length - codec::OP_LENGTH,
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
                    total_length - codec::OP_LENGTH,
                ),
            },
            IndexType::AE => match position {
                0 => (
                    codec::CODEC_LENGTH,
                    codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH,
                ),
                1.. => (
                    codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH,
                    total_length - codec::OP_LENGTH,
                ),
            },
            IndexType::AV => match position {
                0 => (
                    codec::CODEC_LENGTH,
                    codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH,
                ),
                1.. => (
                    codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH,
                    total_length - codec::OP_LENGTH,
                ),
            },
            IndexType::VAE => {
                todo!("VAE index not (yet) supported")
            }
        };
        bytes.get_slice(start, end)
    }
}

pub fn extract_value<T: GetSlice + AsRef<[u8]>>(bytes: T, position: usize, index: IndexType) -> T {
    make_extractor(position, index)(bytes)
}


pub fn prefix_extractor<T: GetSlice + AsRef<[u8]>>(
    position: usize,
    index: IndexType,
) -> impl Fn(T) -> T {
    move |bytes: T| {
        check_position(position, index);
        let total_length = bytes.as_ref().len();

        let end= match index {
            IndexType::EAV => match position {
                0 => codec::CODEC_LENGTH,
                1 => codec::CODEC_LENGTH + codec::ENTITY_LENGTH,
                2.. => codec::CODEC_LENGTH + codec::ENTITY_LENGTH + codec::ATTRIBUTE_LENGTH,
            },
            IndexType::AVE => match position {
                0 => codec::CODEC_LENGTH,
                1 => codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH,
                2.. => total_length - codec::ENTITY_LENGTH - codec::OP_LENGTH,
            },
            IndexType::AEV => match position {
                0 => codec::CODEC_LENGTH,
                1 => codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH,
                2.. => codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH + codec::ENTITY_LENGTH,
            },
            IndexType::AE => match position {
                0 => codec::CODEC_LENGTH,
                1.. => codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH,
            },
            IndexType::AV => match position {
                0 => codec::CODEC_LENGTH,
                1.. => codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH,
            },
            IndexType::VAE => {
                todo!("VAE index not (yet) supported")
            }
        };
        bytes.get_slice(0, end)
    }
}

pub fn extract_prefix<T: GetSlice + AsRef<[u8]>>(bytes: T, position: usize, index: IndexType) -> T {
    prefix_extractor(position, index)(bytes)
}

/// Extract the next component after the prefix from a SlateDB key
///
/// This is used by GenericPrefixExtender to extract extensions from scanned keys.
/// The function handles both fixed-size components (entity, attribute) and variable-size
/// components (values).
pub fn extract_next_component(key: &[u8], prefix: &[u8]) -> anyhow::Result<Bytes> {
    use anyhow::anyhow;

    if key.len() <= prefix.len() {
        return Err(anyhow!("Key not longer than prefix"));
    }

    // Get bytes after prefix
    let remaining = &key[prefix.len()..];

    // Determine if we're extracting a fixed or variable-length component
    // Fixed-length components are entity (8 bytes) or attribute (8 bytes)
    // Variable-length components are values (everything up to OP byte at end)

    // Check if we have enough bytes for a fixed-size component
    if remaining.len() >= codec::ENTITY_LENGTH + codec::OP_LENGTH {
        // Could be entity or attribute - extract 8 bytes
        Ok(Bytes::copy_from_slice(
            &remaining[..codec::ENTITY_LENGTH],
        ))
    } else if remaining.len() > codec::OP_LENGTH {
        // Variable-length value - everything except OP byte at end
        Ok(Bytes::copy_from_slice(
            &remaining[..remaining.len() - codec::OP_LENGTH],
        ))
    } else {
        Err(anyhow!("Remaining bytes too short to extract component"))
    }
}