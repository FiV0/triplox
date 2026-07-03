#[derive(Debug, Clone, Copy)]
#[allow(clippy::upper_case_acronyms)]
pub enum IndexType {
    EAV,
    AVE,
    AEV,
    VAE,
    AE,
    AV,
}

pub(crate) fn remove_index_type(bytes: bytes::Bytes) -> bytes::Bytes {
    // Zero-copy: slices the underlying buffer instead of reallocating.
    bytes.slice(1..)
}

pub(crate) fn add_index_type(bytes: bytes::Bytes, index_type: IndexType) -> bytes::Bytes {
    let mut key = Vec::with_capacity(bytes.len() + 1);
    key.push(index_type as u8);
    key.extend_from_slice(&bytes);
    bytes::Bytes::from(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_then_remove_round_trips() {
        let key = bytes::Bytes::from_static(b"payload");
        let with_type = add_index_type(key.clone(), IndexType::AVE);
        assert_eq!(with_type[0], IndexType::AVE as u8);
        assert_eq!(remove_index_type(with_type), key);
    }
}
