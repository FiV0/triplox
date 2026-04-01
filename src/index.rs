#[derive(Debug, Clone, Copy)]
pub enum IndexType { EAV, AVE, AEV, AE, AV}

pub (crate) fn remove_index_type(bytes: bytes::Bytes) -> bytes::Bytes {
    let mut bytes = bytes.to_vec();
    let _ = bytes.split_off(1);
    bytes::Bytes::from(bytes)
}

pub (crate) fn add_index_type(bytes: bytes::Bytes, index_type: IndexType) -> bytes::Bytes {
    let mut bytes = bytes.to_vec();
    bytes.insert(0, index_type as u8);
    bytes::Bytes::from(bytes)
}
