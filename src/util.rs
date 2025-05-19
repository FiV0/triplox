use rand::Rng;
use std::ops::Bound;
use bytes::Bytes;

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

pub type Range = (Bound<Bytes>, Bound<Bytes>);

pub fn prefix_successor(prefix: &[u8]) -> Vec<u8> {
    let mut successor = prefix.to_vec();
    successor.push(0xFF);
    successor
}

pub fn create_prefix_range(prefix: &[u8]) -> Range {
    let start = Bound::Included(Bytes::copy_from_slice(prefix));
    let end = Bound::Included(Bytes::copy_from_slice(&prefix_successor(prefix)));
    (start, end)
}

pub fn concat_bytes(parts: &[&[u8]]) -> Vec<u8> {
    let mut result = Vec::new();
    for part in parts {
        result.extend(*part);
    }
    result
}