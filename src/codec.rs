#![allow(dead_code)]
use std::collections::BTreeMap;
use std::fmt;

use anyhow::Error;
use chrono::{DateTime, Utc};
use edn::symbols::Keyword;
use uuid::Uuid;

use crate::clock::Instant;
use crate::index::IndexType;
use crate::ops::DataType;
use crate::protocol::{
    data_type_tag, micros_to_datetime, TAG_BIG_INT, TAG_BOOLEAN, TAG_BYTES, TAG_DOUBLE, TAG_FLOAT,
    TAG_INSTANT, TAG_KEYWORD, TAG_LONG, TAG_MAP, TAG_STRING, TAG_TUPLE, TAG_UUID, TAG_VECTOR,
};

// ---------------------------------------------------------------------------
// Index key constants
// ---------------------------------------------------------------------------

// TODO should this become 2 bytes?
pub const INDEX_VERSION: u8 = 1;

// TODO use these throughout the codebase
pub const CODEC_LENGTH: usize = 1;
// Entity IDs are encoded as DataType::Long (1-byte type tag + 8-byte order-preserving i64).
pub const ENTITY_LENGTH: usize = 9;
pub const ATTRIBUTE_LENGTH: usize = 8;
pub const OP_LENGTH: usize = 1;
pub const TX_EID_LENGTH: usize = 8;

pub const EAV: u8 = 0;
pub const AVE: u8 = 1;
pub const AEV: u8 = 2;
pub const AE: u8 = 3;
pub const AV: u8 = 4;

pub const STATS_INDEX: u8 = 128;
pub const META_INDEX: u8 = 129;

pub const ADD: u8 = 0;
pub const DELETE: u8 = 1;
pub const RETRACT: u8 = 2;

/// Suffix length: tx_eid (8 bytes) + op (1 byte), used for end-of-key extraction.
pub const TX_EID_OP_SUFFIX: usize = TX_EID_LENGTH + OP_LENGTH;

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

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct DecodeError {
    pub message: String,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DecodeError: {}", self.message)
    }
}

impl std::error::Error for DecodeError {}

impl From<String> for DecodeError {
    fn from(message: String) -> Self {
        DecodeError { message }
    }
}

impl From<&str> for DecodeError {
    fn from(message: &str) -> Self {
        DecodeError {
            message: message.to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// Traits
// ---------------------------------------------------------------------------

pub trait Encode {
    fn encode(&self) -> Vec<u8>;
}

pub trait Decode: Sized {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError>;
}

// ---------------------------------------------------------------------------
// Signed integers — XOR sign bit + big-endian
// ---------------------------------------------------------------------------

pub fn encode_i64(value: i64, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&encode_i64_bytes(value));
}

pub fn encode_i64_bytes(value: i64) -> [u8; 8] {
    (value ^ i64::MAX).to_be_bytes()
}

pub fn decode_i64(cursor: &mut &[u8]) -> Result<i64, DecodeError> {
    if cursor.len() < 8 {
        return Err("not enough bytes for i64".into());
    }
    let bytes: [u8; 8] = cursor[..8].try_into().unwrap();
    *cursor = &cursor[8..];
    Ok(i64::from_be_bytes(bytes) ^ i64::MAX)
}

pub fn encode_i128(value: i128, buf: &mut Vec<u8>) {
    let encoded = (value ^ i128::MAX).to_be_bytes();
    buf.extend_from_slice(&encoded);
}

pub fn decode_i128(cursor: &mut &[u8]) -> Result<i128, DecodeError> {
    if cursor.len() < 16 {
        return Err("not enough bytes for i128".into());
    }
    let bytes: [u8; 16] = cursor[..16].try_into().unwrap();
    *cursor = &cursor[16..];
    Ok(i128::from_be_bytes(bytes) ^ i128::MAX)
}

// ---------------------------------------------------------------------------
// Unsigned integers — big-endian
// ---------------------------------------------------------------------------

pub fn encode_u64(value: u64, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&value.to_be_bytes());
}

pub fn decode_u64(cursor: &mut &[u8]) -> Result<u64, DecodeError> {
    if cursor.len() < 8 {
        return Err("not enough bytes for u64".into());
    }
    let bytes: [u8; 8] = cursor[..8].try_into().unwrap();
    *cursor = &cursor[8..];
    Ok(u64::from_be_bytes(bytes))
}

// ---------------------------------------------------------------------------
// Floating point — IEEE 754 sortable encoding
// ---------------------------------------------------------------------------

pub fn encode_f64(value: f64, buf: &mut Vec<u8>) {
    let bits = value.to_bits();
    let sign_mask: u64 = 0x8000_0000_0000_0000;
    let sortable = if bits & sign_mask != 0 {
        !bits
    } else {
        bits ^ sign_mask
    };
    buf.extend_from_slice(&sortable.to_be_bytes());
}

pub fn decode_f64(cursor: &mut &[u8]) -> Result<f64, DecodeError> {
    if cursor.len() < 8 {
        return Err("not enough bytes for f64".into());
    }
    let bytes: [u8; 8] = cursor[..8].try_into().unwrap();
    *cursor = &cursor[8..];
    let sortable = u64::from_be_bytes(bytes);
    let sign_mask: u64 = 0x8000_0000_0000_0000;
    let bits = if sortable & sign_mask != 0 {
        sortable ^ sign_mask
    } else {
        !sortable
    };
    Ok(f64::from_bits(bits))
}

pub fn encode_f32(value: f32, buf: &mut Vec<u8>) {
    let bits = value.to_bits();
    let sign_mask: u32 = 0x8000_0000;
    let sortable = if bits & sign_mask != 0 {
        !bits
    } else {
        bits ^ sign_mask
    };
    buf.extend_from_slice(&sortable.to_be_bytes());
}

pub fn decode_f32(cursor: &mut &[u8]) -> Result<f32, DecodeError> {
    if cursor.len() < 4 {
        return Err("not enough bytes for f32".into());
    }
    let bytes: [u8; 4] = cursor[..4].try_into().unwrap();
    *cursor = &cursor[4..];
    let sortable = u32::from_be_bytes(bytes);
    let sign_mask: u32 = 0x8000_0000;
    let bits = if sortable & sign_mask != 0 {
        sortable ^ sign_mask
    } else {
        !sortable
    };
    Ok(f32::from_bits(bits))
}

// ---------------------------------------------------------------------------
// Booleans
// ---------------------------------------------------------------------------

pub fn encode_bool(value: bool, buf: &mut Vec<u8>) {
    buf.push(if value { 0x01 } else { 0x00 });
}

pub fn decode_bool(cursor: &mut &[u8]) -> Result<bool, DecodeError> {
    if cursor.is_empty() {
        return Err("not enough bytes for bool".into());
    }
    let byte = cursor[0];
    *cursor = &cursor[1..];
    match byte {
        0x00 => Ok(false),
        0x01 => Ok(true),
        _ => Err(format!("invalid bool byte: 0x{:02x}", byte).into()),
    }
}

// ---------------------------------------------------------------------------
// Variable-length — escaped terminated encoding
// ---------------------------------------------------------------------------

pub fn encode_bytes(value: &[u8], buf: &mut Vec<u8>) {
    for &b in value {
        match b {
            0x00 => {
                buf.push(0x01);
                buf.push(0x01);
            }
            0x01 => {
                buf.push(0x01);
                buf.push(0x02);
            }
            _ => buf.push(b),
        }
    }
    buf.push(0x00); // terminator
}

pub fn decode_bytes(cursor: &mut &[u8]) -> Result<Vec<u8>, DecodeError> {
    let mut result = Vec::new();
    loop {
        if cursor.is_empty() {
            return Err("unexpected end of input in bytes".into());
        }
        let b = cursor[0];
        *cursor = &cursor[1..];
        match b {
            0x00 => return Ok(result),
            0x01 => {
                if cursor.is_empty() {
                    return Err("unexpected end of input after escape byte".into());
                }
                let next = cursor[0];
                *cursor = &cursor[1..];
                match next {
                    0x01 => result.push(0x00),
                    0x02 => result.push(0x01),
                    _ => {
                        return Err(format!("invalid escape sequence: 0x01 0x{:02x}", next).into());
                    }
                }
            }
            _ => result.push(b),
        }
    }
}

pub fn encode_string(value: &str, buf: &mut Vec<u8>) {
    encode_bytes(value.as_bytes(), buf);
}

pub fn decode_string(cursor: &mut &[u8]) -> Result<String, DecodeError> {
    let bytes = decode_bytes(cursor)?;
    String::from_utf8(bytes).map_err(|e| format!("invalid UTF-8: {}", e).into())
}

// ---------------------------------------------------------------------------
// Derived types
// ---------------------------------------------------------------------------

pub fn encode_instant(value: &DateTime<Utc>, buf: &mut Vec<u8>) {
    encode_i64(value.timestamp_micros(), buf);
}

pub fn decode_instant(cursor: &mut &[u8]) -> Result<DateTime<Utc>, DecodeError> {
    let micros = decode_i64(cursor)?;
    micros_to_datetime(micros).map_err(|e| format!("invalid instant: {}", e).into())
}

pub fn encode_uuid(value: &Uuid, buf: &mut Vec<u8>) {
    buf.extend_from_slice(value.as_bytes());
}

pub fn decode_uuid(cursor: &mut &[u8]) -> Result<Uuid, DecodeError> {
    if cursor.len() < 16 {
        return Err("not enough bytes for UUID".into());
    }
    let bytes: [u8; 16] = cursor[..16].try_into().unwrap();
    *cursor = &cursor[16..];
    Ok(Uuid::from_bytes(bytes))
}

pub fn encode_keyword(value: &Keyword, buf: &mut Vec<u8>) {
    let ns = value.namespace().unwrap_or("");
    encode_string(ns, buf);
    encode_string(value.name(), buf);
}

pub fn decode_keyword(cursor: &mut &[u8]) -> Result<Keyword, DecodeError> {
    let ns = decode_string(cursor)?;
    let name = decode_string(cursor)?;
    if ns.is_empty() {
        Ok(Keyword::plain(name))
    } else {
        Ok(Keyword::namespaced(ns, name))
    }
}

// ---------------------------------------------------------------------------
// Tagged DataType dispatch
// ---------------------------------------------------------------------------

pub fn encode_datatype(value: &DataType, buf: &mut Vec<u8>) {
    buf.push(data_type_tag(value));
    match value {
        DataType::BigInt(v) => encode_i128(*v, buf),
        DataType::Boolean(v) => encode_bool(*v, buf),
        DataType::Bytes(v) => encode_bytes(v, buf),
        DataType::Double(v) => encode_f64(*v, buf),
        DataType::Float(v) => encode_f32(*v, buf),
        DataType::Instant(v) => encode_instant(v, buf),
        DataType::Keyword(v) => encode_keyword(v, buf),
        DataType::Long(v) => encode_i64(*v, buf),
        DataType::String(v) => encode_string(v, buf),
        DataType::Tuple(v) => encode_composite(v, buf),
        DataType::Uuid(v) => encode_uuid(v, buf),
        DataType::Vector(v) => encode_composite(v, buf),
        DataType::Map(v) => encode_map(v, buf),
    }
}

fn encode_composite(elements: &[DataType], buf: &mut Vec<u8>) {
    for elem in elements {
        encode_datatype(elem, buf);
    }
    buf.push(0x00); // end-of-composite marker
}

fn encode_map(map: &BTreeMap<String, DataType>, buf: &mut Vec<u8>) {
    for (key, value) in map {
        buf.push(TAG_STRING);
        encode_string(key, buf);
        encode_datatype(value, buf);
    }
    buf.push(0x00); // end-of-composite marker
}

pub fn decode_datatype(cursor: &mut &[u8]) -> Result<DataType, DecodeError> {
    if cursor.is_empty() {
        return Err("unexpected end of input: expected type tag".into());
    }
    let tag = cursor[0];
    *cursor = &cursor[1..];
    decode_datatype_payload(tag, cursor)
}

fn decode_datatype_payload(tag: u8, cursor: &mut &[u8]) -> Result<DataType, DecodeError> {
    match tag {
        TAG_BIG_INT => Ok(DataType::BigInt(decode_i128(cursor)?)),
        TAG_BOOLEAN => Ok(DataType::Boolean(decode_bool(cursor)?)),
        TAG_BYTES => Ok(DataType::Bytes(decode_bytes(cursor)?)),
        TAG_DOUBLE => Ok(DataType::Double(decode_f64(cursor)?)),
        TAG_FLOAT => Ok(DataType::Float(decode_f32(cursor)?)),
        TAG_INSTANT => Ok(DataType::Instant(decode_instant(cursor)?)),
        TAG_KEYWORD => Ok(DataType::Keyword(decode_keyword(cursor)?)),
        TAG_LONG => Ok(DataType::Long(decode_i64(cursor)?)),
        TAG_STRING => Ok(DataType::String(decode_string(cursor)?)),
        TAG_TUPLE => Ok(DataType::Tuple(decode_composite(cursor)?)),
        TAG_UUID => Ok(DataType::Uuid(decode_uuid(cursor)?)),
        TAG_VECTOR => Ok(DataType::Vector(decode_composite(cursor)?)),
        TAG_MAP => Ok(DataType::Map(decode_map(cursor)?)),
        _ => Err(format!("unknown type tag: 0x{:02x}", tag).into()),
    }
}

fn decode_composite(cursor: &mut &[u8]) -> Result<Vec<DataType>, DecodeError> {
    let mut elements = Vec::new();
    loop {
        if cursor.is_empty() {
            return Err("unexpected end of input in composite".into());
        }
        if cursor[0] == 0x00 {
            *cursor = &cursor[1..]; // consume end marker
            return Ok(elements);
        }
        elements.push(decode_datatype(cursor)?);
    }
}

fn decode_map(cursor: &mut &[u8]) -> Result<BTreeMap<String, DataType>, DecodeError> {
    let mut map = BTreeMap::new();
    loop {
        if cursor.is_empty() {
            return Err("unexpected end of input in map".into());
        }
        if cursor[0] == 0x00 {
            *cursor = &cursor[1..]; // consume end marker
            return Ok(map);
        }
        // Read key: expect TAG_STRING
        let key_tag = cursor[0];
        *cursor = &cursor[1..];
        if key_tag != TAG_STRING {
            return Err(format!("expected TAG_STRING for map key, got 0x{:02x}", key_tag).into());
        }
        let key = decode_string(cursor)?;
        let value = decode_datatype(cursor)?;
        map.insert(key, value);
    }
}

// ---------------------------------------------------------------------------
// skip_datatype — advance cursor past one encoded DataType
// ---------------------------------------------------------------------------

pub fn skip_datatype(cursor: &mut &[u8]) -> Result<(), DecodeError> {
    if cursor.is_empty() {
        return Err("unexpected end of input: expected type tag".into());
    }
    let tag = cursor[0];
    *cursor = &cursor[1..];
    match tag {
        TAG_BIG_INT => skip_n(cursor, 16),
        TAG_BOOLEAN => skip_n(cursor, 1),
        TAG_BYTES => skip_terminated(cursor),
        TAG_DOUBLE => skip_n(cursor, 8),
        TAG_FLOAT => skip_n(cursor, 4),
        TAG_INSTANT => skip_n(cursor, 8),
        TAG_KEYWORD => {
            skip_terminated(cursor)?;
            skip_terminated(cursor)
        }
        TAG_LONG => skip_n(cursor, 8),
        TAG_STRING => skip_terminated(cursor),
        TAG_UUID => skip_n(cursor, 16),
        TAG_TUPLE | TAG_VECTOR => skip_composite(cursor),
        TAG_MAP => skip_map(cursor),
        _ => Err(format!("unknown type tag in skip: 0x{:02x}", tag).into()),
    }
}

fn skip_n(cursor: &mut &[u8], n: usize) -> Result<(), DecodeError> {
    if cursor.len() < n {
        return Err("not enough bytes to skip".into());
    }
    *cursor = &cursor[n..];
    Ok(())
}

fn skip_terminated(cursor: &mut &[u8]) -> Result<(), DecodeError> {
    loop {
        if cursor.is_empty() {
            return Err("unexpected end of input skipping terminated bytes".into());
        }
        let b = cursor[0];
        *cursor = &cursor[1..];
        match b {
            0x00 => return Ok(()),
            0x01 => {
                if cursor.is_empty() {
                    return Err("unexpected end of input after escape byte".into());
                }
                *cursor = &cursor[1..];
            }
            _ => {}
        }
    }
}

fn skip_composite(cursor: &mut &[u8]) -> Result<(), DecodeError> {
    loop {
        if cursor.is_empty() {
            return Err("unexpected end of input in composite".into());
        }
        if cursor[0] == 0x00 {
            *cursor = &cursor[1..];
            return Ok(());
        }
        skip_datatype(cursor)?;
    }
}

fn skip_map(cursor: &mut &[u8]) -> Result<(), DecodeError> {
    loop {
        if cursor.is_empty() {
            return Err("unexpected end of input in map".into());
        }
        if cursor[0] == 0x00 {
            *cursor = &cursor[1..];
            return Ok(());
        }
        skip_datatype(cursor)?; // key (TAG_STRING + terminated)
        skip_datatype(cursor)?; // value
    }
}

// ---------------------------------------------------------------------------
// Trait implementations on DataType
// ---------------------------------------------------------------------------

impl Encode for DataType {
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        encode_datatype(self, &mut buf);
        buf
    }
}

impl Decode for DataType {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        let mut cursor = buf;
        let result = decode_datatype(&mut cursor)?;
        if !cursor.is_empty() {
            return Err(format!(
                "trailing bytes after decode: {} bytes remaining",
                cursor.len()
            )
            .into());
        }
        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use edn::kw;

    // --- Spec value table tests ---

    #[test]
    fn i64_encoding_table() {
        let cases: Vec<(i64, Vec<u8>)> = vec![
            (
                i64::MIN,
                vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
            ),
            (-1, vec![0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
            (0, vec![0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]),
            (1, vec![0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE]),
            (
                i64::MAX,
                vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            ),
        ];
        for (value, expected) in cases {
            let mut buf = Vec::new();
            encode_i64(value, &mut buf);
            assert_eq!(buf, expected, "i64 {} encoded incorrectly", value);
        }
    }

    #[test]
    fn bytes_encoding_table() {
        let cases: Vec<(&[u8], Vec<u8>)> = vec![
            (&[], vec![0x00]),
            (b"hello", vec![b'h', b'e', b'l', b'l', b'o', 0x00]),
            (&[0x00], vec![0x01, 0x01, 0x00]),
            (&[0x01], vec![0x01, 0x02, 0x00]),
            (&[0x00, 0x01], vec![0x01, 0x01, 0x01, 0x02, 0x00]),
        ];
        for (input, expected) in cases {
            let mut buf = Vec::new();
            encode_bytes(input, &mut buf);
            assert_eq!(buf, expected, "bytes {:?} encoded incorrectly", input);
        }
    }

    // --- Round-trip tests ---

    #[test]
    fn roundtrip_i64() {
        for &v in &[i64::MIN, -1_000_000, -1, 0, 1, 1_000_000, i64::MAX] {
            let mut buf = Vec::new();
            encode_i64(v, &mut buf);
            let mut cursor = buf.as_slice();
            assert_eq!(decode_i64(&mut cursor).unwrap(), v);
            assert!(cursor.is_empty());
        }
    }

    #[test]
    fn roundtrip_i128() {
        for &v in &[i128::MIN, -1, 0, 1, i128::MAX] {
            let mut buf = Vec::new();
            encode_i128(v, &mut buf);
            let mut cursor = buf.as_slice();
            assert_eq!(decode_i128(&mut cursor).unwrap(), v);
            assert!(cursor.is_empty());
        }
    }

    #[test]
    fn roundtrip_u64() {
        for &v in &[0u64, 1, u64::MAX / 2, u64::MAX] {
            let mut buf = Vec::new();
            encode_u64(v, &mut buf);
            let mut cursor = buf.as_slice();
            assert_eq!(decode_u64(&mut cursor).unwrap(), v);
            assert!(cursor.is_empty());
        }
    }

    #[test]
    fn roundtrip_f64() {
        for &v in &[
            f64::NEG_INFINITY,
            -1.5,
            -0.0,
            0.0,
            1.5,
            f64::INFINITY,
            f64::NAN,
        ] {
            let mut buf = Vec::new();
            encode_f64(v, &mut buf);
            let mut cursor = buf.as_slice();
            let decoded = decode_f64(&mut cursor).unwrap();
            if v.is_nan() {
                assert!(decoded.is_nan());
            } else {
                assert_eq!(
                    decoded.to_bits(),
                    v.to_bits(),
                    "f64 {} round-trip failed",
                    v
                );
            }
            assert!(cursor.is_empty());
        }
    }

    #[test]
    fn roundtrip_f32() {
        for &v in &[
            f32::NEG_INFINITY,
            -1.5,
            -0.0,
            0.0,
            1.5,
            f32::INFINITY,
            f32::NAN,
        ] {
            let mut buf = Vec::new();
            encode_f32(v, &mut buf);
            let mut cursor = buf.as_slice();
            let decoded = decode_f32(&mut cursor).unwrap();
            if v.is_nan() {
                assert!(decoded.is_nan());
            } else {
                assert_eq!(
                    decoded.to_bits(),
                    v.to_bits(),
                    "f32 {} round-trip failed",
                    v
                );
            }
            assert!(cursor.is_empty());
        }
    }

    #[test]
    fn roundtrip_bool() {
        for &v in &[false, true] {
            let mut buf = Vec::new();
            encode_bool(v, &mut buf);
            let mut cursor = buf.as_slice();
            assert_eq!(decode_bool(&mut cursor).unwrap(), v);
            assert!(cursor.is_empty());
        }
    }

    #[test]
    fn roundtrip_bytes() {
        let cases: Vec<Vec<u8>> = vec![
            vec![],
            vec![0x00],
            vec![0x01],
            vec![0x00, 0x01, 0x02, 0xFF],
            b"hello world".to_vec(),
        ];
        for input in cases {
            let mut buf = Vec::new();
            encode_bytes(&input, &mut buf);
            let mut cursor = buf.as_slice();
            assert_eq!(decode_bytes(&mut cursor).unwrap(), input);
            assert!(cursor.is_empty());
        }
    }

    #[test]
    fn roundtrip_string() {
        for v in &["", "hello", "hello world", "\u{1F600}", "abc\x00def"] {
            let mut buf = Vec::new();
            encode_string(v, &mut buf);
            let mut cursor = buf.as_slice();
            assert_eq!(decode_string(&mut cursor).unwrap(), *v);
            assert!(cursor.is_empty());
        }
    }

    #[test]
    fn roundtrip_instant() {
        let instants = vec![
            Utc.timestamp_opt(0, 0).unwrap(),
            Utc.timestamp_opt(1_000_000, 0).unwrap(),
            Utc.timestamp_opt(-1, 999_000_000).unwrap(), // just before epoch
            Utc.timestamp_micros(1_234_567_890_123_456).unwrap(),
        ];
        for v in instants {
            let mut buf = Vec::new();
            encode_instant(&v, &mut buf);
            let mut cursor = buf.as_slice();
            assert_eq!(decode_instant(&mut cursor).unwrap(), v);
            assert!(cursor.is_empty());
        }
    }

    #[test]
    fn roundtrip_uuid() {
        let uuids = vec![
            Uuid::nil(),
            Uuid::max(),
            Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
        ];
        for v in uuids {
            let mut buf = Vec::new();
            encode_uuid(&v, &mut buf);
            let mut cursor = buf.as_slice();
            assert_eq!(decode_uuid(&mut cursor).unwrap(), v);
            assert!(cursor.is_empty());
        }
    }

    #[test]
    fn roundtrip_keyword() {
        let keywords = vec![kw!(:foo), kw!(:db/ident), kw!(:my.ns/attr)];
        for v in keywords {
            let mut buf = Vec::new();
            encode_keyword(&v, &mut buf);
            let mut cursor = buf.as_slice();
            assert_eq!(decode_keyword(&mut cursor).unwrap(), v);
            assert!(cursor.is_empty());
        }
    }

    // --- DataType round-trip tests ---

    #[test]
    fn roundtrip_datatype_all_variants() {
        let values = vec![
            DataType::BigInt(42),
            DataType::BigInt(i128::MIN),
            DataType::Boolean(true),
            DataType::Boolean(false),
            DataType::Bytes(vec![0x00, 0x01, 0xFF]),
            DataType::Double(3.15),
            DataType::Float(2.5),
            DataType::Instant(Utc.timestamp_opt(1_000_000, 0).unwrap()),
            DataType::Keyword(kw!(:foo)),
            DataType::Keyword(kw!(:db/ident)),
            DataType::Long(42),
            DataType::Long(i64::MIN),
            DataType::String("hello".to_string()),
            DataType::String("".to_string()),
            DataType::Tuple(vec![DataType::Long(1), DataType::String("a".to_string())]),
            DataType::Uuid(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()),
            DataType::Vector(vec![DataType::Long(1), DataType::Long(2)]),
            DataType::Map({
                let mut m = BTreeMap::new();
                m.insert("key".to_string(), DataType::Long(42));
                m
            }),
        ];
        for v in values {
            let encoded = v.encode();
            let decoded = DataType::decode(&encoded).unwrap();
            assert_eq!(decoded, v, "DataType round-trip failed for {:?}", v);
        }
    }

    #[test]
    fn roundtrip_datatype_empty_composites() {
        let values = vec![
            DataType::Tuple(vec![]),
            DataType::Vector(vec![]),
            DataType::Map(BTreeMap::new()),
            DataType::Bytes(vec![]),
            DataType::String(String::new()),
        ];
        for v in values {
            let encoded = v.encode();
            let decoded = DataType::decode(&encoded).unwrap();
            assert_eq!(decoded, v);
        }
    }

    #[test]
    fn roundtrip_datatype_nested_composite() {
        let nested = DataType::Vector(vec![
            DataType::Tuple(vec![
                DataType::Long(1),
                DataType::String("inner".to_string()),
            ]),
            DataType::Map({
                let mut m = BTreeMap::new();
                m.insert("k".to_string(), DataType::Boolean(true));
                m
            }),
        ]);
        let encoded = nested.encode();
        let decoded = DataType::decode(&encoded).unwrap();
        assert_eq!(decoded, nested);
    }

    // --- Order-preservation tests ---

    #[test]
    fn order_i64() {
        let values = [i64::MIN, -1_000, -1, 0, 1, 1_000, i64::MAX];
        for window in values.windows(2) {
            let mut a_buf = Vec::new();
            let mut b_buf = Vec::new();
            encode_i64(window[0], &mut a_buf);
            encode_i64(window[1], &mut b_buf);
            assert!(
                a_buf > b_buf,
                "{} should encode after {}",
                window[0],
                window[1]
            );
        }
    }

    #[test]
    fn order_i128() {
        let values = [i128::MIN, -1, 0, 1, i128::MAX];
        for window in values.windows(2) {
            let mut a_buf = Vec::new();
            let mut b_buf = Vec::new();
            encode_i128(window[0], &mut a_buf);
            encode_i128(window[1], &mut b_buf);
            assert!(a_buf > b_buf);
        }
    }

    #[test]
    fn order_u64() {
        let values = [0u64, 1, 100, u64::MAX];
        for window in values.windows(2) {
            let mut a_buf = Vec::new();
            let mut b_buf = Vec::new();
            encode_u64(window[0], &mut a_buf);
            encode_u64(window[1], &mut b_buf);
            assert!(a_buf < b_buf);
        }
    }

    #[test]
    fn order_f64() {
        // Uses <= because -0.0 and 0.0 encode to the same bytes (IEEE 754 sortable
        // encoding collapses them). Round-trip preserves the sign bit, but byte
        // ordering treats them as equal.
        let values = [f64::NEG_INFINITY, -1.5, -0.0, 0.0, 1.5, f64::INFINITY];
        for window in values.windows(2) {
            let mut a_buf = Vec::new();
            let mut b_buf = Vec::new();
            encode_f64(window[0], &mut a_buf);
            encode_f64(window[1], &mut b_buf);
            assert!(
                a_buf <= b_buf,
                "{} should encode <= {}",
                window[0],
                window[1]
            );
        }
    }

    #[test]
    fn order_f32() {
        let values = [f32::NEG_INFINITY, -1.5, 0.0, 1.5, f32::INFINITY];
        for window in values.windows(2) {
            let mut a_buf = Vec::new();
            let mut b_buf = Vec::new();
            encode_f32(window[0], &mut a_buf);
            encode_f32(window[1], &mut b_buf);
            assert!(a_buf < b_buf);
        }
    }

    #[test]
    fn order_bool() {
        let mut f_buf = Vec::new();
        let mut t_buf = Vec::new();
        encode_bool(false, &mut f_buf);
        encode_bool(true, &mut t_buf);
        assert!(f_buf < t_buf);
    }

    #[test]
    fn order_string() {
        let values = ["", "a", "aa", "ab", "b"];
        for window in values.windows(2) {
            let mut a_buf = Vec::new();
            let mut b_buf = Vec::new();
            encode_string(window[0], &mut a_buf);
            encode_string(window[1], &mut b_buf);
            assert!(
                a_buf < b_buf,
                "{:?} should encode before {:?}",
                window[0],
                window[1]
            );
        }
    }

    #[test]
    fn order_bytes() {
        let values: Vec<Vec<u8>> =
            vec![vec![], vec![0x00], vec![0x00, 0x00], vec![0x01], vec![0xFF]];
        for window in values.windows(2) {
            let mut a_buf = Vec::new();
            let mut b_buf = Vec::new();
            encode_bytes(&window[0], &mut a_buf);
            encode_bytes(&window[1], &mut b_buf);
            assert!(
                a_buf < b_buf,
                "{:?} should encode before {:?}",
                window[0],
                window[1]
            );
        }
    }

    #[test]
    fn order_keyword() {
        let values = [
            kw!(:a), // empty ns sorts first
            kw!(:b),
            kw!(:a/a),
            kw!(:a/b),
            kw!(:b/a),
        ];
        for window in values.windows(2) {
            let mut a_buf = Vec::new();
            let mut b_buf = Vec::new();
            encode_keyword(&window[0], &mut a_buf);
            encode_keyword(&window[1], &mut b_buf);
            assert!(
                a_buf < b_buf,
                "{:?} should encode before {:?}",
                window[0],
                window[1]
            );
        }
    }

    // --- Prefix-free tests ---

    #[test]
    fn prefix_free_strings() {
        let short = DataType::String("abc".to_string()).encode();
        let long = DataType::String("abcd".to_string()).encode();
        assert!(!long.starts_with(&short));
    }

    #[test]
    fn prefix_free_bytes() {
        let short = DataType::Bytes(vec![1, 2, 3]).encode();
        let long = DataType::Bytes(vec![1, 2, 3, 4]).encode();
        assert!(!long.starts_with(&short));
    }

    // --- Composite ordering tests ---

    #[test]
    fn tuple_prefix_ordering() {
        let short = DataType::Tuple(vec![DataType::Long(1)]).encode();
        let long = DataType::Tuple(vec![DataType::Long(1), DataType::Long(2)]).encode();
        assert!(short < long);
    }

    #[test]
    fn vector_prefix_ordering() {
        let short = DataType::Vector(vec![DataType::Long(1)]).encode();
        let long = DataType::Vector(vec![DataType::Long(1), DataType::Long(2)]).encode();
        assert!(short < long);
    }

    // --- skip_datatype tests ---

    #[test]
    fn skip_then_decode() {
        let first = DataType::String("skip me".to_string());
        let second = DataType::Long(42);

        let mut buf = Vec::new();
        encode_datatype(&first, &mut buf);
        encode_datatype(&second, &mut buf);

        let mut cursor = buf.as_slice();
        skip_datatype(&mut cursor).unwrap();
        let decoded = decode_datatype(&mut cursor).unwrap();
        assert_eq!(decoded, second);
        assert!(cursor.is_empty());
    }

    #[test]
    fn skip_all_types() {
        let values = vec![
            DataType::BigInt(1),
            DataType::Boolean(true),
            DataType::Bytes(vec![0, 1, 2]),
            DataType::Double(3.15),
            DataType::Float(2.5),
            DataType::Instant(Utc.timestamp_opt(1_000_000, 0).unwrap()),
            DataType::Keyword(kw!(:db/id)),
            DataType::Long(99),
            DataType::String("test".to_string()),
            DataType::Tuple(vec![DataType::Long(1)]),
            DataType::Uuid(Uuid::nil()),
            DataType::Vector(vec![DataType::Boolean(false)]),
            DataType::Map({
                let mut m = BTreeMap::new();
                m.insert("k".to_string(), DataType::Long(1));
                m
            }),
        ];

        // Encode all values into one buffer, then skip them all
        let mut buf = Vec::new();
        for v in &values {
            encode_datatype(v, &mut buf);
        }
        let sentinel = DataType::Long(999);
        encode_datatype(&sentinel, &mut buf);

        let mut cursor = buf.as_slice();
        for _ in &values {
            skip_datatype(&mut cursor).unwrap();
        }
        let decoded = decode_datatype(&mut cursor).unwrap();
        assert_eq!(decoded, sentinel);
        assert!(cursor.is_empty());
    }

    // --- Edge case: trailing bytes ---

    #[test]
    fn decode_trait_rejects_trailing_bytes() {
        let mut buf = DataType::Long(1).encode();
        buf.push(0xFF);
        assert!(DataType::decode(&buf).is_err());
    }
}
