//! Shared protocol constants and types used by the storage codec, the
//! HTTP/2 server, and the msgpack wire codec.
//!
//! The actual wire encoding lives in [`crate::msgpack_codec`]; this module
//! holds the data-type tag taxonomy (also used by the storage codec to keep
//! a single set of type identifiers across storage and wire), the error code
//! enum, and the column description used in query response schemas.

use anyhow::{anyhow, bail, Result};
use chrono::{DateTime, TimeZone, Utc};

use crate::ops::DataType;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convert microseconds since Unix epoch to a `DateTime<Utc>`.
///
/// Uses `div_euclid`/`rem_euclid` so that pre-epoch (negative) timestamps
/// are decoded correctly — Rust's `/` truncates toward zero which gives
/// wrong results for negative values.
pub fn micros_to_datetime(micros: i64) -> Result<DateTime<Utc>> {
    let secs = micros.div_euclid(1_000_000);
    let nanos = (micros.rem_euclid(1_000_000) as u32) * 1000;
    Utc.timestamp_opt(secs, nanos)
        .single()
        .ok_or_else(|| anyhow!("Invalid timestamp: {} micros", micros))
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default maximum HTTP body size (64 MB).
pub const DEFAULT_MAX_MESSAGE_SIZE: u32 = 64 * 1024 * 1024;

/// `ErrorResponse` severity byte for non-fatal errors.
pub const SEVERITY_ERROR: u8 = b'E';
/// `ErrorResponse` severity byte for fatal errors.
pub const SEVERITY_FATAL: u8 = b'F';

// DataType tag bytes — shared between the storage codec (`crate::codec`)
// and column descriptions in query responses.
pub const TAG_BIG_INT: u8 = 1;
pub const TAG_BOOLEAN: u8 = 2;
pub const TAG_BYTES: u8 = 3;
pub const TAG_DOUBLE: u8 = 4;
pub const TAG_FLOAT: u8 = 5;
pub const TAG_INSTANT: u8 = 6;
pub const TAG_LONG: u8 = 7;
/// Reserved for a future `DataType::Ref` variant.
pub const TAG_REF: u8 = 8;
pub const TAG_STRING: u8 = 9;
pub const TAG_UUID: u8 = 10;
pub const TAG_VECTOR: u8 = 11;
pub const TAG_MAP: u8 = 12;
pub const TAG_KEYWORD: u8 = 13;
/// Column-description-only tag for unions of concrete types.
pub const TAG_UNION: u8 = 127;
/// Column-description-only tag for indeterminate column types.
pub const TAG_UNKNOWN: u8 = 255;

// ---------------------------------------------------------------------------
// Error Codes
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum ErrorCode {
    // Connection errors (1xxx)
    ProtocolVersionMismatch = 1000,
    InvalidStartup = 1001,
    // Query errors (2xxx)
    ParseError = 2000,
    QueryError = 2001,
    InvalidQuery = 2002,
    EmptyQuery = 2003,
    // Transaction errors (3xxx)
    TxError = 3000,
    TxAborted = 3001,
    // Internal/protocol errors (5xxx)
    InternalError = 5000,
    MessageTooLarge = 5001,
    InvalidMessageType = 5002,
    QueryCancelled = 5003,
    ServerShuttingDown = 5004,
    // DB handle errors (6xxx)
    InvalidDbHandle = 6000,
    TooManyOpenDbs = 6001,
}

impl ErrorCode {
    pub fn from_u16(code: u16) -> Result<Self> {
        match code {
            1000 => Ok(ErrorCode::ProtocolVersionMismatch),
            1001 => Ok(ErrorCode::InvalidStartup),
            2000 => Ok(ErrorCode::ParseError),
            2001 => Ok(ErrorCode::QueryError),
            2002 => Ok(ErrorCode::InvalidQuery),
            2003 => Ok(ErrorCode::EmptyQuery),
            3000 => Ok(ErrorCode::TxError),
            3001 => Ok(ErrorCode::TxAborted),
            5000 => Ok(ErrorCode::InternalError),
            5001 => Ok(ErrorCode::MessageTooLarge),
            5002 => Ok(ErrorCode::InvalidMessageType),
            5003 => Ok(ErrorCode::QueryCancelled),
            5004 => Ok(ErrorCode::ServerShuttingDown),
            6000 => Ok(ErrorCode::InvalidDbHandle),
            6001 => Ok(ErrorCode::TooManyOpenDbs),
            _ => bail!("Unknown error code: {}", code),
        }
    }

    pub fn as_u16(self) -> u16 {
        self as u16
    }
}

// ---------------------------------------------------------------------------
// Column Description
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDescription {
    pub name: String,
    pub data_type: u8,
    /// For Union columns (`data_type == TAG_UNION`), the concrete member type
    /// tags. `None` for concrete types and `TAG_UNKNOWN`.
    pub members: Option<Vec<u8>>,
}

// ---------------------------------------------------------------------------
// DataType tag dispatch
// ---------------------------------------------------------------------------

/// Returns the DataType tag byte for a value (see `TAG_*` constants).
pub fn data_type_tag(dt: &DataType) -> u8 {
    match dt {
        DataType::BigInt(_) => TAG_BIG_INT,
        DataType::Boolean(_) => TAG_BOOLEAN,
        DataType::Bytes(_) => TAG_BYTES,
        DataType::Double(_) => TAG_DOUBLE,
        DataType::Float(_) => TAG_FLOAT,
        DataType::Instant(_) => TAG_INSTANT,
        DataType::Keyword(_) => TAG_KEYWORD,
        DataType::Long(_) => TAG_LONG,
        DataType::String(_) => TAG_STRING,
        DataType::Uuid(_) => TAG_UUID,
        DataType::Vector(_) => TAG_VECTOR,
        DataType::Map(_) => TAG_MAP,
    }
}
