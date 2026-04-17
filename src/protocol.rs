//! Wire protocol codec for the Triplox client/server architecture.
//!
//! Implements the binary protocol defined in `design/WIRE_PROTOCOL.md` (version 0.1).
//! Provides message types, framing, and encoding/decoding for DataType and TxOp values.

use std::collections::BTreeMap;

use anyhow::{anyhow, bail, Result};
use chrono::{DateTime, TimeZone, Utc};
use edn::symbols::Keyword;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use uuid::Uuid;

use std::str::FromStr;

use crate::ops::{DataType, EntityRef, QueryArg, TxOp};

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

/// Protocol version
pub const PROTOCOL_VERSION_MAJOR: u16 = 0;
pub const PROTOCOL_VERSION_MINOR: u16 = 1;

/// Default maximum message size (64 MB)
pub const DEFAULT_MAX_MESSAGE_SIZE: u32 = 64 * 1024 * 1024;

// Frontend message type bytes
pub const MSG_OPEN_DB: u8 = b'O';
pub const MSG_CLOSE_DB: u8 = b'L';
pub const MSG_QUERY: u8 = b'Q';
pub const MSG_EXECUTE: u8 = b'E';
pub const MSG_SUBSCRIBE: u8 = b'S';
pub const MSG_UNSUBSCRIBE: u8 = b'U';
pub const MSG_TERMINATE: u8 = b'X';

// Backend message type bytes
pub const MSG_AUTHENTICATION_OK: u8 = b'R';
pub const MSG_DB_OPENED: u8 = b'H';
pub const MSG_DB_CLOSED: u8 = b'J';
pub const MSG_ROW_DESCRIPTION: u8 = b'T';
pub const MSG_DATA_ROW: u8 = b'D';
pub const MSG_DATA_BATCH_COMPLETE: u8 = b'B';
pub const MSG_READY_FOR_QUERY: u8 = b'Z';
pub const MSG_TX_KEY: u8 = b'Y';
pub const MSG_TX_RESULT: u8 = b'G';
pub const MSG_UNSUBSCRIBE_COMPLETE: u8 = b'N';
pub const MSG_HEARTBEAT: u8 = b'K';
pub const MSG_ERROR_RESPONSE: u8 = b'W';

// ReadyForQuery status bytes
pub const STATUS_IDLE: u8 = b'I';
pub const STATUS_SUBSCRIBED: u8 = b'S';

// ErrorResponse severity bytes
pub const SEVERITY_ERROR: u8 = b'E';
pub const SEVERITY_FATAL: u8 = b'F';

// DataType tag bytes
pub const TAG_BIG_INT: u8 = 1;
pub const TAG_BOOLEAN: u8 = 2;
pub const TAG_BYTES: u8 = 3;
pub const TAG_DOUBLE: u8 = 4;
pub const TAG_FLOAT: u8 = 5;
pub const TAG_INSTANT: u8 = 6;
pub const TAG_LONG: u8 = 7;
pub const TAG_REF: u8 = 8;
pub const TAG_STRING: u8 = 9;
pub const TAG_TUPLE: u8 = 10;
pub const TAG_UUID: u8 = 11;
pub const TAG_VECTOR: u8 = 12;
pub const TAG_MAP: u8 = 13;
pub const TAG_KEYWORD: u8 = 14;
pub const TAG_UNKNOWN: u8 = 255;

// TxOp tag bytes
pub const TXOP_PUT: u8 = 0;
pub const TXOP_ADD: u8 = 1;
pub const TXOP_RETRACT: u8 = 2;
pub const TXOP_DELETE: u8 = 3;
pub const TXOP_ERASE: u8 = 4;

// EntityRef tag bytes
pub const ENTITY_REF_ID: u8 = 0x90;
pub const ENTITY_REF_TEMPID: u8 = 0x91;
pub const ENTITY_REF_IDENT: u8 = 0x92;
pub const ENTITY_REF_LOOKUP: u8 = 0x93;

// QueryArg tag bytes
pub const QUERY_ARG_SCALAR: u8 = 0;
pub const QUERY_ARG_COLLECTION: u8 = 1;
pub const QUERY_ARG_TUPLE: u8 = 2;
pub const QUERY_ARG_RELATION: u8 = 3;

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
    // Subscription errors (4xxx)
    SubscriptionError = 4000,
    SubscriptionTimeout = 4001,
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
            4000 => Ok(ErrorCode::SubscriptionError),
            4001 => Ok(ErrorCode::SubscriptionTimeout),
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
}

// ---------------------------------------------------------------------------
// Frontend Messages (Client → Server)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum FrontendMessage {
    Startup {
        version_major: u16,
        version_minor: u16,
        params: BTreeMap<String, String>,
    },
    OpenDb {
        tx_id: Option<i64>,
        system_time: Option<i64>,
    },
    CloseDb {
        db_id: u32,
    },
    Query {
        query_string: String,
        db_id: u32,
        args: Vec<QueryArg>,
    },
    Execute {
        ops: Vec<TxOp>,
        await_indexing: bool,
    },
    Subscribe {
        query_string: String,
        db_id: u32,
        args: Vec<QueryArg>,
    },
    Unsubscribe,
    Terminate,
}

// ---------------------------------------------------------------------------
// Backend Messages (Server → Client)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum BackendMessage {
    AuthenticationOk {
        server_version: String,
    },
    DbOpened {
        db_id: u32,
        tx_id: i64,
    },
    DbClosed {
        db_id: u32,
    },
    RowDescription {
        columns: Vec<ColumnDescription>,
    },
    DataRow {
        values: Vec<DataType>,
    },
    DataBatchComplete {
        tx_id: i64,
    },
    ReadyForQuery {
        status: u8,
    },
    TxKey {
        tx_id: i64,
        system_time: i64,
    },
    TxResult {
        status: u8,
        tx_id: i64,
        system_time: i64,
        error_message: Option<String>,
    },
    UnsubscribeComplete,
    Heartbeat,
    ErrorResponse {
        severity: u8,
        code: u16,
        message: String,
        detail: Option<String>,
        hint: Option<String>,
    },
}

// ---------------------------------------------------------------------------
// Wire Encoding: Primitives (to Vec<u8> buffer)
// ---------------------------------------------------------------------------

fn encode_bool(buf: &mut Vec<u8>, v: bool) {
    buf.push(if v { 0x01 } else { 0x00 });
}

fn encode_u8(buf: &mut Vec<u8>, v: u8) {
    buf.push(v);
}

fn encode_u16(buf: &mut Vec<u8>, v: u16) {
    buf.extend_from_slice(&v.to_be_bytes());
}

fn encode_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_be_bytes());
}

fn encode_i64(buf: &mut Vec<u8>, v: i64) {
    buf.extend_from_slice(&v.to_be_bytes());
}

fn encode_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_be_bytes());
}

fn encode_i128(buf: &mut Vec<u8>, v: i128) {
    buf.extend_from_slice(&v.to_be_bytes());
}

fn encode_f32(buf: &mut Vec<u8>, v: f32) {
    buf.extend_from_slice(&v.to_be_bytes());
}

fn encode_f64(buf: &mut Vec<u8>, v: f64) {
    buf.extend_from_slice(&v.to_be_bytes());
}

/// # Panics
/// Panics if `s` exceeds `u32::MAX` bytes. This is unreachable in practice
/// because `write_frontend_message`/`write_backend_message` enforce a 64 MB
/// message-size limit before encoding, which is well below `u32::MAX` (~4 GB).
fn encode_string(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    encode_u32(
        buf,
        u32::try_from(bytes.len()).expect("string exceeds u32::MAX bytes"),
    );
    buf.extend_from_slice(bytes);
}

/// # Panics
/// Panics if `b` exceeds `u32::MAX` bytes. Unreachable — see [`encode_string`].
fn encode_bytes(buf: &mut Vec<u8>, b: &[u8]) {
    encode_u32(
        buf,
        u32::try_from(b.len()).expect("byte array exceeds u32::MAX bytes"),
    );
    buf.extend_from_slice(b);
}

fn encode_option_i64(buf: &mut Vec<u8>, opt: &Option<i64>) {
    match opt {
        None => buf.push(0x00),
        Some(v) => {
            buf.push(0x01);
            encode_i64(buf, *v);
        }
    }
}

fn encode_option_u64(buf: &mut Vec<u8>, opt: &Option<u64>) {
    match opt {
        None => buf.push(0x00),
        Some(v) => {
            buf.push(0x01);
            encode_u64(buf, *v);
        }
    }
}

fn encode_option_string(buf: &mut Vec<u8>, opt: &Option<String>) {
    match opt {
        None => buf.push(0x00),
        Some(s) => {
            buf.push(0x01);
            encode_string(buf, s);
        }
    }
}

fn encode_string_map(buf: &mut Vec<u8>, map: &BTreeMap<String, String>) {
    encode_u32(buf, map.len() as u32);
    for (k, v) in map {
        encode_string(buf, k);
        encode_string(buf, v);
    }
}

// ---------------------------------------------------------------------------
// Wire Encoding: DataType
// ---------------------------------------------------------------------------

/// Returns the DataTypeTag byte for a DataType value.
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
        DataType::Tuple(_) => TAG_TUPLE,
        DataType::Uuid(_) => TAG_UUID,
        DataType::Vector(_) => TAG_VECTOR,
        DataType::Map(_) => TAG_MAP,
    }
}

fn encode_data_type(buf: &mut Vec<u8>, dt: &DataType) {
    encode_u8(buf, data_type_tag(dt));
    match dt {
        DataType::BigInt(v) => encode_i128(buf, *v),
        DataType::Boolean(v) => encode_bool(buf, *v),
        DataType::Bytes(v) => encode_bytes(buf, v),
        DataType::Double(v) => encode_f64(buf, *v),
        DataType::Float(v) => encode_f32(buf, *v),
        DataType::Instant(v) => encode_i64(buf, v.timestamp_micros()),
        DataType::Keyword(v) => encode_string(buf, &v.to_string()),
        DataType::Long(v) => encode_i64(buf, *v),
        DataType::String(v) => encode_string(buf, v),
        DataType::Tuple(v) => encode_data_type_vec(buf, v),
        DataType::Uuid(v) => buf.extend_from_slice(v.as_bytes()),
        DataType::Vector(v) => encode_data_type_vec(buf, v),
        DataType::Map(v) => encode_data_type_map(buf, v),
    }
}

fn encode_data_type_vec(buf: &mut Vec<u8>, vec: &[DataType]) {
    encode_u32(buf, vec.len() as u32);
    for dt in vec {
        encode_data_type(buf, dt);
    }
}

fn encode_data_type_map(buf: &mut Vec<u8>, map: &BTreeMap<String, DataType>) {
    encode_u32(buf, map.len() as u32);
    for (k, v) in map {
        encode_string(buf, k);
        encode_data_type(buf, v);
    }
}

// ---------------------------------------------------------------------------
// Wire Encoding: EntityRef / TxOp
// ---------------------------------------------------------------------------

fn encode_entity_ref(buf: &mut Vec<u8>, er: &EntityRef) {
    match er {
        EntityRef::Id(id) => {
            encode_u8(buf, ENTITY_REF_ID);
            encode_i64(buf, *id);
        }
        EntityRef::TempId(s) => {
            encode_u8(buf, ENTITY_REF_TEMPID);
            encode_string(buf, s);
        }
        EntityRef::Ident(kw) => {
            encode_u8(buf, ENTITY_REF_IDENT);
            encode_string(buf, &kw.to_string());
        }
        EntityRef::LookupRef(kw, dt) => {
            encode_u8(buf, ENTITY_REF_LOOKUP);
            encode_string(buf, &kw.to_string());
            encode_data_type(buf, dt);
        }
    }
}

fn encode_keyword_datatype_map(buf: &mut Vec<u8>, map: &BTreeMap<Keyword, DataType>) {
    encode_u32(buf, map.len() as u32);
    for (k, v) in map {
        encode_string(buf, &k.to_string());
        encode_data_type(buf, v);
    }
}

fn encode_tx_op(buf: &mut Vec<u8>, op: &TxOp) {
    match op {
        TxOp::Put(map) => {
            encode_u8(buf, TXOP_PUT);
            encode_keyword_datatype_map(buf, map);
        }
        TxOp::Add {
            entity,
            attribute,
            value,
        } => {
            encode_u8(buf, TXOP_ADD);
            encode_entity_ref(buf, entity);
            encode_string(buf, &attribute.to_string());
            encode_data_type(buf, value);
        }
        TxOp::Retract {
            entity,
            attribute,
            value,
        } => {
            encode_u8(buf, TXOP_RETRACT);
            encode_entity_ref(buf, entity);
            encode_string(buf, &attribute.to_string());
            encode_data_type(buf, value);
        }
        TxOp::Delete(er) => {
            encode_u8(buf, TXOP_DELETE);
            encode_entity_ref(buf, er);
        }
        TxOp::Erase(er) => {
            encode_u8(buf, TXOP_ERASE);
            encode_entity_ref(buf, er);
        }
    }
}

fn encode_tx_ops(buf: &mut Vec<u8>, ops: &[TxOp]) {
    encode_u32(buf, ops.len() as u32);
    for op in ops {
        encode_tx_op(buf, op);
    }
}

fn encode_query_arg(buf: &mut Vec<u8>, arg: &QueryArg) {
    match arg {
        QueryArg::Scalar(dt) => {
            encode_u8(buf, QUERY_ARG_SCALAR);
            encode_data_type(buf, dt);
        }
        QueryArg::Collection(items) => {
            encode_u8(buf, QUERY_ARG_COLLECTION);
            encode_data_type_vec(buf, items);
        }
        QueryArg::Tuple(items) => {
            encode_u8(buf, QUERY_ARG_TUPLE);
            encode_data_type_vec(buf, items);
        }
        QueryArg::Relation(rows) => {
            encode_u8(buf, QUERY_ARG_RELATION);
            encode_u32(buf, rows.len() as u32);
            for row in rows {
                encode_data_type_vec(buf, row);
            }
        }
    }
}

fn encode_query_args(buf: &mut Vec<u8>, args: &[QueryArg]) {
    encode_u32(buf, args.len() as u32);
    for arg in args {
        encode_query_arg(buf, arg);
    }
}

// ---------------------------------------------------------------------------
// Wire Decoding: Primitives (from &[u8] cursor)
// ---------------------------------------------------------------------------

/// A cursor over a byte slice for decoding.
pub(crate) struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    pub(crate) fn new(data: &'a [u8]) -> Self {
        Cursor { data, pos: 0 }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    fn read_bytes(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.pos + n > self.data.len() {
            bail!(
                "Unexpected end of message: need {} bytes, have {}",
                n,
                self.remaining()
            );
        }
        let slice = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let b = self.read_bytes(N)?;
        Ok(b.try_into().expect("read_bytes guarantees exact length"))
    }

    fn read_u8(&mut self) -> Result<u8> {
        let b = self.read_bytes(1)?;
        Ok(b[0])
    }

    fn read_bool(&mut self) -> Result<bool> {
        let b = self.read_u8()?;
        match b {
            0x00 => Ok(false),
            0x01 => Ok(true),
            _ => bail!("Invalid boolean byte: 0x{:02x}", b),
        }
    }

    fn read_u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.read_array()?))
    }

    fn read_u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.read_array()?))
    }

    fn read_i64(&mut self) -> Result<i64> {
        Ok(i64::from_be_bytes(self.read_array()?))
    }

    fn read_u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.read_array()?))
    }

    fn read_i128(&mut self) -> Result<i128> {
        Ok(i128::from_be_bytes(self.read_array()?))
    }

    fn read_f32(&mut self) -> Result<f32> {
        Ok(f32::from_be_bytes(self.read_array()?))
    }

    fn read_f64(&mut self) -> Result<f64> {
        Ok(f64::from_be_bytes(self.read_array()?))
    }

    fn read_string(&mut self) -> Result<String> {
        let len = self.read_u32()? as usize;
        let b = self.read_bytes(len)?;
        Ok(String::from_utf8(b.to_vec())?)
    }

    fn read_byte_array(&mut self) -> Result<Vec<u8>> {
        let len = self.read_u32()? as usize;
        let b = self.read_bytes(len)?;
        Ok(b.to_vec())
    }

    fn read_option_i64(&mut self) -> Result<Option<i64>> {
        let tag = self.read_u8()?;
        match tag {
            0x00 => Ok(None),
            0x01 => Ok(Some(self.read_i64()?)),
            _ => bail!("Invalid option tag: 0x{:02x}", tag),
        }
    }

    fn read_option_u64(&mut self) -> Result<Option<u64>> {
        let tag = self.read_u8()?;
        match tag {
            0x00 => Ok(None),
            0x01 => Ok(Some(self.read_u64()?)),
            _ => bail!("Invalid option tag: 0x{:02x}", tag),
        }
    }

    fn read_option_string(&mut self) -> Result<Option<String>> {
        let tag = self.read_u8()?;
        match tag {
            0x00 => Ok(None),
            0x01 => Ok(Some(self.read_string()?)),
            _ => bail!("Invalid option tag: 0x{:02x}", tag),
        }
    }

    fn read_string_map(&mut self) -> Result<BTreeMap<String, String>> {
        let count = self.read_u32()? as usize;
        if count > self.remaining() {
            bail!(
                "String map count {} exceeds remaining bytes {}",
                count,
                self.remaining()
            );
        }
        let mut map = BTreeMap::new();
        for _ in 0..count {
            let k = self.read_string()?;
            let v = self.read_string()?;
            map.insert(k, v);
        }
        Ok(map)
    }
}

// ---------------------------------------------------------------------------
// Wire Decoding: DataType
// ---------------------------------------------------------------------------

fn decode_data_type(cursor: &mut Cursor) -> Result<DataType> {
    let tag = cursor.read_u8()?;
    match tag {
        TAG_BIG_INT => Ok(DataType::BigInt(cursor.read_i128()?)),
        TAG_BOOLEAN => Ok(DataType::Boolean(cursor.read_bool()?)),
        TAG_BYTES => Ok(DataType::Bytes(cursor.read_byte_array()?)),
        TAG_DOUBLE => Ok(DataType::Double(cursor.read_f64()?)),
        TAG_FLOAT => Ok(DataType::Float(cursor.read_f32()?)),
        TAG_INSTANT => {
            let micros = cursor.read_i64()?;
            Ok(DataType::Instant(micros_to_datetime(micros)?))
        }
        TAG_LONG => Ok(DataType::Long(cursor.read_i64()?)),
        // TODO: support TAG_REF once DataType::Ref is added
        TAG_REF => bail!("TAG_REF is not currently supported"),
        TAG_STRING => Ok(DataType::String(cursor.read_string()?)),
        TAG_TUPLE => Ok(DataType::Tuple(decode_data_type_vec(cursor)?)),
        TAG_UUID => Ok(DataType::Uuid(Uuid::from_bytes(cursor.read_array()?))),
        TAG_VECTOR => Ok(DataType::Vector(decode_data_type_vec(cursor)?)),
        TAG_MAP => Ok(DataType::Map(decode_data_type_map(cursor)?)),
        TAG_KEYWORD => {
            let s = cursor.read_string()?;
            Ok(DataType::Keyword(Keyword::from_str(&s)?))
        }
        _ => bail!("Unknown DataType tag: {}", tag),
    }
}

fn decode_data_type_vec(cursor: &mut Cursor) -> Result<Vec<DataType>> {
    let count = cursor.read_u32()? as usize;
    if count > cursor.remaining() {
        bail!(
            "Vec<DataType> count {} exceeds remaining bytes {}",
            count,
            cursor.remaining()
        );
    }
    let mut vec = Vec::with_capacity(count);
    for _ in 0..count {
        vec.push(decode_data_type(cursor)?);
    }
    Ok(vec)
}

fn decode_data_type_map(cursor: &mut Cursor) -> Result<BTreeMap<String, DataType>> {
    let count = cursor.read_u32()? as usize;
    if count > cursor.remaining() {
        bail!(
            "Map count {} exceeds remaining bytes {}",
            count,
            cursor.remaining()
        );
    }
    let mut map = BTreeMap::new();
    for _ in 0..count {
        let k = cursor.read_string()?;
        let v = decode_data_type(cursor)?;
        map.insert(k, v);
    }
    Ok(map)
}

// ---------------------------------------------------------------------------
// Wire Decoding: EntityRef / TxOp
// ---------------------------------------------------------------------------

fn decode_entity_ref(cursor: &mut Cursor) -> Result<EntityRef> {
    let tag = cursor.read_u8()?;
    match tag {
        ENTITY_REF_ID => Ok(EntityRef::Id(cursor.read_i64()?)),
        ENTITY_REF_TEMPID => Ok(EntityRef::TempId(cursor.read_string()?)),
        ENTITY_REF_IDENT => {
            let s = cursor.read_string()?;
            Ok(EntityRef::Ident(Keyword::from_str(&s)?))
        }
        ENTITY_REF_LOOKUP => {
            let s = cursor.read_string()?;
            let kw = Keyword::from_str(&s)?;
            let dt = decode_data_type(cursor)?;
            Ok(EntityRef::LookupRef(kw, dt))
        }
        _ => bail!("Unknown EntityRef tag: 0x{:02x}", tag),
    }
}

fn decode_keyword_datatype_map(cursor: &mut Cursor) -> Result<BTreeMap<Keyword, DataType>> {
    let count = cursor.read_u32()? as usize;
    if count > cursor.remaining() {
        bail!(
            "Keyword-DataType map count {} exceeds remaining bytes {}",
            count,
            cursor.remaining()
        );
    }
    let mut map = BTreeMap::new();
    for _ in 0..count {
        let kw = Keyword::from_str(&cursor.read_string()?)?;
        let val = decode_data_type(cursor)?;
        map.insert(kw, val);
    }
    Ok(map)
}

fn decode_tx_op(cursor: &mut Cursor) -> Result<TxOp> {
    let tag = cursor.read_u8()?;
    match tag {
        TXOP_PUT => {
            let map = decode_keyword_datatype_map(cursor)?;
            Ok(TxOp::Put(map))
        }
        TXOP_ADD => {
            let entity = decode_entity_ref(cursor)?;
            let attribute = Keyword::from_str(&cursor.read_string()?)?;
            let value = decode_data_type(cursor)?;
            Ok(TxOp::Add {
                entity,
                attribute,
                value,
            })
        }
        TXOP_RETRACT => {
            let entity = decode_entity_ref(cursor)?;
            let attribute = Keyword::from_str(&cursor.read_string()?)?;
            let value = decode_data_type(cursor)?;
            Ok(TxOp::Retract {
                entity,
                attribute,
                value,
            })
        }
        TXOP_DELETE => Ok(TxOp::Delete(decode_entity_ref(cursor)?)),
        TXOP_ERASE => Ok(TxOp::Erase(decode_entity_ref(cursor)?)),
        _ => bail!("Unknown TxOp tag: {}", tag),
    }
}

fn decode_tx_ops(cursor: &mut Cursor) -> Result<Vec<TxOp>> {
    let count = cursor.read_u32()? as usize;
    if count > cursor.remaining() {
        bail!(
            "Vec<TxOp> count {} exceeds remaining bytes {}",
            count,
            cursor.remaining()
        );
    }
    let mut ops = Vec::with_capacity(count);
    for _ in 0..count {
        ops.push(decode_tx_op(cursor)?);
    }
    Ok(ops)
}

fn decode_query_arg(cursor: &mut Cursor) -> Result<QueryArg> {
    let tag = cursor.read_u8()?;
    match tag {
        QUERY_ARG_SCALAR => Ok(QueryArg::Scalar(decode_data_type(cursor)?)),
        QUERY_ARG_COLLECTION => Ok(QueryArg::Collection(decode_data_type_vec(cursor)?)),
        QUERY_ARG_TUPLE => Ok(QueryArg::Tuple(decode_data_type_vec(cursor)?)),
        QUERY_ARG_RELATION => {
            let row_count = cursor.read_u32()? as usize;
            if row_count > cursor.remaining() {
                bail!(
                    "Relation row count {} exceeds remaining bytes {}",
                    row_count,
                    cursor.remaining()
                );
            }
            let mut rows = Vec::with_capacity(row_count);
            for _ in 0..row_count {
                rows.push(decode_data_type_vec(cursor)?);
            }
            Ok(QueryArg::Relation(rows))
        }
        _ => bail!("Unknown QueryArg tag: {}", tag),
    }
}

fn decode_query_args(cursor: &mut Cursor) -> Result<Vec<QueryArg>> {
    let count = cursor.read_u32()? as usize;
    if count > cursor.remaining() {
        bail!(
            "Vec<QueryArg> count {} exceeds remaining bytes {}",
            count,
            cursor.remaining()
        );
    }
    let mut args = Vec::with_capacity(count);
    for _ in 0..count {
        args.push(decode_query_arg(cursor)?);
    }
    Ok(args)
}

// ---------------------------------------------------------------------------
// Message Payload Encoding
// ---------------------------------------------------------------------------

pub(crate) fn encode_frontend_payload(buf: &mut Vec<u8>, msg: &FrontendMessage) {
    match msg {
        FrontendMessage::Startup {
            version_major,
            version_minor,
            params,
        } => {
            encode_u16(buf, *version_major);
            encode_u16(buf, *version_minor);
            encode_string_map(buf, params);
        }
        FrontendMessage::OpenDb { tx_id, system_time } => {
            encode_option_i64(buf, tx_id);
            encode_option_i64(buf, system_time);
        }
        FrontendMessage::CloseDb { db_id } => {
            encode_u32(buf, *db_id);
        }
        FrontendMessage::Query {
            query_string,
            db_id,
            args,
        } => {
            encode_string(buf, query_string);
            encode_u32(buf, *db_id);
            encode_query_args(buf, args);
        }
        FrontendMessage::Execute {
            ops,
            await_indexing,
        } => {
            encode_tx_ops(buf, ops);
            encode_bool(buf, *await_indexing);
        }
        FrontendMessage::Subscribe {
            query_string,
            db_id,
            args,
        } => {
            encode_string(buf, query_string);
            encode_u32(buf, *db_id);
            encode_query_args(buf, args);
        }
        FrontendMessage::Unsubscribe => {}
        FrontendMessage::Terminate => {}
    }
}

pub(crate) fn encode_backend_payload(buf: &mut Vec<u8>, msg: &BackendMessage) {
    match msg {
        BackendMessage::AuthenticationOk { server_version } => {
            encode_string(buf, server_version);
        }
        BackendMessage::DbOpened { db_id, tx_id } => {
            encode_u32(buf, *db_id);
            encode_i64(buf, *tx_id);
        }
        BackendMessage::DbClosed { db_id } => {
            encode_u32(buf, *db_id);
        }
        BackendMessage::RowDescription { columns } => {
            encode_u32(buf, columns.len() as u32);
            for col in columns {
                encode_string(buf, &col.name);
                encode_u8(buf, col.data_type);
            }
        }
        BackendMessage::DataRow { values } => {
            encode_data_type_vec(buf, values);
        }
        BackendMessage::DataBatchComplete { tx_id } => {
            encode_i64(buf, *tx_id);
        }
        BackendMessage::ReadyForQuery { status } => {
            encode_u8(buf, *status);
        }
        BackendMessage::TxKey { tx_id, system_time } => {
            encode_i64(buf, *tx_id);
            encode_i64(buf, *system_time);
        }
        BackendMessage::TxResult {
            status,
            tx_id,
            system_time,
            error_message,
        } => {
            encode_u8(buf, *status);
            encode_i64(buf, *tx_id);
            encode_i64(buf, *system_time);
            encode_option_string(buf, error_message);
        }
        BackendMessage::UnsubscribeComplete => {}
        BackendMessage::Heartbeat => {}
        BackendMessage::ErrorResponse {
            severity,
            code,
            message,
            detail,
            hint,
        } => {
            encode_u8(buf, *severity);
            encode_u16(buf, *code);
            encode_string(buf, message);
            encode_option_string(buf, detail);
            encode_option_string(buf, hint);
        }
    }
}

// ---------------------------------------------------------------------------
// Message Payload Decoding
// ---------------------------------------------------------------------------

pub(crate) fn decode_frontend_payload(msg_type: u8, cursor: &mut Cursor) -> Result<FrontendMessage> {
    match msg_type {
        MSG_OPEN_DB => Ok(FrontendMessage::OpenDb {
            tx_id: cursor.read_option_i64()?,
            system_time: cursor.read_option_i64()?,
        }),
        MSG_CLOSE_DB => Ok(FrontendMessage::CloseDb {
            db_id: cursor.read_u32()?,
        }),
        MSG_QUERY => Ok(FrontendMessage::Query {
            query_string: cursor.read_string()?,
            db_id: cursor.read_u32()?,
            args: decode_query_args(cursor)?,
        }),
        MSG_EXECUTE => {
            let ops = decode_tx_ops(cursor)?;
            let await_indexing = cursor.read_bool()?;
            Ok(FrontendMessage::Execute {
                ops,
                await_indexing,
            })
        }
        MSG_SUBSCRIBE => Ok(FrontendMessage::Subscribe {
            query_string: cursor.read_string()?,
            db_id: cursor.read_u32()?,
            args: decode_query_args(cursor)?,
        }),
        MSG_UNSUBSCRIBE => Ok(FrontendMessage::Unsubscribe),
        MSG_TERMINATE => Ok(FrontendMessage::Terminate),
        _ => bail!("Unknown frontend message type: 0x{:02x}", msg_type),
    }
}

pub(crate) fn decode_backend_payload(msg_type: u8, cursor: &mut Cursor) -> Result<BackendMessage> {
    match msg_type {
        MSG_AUTHENTICATION_OK => Ok(BackendMessage::AuthenticationOk {
            server_version: cursor.read_string()?,
        }),
        MSG_DB_OPENED => Ok(BackendMessage::DbOpened {
            db_id: cursor.read_u32()?,
            tx_id: cursor.read_i64()?,
        }),
        MSG_DB_CLOSED => Ok(BackendMessage::DbClosed {
            db_id: cursor.read_u32()?,
        }),
        MSG_ROW_DESCRIPTION => {
            let count = cursor.read_u32()? as usize;
            if count > cursor.remaining() {
                bail!(
                    "RowDescription column count {} exceeds remaining bytes {}",
                    count,
                    cursor.remaining()
                );
            }
            let mut columns = Vec::with_capacity(count);
            for _ in 0..count {
                columns.push(ColumnDescription {
                    name: cursor.read_string()?,
                    data_type: cursor.read_u8()?,
                });
            }
            Ok(BackendMessage::RowDescription { columns })
        }
        MSG_DATA_ROW => {
            let values = decode_data_type_vec(cursor)?;
            Ok(BackendMessage::DataRow { values })
        }
        MSG_DATA_BATCH_COMPLETE => Ok(BackendMessage::DataBatchComplete {
            tx_id: cursor.read_i64()?,
        }),
        MSG_READY_FOR_QUERY => Ok(BackendMessage::ReadyForQuery {
            status: cursor.read_u8()?,
        }),
        MSG_TX_KEY => Ok(BackendMessage::TxKey {
            tx_id: cursor.read_i64()?,
            system_time: cursor.read_i64()?,
        }),
        MSG_TX_RESULT => Ok(BackendMessage::TxResult {
            status: cursor.read_u8()?,
            tx_id: cursor.read_i64()?,
            system_time: cursor.read_i64()?,
            error_message: cursor.read_option_string()?,
        }),
        MSG_UNSUBSCRIBE_COMPLETE => Ok(BackendMessage::UnsubscribeComplete),
        MSG_HEARTBEAT => Ok(BackendMessage::Heartbeat),
        MSG_ERROR_RESPONSE => Ok(BackendMessage::ErrorResponse {
            severity: cursor.read_u8()?,
            code: cursor.read_u16()?,
            message: cursor.read_string()?,
            detail: cursor.read_option_string()?,
            hint: cursor.read_option_string()?,
        }),
        _ => bail!("Unknown backend message type: 0x{:02x}", msg_type),
    }
}

// ---------------------------------------------------------------------------
// Framed Message Reading/Writing (async, over TCP)
// ---------------------------------------------------------------------------

/// Read a frontend message from the stream.
/// If `is_startup` is true, expects a Startup message (no type byte).
pub async fn read_frontend_message<R: AsyncRead + Unpin>(
    reader: &mut R,
    is_startup: bool,
    max_message_size: u32,
) -> Result<FrontendMessage> {
    if is_startup {
        // Startup: no type byte, just [length][payload]
        let length = reader.read_u32().await?;
        if length < 4 {
            bail!("Invalid startup message length: {}", length);
        }
        let payload_size = length - 4;
        if payload_size > max_message_size {
            bail!("Startup message too large: {} bytes", payload_size);
        }
        let mut payload = vec![0u8; payload_size as usize];
        reader.read_exact(&mut payload).await?;

        let mut cursor = Cursor::new(&payload);
        let version_major = cursor.read_u16()?;
        let version_minor = cursor.read_u16()?;
        let params = cursor.read_string_map()?;
        Ok(FrontendMessage::Startup {
            version_major,
            version_minor,
            params,
        })
    } else {
        let msg_type = reader.read_u8().await?;
        let length = reader.read_u32().await?;
        if length < 4 {
            bail!("Invalid message length: {}", length);
        }
        let payload_size = length - 4;
        if payload_size > max_message_size {
            bail!("Message too large: {} bytes", payload_size);
        }
        let mut payload = vec![0u8; payload_size as usize];
        if payload_size > 0 {
            reader.read_exact(&mut payload).await?;
        }

        let mut cursor = Cursor::new(&payload);
        decode_frontend_payload(msg_type, &mut cursor)
    }
}

/// Write a frontend message to the stream.
pub async fn write_frontend_message<W: AsyncWrite + Unpin>(
    writer: &mut W,
    msg: &FrontendMessage,
) -> Result<()> {
    let mut payload = Vec::new();
    encode_frontend_payload(&mut payload, msg);

    let payload_len = payload.len() + 4;
    let length = u32::try_from(payload_len)
        .map_err(|_| anyhow!("message payload too large: {} bytes", payload.len()))?;
    if length > DEFAULT_MAX_MESSAGE_SIZE + 4 {
        bail!("message payload exceeds max size: {} bytes", payload.len());
    }

    match msg {
        FrontendMessage::Startup { .. } => {
            // Startup: no type byte, just [length][payload]
            writer.write_u32(length).await?;
            writer.write_all(&payload).await?;
        }
        _ => {
            let type_byte = match msg {
                FrontendMessage::OpenDb { .. } => MSG_OPEN_DB,
                FrontendMessage::CloseDb { .. } => MSG_CLOSE_DB,
                FrontendMessage::Query { .. } => MSG_QUERY,
                FrontendMessage::Execute { .. } => MSG_EXECUTE,
                FrontendMessage::Subscribe { .. } => MSG_SUBSCRIBE,
                FrontendMessage::Unsubscribe => MSG_UNSUBSCRIBE,
                FrontendMessage::Terminate => MSG_TERMINATE,
                FrontendMessage::Startup { .. } => unreachable!(),
            };
            writer.write_u8(type_byte).await?;
            writer.write_u32(length).await?;
            writer.write_all(&payload).await?;
        }
    }
    Ok(())
}

/// Read a backend message from the stream.
pub async fn read_backend_message<R: AsyncRead + Unpin>(
    reader: &mut R,
    max_message_size: u32,
) -> Result<BackendMessage> {
    let msg_type = reader.read_u8().await?;
    let length = reader.read_u32().await?;
    if length < 4 {
        bail!("Invalid message length: {}", length);
    }
    let payload_size = length - 4;
    if payload_size > max_message_size {
        bail!("Message too large: {} bytes", payload_size);
    }
    let mut payload = vec![0u8; payload_size as usize];
    if payload_size > 0 {
        reader.read_exact(&mut payload).await?;
    }

    let mut cursor = Cursor::new(&payload);
    decode_backend_payload(msg_type, &mut cursor)
}

/// Write a backend message to the stream.
pub async fn write_backend_message<W: AsyncWrite + Unpin>(
    writer: &mut W,
    msg: &BackendMessage,
) -> Result<()> {
    let mut payload = Vec::new();
    encode_backend_payload(&mut payload, msg);

    let payload_len = payload.len() + 4;
    let length = u32::try_from(payload_len)
        .map_err(|_| anyhow!("message payload too large: {} bytes", payload.len()))?;
    if length > DEFAULT_MAX_MESSAGE_SIZE + 4 {
        bail!("message payload exceeds max size: {} bytes", payload.len());
    }

    let type_byte = match msg {
        BackendMessage::AuthenticationOk { .. } => MSG_AUTHENTICATION_OK,
        BackendMessage::DbOpened { .. } => MSG_DB_OPENED,
        BackendMessage::DbClosed { .. } => MSG_DB_CLOSED,
        BackendMessage::RowDescription { .. } => MSG_ROW_DESCRIPTION,
        BackendMessage::DataRow { .. } => MSG_DATA_ROW,
        BackendMessage::DataBatchComplete { .. } => MSG_DATA_BATCH_COMPLETE,
        BackendMessage::ReadyForQuery { .. } => MSG_READY_FOR_QUERY,
        BackendMessage::TxKey { .. } => MSG_TX_KEY,
        BackendMessage::TxResult { .. } => MSG_TX_RESULT,
        BackendMessage::UnsubscribeComplete => MSG_UNSUBSCRIBE_COMPLETE,
        BackendMessage::Heartbeat => MSG_HEARTBEAT,
        BackendMessage::ErrorResponse { .. } => MSG_ERROR_RESPONSE,
    };

    writer.write_u8(type_byte).await?;
    writer.write_u32(length).await?;
    writer.write_all(&payload).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// HTTP Body Encode/Decode Helpers
// ---------------------------------------------------------------------------

/// Encode an OpenDb request body: option_i64(tx_id) + option_i64(system_time)
pub fn encode_open_db_request(tx_id: Option<i64>, system_time: Option<i64>) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_option_i64(&mut buf, &tx_id);
    encode_option_i64(&mut buf, &system_time);
    buf
}

/// Decode an OpenDb request body.
pub fn decode_open_db_request(data: &[u8]) -> Result<(Option<i64>, Option<i64>)> {
    let mut cursor = Cursor::new(data);
    let tx_id = cursor.read_option_i64()?;
    let system_time = cursor.read_option_i64()?;
    Ok((tx_id, system_time))
}

/// Encode a DbOpened response body: u32(db_id) + i64(tx_id)
pub fn encode_db_opened_response(db_id: u32, tx_id: i64) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_u32(&mut buf, db_id);
    encode_i64(&mut buf, tx_id);
    buf
}

/// Decode a DbOpened response body.
pub fn decode_db_opened_response(data: &[u8]) -> Result<(u32, i64)> {
    let mut cursor = Cursor::new(data);
    let db_id = cursor.read_u32()?;
    let tx_id = cursor.read_i64()?;
    Ok((db_id, tx_id))
}

/// Encode a DbClosed response body: u32(db_id)
pub fn encode_db_closed_response(db_id: u32) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_u32(&mut buf, db_id);
    buf
}

/// Encode a query request body: string(query) + query_args(args)
pub fn encode_query_request(query: &str, args: &[QueryArg]) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_string(&mut buf, query);
    encode_query_args(&mut buf, args);
    buf
}

/// Decode a query request body.
pub fn decode_query_request(data: &[u8]) -> Result<(String, Vec<QueryArg>)> {
    let mut cursor = Cursor::new(data);
    let query = cursor.read_string()?;
    let args = decode_query_args(&mut cursor)?;
    Ok((query, args))
}

/// Encode a query response as concatenated framed backend messages:
/// [RowDescription frame][DataRow frame]*
pub fn encode_query_response(columns: &[ColumnDescription], rows: &[Vec<DataType>]) -> Vec<u8> {
    let mut buf = Vec::new();

    // RowDescription frame
    let row_desc = BackendMessage::RowDescription {
        columns: columns.to_vec(),
    };
    let mut payload = Vec::new();
    encode_backend_payload(&mut payload, &row_desc);
    buf.push(MSG_ROW_DESCRIPTION);
    encode_u32(&mut buf, (payload.len() + 4) as u32);
    buf.extend_from_slice(&payload);

    // DataRow frames
    for row in rows {
        let data_row = BackendMessage::DataRow {
            values: row.clone(),
        };
        let mut payload = Vec::new();
        encode_backend_payload(&mut payload, &data_row);
        buf.push(MSG_DATA_ROW);
        encode_u32(&mut buf, (payload.len() + 4) as u32);
        buf.extend_from_slice(&payload);
    }

    buf
}

/// Decode a query response from concatenated framed backend messages.
pub fn decode_query_response(data: &[u8]) -> Result<(Vec<ColumnDescription>, Vec<Vec<DataType>>)> {
    let mut pos = 0;
    let mut columns = Vec::new();
    let mut rows = Vec::new();

    while pos < data.len() {
        if pos + 5 > data.len() {
            bail!("Incomplete message frame at position {}", pos);
        }
        let msg_type = data[pos];
        pos += 1;
        let length = u32::from_be_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if length < 4 {
            bail!("Invalid message length: {}", length);
        }
        let payload_size = length - 4;
        if pos + payload_size > data.len() {
            bail!("Message payload truncated at position {}", pos);
        }
        let payload = &data[pos..pos + payload_size];
        pos += payload_size;

        let mut cursor = Cursor::new(payload);
        let msg = decode_backend_payload(msg_type, &mut cursor)?;
        match msg {
            BackendMessage::RowDescription { columns: cols } => {
                columns = cols;
            }
            BackendMessage::DataRow { values } => {
                rows.push(values);
            }
            other => bail!("Unexpected message in query response: {:?}", other),
        }
    }

    Ok((columns, rows))
}

/// Encode an Execute request body (submit or execute): tx_ops(ops)
pub fn encode_execute_request(ops: &[TxOp]) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_tx_ops(&mut buf, ops);
    buf
}

/// Decode an Execute request body.
pub fn decode_execute_request(data: &[u8]) -> Result<Vec<TxOp>> {
    let mut cursor = Cursor::new(data);
    decode_tx_ops(&mut cursor)
}

/// Encode a TxKey response body: i64(tx_id) + i64(system_time)
pub fn encode_tx_key_response(tx_id: i64, system_time: i64) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_i64(&mut buf, tx_id);
    encode_i64(&mut buf, system_time);
    buf
}

/// Decode a TxKey response body.
pub fn decode_tx_key_response(data: &[u8]) -> Result<(i64, i64)> {
    let mut cursor = Cursor::new(data);
    let tx_id = cursor.read_i64()?;
    let system_time = cursor.read_i64()?;
    Ok((tx_id, system_time))
}

/// Encode a TxResult response body: u8(status) + i64(tx_id) + i64(system_time) + option_string(error)
pub fn encode_tx_result_response(
    status: u8,
    tx_id: i64,
    system_time: i64,
    error_message: &Option<String>,
) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_u8(&mut buf, status);
    encode_i64(&mut buf, tx_id);
    encode_i64(&mut buf, system_time);
    encode_option_string(&mut buf, error_message);
    buf
}

/// Decode a TxResult response body.
pub fn decode_tx_result_response(data: &[u8]) -> Result<(u8, i64, i64, Option<String>)> {
    let mut cursor = Cursor::new(data);
    let status = cursor.read_u8()?;
    let tx_id = cursor.read_i64()?;
    let system_time = cursor.read_i64()?;
    let error_message = cursor.read_option_string()?;
    Ok((status, tx_id, system_time, error_message))
}

/// Encode an ErrorResponse body.
pub fn encode_error_body(
    severity: u8,
    code: u16,
    message: &str,
    detail: &Option<String>,
    hint: &Option<String>,
) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_u8(&mut buf, severity);
    encode_u16(&mut buf, code);
    encode_string(&mut buf, message);
    encode_option_string(&mut buf, detail);
    encode_option_string(&mut buf, hint);
    buf
}

/// Decode an ErrorResponse body.
pub fn decode_error_body(data: &[u8]) -> Result<(u8, u16, String, Option<String>, Option<String>)> {
    let mut cursor = Cursor::new(data);
    let severity = cursor.read_u8()?;
    let code = cursor.read_u16()?;
    let message = cursor.read_string()?;
    let detail = cursor.read_option_string()?;
    let hint = cursor.read_option_string()?;
    Ok((severity, code, message, detail, hint))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::EntityRef;
    use edn::kw;
    use std::collections::BTreeMap;

    // Helper: encode a frontend message to bytes and decode it back.
    async fn roundtrip_frontend(msg: &FrontendMessage) -> FrontendMessage {
        let mut buf = Vec::new();
        write_frontend_message(&mut buf, msg).await.unwrap();

        let is_startup = matches!(msg, FrontendMessage::Startup { .. });
        let mut cursor = &buf[..];
        read_frontend_message(&mut cursor, is_startup, DEFAULT_MAX_MESSAGE_SIZE)
            .await
            .unwrap()
    }

    // Helper: encode a backend message to bytes and decode it back.
    async fn roundtrip_backend(msg: &BackendMessage) -> BackendMessage {
        let mut buf = Vec::new();
        write_backend_message(&mut buf, msg).await.unwrap();

        let mut cursor = &buf[..];
        read_backend_message(&mut cursor, DEFAULT_MAX_MESSAGE_SIZE)
            .await
            .unwrap()
    }

    // -- DataType round-trip tests --

    fn roundtrip_data_type(dt: &DataType) -> DataType {
        let mut buf = Vec::new();
        encode_data_type(&mut buf, dt);
        let mut cursor = Cursor::new(&buf);
        decode_data_type(&mut cursor).unwrap()
    }

    #[test]
    fn test_data_type_bigint() {
        let dt = DataType::BigInt(123456789012345678901234567890);
        assert_eq!(roundtrip_data_type(&dt), dt);
    }

    #[test]
    fn test_data_type_boolean() {
        assert_eq!(
            roundtrip_data_type(&DataType::Boolean(true)),
            DataType::Boolean(true)
        );
        assert_eq!(
            roundtrip_data_type(&DataType::Boolean(false)),
            DataType::Boolean(false)
        );
    }

    #[test]
    fn test_data_type_bytes() {
        let dt = DataType::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(roundtrip_data_type(&dt), dt);
    }

    #[test]
    fn test_data_type_double() {
        let dt = DataType::Double(std::f64::consts::PI);
        assert_eq!(roundtrip_data_type(&dt), dt);
    }

    #[test]
    fn test_data_type_float() {
        let dt = DataType::Float(std::f32::consts::E);
        assert_eq!(roundtrip_data_type(&dt), dt);
    }

    #[test]
    fn test_data_type_instant() {
        let dt = DataType::Instant(Utc::now());
        // Round-trip loses sub-microsecond precision, so we encode/decode and compare micros
        let rt = roundtrip_data_type(&dt);
        if let (DataType::Instant(a), DataType::Instant(b)) = (&dt, &rt) {
            assert_eq!(a.timestamp_micros(), b.timestamp_micros());
        } else {
            panic!("Expected Instant");
        }
    }

    #[test]
    fn test_data_type_keyword_plain() {
        let dt = DataType::Keyword(kw!(:foo));
        assert_eq!(roundtrip_data_type(&dt), dt);
    }

    #[test]
    fn test_data_type_keyword_namespaced() {
        let dt = DataType::Keyword(kw!(:person/name));
        assert_eq!(roundtrip_data_type(&dt), dt);
    }

    #[test]
    fn test_data_type_long() {
        let dt = DataType::Long(i64::MAX);
        assert_eq!(roundtrip_data_type(&dt), dt);
    }

    #[test]
    fn test_data_type_string() {
        let dt = DataType::String("hello world".to_string());
        assert_eq!(roundtrip_data_type(&dt), dt);
    }

    #[test]
    fn test_data_type_uuid() {
        let dt = DataType::Uuid(Uuid::new_v4());
        assert_eq!(roundtrip_data_type(&dt), dt);
    }

    #[test]
    fn test_data_type_vector() {
        let dt = DataType::Vector(vec![
            DataType::Long(1),
            DataType::String("two".to_string()),
            DataType::Boolean(true),
        ]);
        assert_eq!(roundtrip_data_type(&dt), dt);
    }

    #[test]
    fn test_data_type_tuple() {
        let dt = DataType::Tuple(vec![DataType::Long(42), DataType::Boolean(true)]);
        assert_eq!(roundtrip_data_type(&dt), dt);
    }

    #[test]
    fn test_data_type_map() {
        let mut map = BTreeMap::new();
        map.insert("name".to_string(), DataType::String("alice".to_string()));
        map.insert("age".to_string(), DataType::Long(30));
        let dt = DataType::Map(map);
        assert_eq!(roundtrip_data_type(&dt), dt);
    }

    #[test]
    fn test_data_type_nested() {
        let mut inner_map = BTreeMap::new();
        inner_map.insert("x".to_string(), DataType::Long(1));
        let dt = DataType::Vector(vec![
            DataType::Map(inner_map),
            DataType::Tuple(vec![
                DataType::String("hello".to_string()),
                DataType::Boolean(false),
            ]),
        ]);
        assert_eq!(roundtrip_data_type(&dt), dt);
    }

    // -- TxOp round-trip tests --

    fn roundtrip_tx_op(op: &TxOp) -> TxOp {
        let mut buf = Vec::new();
        encode_tx_op(&mut buf, op);
        let mut cursor = Cursor::new(&buf);
        decode_tx_op(&mut cursor).unwrap()
    }

    #[test]
    fn test_tx_op_put() {
        let op = TxOp::put(vec![
            (kw!(:name), DataType::String("alice".to_string())),
            (kw!(:age), DataType::Long(30)),
        ]);
        assert_eq!(roundtrip_tx_op(&op), op);
    }

    #[test]
    fn test_tx_op_put_with_id() {
        let op = TxOp::put(vec![
            (kw!(:db/id), DataType::Long(1)),
            (kw!(:name), DataType::String("alice".to_string())),
        ]);
        assert_eq!(roundtrip_tx_op(&op), op);
    }

    #[test]
    fn test_tx_op_put_with_tempid() {
        let op = TxOp::put(vec![
            (kw!(:db/id), DataType::String("temp-1".to_string())),
            (kw!(:name), DataType::String("bob".to_string())),
        ]);
        assert_eq!(roundtrip_tx_op(&op), op);
    }

    #[test]
    fn test_tx_op_add() {
        let op = TxOp::Add {
            entity: EntityRef::Id(42),
            attribute: kw!(:email),
            value: DataType::String("test@example.com".to_string()),
        };
        assert_eq!(roundtrip_tx_op(&op), op);
    }

    #[test]
    fn test_tx_op_add_with_ref_value() {
        let op = TxOp::Add {
            entity: EntityRef::Id(42),
            attribute: kw!(:friend),
            value: DataType::String("temp-2".to_string()),
        };
        assert_eq!(roundtrip_tx_op(&op), op);
    }

    #[test]
    fn test_tx_op_retract() {
        let op = TxOp::Retract {
            entity: EntityRef::Id(42),
            attribute: kw!(:email),
            value: DataType::String("old@example.com".to_string()),
        };
        assert_eq!(roundtrip_tx_op(&op), op);
    }

    #[test]
    fn test_tx_op_delete() {
        let op = TxOp::Delete(EntityRef::Id(99));
        assert_eq!(roundtrip_tx_op(&op), op);
    }

    #[test]
    fn test_tx_op_erase() {
        let op = TxOp::Erase(EntityRef::Id(100));
        assert_eq!(roundtrip_tx_op(&op), op);
    }

    #[test]
    fn test_entity_ref_roundtrip() {
        fn roundtrip_er(er: &EntityRef) -> EntityRef {
            let mut buf = Vec::new();
            encode_entity_ref(&mut buf, er);
            let mut cursor = Cursor::new(&buf);
            decode_entity_ref(&mut cursor).unwrap()
        }
        assert_eq!(roundtrip_er(&EntityRef::Id(42)), EntityRef::Id(42));
        assert_eq!(
            roundtrip_er(&EntityRef::TempId("t-1".to_string())),
            EntityRef::TempId("t-1".to_string())
        );
        assert_eq!(
            roundtrip_er(&EntityRef::Ident(kw!(:person/name))),
            EntityRef::Ident(kw!(:person/name))
        );
        assert_eq!(
            roundtrip_er(&EntityRef::LookupRef(
                kw!(:email),
                DataType::String("a@b.com".to_string())
            )),
            EntityRef::LookupRef(kw!(:email), DataType::String("a@b.com".to_string())),
        );
    }

    // -- Frontend message round-trip tests --

    #[tokio::test]
    async fn test_startup_roundtrip() {
        let mut params = BTreeMap::new();
        params.insert("client_name".to_string(), "test-client".to_string());
        let msg = FrontendMessage::Startup {
            version_major: 0,
            version_minor: 1,
            params,
        };
        assert_eq!(roundtrip_frontend(&msg).await, msg);
    }

    #[tokio::test]
    async fn test_open_db_roundtrip() {
        let msg = FrontendMessage::OpenDb {
            tx_id: Some(42),
            system_time: Some(1700000000000000),
        };
        assert_eq!(roundtrip_frontend(&msg).await, msg);

        let msg = FrontendMessage::OpenDb {
            tx_id: None,
            system_time: None,
        };
        assert_eq!(roundtrip_frontend(&msg).await, msg);
    }

    #[tokio::test]
    async fn test_close_db_roundtrip() {
        let msg = FrontendMessage::CloseDb { db_id: 7 };
        assert_eq!(roundtrip_frontend(&msg).await, msg);
    }

    #[tokio::test]
    async fn test_query_roundtrip() {
        let msg = FrontendMessage::Query {
            query_string: "{:find [?e ?name] :where [[?e :person/name ?name]]}".to_string(),
            db_id: 1,
            args: vec![],
        };
        assert_eq!(roundtrip_frontend(&msg).await, msg);
    }

    #[tokio::test]
    async fn test_execute_roundtrip() {
        let msg = FrontendMessage::Execute {
            ops: vec![
                TxOp::Add {
                    entity: "alice".into(),
                    attribute: kw!(:name),
                    value: DataType::String("alice".to_string()),
                },
                TxOp::Delete(EntityRef::Id(99)),
            ],
            await_indexing: true,
        };
        assert_eq!(roundtrip_frontend(&msg).await, msg);
    }

    #[tokio::test]
    async fn test_subscribe_roundtrip() {
        let msg = FrontendMessage::Subscribe {
            query_string: "{:find [?name] :where [[?e :person/name ?name]]}".to_string(),
            db_id: 3,
            args: vec![],
        };
        assert_eq!(roundtrip_frontend(&msg).await, msg);
    }

    #[tokio::test]
    async fn test_unsubscribe_roundtrip() {
        assert_eq!(
            roundtrip_frontend(&FrontendMessage::Unsubscribe).await,
            FrontendMessage::Unsubscribe
        );
    }

    #[tokio::test]
    async fn test_terminate_roundtrip() {
        assert_eq!(
            roundtrip_frontend(&FrontendMessage::Terminate).await,
            FrontendMessage::Terminate
        );
    }

    // -- Backend message round-trip tests --

    #[tokio::test]
    async fn test_authentication_ok_roundtrip() {
        let msg = BackendMessage::AuthenticationOk {
            server_version: "triplox 0.1.0".to_string(),
        };
        assert_eq!(roundtrip_backend(&msg).await, msg);
    }

    #[tokio::test]
    async fn test_db_opened_roundtrip() {
        let msg = BackendMessage::DbOpened {
            db_id: 5,
            tx_id: 42,
        };
        assert_eq!(roundtrip_backend(&msg).await, msg);
    }

    #[tokio::test]
    async fn test_db_closed_roundtrip() {
        let msg = BackendMessage::DbClosed { db_id: 5 };
        assert_eq!(roundtrip_backend(&msg).await, msg);
    }

    #[tokio::test]
    async fn test_row_description_roundtrip() {
        let msg = BackendMessage::RowDescription {
            columns: vec![
                ColumnDescription {
                    name: "?e".to_string(),
                    data_type: TAG_LONG,
                },
                ColumnDescription {
                    name: "?name".to_string(),
                    data_type: TAG_STRING,
                },
            ],
        };
        assert_eq!(roundtrip_backend(&msg).await, msg);
    }

    #[tokio::test]
    async fn test_data_row_query_mode_roundtrip() {
        let msg = BackendMessage::DataRow {
            values: vec![DataType::Long(1), DataType::String("alice".to_string())],
        };
        assert_eq!(roundtrip_backend(&msg).await, msg);
    }

    #[tokio::test]
    async fn test_data_batch_complete_roundtrip() {
        let msg = BackendMessage::DataBatchComplete { tx_id: 100 };
        assert_eq!(roundtrip_backend(&msg).await, msg);
    }

    #[tokio::test]
    async fn test_ready_for_query_roundtrip() {
        let msg = BackendMessage::ReadyForQuery {
            status: STATUS_IDLE,
        };
        assert_eq!(roundtrip_backend(&msg).await, msg);

        let msg = BackendMessage::ReadyForQuery {
            status: STATUS_SUBSCRIBED,
        };
        assert_eq!(roundtrip_backend(&msg).await, msg);
    }

    #[tokio::test]
    async fn test_tx_key_roundtrip() {
        let msg = BackendMessage::TxKey {
            tx_id: 42,
            system_time: 1700000000000000,
        };
        assert_eq!(roundtrip_backend(&msg).await, msg);
    }

    #[tokio::test]
    async fn test_tx_result_committed_roundtrip() {
        let msg = BackendMessage::TxResult {
            status: 0,
            tx_id: 42,
            system_time: 1700000000000000,
            error_message: None,
        };
        assert_eq!(roundtrip_backend(&msg).await, msg);
    }

    #[tokio::test]
    async fn test_tx_result_aborted_roundtrip() {
        let msg = BackendMessage::TxResult {
            status: 1,
            tx_id: 42,
            system_time: 1700000000000000,
            error_message: Some("transaction aborted: constraint violation".to_string()),
        };
        assert_eq!(roundtrip_backend(&msg).await, msg);
    }

    #[tokio::test]
    async fn test_unsubscribe_complete_roundtrip() {
        assert_eq!(
            roundtrip_backend(&BackendMessage::UnsubscribeComplete).await,
            BackendMessage::UnsubscribeComplete
        );
    }

    #[tokio::test]
    async fn test_heartbeat_roundtrip() {
        assert_eq!(
            roundtrip_backend(&BackendMessage::Heartbeat).await,
            BackendMessage::Heartbeat
        );
    }

    #[tokio::test]
    async fn test_error_response_roundtrip() {
        let msg = BackendMessage::ErrorResponse {
            severity: SEVERITY_ERROR,
            code: ErrorCode::ParseError.as_u16(),
            message: "syntax error in query".to_string(),
            detail: Some("unexpected token at position 42".to_string()),
            hint: Some("check your EDN syntax".to_string()),
        };
        assert_eq!(roundtrip_backend(&msg).await, msg);
    }

    #[tokio::test]
    async fn test_error_response_minimal_roundtrip() {
        let msg = BackendMessage::ErrorResponse {
            severity: SEVERITY_FATAL,
            code: ErrorCode::ProtocolVersionMismatch.as_u16(),
            message: "unsupported protocol version".to_string(),
            detail: None,
            hint: None,
        };
        assert_eq!(roundtrip_backend(&msg).await, msg);
    }

    // -- Error code tests --

    #[test]
    fn test_error_code_roundtrip() {
        let codes = [
            ErrorCode::ProtocolVersionMismatch,
            ErrorCode::InvalidStartup,
            ErrorCode::ParseError,
            ErrorCode::QueryError,
            ErrorCode::InvalidQuery,
            ErrorCode::EmptyQuery,
            ErrorCode::TxError,
            ErrorCode::TxAborted,
            ErrorCode::SubscriptionError,
            ErrorCode::SubscriptionTimeout,
            ErrorCode::InternalError,
            ErrorCode::MessageTooLarge,
            ErrorCode::InvalidMessageType,
            ErrorCode::QueryCancelled,
            ErrorCode::ServerShuttingDown,
            ErrorCode::InvalidDbHandle,
            ErrorCode::TooManyOpenDbs,
        ];
        for code in codes {
            assert_eq!(ErrorCode::from_u16(code.as_u16()).unwrap(), code);
        }
    }
}
