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

use crate::ops::{Attribute, DataType, Document, EntityId, Triple, TxOp, Value};

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
pub const MSG_BASIS_FOR_TX: u8 = b'F';
pub const MSG_TERMINATE: u8 = b'X';

// Backend message type bytes
pub const MSG_AUTHENTICATION_OK: u8 = b'R';
pub const MSG_DB_OPENED: u8 = b'H';
pub const MSG_DB_CLOSED: u8 = b'J';
pub const MSG_ROW_DESCRIPTION: u8 = b'T';
pub const MSG_DATA_ROW: u8 = b'D';
pub const MSG_COMMAND_COMPLETE: u8 = b'C';
pub const MSG_DATA_BATCH_COMPLETE: u8 = b'B';
pub const MSG_READY_FOR_QUERY: u8 = b'Z';
pub const MSG_TX_KEY: u8 = b'Y';
pub const MSG_TX_RESULT: u8 = b'G';
pub const MSG_UNSUBSCRIBE_COMPLETE: u8 = b'N';
pub const MSG_HEARTBEAT: u8 = b'K';
pub const MSG_BASIS_RESULT: u8 = b'A';
pub const MSG_ERROR_RESPONSE: u8 = b'W';

// ReadyForQuery status bytes
pub const STATUS_IDLE: u8 = b'I';
pub const STATUS_SUBSCRIBED: u8 = b'S';

// ErrorResponse severity bytes
pub const SEVERITY_ERROR: u8 = b'E';
pub const SEVERITY_FATAL: u8 = b'F';

// DataType tag bytes
pub const TAG_NIL: u8 = 0;
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
        basis_tx_id: Option<i64>,
        basis_system_time: Option<i64>,
        basis_seq_num: Option<u64>,
    },
    CloseDb {
        db_id: u32,
    },
    Query {
        query_string: String,
        db_id: u32,
    },
    Execute {
        ops: Vec<TxOp>,
        await_indexing: bool,
    },
    Subscribe {
        query_string: String,
        db_id: u32,
    },
    Unsubscribe,
    BasisForTx {
        tx_id: i64,
    },
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
    CommandComplete {
        tag: String,
        row_count: u64,
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
        seq_num: u64,
        error_message: Option<String>,
    },
    BasisResult {
        tx_id: i64,
        system_time: i64,
        seq_num: u64,
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

fn encode_i8(buf: &mut Vec<u8>, v: i8) {
    buf.push(v as u8);
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

fn encode_string(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    encode_u32(buf, u32::try_from(bytes.len()).expect("string exceeds u32::MAX bytes"));
    buf.extend_from_slice(bytes);
}

fn encode_bytes(buf: &mut Vec<u8>, b: &[u8]) {
    encode_u32(buf, u32::try_from(b.len()).expect("byte array exceeds u32::MAX bytes"));
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
// Wire Encoding: TxOp
// ---------------------------------------------------------------------------

fn encode_triple(buf: &mut Vec<u8>, triple: &Triple) {
    encode_i64(buf, triple.entity.0);
    encode_string(buf, &triple.attribute.0);
    encode_data_type(buf, &triple.value.0);
}

fn encode_tx_op(buf: &mut Vec<u8>, op: &TxOp) {
    match op {
        TxOp::Put(doc) => {
            encode_u8(buf, TXOP_PUT);
            encode_data_type_map(buf, &doc.0);
        }
        TxOp::Add(triple) => {
            encode_u8(buf, TXOP_ADD);
            encode_triple(buf, triple);
        }
        TxOp::Retract(triple) => {
            encode_u8(buf, TXOP_RETRACT);
            encode_triple(buf, triple);
        }
        TxOp::Delete(eid) => {
            encode_u8(buf, TXOP_DELETE);
            encode_i64(buf, eid.0);
        }
        TxOp::Erase(eid) => {
            encode_u8(buf, TXOP_ERASE);
            encode_i64(buf, eid.0);
        }
    }
}

fn encode_tx_ops(buf: &mut Vec<u8>, ops: &[TxOp]) {
    encode_u32(buf, ops.len() as u32);
    for op in ops {
        encode_tx_op(buf, op);
    }
}

// ---------------------------------------------------------------------------
// Wire Decoding: Primitives (from &[u8] cursor)
// ---------------------------------------------------------------------------

/// A cursor over a byte slice for decoding.
struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Cursor { data, pos: 0 }
    }

    fn remaining(&self) -> usize {
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

    fn read_i8(&mut self) -> Result<i8> {
        Ok(self.read_u8()? as i8)
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

/// Parse a keyword string like ":foo/bar" or ":baz" into a Keyword.
fn parse_keyword_string(s: &str) -> Result<Keyword> {
    let s = s.strip_prefix(':').ok_or_else(|| anyhow!("Keyword must start with ':'"))?;
    match s.split_once('/') {
        Some((ns, name)) => Ok(Keyword::namespaced(ns, name)),
        None => Ok(Keyword::plain(s)),
    }
}

fn decode_data_type(cursor: &mut Cursor) -> Result<DataType> {
    let tag = cursor.read_u8()?;
    match tag {
        TAG_NIL => bail!("TAG_NIL is no longer a valid DataType"),
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
        TAG_UUID => {
            Ok(DataType::Uuid(Uuid::from_bytes(cursor.read_array()?)))
        }
        TAG_VECTOR => Ok(DataType::Vector(decode_data_type_vec(cursor)?)),
        TAG_MAP => Ok(DataType::Map(decode_data_type_map(cursor)?)),
        TAG_KEYWORD => {
            let s = cursor.read_string()?;
            Ok(DataType::Keyword(parse_keyword_string(&s)?))
        }
        _ => bail!("Unknown DataType tag: {}", tag),
    }
}

fn decode_data_type_vec(cursor: &mut Cursor) -> Result<Vec<DataType>> {
    let count = cursor.read_u32()? as usize;
    let mut vec = Vec::with_capacity(count);
    for _ in 0..count {
        vec.push(decode_data_type(cursor)?);
    }
    Ok(vec)
}

fn decode_data_type_map(cursor: &mut Cursor) -> Result<BTreeMap<String, DataType>> {
    let count = cursor.read_u32()? as usize;
    let mut map = BTreeMap::new();
    for _ in 0..count {
        let k = cursor.read_string()?;
        let v = decode_data_type(cursor)?;
        map.insert(k, v);
    }
    Ok(map)
}

// ---------------------------------------------------------------------------
// Wire Decoding: TxOp
// ---------------------------------------------------------------------------

fn decode_triple(cursor: &mut Cursor) -> Result<Triple> {
    let entity = EntityId(cursor.read_i64()?);
    let attribute = Attribute(cursor.read_string()?);
    let value = Value::new(decode_data_type(cursor)?);
    Ok(Triple {
        entity,
        attribute,
        value,
    })
}

fn decode_tx_op(cursor: &mut Cursor) -> Result<TxOp> {
    let tag = cursor.read_u8()?;
    match tag {
        TXOP_PUT => {
            let map = decode_data_type_map(cursor)?;
            Ok(TxOp::Put(Document(map)))
        }
        TXOP_ADD => Ok(TxOp::Add(decode_triple(cursor)?)),
        TXOP_RETRACT => Ok(TxOp::Retract(decode_triple(cursor)?)),
        TXOP_DELETE => Ok(TxOp::Delete(EntityId(cursor.read_i64()?))),
        TXOP_ERASE => Ok(TxOp::Erase(EntityId(cursor.read_i64()?))),
        _ => bail!("Unknown TxOp tag: {}", tag),
    }
}

fn decode_tx_ops(cursor: &mut Cursor) -> Result<Vec<TxOp>> {
    let count = cursor.read_u32()? as usize;
    let mut ops = Vec::with_capacity(count);
    for _ in 0..count {
        ops.push(decode_tx_op(cursor)?);
    }
    Ok(ops)
}

// ---------------------------------------------------------------------------
// Message Payload Encoding
// ---------------------------------------------------------------------------

fn encode_frontend_payload(buf: &mut Vec<u8>, msg: &FrontendMessage) {
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
        FrontendMessage::OpenDb { basis_tx_id, basis_system_time, basis_seq_num } => {
            encode_option_i64(buf, basis_tx_id);
            encode_option_i64(buf, basis_system_time);
            encode_option_u64(buf, basis_seq_num);
        }
        FrontendMessage::CloseDb { db_id } => {
            encode_u32(buf, *db_id);
        }
        FrontendMessage::Query {
            query_string,
            db_id,
        } => {
            encode_string(buf, query_string);
            encode_u32(buf, *db_id);
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
        } => {
            encode_string(buf, query_string);
            encode_u32(buf, *db_id);
        }
        FrontendMessage::Unsubscribe => {}
        FrontendMessage::BasisForTx { tx_id } => {
            encode_i64(buf, *tx_id);
        }
        FrontendMessage::Terminate => {}
    }
}

fn encode_backend_payload(buf: &mut Vec<u8>, msg: &BackendMessage) {
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
        BackendMessage::CommandComplete { tag, row_count } => {
            encode_string(buf, tag);
            encode_u64(buf, *row_count);
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
            seq_num,
            error_message,
        } => {
            encode_u8(buf, *status);
            encode_i64(buf, *tx_id);
            encode_i64(buf, *system_time);
            encode_u64(buf, *seq_num);
            encode_option_string(buf, error_message);
        }
        BackendMessage::BasisResult { tx_id, system_time, seq_num } => {
            encode_i64(buf, *tx_id);
            encode_i64(buf, *system_time);
            encode_u64(buf, *seq_num);
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

fn decode_frontend_payload(msg_type: u8, cursor: &mut Cursor) -> Result<FrontendMessage> {
    match msg_type {
        MSG_OPEN_DB => Ok(FrontendMessage::OpenDb {
            basis_tx_id: cursor.read_option_i64()?,
            basis_system_time: cursor.read_option_i64()?,
            basis_seq_num: cursor.read_option_u64()?,
        }),
        MSG_CLOSE_DB => Ok(FrontendMessage::CloseDb {
            db_id: cursor.read_u32()?,
        }),
        MSG_QUERY => Ok(FrontendMessage::Query {
            query_string: cursor.read_string()?,
            db_id: cursor.read_u32()?,
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
        }),
        MSG_UNSUBSCRIBE => Ok(FrontendMessage::Unsubscribe),
        MSG_BASIS_FOR_TX => Ok(FrontendMessage::BasisForTx {
            tx_id: cursor.read_i64()?,
        }),
        MSG_TERMINATE => Ok(FrontendMessage::Terminate),
        _ => bail!("Unknown frontend message type: 0x{:02x}", msg_type),
    }
}

fn decode_backend_payload(
    msg_type: u8,
    cursor: &mut Cursor,
) -> Result<BackendMessage> {
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
        MSG_COMMAND_COMPLETE => Ok(BackendMessage::CommandComplete {
            tag: cursor.read_string()?,
            row_count: cursor.read_u64()?,
        }),
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
            seq_num: cursor.read_u64()?,
            error_message: cursor.read_option_string()?,
        }),
        MSG_BASIS_RESULT => Ok(BackendMessage::BasisResult {
            tx_id: cursor.read_i64()?,
            system_time: cursor.read_i64()?,
            seq_num: cursor.read_u64()?,
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

    match msg {
        FrontendMessage::Startup { .. } => {
            // Startup: no type byte, just [length][payload]
            let length = (payload.len() + 4) as u32;
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
                FrontendMessage::BasisForTx { .. } => MSG_BASIS_FOR_TX,
                FrontendMessage::Terminate => MSG_TERMINATE,
                FrontendMessage::Startup { .. } => unreachable!(),
            };
            let length = (payload.len() + 4) as u32;
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

    let type_byte = match msg {
        BackendMessage::AuthenticationOk { .. } => MSG_AUTHENTICATION_OK,
        BackendMessage::DbOpened { .. } => MSG_DB_OPENED,
        BackendMessage::DbClosed { .. } => MSG_DB_CLOSED,
        BackendMessage::RowDescription { .. } => MSG_ROW_DESCRIPTION,
        BackendMessage::DataRow { .. } => MSG_DATA_ROW,
        BackendMessage::CommandComplete { .. } => MSG_COMMAND_COMPLETE,
        BackendMessage::DataBatchComplete { .. } => MSG_DATA_BATCH_COMPLETE,
        BackendMessage::ReadyForQuery { .. } => MSG_READY_FOR_QUERY,
        BackendMessage::TxKey { .. } => MSG_TX_KEY,
        BackendMessage::TxResult { .. } => MSG_TX_RESULT,
        BackendMessage::BasisResult { .. } => MSG_BASIS_RESULT,
        BackendMessage::UnsubscribeComplete => MSG_UNSUBSCRIBE_COMPLETE,
        BackendMessage::Heartbeat => MSG_HEARTBEAT,
        BackendMessage::ErrorResponse { .. } => MSG_ERROR_RESPONSE,
    };

    let length = (payload.len() + 4) as u32;
    writer.write_u8(type_byte).await?;
    writer.write_u32(length).await?;
    writer.write_all(&payload).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
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
        let dt = DataType::Keyword(Keyword::plain("foo"));
        assert_eq!(roundtrip_data_type(&dt), dt);
    }

    #[test]
    fn test_data_type_keyword_namespaced() {
        let dt = DataType::Keyword(Keyword::namespaced("person", "name"));
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
            DataType::Tuple(vec![DataType::String("hello".to_string()), DataType::Boolean(false)]),
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
        let mut map = BTreeMap::new();
        map.insert("db/id".to_string(), DataType::Long(1));
        map.insert("name".to_string(), DataType::String("alice".to_string()));
        let op = TxOp::Put(Document(map));
        assert_eq!(roundtrip_tx_op(&op), op);
    }

    #[test]
    fn test_tx_op_add() {
        let op = TxOp::Add(Triple {
            entity: EntityId(42),
            attribute: Attribute("email".to_string()),
            value: Value::new(DataType::String("test@example.com".to_string())),
        });
        assert_eq!(roundtrip_tx_op(&op), op);
    }

    #[test]
    fn test_tx_op_retract() {
        let op = TxOp::Retract(Triple {
            entity: EntityId(42),
            attribute: Attribute("email".to_string()),
            value: Value::new(DataType::String("old@example.com".to_string())),
        });
        assert_eq!(roundtrip_tx_op(&op), op);
    }

    #[test]
    fn test_tx_op_delete() {
        let op = TxOp::Delete(EntityId(99));
        assert_eq!(roundtrip_tx_op(&op), op);
    }

    #[test]
    fn test_tx_op_erase() {
        let op = TxOp::Erase(EntityId(100));
        assert_eq!(roundtrip_tx_op(&op), op);
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
            basis_tx_id: Some(42),
            basis_system_time: Some(1700000000000000),
            basis_seq_num: Some(7),
        };
        assert_eq!(roundtrip_frontend(&msg).await, msg);

        let msg = FrontendMessage::OpenDb {
            basis_tx_id: None,
            basis_system_time: None,
            basis_seq_num: None,
        };
        assert_eq!(roundtrip_frontend(&msg).await, msg);
    }

    #[tokio::test]
    async fn test_basis_for_tx_roundtrip() {
        let msg = FrontendMessage::BasisForTx { tx_id: 42 };
        assert_eq!(roundtrip_frontend(&msg).await, msg);

        let msg = BackendMessage::BasisResult {
            tx_id: 42,
            system_time: 1_000_000,
            seq_num: 7,
        };
        assert_eq!(roundtrip_backend(&msg).await, msg);
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
        };
        assert_eq!(roundtrip_frontend(&msg).await, msg);
    }

    #[tokio::test]
    async fn test_execute_roundtrip() {
        let mut map = BTreeMap::new();
        map.insert("db/id".to_string(), DataType::Long(1));
        map.insert("name".to_string(), DataType::String("alice".to_string()));
        let msg = FrontendMessage::Execute {
            ops: vec![
                TxOp::Put(Document(map)),
                TxOp::Delete(EntityId(99)),
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
            values: vec![
                DataType::Long(1),
                DataType::String("alice".to_string()),
            ],
        };
        assert_eq!(roundtrip_backend(&msg).await, msg);
    }

    #[tokio::test]
    async fn test_command_complete_roundtrip() {
        let msg = BackendMessage::CommandComplete {
            tag: "SELECT".to_string(),
            row_count: 42,
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
            seq_num: 7,
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
            seq_num: 0,
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
