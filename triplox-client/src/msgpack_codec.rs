//! MessagePack codec for the Triplox HTTP/2 wire protocol.
//!
//! Each HTTP body is a single msgpack value. DataType primitives map to native
//! msgpack types (bool, int, float, str, bin, array, map). Triplox-specific
//! types use ext type codes:
//!
//! | Ext code | Type    | Payload                                        |
//! |---------:|---------|------------------------------------------------|
//! | -1       | Instant | msgpack Timestamp (4/8/12 bytes, nanosecond)   |
//! | 1        | BigInt  | 16 bytes, big-endian i128                      |
//! | 2        | Uuid    | 16 bytes, RFC 4122 raw                         |
//! | 3        | Keyword | UTF-8 "ns/name" (no leading `:`)              |
//!
//! Tagged unions (`EntityRef`, `TxOp`, `QueryArg`) use `{"kind": "<variant>", ...}` maps.

use std::collections::BTreeMap;
use std::io::Write;

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use edn::symbols::Keyword;
use rmpv::Value;
use uuid::Uuid;

use crate::ops::{DataType, EntityRef, QueryArg, TxOp};
use crate::protocol::ColumnDescription;
use crate::transaction::{TxBasis, TxKey};

// =============================================================================
// Ext type codes
// =============================================================================

pub const EXT_TIMESTAMP: i8 = -1;
pub const EXT_BIGINT: i8 = 1;
pub const EXT_UUID: i8 = 2;
pub const EXT_KEYWORD: i8 = 3;

// =============================================================================
// Keyword wire format (no leading `:`)
// =============================================================================

fn keyword_to_wire(kw: &Keyword) -> String {
    match kw.namespace() {
        Some(ns) => format!("{}/{}", ns, kw.name()),
        None => kw.name().to_string(),
    }
}

fn keyword_from_wire(s: &str) -> Result<Keyword> {
    match s.split_once('/') {
        Some((ns, name)) if !ns.is_empty() && !name.is_empty() => Ok(Keyword::namespaced(ns, name)),
        Some(_) => bail!("invalid keyword wire format: {:?}", s),
        None if s.is_empty() => bail!("empty keyword"),
        None => Ok(Keyword::plain(s)),
    }
}

// =============================================================================
// Ext payload encoders / decoders
// =============================================================================

fn write_ext<W: Write>(w: &mut W, ty: i8, payload: &[u8]) -> Result<()> {
    rmp::encode::write_ext_meta(w, payload.len() as u32, ty)?;
    w.write_all(payload)?;
    Ok(())
}

fn write_timestamp<W: Write>(w: &mut W, dt: &DateTime<Utc>) -> Result<()> {
    let secs = dt.timestamp();
    let nanos = dt.timestamp_subsec_nanos();
    if nanos == 0 && secs >= 0 && secs <= u32::MAX as i64 {
        let bytes = (secs as u32).to_be_bytes();
        write_ext(w, EXT_TIMESTAMP, &bytes)
    } else if secs >= 0 && (secs as u64) < (1u64 << 34) {
        let data: u64 = ((nanos as u64) << 34) | (secs as u64);
        let bytes = data.to_be_bytes();
        write_ext(w, EXT_TIMESTAMP, &bytes)
    } else {
        let mut bytes = [0u8; 12];
        bytes[..4].copy_from_slice(&nanos.to_be_bytes());
        bytes[4..].copy_from_slice(&secs.to_be_bytes());
        write_ext(w, EXT_TIMESTAMP, &bytes)
    }
}

fn read_timestamp(payload: &[u8]) -> Result<DateTime<Utc>> {
    let (secs, nanos): (i64, u32) = match payload.len() {
        4 => {
            let secs = u32::from_be_bytes(payload.try_into().unwrap());
            (secs as i64, 0)
        }
        8 => {
            let data = u64::from_be_bytes(payload.try_into().unwrap());
            let nanos = (data >> 34) as u32;
            let secs = (data & 0x0003_ffff_ffff) as i64;
            (secs, nanos)
        }
        12 => {
            let nanos = u32::from_be_bytes(payload[..4].try_into().unwrap());
            let secs = i64::from_be_bytes(payload[4..].try_into().unwrap());
            (secs, nanos)
        }
        n => bail!("invalid msgpack Timestamp payload length {n}"),
    };
    Utc.timestamp_opt(secs, nanos)
        .single()
        .context("invalid timestamp value")
}

fn read_bigint(payload: &[u8]) -> Result<i128> {
    let bytes: [u8; 16] = payload
        .try_into()
        .map_err(|_| anyhow!("BigInt ext payload must be 16 bytes"))?;
    Ok(i128::from_be_bytes(bytes))
}

fn read_uuid(payload: &[u8]) -> Result<Uuid> {
    let bytes: [u8; 16] = payload
        .try_into()
        .map_err(|_| anyhow!("Uuid ext payload must be 16 bytes"))?;
    Ok(Uuid::from_bytes(bytes))
}

fn read_keyword(payload: &[u8]) -> Result<Keyword> {
    let s = std::str::from_utf8(payload).context("keyword payload is not valid UTF-8")?;
    keyword_from_wire(s)
}

// =============================================================================
// DataType
// =============================================================================

pub fn write_data_type<W: Write>(w: &mut W, dt: &DataType) -> Result<()> {
    match dt {
        DataType::Boolean(v) => {
            rmp::encode::write_bool(w, *v)?;
        }
        DataType::Long(v) => {
            rmp::encode::write_sint(w, *v)?;
        }
        DataType::Float(v) => {
            rmp::encode::write_f32(w, *v)?;
        }
        DataType::Double(v) => {
            rmp::encode::write_f64(w, *v)?;
        }
        DataType::String(v) => {
            rmp::encode::write_str(w, v)?;
        }
        DataType::Bytes(v) => {
            rmp::encode::write_bin(w, v)?;
        }
        DataType::Vector(v) => {
            rmp::encode::write_array_len(w, v.len() as u32)?;
            for item in v {
                write_data_type(w, item)?;
            }
        }
        DataType::Map(m) => {
            rmp::encode::write_map_len(w, m.len() as u32)?;
            for (k, v) in m {
                rmp::encode::write_str(w, k)?;
                write_data_type(w, v)?;
            }
        }
        DataType::BigInt(v) => write_ext(w, EXT_BIGINT, &v.to_be_bytes())?,
        DataType::Uuid(v) => write_ext(w, EXT_UUID, v.as_bytes())?,
        DataType::Keyword(v) => {
            let s = keyword_to_wire(v);
            write_ext(w, EXT_KEYWORD, s.as_bytes())?;
        }
        DataType::Instant(v) => write_timestamp(w, v)?,
    }
    Ok(())
}

pub fn data_type_from_value(v: Value) -> Result<DataType> {
    match v {
        Value::Boolean(b) => Ok(DataType::Boolean(b)),
        Value::Integer(n) => n
            .as_i64()
            .map(DataType::Long)
            .ok_or_else(|| anyhow!("integer out of i64 range: {n}")),
        Value::F32(f) => Ok(DataType::Float(f)),
        Value::F64(f) => Ok(DataType::Double(f)),
        Value::String(s) => s
            .into_str()
            .map(DataType::String)
            .ok_or_else(|| anyhow!("string is not valid UTF-8")),
        Value::Binary(b) => Ok(DataType::Bytes(b)),
        Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(data_type_from_value(item)?);
            }
            Ok(DataType::Vector(out))
        }
        Value::Map(entries) => {
            let mut m = BTreeMap::new();
            for (k, v) in entries {
                let key = match k {
                    Value::String(s) => s
                        .into_str()
                        .ok_or_else(|| anyhow!("map key is not valid UTF-8"))?,
                    other => bail!("DataType::Map keys must be strings, got {other:?}"),
                };
                m.insert(key, data_type_from_value(v)?);
            }
            Ok(DataType::Map(m))
        }
        Value::Ext(ty, payload) => match ty {
            EXT_TIMESTAMP => Ok(DataType::Instant(read_timestamp(&payload)?)),
            EXT_BIGINT => Ok(DataType::BigInt(read_bigint(&payload)?)),
            EXT_UUID => Ok(DataType::Uuid(read_uuid(&payload)?)),
            EXT_KEYWORD => Ok(DataType::Keyword(read_keyword(&payload)?)),
            _ => bail!("unknown msgpack ext type {ty}"),
        },
        Value::Nil => bail!("DataType cannot be nil"),
    }
}

pub fn read_data_type(buf: &[u8]) -> Result<(DataType, &[u8])> {
    let mut cursor = buf;
    let value =
        rmpv::decode::read_value(&mut cursor).map_err(|e| anyhow!("msgpack decode error: {e}"))?;
    let dt = data_type_from_value(value)?;
    Ok((dt, cursor))
}

// =============================================================================
// Tagged-union helpers
// =============================================================================

fn map_from_value(v: Value) -> Result<BTreeMap<String, Value>> {
    let entries = match v {
        Value::Map(entries) => entries,
        other => bail!("expected map, got {other:?}"),
    };
    let mut out = BTreeMap::new();
    for (k, v) in entries {
        let key = match k {
            Value::String(s) => s
                .into_str()
                .ok_or_else(|| anyhow!("map key is not valid UTF-8"))?,
            other => bail!("map key must be string, got {other:?}"),
        };
        out.insert(key, v);
    }
    Ok(out)
}

fn take_field(map: &mut BTreeMap<String, Value>, name: &str) -> Result<Value> {
    map.remove(name)
        .ok_or_else(|| anyhow!("missing field {name:?}"))
}

fn take_string(map: &mut BTreeMap<String, Value>, name: &str) -> Result<String> {
    match take_field(map, name)? {
        Value::String(s) => s
            .into_str()
            .ok_or_else(|| anyhow!("field {name:?} is not valid UTF-8")),
        other => bail!("field {name:?} expected string, got {other:?}"),
    }
}

fn take_i64(map: &mut BTreeMap<String, Value>, name: &str) -> Result<i64> {
    match take_field(map, name)? {
        Value::Integer(n) => n
            .as_i64()
            .ok_or_else(|| anyhow!("field {name:?} integer out of i64 range")),
        other => bail!("field {name:?} expected integer, got {other:?}"),
    }
}

fn take_data_type(map: &mut BTreeMap<String, Value>, name: &str) -> Result<DataType> {
    data_type_from_value(take_field(map, name)?)
}

fn write_str_field<W: Write>(w: &mut W, name: &str, value: &str) -> Result<()> {
    rmp::encode::write_str(w, name)?;
    rmp::encode::write_str(w, value)?;
    Ok(())
}

// =============================================================================
// EntityRef
// =============================================================================

pub fn write_entity_ref<W: Write>(w: &mut W, er: &EntityRef) -> Result<()> {
    match er {
        EntityRef::Id(id) => {
            rmp::encode::write_map_len(w, 2)?;
            write_str_field(w, "kind", "id")?;
            rmp::encode::write_str(w, "id")?;
            rmp::encode::write_sint(w, *id)?;
        }
        EntityRef::TempId(s) => {
            rmp::encode::write_map_len(w, 2)?;
            write_str_field(w, "kind", "temp")?;
            write_str_field(w, "temp", s)?;
        }
        EntityRef::Ident(kw) => {
            rmp::encode::write_map_len(w, 2)?;
            write_str_field(w, "kind", "ident")?;
            write_str_field(w, "ident", &keyword_to_wire(kw))?;
        }
        EntityRef::LookupRef(attr, value) => {
            rmp::encode::write_map_len(w, 3)?;
            write_str_field(w, "kind", "lookup")?;
            write_str_field(w, "attr", &keyword_to_wire(attr))?;
            rmp::encode::write_str(w, "value")?;
            write_data_type(w, value)?;
        }
    }
    Ok(())
}

pub fn entity_ref_from_value(v: Value) -> Result<EntityRef> {
    let mut map = map_from_value(v)?;
    let kind = take_string(&mut map, "kind")?;
    match kind.as_str() {
        "id" => Ok(EntityRef::Id(take_i64(&mut map, "id")?)),
        "temp" => Ok(EntityRef::TempId(take_string(&mut map, "temp")?)),
        "ident" => Ok(EntityRef::Ident(keyword_from_wire(&take_string(
            &mut map, "ident",
        )?)?)),
        "lookup" => {
            let attr = keyword_from_wire(&take_string(&mut map, "attr")?)?;
            let value = take_data_type(&mut map, "value")?;
            Ok(EntityRef::LookupRef(attr, value))
        }
        other => bail!("unknown EntityRef kind: {other:?}"),
    }
}

// =============================================================================
// TxOp
// =============================================================================

pub fn write_tx_op<W: Write>(w: &mut W, op: &TxOp) -> Result<()> {
    match op {
        TxOp::Put(doc) => {
            rmp::encode::write_map_len(w, 2)?;
            write_str_field(w, "kind", "put")?;
            rmp::encode::write_str(w, "doc")?;
            rmp::encode::write_map_len(w, doc.len() as u32)?;
            for (k, v) in doc {
                rmp::encode::write_str(w, &keyword_to_wire(k))?;
                write_data_type(w, v)?;
            }
        }
        TxOp::Add {
            entity,
            attribute,
            value,
        } => {
            rmp::encode::write_map_len(w, 4)?;
            write_str_field(w, "kind", "add")?;
            rmp::encode::write_str(w, "entity")?;
            write_entity_ref(w, entity)?;
            write_str_field(w, "attr", &keyword_to_wire(attribute))?;
            rmp::encode::write_str(w, "value")?;
            write_data_type(w, value)?;
        }
        TxOp::Retract {
            entity,
            attribute,
            value,
        } => {
            rmp::encode::write_map_len(w, 4)?;
            write_str_field(w, "kind", "retract")?;
            rmp::encode::write_str(w, "entity")?;
            write_entity_ref(w, entity)?;
            write_str_field(w, "attr", &keyword_to_wire(attribute))?;
            rmp::encode::write_str(w, "value")?;
            write_data_type(w, value)?;
        }
        TxOp::Delete(entity) => {
            rmp::encode::write_map_len(w, 2)?;
            write_str_field(w, "kind", "delete")?;
            rmp::encode::write_str(w, "entity")?;
            write_entity_ref(w, entity)?;
        }
        TxOp::Erase(entity) => {
            rmp::encode::write_map_len(w, 2)?;
            write_str_field(w, "kind", "erase")?;
            rmp::encode::write_str(w, "entity")?;
            write_entity_ref(w, entity)?;
        }
    }
    Ok(())
}

pub fn tx_op_from_value(v: Value) -> Result<TxOp> {
    let mut map = map_from_value(v)?;
    let kind = take_string(&mut map, "kind")?;
    match kind.as_str() {
        "put" => {
            let doc_value = take_field(&mut map, "doc")?;
            let entries = match doc_value {
                Value::Map(entries) => entries,
                other => bail!("Put.doc must be a map, got {other:?}"),
            };
            let mut doc = BTreeMap::new();
            for (k, v) in entries {
                let key_str = match k {
                    Value::String(s) => s
                        .into_str()
                        .ok_or_else(|| anyhow!("Put.doc key is not valid UTF-8"))?,
                    other => bail!("Put.doc key must be string, got {other:?}"),
                };
                doc.insert(keyword_from_wire(&key_str)?, data_type_from_value(v)?);
            }
            Ok(TxOp::Put(doc))
        }
        "add" => {
            let entity = entity_ref_from_value(take_field(&mut map, "entity")?)?;
            let attribute = keyword_from_wire(&take_string(&mut map, "attr")?)?;
            let value = take_data_type(&mut map, "value")?;
            Ok(TxOp::Add {
                entity,
                attribute,
                value,
            })
        }
        "retract" => {
            let entity = entity_ref_from_value(take_field(&mut map, "entity")?)?;
            let attribute = keyword_from_wire(&take_string(&mut map, "attr")?)?;
            let value = take_data_type(&mut map, "value")?;
            Ok(TxOp::Retract {
                entity,
                attribute,
                value,
            })
        }
        "delete" => Ok(TxOp::Delete(entity_ref_from_value(take_field(
            &mut map, "entity",
        )?)?)),
        "erase" => Ok(TxOp::Erase(entity_ref_from_value(take_field(
            &mut map, "entity",
        )?)?)),
        other => bail!("unknown TxOp kind: {other:?}"),
    }
}

// =============================================================================
// QueryArg
// =============================================================================

pub fn write_query_arg<W: Write>(w: &mut W, arg: &QueryArg) -> Result<()> {
    match arg {
        QueryArg::Scalar(dt) => {
            rmp::encode::write_map_len(w, 2)?;
            write_str_field(w, "kind", "scalar")?;
            rmp::encode::write_str(w, "value")?;
            write_data_type(w, dt)?;
        }
        QueryArg::Collection(items) => {
            rmp::encode::write_map_len(w, 2)?;
            write_str_field(w, "kind", "collection")?;
            rmp::encode::write_str(w, "values")?;
            rmp::encode::write_array_len(w, items.len() as u32)?;
            for item in items {
                write_data_type(w, item)?;
            }
        }
        QueryArg::Tuple(items) => {
            rmp::encode::write_map_len(w, 2)?;
            write_str_field(w, "kind", "tuple")?;
            rmp::encode::write_str(w, "values")?;
            rmp::encode::write_array_len(w, items.len() as u32)?;
            for item in items {
                write_data_type(w, item)?;
            }
        }
        QueryArg::Relation(rows) => {
            rmp::encode::write_map_len(w, 2)?;
            write_str_field(w, "kind", "relation")?;
            rmp::encode::write_str(w, "rows")?;
            rmp::encode::write_array_len(w, rows.len() as u32)?;
            for row in rows {
                rmp::encode::write_array_len(w, row.len() as u32)?;
                for v in row {
                    write_data_type(w, v)?;
                }
            }
        }
    }
    Ok(())
}

pub fn query_arg_from_value(v: Value) -> Result<QueryArg> {
    let mut map = map_from_value(v)?;
    let kind = take_string(&mut map, "kind")?;
    match kind.as_str() {
        "scalar" => Ok(QueryArg::Scalar(take_data_type(&mut map, "value")?)),
        "collection" => Ok(QueryArg::Collection(take_data_type_array(
            &mut map, "values",
        )?)),
        "tuple" => Ok(QueryArg::Tuple(take_data_type_array(&mut map, "values")?)),
        "relation" => {
            let rows_value = take_field(&mut map, "rows")?;
            let rows = match rows_value {
                Value::Array(arr) => arr,
                other => bail!("Relation.rows must be an array, got {other:?}"),
            };
            let mut out = Vec::with_capacity(rows.len());
            for row in rows {
                let row = match row {
                    Value::Array(arr) => arr,
                    other => bail!("Relation row must be an array, got {other:?}"),
                };
                let mut typed = Vec::with_capacity(row.len());
                for v in row {
                    typed.push(data_type_from_value(v)?);
                }
                out.push(typed);
            }
            Ok(QueryArg::Relation(out))
        }
        other => bail!("unknown QueryArg kind: {other:?}"),
    }
}

fn take_data_type_array(map: &mut BTreeMap<String, Value>, name: &str) -> Result<Vec<DataType>> {
    let v = take_field(map, name)?;
    let arr = match v {
        Value::Array(arr) => arr,
        other => bail!("field {name:?} expected array, got {other:?}"),
    };
    let mut out = Vec::with_capacity(arr.len());
    for item in arr {
        out.push(data_type_from_value(item)?);
    }
    Ok(out)
}

fn take_optional_string(map: &mut BTreeMap<String, Value>, name: &str) -> Result<Option<String>> {
    let Some(v) = map.remove(name) else {
        return Ok(None);
    };
    match v {
        Value::Nil => Ok(None),
        Value::String(s) => s
            .into_str()
            .map(Some)
            .ok_or_else(|| anyhow!("field {name:?} is not valid UTF-8")),
        other => bail!("field {name:?} expected string or nil, got {other:?}"),
    }
}

fn take_optional_i64(map: &mut BTreeMap<String, Value>, name: &str) -> Result<Option<i64>> {
    let Some(v) = map.remove(name) else {
        return Ok(None);
    };
    match v {
        Value::Nil => Ok(None),
        Value::Integer(n) => n
            .as_i64()
            .map(Some)
            .ok_or_else(|| anyhow!("field {name:?} integer out of i64 range")),
        other => bail!("field {name:?} expected integer or nil, got {other:?}"),
    }
}

fn take_optional_timestamp(
    map: &mut BTreeMap<String, Value>,
    name: &str,
) -> Result<Option<DateTime<Utc>>> {
    let Some(v) = map.remove(name) else {
        return Ok(None);
    };
    match v {
        Value::Nil => Ok(None),
        Value::Ext(EXT_TIMESTAMP, payload) => Ok(Some(read_timestamp(&payload)?)),
        other => bail!("field {name:?} expected timestamp ext or nil, got {other:?}"),
    }
}

fn take_timestamp(map: &mut BTreeMap<String, Value>, name: &str) -> Result<DateTime<Utc>> {
    match take_field(map, name)? {
        Value::Ext(EXT_TIMESTAMP, payload) => read_timestamp(&payload),
        other => bail!("field {name:?} expected timestamp ext, got {other:?}"),
    }
}

fn write_optional_string<W: Write>(w: &mut W, opt: &Option<String>) -> Result<()> {
    match opt {
        Some(s) => {
            rmp::encode::write_str(w, s)?;
        }
        None => {
            rmp::encode::write_nil(w)?;
        }
    }
    Ok(())
}

fn write_optional_i64<W: Write>(w: &mut W, opt: Option<i64>) -> Result<()> {
    match opt {
        Some(v) => {
            rmp::encode::write_sint(w, v)?;
        }
        None => {
            rmp::encode::write_nil(w)?;
        }
    }
    Ok(())
}

fn write_optional_timestamp<W: Write>(w: &mut W, opt: Option<DateTime<Utc>>) -> Result<()> {
    match opt {
        Some(t) => write_timestamp(w, &t)?,
        None => {
            rmp::encode::write_nil(w)?;
        }
    }
    Ok(())
}

fn read_body_value(data: &[u8]) -> Result<Value> {
    let mut cursor = data;
    let v =
        rmpv::decode::read_value(&mut cursor).map_err(|e| anyhow!("msgpack decode error: {e}"))?;
    if !cursor.is_empty() {
        bail!("trailing bytes after msgpack body");
    }
    Ok(v)
}

// =============================================================================
// HTTP body encode / decode
// =============================================================================

#[derive(Debug, Clone, PartialEq)]
pub struct OpenDbRequest {
    pub tx_id: Option<i64>,
    pub system_time: Option<DateTime<Utc>>,
    pub tx_eid: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DbOpenedResponse {
    pub tx_id: i64,
    pub system_time: DateTime<Utc>,
    pub tx_eid: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryRequest {
    pub db: TxBasis,
    pub query: String,
    pub args: Vec<QueryArg>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryResponse {
    pub columns: Vec<ColumnDescription>,
    pub rows: Vec<Vec<DataType>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExecuteRequest {
    pub ops: Vec<TxOp>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TxKeyResponse {
    pub tx_id: i64,
    pub system_time: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TxResultResponse {
    pub status: u8,
    pub tx_id: i64,
    pub system_time: DateTime<Utc>,
    pub tx_eid: i64,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ErrorResponseBody {
    pub severity: u8,
    pub code: u16,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
}

/// Encode an OpenDb request: `{"tx_id": int|nil, "system_time": Timestamp|nil, "tx_eid": int|nil}`.
pub fn encode_open_db_request(req: &OpenDbRequest) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    rmp::encode::write_map_len(&mut buf, 3)?;
    rmp::encode::write_str(&mut buf, "tx_id")?;
    write_optional_i64(&mut buf, req.tx_id)?;
    rmp::encode::write_str(&mut buf, "system_time")?;
    write_optional_timestamp(&mut buf, req.system_time)?;
    rmp::encode::write_str(&mut buf, "tx_eid")?;
    write_optional_i64(&mut buf, req.tx_eid)?;
    Ok(buf)
}

pub fn decode_open_db_request(data: &[u8]) -> Result<OpenDbRequest> {
    let mut map = map_from_value(read_body_value(data)?)?;
    let tx_id = take_optional_i64(&mut map, "tx_id")?;
    let system_time = take_optional_timestamp(&mut map, "system_time")?;
    let tx_eid = take_optional_i64(&mut map, "tx_eid")?;
    Ok(OpenDbRequest {
        tx_id,
        system_time,
        tx_eid,
    })
}

/// Encode a DbOpened response: `{"tx_id": int, "system_time": Timestamp, "tx_eid": int}`.
pub fn encode_db_opened_response(resp: &DbOpenedResponse) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    rmp::encode::write_map_len(&mut buf, 3)?;
    rmp::encode::write_str(&mut buf, "tx_id")?;
    rmp::encode::write_sint(&mut buf, resp.tx_id)?;
    rmp::encode::write_str(&mut buf, "system_time")?;
    write_timestamp(&mut buf, &resp.system_time)?;
    rmp::encode::write_str(&mut buf, "tx_eid")?;
    rmp::encode::write_sint(&mut buf, resp.tx_eid)?;
    Ok(buf)
}

pub fn decode_db_opened_response(data: &[u8]) -> Result<DbOpenedResponse> {
    let mut map = map_from_value(read_body_value(data)?)?;
    let tx_id = take_i64(&mut map, "tx_id")?;
    let system_time = take_timestamp(&mut map, "system_time")?;
    let tx_eid = take_i64(&mut map, "tx_eid")?;
    Ok(DbOpenedResponse {
        tx_id,
        system_time,
        tx_eid,
    })
}

fn write_tx_basis<W: Write>(w: &mut W, basis: &TxBasis) -> Result<()> {
    rmp::encode::write_map_len(w, 3)?;
    rmp::encode::write_str(w, "tx_id")?;
    rmp::encode::write_sint(w, basis.tx_key.tx_id)?;
    rmp::encode::write_str(w, "system_time")?;
    write_timestamp(w, &basis.tx_key.system_time)?;
    rmp::encode::write_str(w, "tx_eid")?;
    rmp::encode::write_sint(w, basis.tx_eid)?;
    Ok(())
}

fn tx_basis_from_value(value: Value) -> Result<TxBasis> {
    let mut map = map_from_value(value)?;
    Ok(TxBasis {
        tx_key: TxKey {
            tx_id: take_i64(&mut map, "tx_id")?,
            system_time: take_timestamp(&mut map, "system_time")?,
        },
        tx_eid: take_i64(&mut map, "tx_eid")?,
    })
}

/// Encode a Query request: `{"db": TxBasis, "query": str, "args": [QueryArg, ...]}`.
pub fn encode_query_request(req: &QueryRequest) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    rmp::encode::write_map_len(&mut buf, 3)?;
    rmp::encode::write_str(&mut buf, "db")?;
    write_tx_basis(&mut buf, &req.db)?;
    rmp::encode::write_str(&mut buf, "query")?;
    rmp::encode::write_str(&mut buf, &req.query)?;
    rmp::encode::write_str(&mut buf, "args")?;
    rmp::encode::write_array_len(&mut buf, req.args.len() as u32)?;
    for arg in &req.args {
        write_query_arg(&mut buf, arg)?;
    }
    Ok(buf)
}

pub fn decode_query_request(data: &[u8]) -> Result<QueryRequest> {
    let mut map = map_from_value(read_body_value(data)?)?;
    let db = tx_basis_from_value(take_field(&mut map, "db")?)?;
    let query = take_string(&mut map, "query")?;
    let arr = match take_field(&mut map, "args")? {
        Value::Array(arr) => arr,
        other => bail!("field \"args\" expected array, got {other:?}"),
    };
    let mut args = Vec::with_capacity(arr.len());
    for item in arr {
        args.push(query_arg_from_value(item)?);
    }
    Ok(QueryRequest { db, query, args })
}

/// Encode a query response: `{"columns": [...], "rows": [[...], ...]}`.
pub fn encode_query_response(resp: &QueryResponse) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    rmp::encode::write_map_len(&mut buf, 2)?;
    rmp::encode::write_str(&mut buf, "columns")?;
    rmp::encode::write_array_len(&mut buf, resp.columns.len() as u32)?;
    for col in &resp.columns {
        write_column_description(&mut buf, col)?;
    }
    rmp::encode::write_str(&mut buf, "rows")?;
    rmp::encode::write_array_len(&mut buf, resp.rows.len() as u32)?;
    for row in &resp.rows {
        rmp::encode::write_array_len(&mut buf, row.len() as u32)?;
        for v in row {
            write_data_type(&mut buf, v)?;
        }
    }
    Ok(buf)
}

pub fn decode_query_response(data: &[u8]) -> Result<QueryResponse> {
    let mut map = map_from_value(read_body_value(data)?)?;
    let cols_arr = match take_field(&mut map, "columns")? {
        Value::Array(arr) => arr,
        other => bail!("field \"columns\" expected array, got {other:?}"),
    };
    let mut columns = Vec::with_capacity(cols_arr.len());
    for item in cols_arr {
        columns.push(column_description_from_value(item)?);
    }
    let rows_arr = match take_field(&mut map, "rows")? {
        Value::Array(arr) => arr,
        other => bail!("field \"rows\" expected array, got {other:?}"),
    };
    let mut rows = Vec::with_capacity(rows_arr.len());
    for row in rows_arr {
        let row_arr = match row {
            Value::Array(arr) => arr,
            other => bail!("row expected array, got {other:?}"),
        };
        let mut typed = Vec::with_capacity(row_arr.len());
        for v in row_arr {
            typed.push(data_type_from_value(v)?);
        }
        rows.push(typed);
    }
    Ok(QueryResponse { columns, rows })
}

fn write_column_description<W: Write>(w: &mut W, col: &ColumnDescription) -> Result<()> {
    let len = if col.members.is_some() { 3 } else { 2 };
    rmp::encode::write_map_len(w, len)?;
    rmp::encode::write_str(w, "name")?;
    rmp::encode::write_str(w, &col.name)?;
    rmp::encode::write_str(w, "type")?;
    rmp::encode::write_uint(w, col.data_type as u64)?;
    if let Some(members) = &col.members {
        rmp::encode::write_str(w, "members")?;
        rmp::encode::write_array_len(w, members.len() as u32)?;
        for m in members {
            rmp::encode::write_uint(w, *m as u64)?;
        }
    }
    Ok(())
}

fn column_description_from_value(v: Value) -> Result<ColumnDescription> {
    let mut map = map_from_value(v)?;
    let name = take_string(&mut map, "name")?;
    let data_type_i64 = take_i64(&mut map, "type")?;
    if data_type_i64 < 0 || data_type_i64 > u8::MAX as i64 {
        bail!("column type tag out of u8 range: {data_type_i64}");
    }
    let data_type = data_type_i64 as u8;
    let members = match map.remove("members") {
        None => None,
        Some(Value::Nil) => None,
        Some(Value::Array(arr)) => {
            let mut tags = Vec::with_capacity(arr.len());
            for item in arr {
                let tag = match item {
                    Value::Integer(n) => n
                        .as_u64()
                        .ok_or_else(|| anyhow!("union member tag not unsigned"))?,
                    other => bail!("union member must be integer, got {other:?}"),
                };
                if tag > u8::MAX as u64 {
                    bail!("union member tag out of u8 range: {tag}");
                }
                tags.push(tag as u8);
            }
            Some(tags)
        }
        Some(other) => bail!("\"members\" expected array, got {other:?}"),
    };
    Ok(ColumnDescription {
        name,
        data_type,
        members,
    })
}

/// Encode an Execute request: `{"ops": [TxOp, ...]}`.
pub fn encode_execute_request(req: &ExecuteRequest) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    rmp::encode::write_map_len(&mut buf, 1)?;
    rmp::encode::write_str(&mut buf, "ops")?;
    rmp::encode::write_array_len(&mut buf, req.ops.len() as u32)?;
    for op in &req.ops {
        write_tx_op(&mut buf, op)?;
    }
    Ok(buf)
}

pub fn decode_execute_request(data: &[u8]) -> Result<ExecuteRequest> {
    let mut map = map_from_value(read_body_value(data)?)?;
    let arr = match take_field(&mut map, "ops")? {
        Value::Array(arr) => arr,
        other => bail!("field \"ops\" expected array, got {other:?}"),
    };
    let mut ops = Vec::with_capacity(arr.len());
    for item in arr {
        ops.push(tx_op_from_value(item)?);
    }
    Ok(ExecuteRequest { ops })
}

/// Encode a TxKey response: `{"tx_id": int, "system_time": Timestamp}`.
pub fn encode_tx_key_response(resp: &TxKeyResponse) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    rmp::encode::write_map_len(&mut buf, 2)?;
    rmp::encode::write_str(&mut buf, "tx_id")?;
    rmp::encode::write_sint(&mut buf, resp.tx_id)?;
    rmp::encode::write_str(&mut buf, "system_time")?;
    write_timestamp(&mut buf, &resp.system_time)?;
    Ok(buf)
}

pub fn decode_tx_key_response(data: &[u8]) -> Result<TxKeyResponse> {
    let mut map = map_from_value(read_body_value(data)?)?;
    let tx_id = take_i64(&mut map, "tx_id")?;
    let system_time = take_timestamp(&mut map, "system_time")?;
    Ok(TxKeyResponse { tx_id, system_time })
}

/// Encode a TxResult response.
pub fn encode_tx_result_response(resp: &TxResultResponse) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    rmp::encode::write_map_len(&mut buf, 5)?;
    rmp::encode::write_str(&mut buf, "status")?;
    rmp::encode::write_uint(&mut buf, resp.status as u64)?;
    rmp::encode::write_str(&mut buf, "tx_id")?;
    rmp::encode::write_sint(&mut buf, resp.tx_id)?;
    rmp::encode::write_str(&mut buf, "system_time")?;
    write_timestamp(&mut buf, &resp.system_time)?;
    rmp::encode::write_str(&mut buf, "tx_eid")?;
    rmp::encode::write_sint(&mut buf, resp.tx_eid)?;
    rmp::encode::write_str(&mut buf, "error_message")?;
    write_optional_string(&mut buf, &resp.error_message)?;
    Ok(buf)
}

pub fn decode_tx_result_response(data: &[u8]) -> Result<TxResultResponse> {
    let mut map = map_from_value(read_body_value(data)?)?;
    let status_i64 = take_i64(&mut map, "status")?;
    if status_i64 < 0 || status_i64 > u8::MAX as i64 {
        bail!("status out of u8 range: {status_i64}");
    }
    let tx_id = take_i64(&mut map, "tx_id")?;
    let system_time = take_timestamp(&mut map, "system_time")?;
    let tx_eid = take_i64(&mut map, "tx_eid")?;
    let error_message = take_optional_string(&mut map, "error_message")?;
    Ok(TxResultResponse {
        status: status_i64 as u8,
        tx_id,
        system_time,
        tx_eid,
        error_message,
    })
}

/// Encode an ErrorResponse body.
pub fn encode_error_body(resp: &ErrorResponseBody) -> Result<Vec<u8>> {
    let severity_str = match resp.severity {
        b'E' => "E",
        b'F' => "F",
        other => bail!("invalid severity byte: {other:#x}"),
    };
    let mut buf = Vec::new();
    rmp::encode::write_map_len(&mut buf, 5)?;
    rmp::encode::write_str(&mut buf, "severity")?;
    rmp::encode::write_str(&mut buf, severity_str)?;
    rmp::encode::write_str(&mut buf, "code")?;
    rmp::encode::write_uint(&mut buf, resp.code as u64)?;
    rmp::encode::write_str(&mut buf, "message")?;
    rmp::encode::write_str(&mut buf, &resp.message)?;
    rmp::encode::write_str(&mut buf, "detail")?;
    write_optional_string(&mut buf, &resp.detail)?;
    rmp::encode::write_str(&mut buf, "hint")?;
    write_optional_string(&mut buf, &resp.hint)?;
    Ok(buf)
}

pub fn decode_error_body(data: &[u8]) -> Result<ErrorResponseBody> {
    let mut map = map_from_value(read_body_value(data)?)?;
    let severity_str = take_string(&mut map, "severity")?;
    let severity = match severity_str.as_str() {
        "E" => b'E',
        "F" => b'F',
        other => bail!("invalid severity string: {other:?}"),
    };
    let code_i64 = take_i64(&mut map, "code")?;
    if code_i64 < 0 || code_i64 > u16::MAX as i64 {
        bail!("code out of u16 range: {code_i64}");
    }
    let message = take_string(&mut map, "message")?;
    let detail = take_optional_string(&mut map, "detail")?;
    let hint = take_optional_string(&mut map, "hint")?;
    Ok(ErrorResponseBody {
        severity,
        code: code_i64 as u16,
        message,
        detail,
        hint,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use edn::kw;

    fn round_trip(dt: &DataType) -> DataType {
        let mut buf = Vec::new();
        write_data_type(&mut buf, dt).expect("encode");
        let (decoded, rest) = read_data_type(&buf).expect("decode");
        assert!(rest.is_empty(), "unexpected trailing bytes");
        decoded
    }

    #[test]
    fn round_trip_boolean() {
        assert_eq!(
            round_trip(&DataType::Boolean(true)),
            DataType::Boolean(true)
        );
        assert_eq!(
            round_trip(&DataType::Boolean(false)),
            DataType::Boolean(false)
        );
    }

    #[test]
    fn round_trip_long() {
        for v in [
            0i64,
            1,
            -1,
            i64::MAX,
            i64::MIN,
            127,
            128,
            -32,
            -33,
            i32::MAX as i64,
            i32::MIN as i64,
        ] {
            assert_eq!(round_trip(&DataType::Long(v)), DataType::Long(v), "v = {v}");
        }
    }

    #[test]
    fn round_trip_float_and_double_distinct() {
        let f = DataType::Float(1.5_f32);
        let d = DataType::Double(1.5_f64);
        assert_eq!(round_trip(&f), f);
        assert_eq!(round_trip(&d), d);
        // Ensure the wire is a 5-byte float32 vs 9-byte float64.
        let mut fb = Vec::new();
        write_data_type(&mut fb, &f).unwrap();
        let mut db = Vec::new();
        write_data_type(&mut db, &d).unwrap();
        assert_eq!(fb.len(), 5);
        assert_eq!(db.len(), 9);
    }

    #[test]
    fn round_trip_string_and_bytes() {
        for s in ["", "a", "hello", "𝄞 unicode 漢字"] {
            assert_eq!(
                round_trip(&DataType::String(s.into())),
                DataType::String(s.into())
            );
        }
        for b in [vec![], vec![0u8], vec![0, 1, 255], (0u8..=255).collect()] {
            assert_eq!(round_trip(&DataType::Bytes(b.clone())), DataType::Bytes(b));
        }
    }

    #[test]
    fn round_trip_bigint() {
        for v in [0i128, 1, -1, i128::MAX, i128::MIN, i64::MAX as i128 + 1] {
            assert_eq!(
                round_trip(&DataType::BigInt(v)),
                DataType::BigInt(v),
                "v = {v}"
            );
        }
    }

    #[test]
    fn round_trip_uuid() {
        let u = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        assert_eq!(round_trip(&DataType::Uuid(u)), DataType::Uuid(u));
        assert_eq!(
            round_trip(&DataType::Uuid(Uuid::nil())),
            DataType::Uuid(Uuid::nil())
        );
    }

    #[test]
    fn round_trip_keyword() {
        let plain = DataType::Keyword(kw!(:foo));
        let ns = DataType::Keyword(kw!(:person/name));
        assert_eq!(round_trip(&plain), plain);
        assert_eq!(round_trip(&ns), ns);
    }

    #[test]
    fn round_trip_instant() {
        // 4-byte form (post-1970, no nanos, fits u32 secs)
        let a = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
        // 8-byte form (post-1970, nanos)
        let b = Utc.timestamp_opt(1_700_000_000, 123_456_789).unwrap();
        // 12-byte form (pre-1970)
        let c = Utc.timestamp_opt(-1_000_000_000, 500_000_000).unwrap();
        // 12-byte form (post-2106)
        let d = Utc.timestamp_opt(1u64.wrapping_shl(34) as i64, 0).unwrap();
        for instant in [a, b, c, d] {
            assert_eq!(
                round_trip(&DataType::Instant(instant)),
                DataType::Instant(instant),
                "instant = {instant}"
            );
        }
    }

    #[test]
    fn round_trip_vector() {
        let empty = DataType::Vector(vec![]);
        assert_eq!(round_trip(&empty), empty);
        let v = DataType::Vector(vec![
            DataType::Long(1),
            DataType::String("two".into()),
            DataType::Boolean(true),
        ]);
        assert_eq!(round_trip(&v), v);
        // Nested
        let nested = DataType::Vector(vec![DataType::Vector(vec![DataType::Long(42)])]);
        assert_eq!(round_trip(&nested), nested);
    }

    #[test]
    fn round_trip_map() {
        let mut m = BTreeMap::new();
        m.insert("a".to_string(), DataType::Long(1));
        m.insert("b".to_string(), DataType::String("x".into()));
        let dt = DataType::Map(m);
        assert_eq!(round_trip(&dt), dt);
        // Empty
        assert_eq!(
            round_trip(&DataType::Map(BTreeMap::new())),
            DataType::Map(BTreeMap::new())
        );
        // Nested
        let mut inner = BTreeMap::new();
        inner.insert("k".to_string(), DataType::Long(99));
        let mut outer = BTreeMap::new();
        outer.insert("nested".to_string(), DataType::Map(inner));
        let dt = DataType::Map(outer);
        assert_eq!(round_trip(&dt), dt);
    }

    fn read_value_from_buf(buf: &[u8]) -> (Value, &[u8]) {
        let mut cursor = buf;
        let v = rmpv::decode::read_value(&mut cursor).unwrap();
        (v, cursor)
    }

    fn round_trip_entity_ref(er: &EntityRef) -> EntityRef {
        let mut buf = Vec::new();
        write_entity_ref(&mut buf, er).unwrap();
        let (value, rest) = read_value_from_buf(&buf);
        assert!(rest.is_empty());
        entity_ref_from_value(value).unwrap()
    }

    fn round_trip_tx_op(op: &TxOp) -> TxOp {
        let mut buf = Vec::new();
        write_tx_op(&mut buf, op).unwrap();
        let (value, rest) = read_value_from_buf(&buf);
        assert!(rest.is_empty());
        tx_op_from_value(value).unwrap()
    }

    fn round_trip_query_arg(arg: &QueryArg) -> QueryArg {
        let mut buf = Vec::new();
        write_query_arg(&mut buf, arg).unwrap();
        let (value, rest) = read_value_from_buf(&buf);
        assert!(rest.is_empty());
        query_arg_from_value(value).unwrap()
    }

    #[test]
    fn round_trip_entity_ref_variants() {
        let cases = vec![
            EntityRef::Id(42),
            EntityRef::Id(-1),
            EntityRef::TempId("tempid-1".into()),
            EntityRef::Ident(kw!(:person/name)),
            EntityRef::Ident(kw!(:plain)),
            EntityRef::LookupRef(kw!(:user/email), DataType::String("a@b.c".into())),
        ];
        for er in cases {
            assert_eq!(round_trip_entity_ref(&er), er);
        }
    }

    #[test]
    fn round_trip_tx_op_variants() {
        let cases = vec![
            TxOp::put(vec![
                (kw!(:db/id), DataType::Long(1)),
                (kw!(:person/name), DataType::String("alice".into())),
            ]),
            TxOp::Add {
                entity: EntityRef::Id(1),
                attribute: kw!(:person/age),
                value: DataType::Long(30),
            },
            TxOp::Retract {
                entity: EntityRef::Ident(kw!(:db/ident)),
                attribute: kw!(:db/doc),
                value: DataType::String("doc".into()),
            },
            TxOp::Delete(EntityRef::Id(99)),
            TxOp::Erase(EntityRef::Id(100)),
        ];
        for op in cases {
            assert_eq!(round_trip_tx_op(&op), op);
        }
    }

    #[test]
    fn round_trip_query_arg_variants() {
        let cases = vec![
            QueryArg::Scalar(DataType::String("alice".into())),
            QueryArg::Scalar(DataType::Long(7)),
            QueryArg::Collection(vec![
                DataType::Long(1),
                DataType::Long(2),
                DataType::Long(3),
            ]),
            QueryArg::Tuple(vec![DataType::String("x".into()), DataType::Long(99)]),
            QueryArg::Relation(vec![
                vec![DataType::Long(1), DataType::String("a".into())],
                vec![DataType::Long(2), DataType::String("b".into())],
            ]),
        ];
        for arg in cases {
            assert_eq!(round_trip_query_arg(&arg), arg);
        }
    }

    #[test]
    fn round_trip_open_db_request_bodies() {
        for (tx_id, system_time, tx_eid) in [
            (None, None, None),
            (
                Some(42i64),
                Some(Utc.timestamp_opt(1_700_000_000, 0).unwrap()),
                Some(100i64),
            ),
            (Some(-1), Some(Utc.timestamp_opt(0, 1).unwrap()), Some(101)),
        ] {
            let request = OpenDbRequest {
                tx_id,
                system_time,
                tx_eid,
            };
            let buf = encode_open_db_request(&request).unwrap();
            assert_eq!(decode_open_db_request(&buf).unwrap(), request);
        }
    }

    #[test]
    fn round_trip_db_opened_response_body() {
        let system_time = Utc.timestamp_opt(1_700_000_001, 0).unwrap();
        let response = DbOpenedResponse {
            tx_id: 7,
            system_time,
            tx_eid: 12345,
        };
        let buf = encode_db_opened_response(&response).unwrap();
        assert_eq!(decode_db_opened_response(&buf).unwrap(), response);
    }

    #[test]
    fn round_trip_query_request_body() {
        let q = "{:find [?n] :where [[?e :name ?n]]}";
        let db = TxBasis {
            tx_key: TxKey {
                tx_id: 42,
                system_time: Utc.timestamp_opt(1_700_000_002, 0).unwrap(),
            },
            tx_eid: 100,
        };
        let args = vec![
            QueryArg::Scalar(DataType::Long(7)),
            QueryArg::Collection(vec![
                DataType::String("a".into()),
                DataType::String("b".into()),
            ]),
        ];
        let request = QueryRequest {
            db,
            query: q.into(),
            args,
        };
        let buf = encode_query_request(&request).unwrap();
        assert_eq!(decode_query_request(&buf).unwrap(), request);
    }

    #[test]
    fn round_trip_query_response_body() {
        let columns = vec![
            ColumnDescription {
                name: "?e".into(),
                data_type: 7,
                members: None,
            },
            ColumnDescription {
                name: "?val".into(),
                data_type: 127,
                members: Some(vec![7, 9]),
            },
        ];
        let rows = vec![
            vec![DataType::Long(1), DataType::String("x".into())],
            vec![DataType::Long(2), DataType::Long(99)],
        ];
        let response = QueryResponse { columns, rows };
        let buf = encode_query_response(&response).unwrap();
        assert_eq!(decode_query_response(&buf).unwrap(), response);
    }

    #[test]
    fn round_trip_execute_request_body() {
        let ops = vec![
            TxOp::put(vec![(kw!(:name), DataType::String("alice".into()))]),
            TxOp::Add {
                entity: EntityRef::Id(42),
                attribute: kw!(:age),
                value: DataType::Long(30),
            },
        ];
        let request = ExecuteRequest { ops };
        let buf = encode_execute_request(&request).unwrap();
        assert_eq!(decode_execute_request(&buf).unwrap(), request);
    }

    #[test]
    fn round_trip_tx_key_response_body() {
        let now = Utc.timestamp_opt(1_700_000_000, 123_456).unwrap();
        let response = TxKeyResponse {
            tx_id: 101,
            system_time: now,
        };
        let buf = encode_tx_key_response(&response).unwrap();
        assert_eq!(decode_tx_key_response(&buf).unwrap(), response);
    }

    #[test]
    fn round_trip_tx_result_response_body() {
        let now = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
        for (status, err) in [(0u8, None), (1u8, Some("boom".to_string()))] {
            let response = TxResultResponse {
                status,
                tx_id: 7,
                system_time: now,
                tx_eid: 123,
                error_message: err,
            };
            let buf = encode_tx_result_response(&response).unwrap();
            assert_eq!(decode_tx_result_response(&buf).unwrap(), response);
        }
    }

    #[test]
    fn round_trip_error_body() {
        let response = ErrorResponseBody {
            severity: b'E',
            code: 2001,
            message: "parse error".into(),
            detail: Some("near token X".into()),
            hint: None,
        };
        let buf = encode_error_body(&response).unwrap();
        assert_eq!(decode_error_body(&buf).unwrap(), response);

        let response = ErrorResponseBody {
            severity: b'F',
            code: 1000,
            message: "fatal".into(),
            detail: None,
            hint: None,
        };
        let buf = encode_error_body(&response).unwrap();
        assert_eq!(decode_error_body(&buf).unwrap(), response);
    }

    #[test]
    fn keyword_wire_format_strips_leading_colon() {
        let ns = kw!(:person/name);
        let mut buf = Vec::new();
        write_data_type(&mut buf, &DataType::Keyword(ns)).unwrap();
        // ext-3 with payload "person/name"
        // Marker for fixext is 0xd4..0xd8 for size 1/2/4/8/16, ext8 is 0xc7.
        // payload "person/name" is 11 bytes → ext8 (0xc7) + len(0x0b) + type(0x03) + 11 bytes
        assert_eq!(buf[0], 0xc7);
        assert_eq!(buf[1], 11);
        assert_eq!(buf[2] as i8, EXT_KEYWORD);
        assert_eq!(&buf[3..], b"person/name");
    }

    // ---------------------------------------------------------------------
    // Negative-path tests
    // ---------------------------------------------------------------------

    fn pack(v: Value) -> Vec<u8> {
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &v).unwrap();
        buf
    }

    #[test]
    fn decode_open_db_rejects_non_map_body() {
        // Optional fields silently default to None when absent (forward-compat).
        // But a non-map body is unambiguously malformed.
        assert!(decode_open_db_request(&pack(Value::Integer(1.into()))).is_err());
    }

    #[test]
    fn decode_db_opened_rejects_wrong_type() {
        // {"tx_id": "not an int", "system_time": ts, "tx_eid": 7}
        let body = pack(Value::Map(vec![
            (Value::String("tx_id".into()), Value::String("nope".into())),
            (
                Value::String("system_time".into()),
                Value::Ext(EXT_TIMESTAMP, vec![0, 0, 0, 0]),
            ),
            (Value::String("tx_eid".into()), Value::Integer(7.into())),
        ]));
        assert!(decode_db_opened_response(&body).is_err());
    }

    #[test]
    fn decode_db_opened_rejects_missing_system_time() {
        let body = pack(Value::Map(vec![
            (Value::String("tx_id".into()), Value::Integer(1.into())),
            (Value::String("tx_eid".into()), Value::Integer(2.into())),
        ]));
        assert!(decode_db_opened_response(&body).is_err());
    }

    #[test]
    fn decode_tx_result_rejects_status_overflow() {
        // status = 256 — out of u8 range
        let body = pack(Value::Map(vec![
            (Value::String("status".into()), Value::Integer(256.into())),
            (Value::String("tx_id".into()), Value::Integer(1.into())),
            (
                Value::String("system_time".into()),
                Value::Ext(EXT_TIMESTAMP, vec![0, 0, 0, 0]),
            ),
            (Value::String("error_message".into()), Value::Nil),
        ]));
        assert!(decode_tx_result_response(&body).is_err());
    }

    #[test]
    fn decode_error_rejects_invalid_severity() {
        let body = pack(Value::Map(vec![
            (Value::String("severity".into()), Value::String("Q".into())),
            (Value::String("code".into()), Value::Integer(1.into())),
            (Value::String("message".into()), Value::String("x".into())),
            (Value::String("detail".into()), Value::Nil),
            (Value::String("hint".into()), Value::Nil),
        ]));
        assert!(decode_error_body(&body).is_err());
    }

    #[test]
    fn encode_error_rejects_invalid_severity() {
        let r = encode_error_body(&ErrorResponseBody {
            severity: b'X',
            code: 1,
            message: "x".into(),
            detail: None,
            hint: None,
        });
        assert!(r.is_err(), "expected bail on invalid severity");
    }

    #[test]
    fn decode_unknown_msgpack_ext_fails() {
        // ext code 99 is not assigned
        let body = pack(Value::Ext(99, vec![0; 4]));
        assert!(read_data_type(&body).is_err());
    }

    #[test]
    fn decode_data_type_rejects_nil() {
        let body = pack(Value::Nil);
        assert!(read_data_type(&body).is_err());
    }

    #[test]
    fn decode_bigint_rejects_wrong_payload_length() {
        // BigInt payload must be exactly 16 bytes
        let body = pack(Value::Ext(EXT_BIGINT, vec![0; 8]));
        assert!(read_data_type(&body).is_err());
        let body = pack(Value::Ext(EXT_UUID, vec![0; 4]));
        assert!(read_data_type(&body).is_err());
    }

    #[test]
    fn decode_keyword_rejects_empty_or_partial() {
        // Empty keyword
        let body = pack(Value::Ext(EXT_KEYWORD, b"".to_vec()));
        assert!(read_data_type(&body).is_err());
        // ":foo" (with leading colon — wire form should never include it)
        let body = pack(Value::Ext(EXT_KEYWORD, b"/name".to_vec()));
        assert!(read_data_type(&body).is_err());
    }

    #[test]
    fn decode_tagged_union_rejects_unknown_kind() {
        let body = pack(Value::Map(vec![(
            Value::String("kind".into()),
            Value::String("xyzzy".into()),
        )]));
        assert!(entity_ref_from_value(rmpv::decode::read_value(&mut &body[..]).unwrap()).is_err());
        assert!(tx_op_from_value(rmpv::decode::read_value(&mut &body[..]).unwrap()).is_err());
        assert!(query_arg_from_value(rmpv::decode::read_value(&mut &body[..]).unwrap()).is_err());
    }

    #[test]
    fn decode_body_rejects_trailing_bytes() {
        // Two consecutive valid maps — only one body is allowed.
        let one = encode_open_db_request(&OpenDbRequest {
            tx_id: None,
            system_time: None,
            tx_eid: None,
        })
        .unwrap();
        let mut two = one.clone();
        two.extend_from_slice(&one);
        assert!(decode_open_db_request(&two).is_err());
    }

    #[test]
    fn decode_tagged_union_accepts_any_key_order() {
        // Spec requires decoders to accept any field order; "kind" doesn't
        // have to come first.
        let body = pack(Value::Map(vec![
            (Value::String("id".into()), Value::Integer(42.into())),
            (Value::String("kind".into()), Value::String("id".into())),
        ]));
        let er = entity_ref_from_value(rmpv::decode::read_value(&mut &body[..]).unwrap()).unwrap();
        assert_eq!(er, EntityRef::Id(42));
    }
}
