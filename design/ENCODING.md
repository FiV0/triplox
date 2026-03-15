# Triplox Storage Encoding Specification

**Version**: 0.1
**Status**: Design

---

## 1. Overview

Triplox currently uses Rust's `serde` + `bincode` for all serialization of index keys and values stored in SlateDB. This has several problems:

1. **No order preservation.** Bincode uses little-endian encoding for integers. SlateDB compares keys lexicographically, so bincode-encoded i64 keys do not sort in numeric order.
2. **Wasted space.** Bincode uses 4-byte variant tags for every `DataType` enum, so encoding entity IDs as `DataType::Long(id)` costs 12 bytes instead of 8.
3. **No raw-byte predicate evaluation.** Comparing two encoded values requires full deserialization. An order-preserving encoding allows comparison directly on encoded bytes.
4. **No control over format.** Bincode's format is not stable across versions and is not designed for sorted storage.

This specification defines two encoding modes:

- **Key Encoding**: Order-preserving, prefix-free encoding for SlateDB index keys. Guarantees that `memcmp(encode(a), encode(b))` matches the logical ordering of `a` and `b`.
- **Value Encoding**: Compact, non-order-preserving encoding for SlateDB values and general serialization where ordering does not matter.

Both modes use explicit hand-rolled encoding, implemented in `src/codec.rs`.

### Reference Systems

- **CockroachDB** (`pkg/util/encoding/encoding.go`): Hand-rolled encode/decode per type. Key encoding is order-preserving with type markers. Value encoding is compact with column-ID-delta + type tag.
- **OpenData** (`common/src/serde/`): Modular encoding primitives — `terminated_bytes`, `sortable`, `encoding`, `varint`. Encode/Decode traits defined per downstream crate; shared primitives in common.

---

## 2. Conventions

### 2.1 Endianness

- **Key encoding**: Big-endian. Lexicographic byte comparison matches numeric comparison for unsigned values.
- **Value encoding**: Little-endian. No ordering requirement; prefer decode speed on common architectures (x86, ARM).

### 2.2 Encode/Decode Traits

A single `Encode`/`Decode` trait pair, defined in `src/codec.rs`:

```rust
pub trait Encode {
    fn encode(&self, buf: &mut Vec<u8>);
}

pub trait Decode: Sized {
    fn decode(cursor: &mut &[u8]) -> Result<Self, DecodeError>;
}
```

- `encode` appends to a `Vec<u8>`.
- `decode` reads from a `&mut &[u8]` slice, advancing the cursor past consumed bytes.

Since triplox owns both the traits and `DataType`, there are no orphan-rule issues (unlike OpenData, which must define traits per downstream crate).

### 2.3 Shared Encoding Primitives

The traits delegate to standalone encoding primitives for the actual byte manipulation:

```
┌────────────────────┬──────────────────────────────────────────────────────────────┐
│ Primitive          │ Purpose                                                      │
├────────────────────┼──────────────────────────────────────────────────────────────┤
│ sortable           │ Bit-flip encodings for signed integers and floats that       │
│                    │ make them sort correctly as raw bytes.                       │
├────────────────────┼──────────────────────────────────────────────────────────────┤
│ terminated_bytes   │ Variable-length byte sequences terminated with 0x00, with    │
│                    │ 0x00/0x01 escaped. Preserves lexicographic ordering.         │
├────────────────────┼──────────────────────────────────────────────────────────────┤
│ length_prefixed    │ u32 LE length + raw bytes. For value encoding of strings     │
│                    │ and byte arrays.                                             │
├────────────────────┼──────────────────────────────────────────────────────────────┤
│ lex_increment      │ Increment a byte slice lexicographically. Used for prefix    │
│                    │ range query upper bounds.                                    │
└────────────────────┴──────────────────────────────────────────────────────────────┘
```

### 2.4 Error Type

```rust
pub struct DecodeError {
    pub message: String,
}
```

A single error type for all decoding failures.

### 2.5 Type Tag Policy

Type tags are used **only** in:
1. **Value encoding** — KV values stored in SlateDB
2. **Heterogeneous collection interiors** — elements of `Vector`, `Map`, `Tuple`

In key position, the schema determines the type, so no type tag is emitted. Entity IDs and attribute IDs are always `i64` and encoded as raw 8-byte order-preserving integers.

### 2.6 Type Tag Values

Reuse the wire protocol tags from `design/WIRE_PROTOCOL.md`:

| Tag | Name    | Rust Type                      |
|-----|---------|--------------------------------|
| 1   | BigInt  | `i128`                         |
| 2   | Boolean | `bool`                         |
| 3   | Bytes   | `Vec<u8>`                      |
| 4   | Double  | `f64`                          |
| 5   | Float   | `f32`                          |
| 6   | Instant | `DateTime<Utc>` (as i64 micros)|
| 7   | Long    | `i64`                          |
| 8   | Ref     | `i64` (reserved)               |
| 9   | String  | `String`                       |
| 10  | Tuple   | `Vec<DataType>`                |
| 11  | Uuid    | `Uuid` (16 bytes)              |
| 12  | Vector  | `Vec<DataType>`                |
| 13  | Map     | `BTreeMap<String, DataType>`   |
| 14  | Keyword | `Keyword`                      |

---

## 3. Key Encoding (Order-Preserving)

All key encodings guarantee: `memcmp(encode(a), encode(b))` equals `DataType::partial_compare(a, b)` for values of the same type.

### 3.1 Signed Integers: `i64`, `i128`

XOR sign bit, then big-endian bytes.

```
i64:  encoded = (value ^ 0x8000_0000_0000_0000).to_be_bytes()        // 8 bytes
i128: encoded = (value ^ (1_i128 << 127)).to_be_bytes()              // 16 bytes
```

XORing the sign bit maps the signed range `[MIN, MAX]` to the unsigned range `[0, UMAX]`, preserving order. Big-endian ensures the most significant byte is compared first.

| Value         | Encoded (hex)              |
|---------------|----------------------------|
| `i64::MIN`    | `00 00 00 00 00 00 00 00`  |
| `-1`          | `7F FF FF FF FF FF FF FF`  |
| `0`           | `80 00 00 00 00 00 00 00`  |
| `1`           | `80 00 00 00 00 00 00 01`  |
| `i64::MAX`    | `FF FF FF FF FF FF FF FF`  |

**Size**: i64 = 8 bytes. i128 = 16 bytes.

### 3.2 Unsigned Integers: `u64`

Big-endian bytes. Used for seq_num and tx_to_seq values.

```
encoded = value.to_be_bytes()   // 8 bytes
```

### 3.3 Floating Point: `f64`, `f32`

IEEE 754 sortable encoding.

```
let bits = value.to_bits();
let sortable = if bits & SIGN_MASK != 0 {
    !bits              // negative: flip all bits
} else {
    bits ^ SIGN_MASK   // positive/zero: flip only sign bit
};
encoded = sortable.to_be_bytes()
```

Where `SIGN_MASK` is `0x8000_0000_0000_0000` for f64, `0x8000_0000` for f32.

IEEE 754 positive floats are already ordered correctly as unsigned integers. Flipping the sign bit maps them above zero. For negatives, IEEE 754 sorts in reverse order of magnitude, so flipping all bits reverses them into correct ascending order.

**NaN handling**: NaN values sort deterministically (all NaNs cluster together), but `DataType::partial_compare` returns `None` for NaN. Callers handle this at a higher level.

**Size**: f64 = 8 bytes. f32 = 4 bytes.

### 3.4 Booleans

```
false => 0x00   (1 byte)
true  => 0x01   (1 byte)
```

### 3.5 Variable-Length Bytes and Strings: Escaped Terminated Encoding

Variable-length data in key position must be **prefix-free**: no encoded value can be a prefix of another. This is required because key components are concatenated without length headers.

**Encoding rules** (follows OpenData's `terminated_bytes`):

1. For each byte `b` in the input:
   - `0x00` → `0x01 0x01`
   - `0x01` → `0x01 0x02`
   - Otherwise → `b`
2. Append terminator: `0x00`

**Decoding rules**:

1. Read bytes:
   - `0x00` → terminator, stop
   - `0x01` → read next byte: `0x01` = literal `0x00`, `0x02` = literal `0x01`, else error
   - Other → literal byte

The terminator `0x00` is the smallest byte value. When one string is a prefix of another, the shorter string's terminator compares less than any data byte in the longer string, yielding correct ordering.

**Strings**: Encode as UTF-8 bytes, then apply escaped terminated encoding. UTF-8 never produces `0x00` for non-null characters, so most strings pass through with zero escape expansion.

**Examples**:

| Input              | Encoded                              |
|--------------------|--------------------------------------|
| `[]` (empty)       | `[0x00]`                             |
| `"hello"`          | `[h, e, l, l, o, 0x00]`             |
| `[0x00]`           | `[0x01, 0x01, 0x00]`                |
| `[0x01]`           | `[0x01, 0x02, 0x00]`                |
| `[0x00, 0x01]`     | `[0x01, 0x01, 0x01, 0x02, 0x00]`    |

### 3.6 Instant (`DateTime<Utc>`)

Convert to microseconds since Unix epoch (as `i64`), then apply signed integer encoding (Section 3.1).

```
let micros = datetime.timestamp_micros();
encode_i64_key(micros, buf);
```

**Size**: 8 bytes.

### 3.7 Uuid

Raw 16 bytes in RFC 4122 layout (network byte order). The `uuid` crate's `as_bytes()` returns them in this order.

```
buf.extend_from_slice(uuid.as_bytes());   // 16 bytes
```

### 3.8 Keyword

A `Keyword` has an optional namespace and a name. Encode as two consecutive terminated byte sequences:

```
Namespaced:   encode_terminated(namespace) + encode_terminated(name)
Plain:        encode_terminated("")         + encode_terminated(name)
```

Plain keywords (empty namespace → just `0x00`) sort before all namespaced keywords.

### 3.9 Composite Types in Keys

Tuple, Vector, and Map are generally not used in key position. For completeness:

Each element is preceded by a 1-byte type tag (Section 2.6), followed by the key-encoded payload. The composite is terminated with a `0x00` end marker.

```
for each element:
    buf.push(type_tag);
    element.key_encode(buf);
buf.push(0x00);  // end-of-composite
```

Tag `0x00` is not assigned to any type, so it serves unambiguously as the end marker. Shorter composites sort before longer ones with the same prefix.

Maps encode entries as sorted key-value pairs (BTreeMap guarantees order).

---

## 4. Value Encoding (Compact, Non-Order-Preserving)

Used for SlateDB values, log records, and any position where ordering is not required.

### 4.1 Tagged DataType Values

Every `DataType` value is encoded as:

```
[type_tag: u8] [payload]
```

| Tag | Name    | Payload                                    | Size         |
|-----|---------|--------------------------------------------|--------------|
| 1   | BigInt  | i128 little-endian                         | 1 + 16 = 17  |
| 2   | Boolean | 1 byte: `0x00`=false, `0x01`=true          | 1 + 1 = 2    |
| 3   | Bytes   | u32 LE length + raw bytes                  | 1 + 4 + N    |
| 4   | Double  | f64 IEEE 754 little-endian                 | 1 + 8 = 9    |
| 5   | Float   | f32 IEEE 754 little-endian                 | 1 + 4 = 5    |
| 6   | Instant | i64 LE (microseconds since epoch)          | 1 + 8 = 9    |
| 7   | Long    | i64 little-endian                          | 1 + 8 = 9    |
| 9   | String  | u32 LE length + UTF-8 bytes                | 1 + 4 + N    |
| 10  | Tuple   | u32 LE count + tagged elements             | 1 + 4 + ...  |
| 11  | Uuid    | 16 raw bytes (RFC 4122)                    | 1 + 16 = 17  |
| 12  | Vector  | u32 LE count + tagged elements             | 1 + 4 + ...  |
| 13  | Map     | u32 LE count + (String key, tagged value)* | 1 + 4 + ...  |
| 14  | Keyword | see Section 4.3                            | variable     |

### 4.2 Untagged Primitives

When the type is known from context (e.g., `TxMeta.seq_num` is always `u64`), values are encoded without a type tag:

```rust
fn encode_i64_value(value: i64, buf: &mut Vec<u8>);    // 8 bytes LE
fn encode_u64_value(value: u64, buf: &mut Vec<u8>);    // 8 bytes LE
fn encode_i128_value(value: i128, buf: &mut Vec<u8>);  // 16 bytes LE
fn encode_string_value(value: &str, buf: &mut Vec<u8>); // u32 LE len + UTF-8
fn encode_bytes_value(value: &[u8], buf: &mut Vec<u8>); // u32 LE len + raw
```

### 4.3 Keyword Value Encoding

```
[tag=14] [flag: u8] [namespace: String (if flag=1)] [name: String]
```

- `flag=0`: plain keyword, only name follows
- `flag=1`: namespaced keyword, namespace then name

Strings are encoded as u32 LE length + UTF-8 bytes.

### 4.4 Collection Value Encoding

**Vector / Tuple**:
```
[tag] [count: u32 LE] [element₀: tagged DataType] [element₁: tagged DataType] ...
```

**Map**:
```
[tag=13] [count: u32 LE] [key₀: String] [value₀: tagged DataType] ...
```

Map keys are `String` (length-prefixed, not type-tagged). Map values are tagged `DataType`.

---

## 5. Index Key Structure

### 5.1 Component Sizes

| Component    | Encoding                   | Size (bytes) |
|--------------|----------------------------|--------------|
| Prefix       | Raw byte (index type)      | 1            |
| Entity ID    | i64 order-preserving (§3.1)| 8            |
| Attribute ID | i64 order-preserving (§3.1)| 8            |
| Value        | Type-specific key encoding | Variable     |
| Op           | Raw byte (ADD/RETRACT)     | 1            |

Entity IDs shrink from 12 bytes (bincode's 4-byte variant tag + 8-byte i64) to 8 bytes.

### 5.2 Index Key Layouts

**EAV**: `[prefix:1] [entity:8] [attribute:8] [value:var] [op:1]`

**AVE**: `[prefix:1] [attribute:8] [value:var] [entity:8] [op:1]`

In AVE, the value is variable-width but the terminated encoding is self-delimiting — the decoder scans for the unescaped `0x00` terminator to find the boundary between value and entity.

**AEV**: `[prefix:1] [attribute:8] [entity:8] [value:var] [op:1]`

**AE**: `[prefix:1] [attribute:8] [entity:8] [op:1]` — fixed 18 bytes

**AV**: `[prefix:1] [attribute:8] [value:var] [op:1]`

**TX_TO_SEQ**: `[prefix:1] [tx_id:8]` — fixed 9 bytes

### 5.3 Value Encoding in Keys

The type is known from the schema (`SchemaCache`), so no type tag is emitted:

```rust
fn encode_datatype_key(value: &DataType, buf: &mut Vec<u8>) {
    match value {
        DataType::Long(v) => encode_i64_key(*v, buf),
        DataType::String(v) => encode_string_key(v, buf),
        DataType::Double(v) => encode_f64_key(*v, buf),
        // ... dispatch per variant
    }
}
```

Decoding requires a `ValueType` parameter:

```rust
fn decode_datatype_key(value_type: ValueType, cursor: &mut &[u8]) -> Result<DataType, DecodeError>;
```

### 5.4 Prefix Scan Boundaries

For prefix scans, construct from fixed-width components:

```rust
fn eav_prefix(entity_id: i64, attribute_id: i64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(17);
    buf.push(EAV);
    encode_i64_key(entity_id, &mut buf);
    encode_i64_key(attribute_id, &mut buf);
    buf
}
```

For range scans over a prefix, compute the exclusive upper bound using `lex_increment`:

```rust
/// Increment a byte slice lexicographically. Returns None if all bytes are 0xFF.
fn lex_increment(data: &[u8]) -> Option<Vec<u8>> {
    let mut result = data.to_vec();
    while let Some(last) = result.last_mut() {
        if *last < 0xFF {
            *last += 1;
            return Some(result);
        }
        result.truncate(result.len() - 1);
    }
    None
}
```

Range: `[prefix, lex_increment(prefix))`

---

## 6. TxMeta Value Encoding

The `TxMeta` struct uses untagged value encoding (fixed schema):

```
[seq_num: u64 LE (8 bytes)] [system_time: i64 LE (8 bytes, microseconds)]
```

Total: 16 bytes.

---

## 7. API Surface

### 7.1 Traits

```rust
pub trait Encode {
    fn encode(&self, buf: &mut Vec<u8>);
}

pub trait Decode: Sized {
    fn decode(cursor: &mut &[u8]) -> Result<Self, DecodeError>;
}
```

### 7.2 Key Encoding Primitives

```rust
// Signed integers — order-preserving (XOR sign bit + BE)
pub fn encode_i64_key(value: i64, buf: &mut Vec<u8>);
pub fn decode_i64_key(cursor: &mut &[u8]) -> Result<i64, DecodeError>;

pub fn encode_i128_key(value: i128, buf: &mut Vec<u8>);
pub fn decode_i128_key(cursor: &mut &[u8]) -> Result<i128, DecodeError>;

// Unsigned integers — BE
pub fn encode_u64_key(value: u64, buf: &mut Vec<u8>);
pub fn decode_u64_key(cursor: &mut &[u8]) -> Result<u64, DecodeError>;

// Floats — IEEE 754 sortable
pub fn encode_f64_key(value: f64, buf: &mut Vec<u8>);
pub fn decode_f64_key(cursor: &mut &[u8]) -> Result<f64, DecodeError>;

pub fn encode_f32_key(value: f32, buf: &mut Vec<u8>);
pub fn decode_f32_key(cursor: &mut &[u8]) -> Result<f32, DecodeError>;

// Booleans
pub fn encode_bool_key(value: bool, buf: &mut Vec<u8>);
pub fn decode_bool_key(cursor: &mut &[u8]) -> Result<bool, DecodeError>;

// Variable-length — escaped terminated encoding
pub fn encode_bytes_key(value: &[u8], buf: &mut Vec<u8>);
pub fn decode_bytes_key(cursor: &mut &[u8]) -> Result<Vec<u8>, DecodeError>;

pub fn encode_string_key(value: &str, buf: &mut Vec<u8>);
pub fn decode_string_key(cursor: &mut &[u8]) -> Result<String, DecodeError>;

// Composite types
pub fn encode_instant_key(value: &DateTime<Utc>, buf: &mut Vec<u8>);
pub fn decode_instant_key(cursor: &mut &[u8]) -> Result<DateTime<Utc>, DecodeError>;

pub fn encode_uuid_key(value: &Uuid, buf: &mut Vec<u8>);
pub fn decode_uuid_key(cursor: &mut &[u8]) -> Result<Uuid, DecodeError>;

pub fn encode_keyword_key(value: &Keyword, buf: &mut Vec<u8>);
pub fn decode_keyword_key(cursor: &mut &[u8]) -> Result<Keyword, DecodeError>;

// DataType dispatch (schema-aware, no type tag)
pub fn encode_datatype_key(value: &DataType, buf: &mut Vec<u8>);
pub fn decode_datatype_key(vt: ValueType, cursor: &mut &[u8]) -> Result<DataType, DecodeError>;
pub fn skip_datatype_key(vt: ValueType, cursor: &mut &[u8]) -> Result<(), DecodeError>;
```

### 7.3 Value Encoding Primitives

```rust
// Tagged DataType (type tag + payload)
pub fn encode_datatype_value(value: &DataType, buf: &mut Vec<u8>);
pub fn decode_datatype_value(cursor: &mut &[u8]) -> Result<DataType, DecodeError>;

// Untagged primitives (known-schema contexts)
pub fn encode_i64_value(value: i64, buf: &mut Vec<u8>);
pub fn decode_i64_value(cursor: &mut &[u8]) -> Result<i64, DecodeError>;

pub fn encode_u64_value(value: u64, buf: &mut Vec<u8>);
pub fn decode_u64_value(cursor: &mut &[u8]) -> Result<u64, DecodeError>;

pub fn encode_string_value(value: &str, buf: &mut Vec<u8>);
pub fn decode_string_value(cursor: &mut &[u8]) -> Result<String, DecodeError>;

pub fn encode_bytes_value(value: &[u8], buf: &mut Vec<u8>);
pub fn decode_bytes_value(cursor: &mut &[u8]) -> Result<Vec<u8>, DecodeError>;
```

### 7.4 Index Key Builders

```rust
pub fn encode_eav_key(entity: i64, attribute: i64, value: &DataType, op: u8) -> Vec<u8>;
pub fn encode_ave_key(attribute: i64, value: &DataType, entity: i64, op: u8) -> Vec<u8>;
pub fn encode_aev_key(attribute: i64, entity: i64, value: &DataType, op: u8) -> Vec<u8>;
pub fn encode_ae_key(attribute: i64, entity: i64, op: u8) -> Vec<u8>;
pub fn encode_av_key(attribute: i64, value: &DataType, op: u8) -> Vec<u8>;
pub fn encode_tx_to_seq_key(tx_id: i64) -> Vec<u8>;

// Prefix builders for scan operations
pub fn eav_prefix(entity: i64, attribute: i64) -> Vec<u8>;
pub fn ave_prefix(attribute: i64) -> Vec<u8>;
pub fn aev_prefix(attribute: i64, entity: i64) -> Vec<u8>;
pub fn ae_prefix(attribute: i64) -> Vec<u8>;
pub fn av_prefix(attribute: i64) -> Vec<u8>;

// Index key decoders
pub fn decode_eav_key(key: &[u8], vt: ValueType) -> Result<(i64, i64, DataType, u8), DecodeError>;
pub fn decode_ave_key(key: &[u8], vt: ValueType) -> Result<(i64, DataType, i64, u8), DecodeError>;
pub fn decode_aev_key(key: &[u8], vt: ValueType) -> Result<(i64, i64, DataType, u8), DecodeError>;
pub fn decode_ae_key(key: &[u8]) -> Result<(i64, i64, u8), DecodeError>;
pub fn decode_av_key(key: &[u8], vt: ValueType) -> Result<(i64, DataType, u8), DecodeError>;
```

### 7.5 Utilities

```rust
pub fn lex_increment(data: &[u8]) -> Option<Vec<u8>>;
```

---

## 8. Migration

The encoding change is breaking — existing bincode data cannot be read with the new encoding.

1. Implement the new codec alongside existing bincode serialization.
2. Add comprehensive tests (Section 9).
3. Flag-day migration: re-index all data from the transaction log using the new encoding.
4. Remove all `bincode::serialize` / `bincode::deserialize` calls from index key construction and `TxMeta` serialization.

---

## 9. Testing Strategy

### 9.1 Round-trip

For every type: `decode(encode(value)) == value`.

### 9.2 Ordering (Key Encoding)

For every orderable type:
```rust
assert_eq!(encode(a).cmp(&encode(b)), a.partial_compare(&b).unwrap());
```

Edge cases: min/max values, zero crossings, NaN, empty strings, strings with embedded nulls.

### 9.3 Prefix-Free (Key Encoding)

For variable-length types:
```rust
assert!(!encode("abcd").starts_with(&encode("abc")));
```

### 9.4 Composite Key Ordering

```rust
let key1 = encode_eav_key(1, 10, &DataType::String("alice".into()), ADD);
let key2 = encode_eav_key(1, 10, &DataType::String("bob".into()), ADD);
assert!(key1 < key2);
```

### 9.5 Property-Based Tests

Use `proptest` to generate random `DataType` values and verify round-trip and ordering properties across thousands of random inputs. Follow the pattern from OpenData's `terminated_bytes` tests.
