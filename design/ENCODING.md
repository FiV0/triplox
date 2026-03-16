# Triplox Storage Encoding Specification

**Version**: 0.1
**Status**: Design

---

## 1. Overview

Triplox uses a custom binary encoding for all data stored in SlateDB. The encoding is order-preserving and prefix-free: `memcmp(encode(a), encode(b))` matches the logical ordering of `a` and `b`. This enables predicate evaluation directly on encoded bytes without deserialization.

All encode/decode functions are implemented in `src/codec.rs`.

---

## 2. Conventions

### 2.1 Endianness

Big-endian throughout. Lexicographic byte comparison matches numeric comparison for unsigned values.

### 2.2 Encode/Decode Traits

A single `Encode`/`Decode` trait pair, defined in `src/codec.rs`:

```rust
pub trait Encode {
    fn encode(&self) -> Vec<u8>;
}

pub trait Decode: Sized {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError>;
}
```

- `encode` returns the complete encoded byte vector.
- `decode` takes a byte slice and returns the decoded value.

For tagged `DataType` values, `decode` reads the type tag and delegates to the appropriate type-specific decoding.

Since triplox owns both the traits and `DataType`, there are no orphan-rule issues — both can be defined in the same crate.

### 2.3 Encoding Primitives

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

All `DataType` values are self-describing: every encoded value is prefixed with a 1-byte type tag. This means decoding never requires schema knowledge — the tag tells the decoder what type follows.

Structural fields with fixed schemas (e.g., entity IDs, attribute IDs) are **not** tagged — they use untagged encoding since the type is known from context.

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

## 3. Type Encodings

All encodings guarantee: `memcmp(encode(a), encode(b))` equals `DataType::partial_compare(a, b)` for values of the same type.

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

Big-endian bytes.

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

Variable-length data must be **prefix-free**: no encoded value can be a prefix of another. This is required because components are concatenated without length headers.

This uses the same escape scheme as OpenData's `terminated_bytes` module. CockroachDB's `EncodeBytesAscending` uses a different approach (escape byte `0x00`, marker byte `0x12`); the OpenData scheme is simpler — no marker byte, and the escape sequences are straightforward to reason about. Both produce correct lexicographic ordering.

**Encoding rules**:

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
encode_i64(micros, buf);
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

### 3.9 Composite Types

Each element is preceded by a 1-byte type tag (Section 2.6), followed by the encoded payload. The composite is terminated with a `0x00` end marker.

```
for each element:
    buf.push(type_tag);
    element.encode(buf);
buf.push(0x00);  // end-of-composite
```

Tag `0x00` is not assigned to any type, so it serves unambiguously as the end marker. Shorter composites sort before longer ones with the same prefix.

Maps encode entries as sorted key-value pairs (BTreeMap guarantees order).

### 3.10 Tagged DataType

All `DataType` values are encoded as:

```
[type_tag: u8] [payload]
```

The payload uses the type-specific encoding from the sections above. The tag makes every value self-describing — the decoder reads the tag byte to determine the type and payload format without external schema knowledge.

Within a given attribute, values of the same type share the same tag byte, so the tag does not affect relative ordering — values still sort by payload.

---

## 4. API Surface

### 4.1 Traits

```rust
pub trait Encode {
    fn encode(&self) -> Vec<u8>;
}

pub trait Decode: Sized {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError>;
}
```

### 4.2 Encoding Primitives

```rust
// Signed integers — order-preserving (XOR sign bit + BE)
pub fn encode_i64(value: i64, buf: &mut Vec<u8>);
pub fn decode_i64(cursor: &mut &[u8]) -> Result<i64, DecodeError>;

pub fn encode_i128(value: i128, buf: &mut Vec<u8>);
pub fn decode_i128(cursor: &mut &[u8]) -> Result<i128, DecodeError>;

// Unsigned integers — BE
pub fn encode_u64(value: u64, buf: &mut Vec<u8>);
pub fn decode_u64(cursor: &mut &[u8]) -> Result<u64, DecodeError>;

// Floats — IEEE 754 sortable
pub fn encode_f64(value: f64, buf: &mut Vec<u8>);
pub fn decode_f64(cursor: &mut &[u8]) -> Result<f64, DecodeError>;

pub fn encode_f32(value: f32, buf: &mut Vec<u8>);
pub fn decode_f32(cursor: &mut &[u8]) -> Result<f32, DecodeError>;

// Booleans
pub fn encode_bool(value: bool, buf: &mut Vec<u8>);
pub fn decode_bool(cursor: &mut &[u8]) -> Result<bool, DecodeError>;

// Variable-length — escaped terminated encoding
pub fn encode_bytes(value: &[u8], buf: &mut Vec<u8>);
pub fn decode_bytes(cursor: &mut &[u8]) -> Result<Vec<u8>, DecodeError>;

pub fn encode_string(value: &str, buf: &mut Vec<u8>);
pub fn decode_string(cursor: &mut &[u8]) -> Result<String, DecodeError>;

// Derived types
pub fn encode_instant(value: &DateTime<Utc>, buf: &mut Vec<u8>);
pub fn decode_instant(cursor: &mut &[u8]) -> Result<DateTime<Utc>, DecodeError>;

pub fn encode_uuid(value: &Uuid, buf: &mut Vec<u8>);
pub fn decode_uuid(cursor: &mut &[u8]) -> Result<Uuid, DecodeError>;

pub fn encode_keyword(value: &Keyword, buf: &mut Vec<u8>);
pub fn decode_keyword(cursor: &mut &[u8]) -> Result<Keyword, DecodeError>;

// DataType dispatch (self-describing: type tag + payload)
pub fn encode_datatype(value: &DataType, buf: &mut Vec<u8>);
pub fn decode_datatype(cursor: &mut &[u8]) -> Result<DataType, DecodeError>;
pub fn skip_datatype(cursor: &mut &[u8]) -> Result<(), DecodeError>;
```

---

## 5. Testing Strategy

### 5.1 Round-trip

For every type: `decode(encode(value)) == value`.

### 5.2 Ordering

For every orderable type:
```rust
assert_eq!(encode(a).cmp(&encode(b)), a.partial_compare(&b).unwrap());
```

Edge cases: min/max values, zero crossings, NaN, empty strings, strings with embedded nulls.

### 5.3 Prefix-Free

For variable-length types:
```rust
assert!(!encode("abcd").starts_with(&encode("abc")));
```
