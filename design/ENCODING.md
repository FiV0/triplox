# Triplox Storage Encoding Specification

Version: 0.1
Status: Design

---

## 1. Overview

Triplox uses a custom binary encoding for all data that it stores in SlateDB. The encoding is order-preserving (the bytes sort in the same order as the values). The encoding is also prefix-free (no encoded value is the start of another). Thus, `memcmp(encode(a), encode(b))` matches the logical order of `a` and `b`. Triplox can evaluate predicates (conditions on values) directly on the encoded bytes. Decoding the bytes first is not necessary.

All encode and decode functions are in `src/codec.rs`.

---

## 2. Conventions

### 2.1 Endianness

All encodings use big-endian byte order (the most significant byte comes first). For unsigned values, a lexicographic byte comparison (byte by byte, from the left) gives the same result as a numeric comparison.

### 2.2 Encode/Decode Traits

The file `src/codec.rs` defines one `Encode`/`Decode` trait pair:

```rust
pub trait Encode {
    fn encode(&self) -> Vec<u8>;
}

pub trait Decode: Sized {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError>;
}
```

The `encode` function returns the full encoded value as a byte vector. The `decode` function takes a byte slice and returns the decoded value.

For tagged `DataType` values, `decode` reads the type tag (a 1-byte code for the type of the value). Then it uses the decode logic for that type.

Because triplox owns both the traits and `DataType`, the orphan rule (Rust forbids impls of foreign traits for foreign types) causes no problems. We can define both in the same crate.

### 2.3 Encoding Primitives

The traits use standalone encoding primitives (small functions that do the byte operations). The table that follows lists them:

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

One error type, `DecodeError`, covers all decoding failures.

### 2.5 Type Tag Policy

All `DataType` values are self-describing (the bytes identify their own type). A 1-byte type tag comes before every encoded value. Thus, decoding never requires information from a schema. The tag tells the decoder which type follows.

Structural fields with fixed schemas do not have a tag. Examples are entity IDs and attribute IDs (the E and A of an entity-attribute-value triple). These fields use untagged encoding because the context gives their type.

### 2.6 Type Tag Values

The encoding reuses the type tags of the wire protocol (the network message format) from `design/PROTOCOL.md`:

| Tag | Name                                                                      | Rust Type                       |
|-----|---------------------------------------------------------------------------|---------------------------------|
| 0   | *(reserved: end marker of a composite, a value that holds values)*        | —                               |
| 1   | BigInt                                                                    | `i128`                          |
| 2   | Boolean                                                                   | `bool`                          |
| 3   | Bytes                                                                     | `Vec<u8>`                       |
| 4   | Double                                                                    | `f64`                           |
| 5   | Float                                                                     | `f32`                           |
| 6   | Instant                                                                   | `DateTime<Utc>` (as i64 micros) |
| 7   | Long                                                                      | `i64`                           |
| 8   | *(reserved: Ref shares tag 7 with Long, and the schema tells them apart)* | —                               |
| 9   | String                                                                    | `String`                        |
| 10  | Uuid                                                                      | `Uuid` (16 bytes)               |
| 11  | Vector                                                                    | `Vec<DataType>`                 |
| 12  | Map                                                                       | `BTreeMap<String, DataType>`    |
| 13  | Keyword                                                                   | `Keyword`                       |

---

## 3. Type Encodings

For values of the same type, all encodings guarantee that `memcmp(encode(a), encode(b))` equals `DataType::partial_compare(a, b)`.

### 3.1 Signed Integers: `i64`, `i128`

The encoding does an XOR on all value bits and then writes the result as big-endian bytes. This gives a descending byte order (the largest values sort first).

```
i64:  encoded = (value ^ 0x7FFF_FFFF_FFFF_FFFF).to_be_bytes()       // 8 bytes
i128: encoded = (value ^ i128::MAX).to_be_bytes()                    // 16 bytes
```

An XOR with `MAX` flips all bits except the sign bit. This maps the signed range `[MIN, MAX]` to the unsigned range `[UMAX, 0]`, and thus reverses the byte order. The big-endian order makes sure that the comparison starts with the most significant byte.

| Value         | Encoded (hex)              |
|---------------|----------------------------|
| `i64::MIN`    | `FF FF FF FF FF FF FF FF`  |
| `-1`          | `80 00 00 00 00 00 00 00`  |
| `0`           | `7F FF FF FF FF FF FF FF`  |
| `1`           | `7F FF FF FF FF FF FF FE`  |
| `i64::MAX`    | `00 00 00 00 00 00 00 00`  |

An encoded i64 is 8 bytes. An encoded i128 is 16 bytes.

### 3.2 Unsigned Integers: `u64`

The encoding writes the value as big-endian bytes.

```
encoded = value.to_be_bytes()   // 8 bytes
```

### 3.3 Floating Point: `f64`, `f32`

The encoding is the IEEE 754 sortable encoding (a bit transform that makes floats sort as bytes).

```
let bits = value.to_bits();
let sortable = if bits & SIGN_MASK != 0 {
    !bits              // negative: flip all bits
} else {
    bits ^ SIGN_MASK   // positive/zero: flip only sign bit
};
encoded = sortable.to_be_bytes()
```

`SIGN_MASK` is `0x8000_0000_0000_0000` for f64 and `0x8000_0000` for f32.

As unsigned integers, positive IEEE 754 floats already sort in the correct order. A flip of the sign bit maps them above zero. For negative values, IEEE 754 sorts in reverse order of magnitude. Thus, a flip of all bits reverses them into the correct ascending order.

NaN values always sort the same way, and all NaN values sort next to each other. But `DataType::partial_compare` returns `None` for NaN. Callers handle this case at a higher level.

An encoded f64 is 8 bytes. An encoded f32 is 4 bytes.

### 3.4 Booleans

```
false => 0x00   (1 byte)
true  => 0x01   (1 byte)
```

### 3.5 Variable-Length Bytes and Strings: Escaped Terminated Encoding

Variable-length data must be prefix-free. No encoded value can be a prefix of another encoded value. This is necessary because the encoding concatenates components without length headers.

This encoding uses the same escape scheme (a rule that replaces special bytes with byte pairs) as the `terminated_bytes` module of OpenData. The `EncodeBytesAscending` function of CockroachDB uses a different method, with the escape byte `0x00` and the marker byte `0x12`. The OpenData scheme is simpler. It has no marker byte, and its escape sequences are easy to reason about. Both schemes give the correct lexicographic order.

To encode, do these steps:

1. For each byte `b` in the input, write the output that follows:
   - `0x00` → `0x01 0x01`
   - `0x01` → `0x01 0x02`
   - Otherwise → `b`
2. Append the terminator (the byte that marks the end): `0x00`.

To decode, do this step:

1. Read the bytes one at a time:
   - `0x00` → the terminator. Stop.
   - `0x01` → read the next byte. `0x01` is a literal `0x00`, `0x02` is a literal `0x01`, and any other byte is an error.
   - Other → a literal byte.

The terminator `0x00` is the smallest byte value. If one string is a prefix of another string, the terminator of the shorter string compares less than any data byte in the longer string. This gives the correct order.

For a string, the encoder converts the string to UTF-8 bytes and then applies the escaped terminated encoding. UTF-8 never produces `0x00` for non-null characters. Thus, the encoding adds no escape bytes to most strings.

The table that follows gives examples:

| Input              | Encoded                              |
|--------------------|--------------------------------------|
| `[]` (empty)       | `[0x00]`                             |
| `"hello"`          | `[h, e, l, l, o, 0x00]`             |
| `[0x00]`           | `[0x01, 0x01, 0x00]`                |
| `[0x01]`           | `[0x01, 0x02, 0x00]`                |
| `[0x00, 0x01]`     | `[0x01, 0x01, 0x01, 0x02, 0x00]`    |

### 3.6 Instant (`DateTime<Utc>`)

The encoding converts the value to microseconds since the Unix epoch, as an `i64`. Then it applies the signed integer encoding (Section 3.1).

```
let micros = datetime.timestamp_micros();
encode_i64(micros, buf);
```

An encoded Instant is 8 bytes.

### 3.7 Uuid

The encoding writes the raw 16 bytes in the RFC 4122 layout (network byte order). The `as_bytes()` function of the `uuid` crate returns the bytes in this order.

```
buf.extend_from_slice(uuid.as_bytes());   // 16 bytes
```

### 3.8 Keyword

A `Keyword` has an optional namespace (a qualifier that groups related keywords) and a name. The encoding is two consecutive terminated byte sequences:

```
Namespaced:   encode_terminated(namespace) + encode_terminated(name)
Plain:        encode_terminated("")         + encode_terminated(name)
```

A plain keyword has an empty namespace, which encodes as only `0x00`. Thus, plain keywords sort before all namespaced keywords.

### 3.9 Composite Types

For each element, the encoding writes a 1-byte type tag (Section 2.6) and then the payload (the encoded bytes of the element). A `0x00` end marker comes after the last element.

```
for each element:
    buf.push(type_tag);
    element.encode(buf);
buf.push(0x00);  // end-of-composite
```

No type uses tag `0x00`. Thus, a `0x00` in a tag position can only be the end marker. If two composites have the same prefix, the shorter composite sorts first.

A Map encodes its entries as key-value pairs in sorted order. BTreeMap guarantees this order.

### 3.10 Tagged DataType

The encoding of each `DataType` value has this form:

```
[type_tag: u8] [payload]
```

The payload uses the type-specific encoding from Sections 3.1 to 3.9. The tag makes every value self-describing. The decoder reads the tag byte to find the type and the payload format. It does not use information from an external schema.

In one attribute, values of the same type have the same tag byte. Thus, the tag does not change their relative order, and the values still sort by payload.

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
