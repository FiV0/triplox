# Implement `DB::entity()` method

## Context

The `DB::entity()` method (in `src/node.rs:65`) is currently `todo!()`. It should work like Datomic's `d/entity`: given an entity ID, return a map of all its current attribute-value pairs at the DB's point-in-time.

## Approach

### 1. Add reverse attribute map to `DB`

`DB` currently stores `attribute_map: HashMap<String, i64>` (name → id). We need the reverse (id → name) to translate attribute IDs from EAV keys back to attribute names.

**File: `src/node.rs`**
- Add `reverse_attribute_map: HashMap<i64, String>` field to `DB`
- Build it from `attribute_map` in `DB::new()` and `DB::from_latest_snapshot()`

### 2. Implement `entity()` method

**File: `src/node.rs`**

The method:
```rust
pub fn entity(&self, eid: i64) -> Result<HashMap<String, DataType>, Error>
```

Steps:
1. Build EAV prefix: `[codec::EAV] + encode_datatype(DataType::Long(eid))`
2. Create a `TemporalFilterIterator` over this prefix using the DB's snapshot
3. The extractor returns the attribute+value portion of the key (everything after prefix, before timestamp+op)
4. For each entry, decode the attribute ID (i64) and value (DataType) from the raw key bytes
5. Look up attribute name via reverse_attribute_map
6. Collect into `HashMap<String, DataType>` (for cardinality-one) or `HashMap<String, Vec<DataType>>` for cardinality-many

**Simplification**: Use `TemporalFilterIterator` with an extractor that returns the attribute+value portion (after entity, before timestamp+op). Then decode attribute_id and value from that. The prefix is `[EAV][entity_bytes]` so the "data" after the prefix is `[attribute:8][value:variable]`.

Extractor: `|key| key.slice(prefix_len..key.len() - codec::TIMESTAMP_OP_SUFFIX)` — returns `[attribute:8][value:variable]`.

### 3. Handle `Eid` type

Currently `Eid` is an empty struct. Change it to wrap `i64` or just use `i64` directly. Using `i64` is simpler and matches `EntityId(i64)` in ops.rs.

### 4. Return type

Return `HashMap<String, Vec<DataType>>` to handle both cardinality-one and cardinality-many attributes. Each attribute name maps to a vec of values.

Alternatively, for a cleaner Datomic-like API, return a dedicated `Entity` struct that lazily or eagerly holds the map. Start simple with `HashMap<String, Vec<DataType>>`.

### 5. Tests

Add tests in `src/node.rs` (or a new test module) that:
- Create a memory node, transact some datoms, call `entity()`, verify the map
- Test entity with multiple attributes
- Test entity with cardinality-many attribute
- Test retracted attributes don't appear
- Test as-of semantics

## Files to modify

- `src/node.rs` — Main implementation: `DB` struct, `entity()` method, `Eid` type
- `src/schema.rs` — Possibly add `reverse_attribute_map()` method to `SchemaCache`

## Key functions to reuse

- `codec::encode_datatype()` (`src/codec.rs`) — encode entity ID for prefix
- `codec::decode_i64()` / `codec::decode_datatype()` (`src/codec.rs`) — decode attr+value
- `TemporalFilterIterator::new()` (`src/iterator/temporal_filter_iterator.rs`) — temporal scan
- `eav_key_to_parts()` (`src/indexer.rs:358`) — could use for decoding, but adds overhead of re-parsing entity

## Verification

```bash
cargo test entity  # run entity-specific tests
cargo test         # full test suite
```
