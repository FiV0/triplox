# Plan: Retract Old Index Entries on Cardinality/One Overwrite

## Context

When a `Put` or `Add` overwrites a cardinality/one attribute, the old value's index keys remain in all 5 indices. Queries then return both old and new values. The fix: before writing new ADD keys, scan for existing values and generate RETRACT keys for the old ones.

TODO reference: `src/indexer.rs:209-210` (`triplox-5ox`)

## Approach

**Generate raw retraction keys in `transact_tx`** rather than synthetic `TxOp::Retract` ops, because a full Retract would also retract the AE index entry — but we don't want that since the attribute is still present on the entity, just with a new value.

## Changes

### 1. `src/indexer.rs` — Add retraction-key generation

Add an async method on `Indexer`:

```rust
async fn retraction_keys_for_cardinality_one_overwrites(
    &self,
    tx_ops: &[TxOp],
) -> Result<Vec<TxIndexKeys>>
```

For each `Put`/`Add` op:
- For each (entity_id, attribute_name, new_value):
  - Look up attribute in `self.schema_cache`. If cardinality != One, skip.
  - Build AEV prefix: `[AEV_PREFIX | attribute_id_bytes | entity_id_bytes]`
  - Scan prefix in `self.slatedb`
  - Parse each entry with `aev_key_to_parts`. Net ADD/RETRACT per value (ADD=0 sorts before RETRACT=2, so for same value they're adjacent).
  - For each active old value that differs from new_value, generate RETRACT keys for **EAV, AVE, AEV, AV only** (NOT AE — attribute still present).

### 2. `src/indexer.rs` — Modify `transact_tx`

Between validation and `build_index_write_batch`, insert:

```rust
let retraction_keys = self.retraction_keys_for_cardinality_one_overwrites(&tx_ops).await?;
let mut write_batch = build_index_write_batch(&tx_ops, &self.schema_cache)?;
for keys in &retraction_keys {
    for key in &keys.eav { write_batch.put(key.as_slice(), &[]); }
    for key in &keys.ave { write_batch.put(key.as_slice(), &[]); }
    for key in &keys.aev { write_batch.put(key.as_slice(), &[]); }
    // keys.ae will be empty — no AE retraction for overwrites
    for key in &keys.av { write_batch.put(key.as_slice(), &[]); }
}
```

Remove/update the TODO comment at line 209-210.

### 3. `src/schema.rs` — Add cardinality/many test attribute

Add a `"tags"` attribute with `Cardinality::Many` to `test_schema_tx()` to enable testing that many-cardinality attributes are not retracted.

### 4. Tests

**In `src/indexer.rs`:**
- **test_cardinality_one_overwrite_retracts_old_value**: Put name="alice", then Put name="bob" for same entity. Verify EAV/AVE/AEV/AV have RETRACT for "alice" and ADD for "bob". Verify AE has only ADD (no RETRACT).
- **test_cardinality_many_no_retraction**: Add tags="tag1", then Add tags="tag2". Both should remain with ADD only.
- **test_same_value_reasserted_no_retraction**: Put name="alice" twice. No RETRACT keys generated.
- **test_overwrite_via_add_triple**: Put name="alice", then Add(name="bob"). Old retracted.

**In `src/node.rs`:**
- **Update `test_db_with_basis_time_travel`** (line 706): Use same entity in both txs, verify only latest value returned at each basis.
- **Add query-level overwrite test**: Put name="alice", overwrite to "bob", query returns only "bob".

## Key files
- `src/indexer.rs` — core logic + unit tests
- `src/schema.rs` — add cardinality/many test attribute
- `src/node.rs` — integration tests
- `src/codec.rs` — key format constants (read-only)

## Verification
1. `cargo test` — all existing tests pass
2. New tests verify overwrite behavior at index and query level
3. Manual verification: the `test_db_with_basis_time_travel` test using same entity confirms time-travel still works correctly with overwrites
