# Plan: Reuse cached DB snapshots when they cover the requested tx_id

## Context

With historical indices on SlateDB, a single DB snapshot can serve queries for any tx_id <= its own tx_id (temporal filtering happens at query time). The current `DbCache` in `src/server.rs` creates and caches a separate snapshot per exact tx_id. This means two clients requesting tx_id=5 and tx_id=7 get separate snapshots even though a snapshot at tx_id=10 could serve both.

**Goal:** Reuse an existing cached snapshot when it covers the requested tx_id, only creating a new snapshot when no cached one suffices. Keep the per-connection lifetime and refcounting model intact.

## Changes

### 1. Make `latest_indexed_tx` and `indexer` accessible — `src/indexer.rs` + `src/node.rs`

The Indexer already tracks `latest_indexed_tx: Option<TxKey>` (line 29) but it's private.

**indexer.rs** — change visibility of the field:
```rust
pub(crate) latest_indexed_tx: Option<TxKey>,
```

**node.rs** — change visibility of the `indexer` field on `Node`:
```rust
pub(crate) indexer: Arc<tokio::sync::RwLock<Indexer>>,
```

This lets the server read `node.indexer.read().await.latest_indexed_tx` cheaply (no snapshot creation) to check the latest tx_id before deciding whether to open a new snapshot for `OpenDb(None, None)`.

### 2. Add `DB::with_tx_key()` — `src/node.rs`

Create a new DB sharing the same underlying SlateDB snapshot but with a different tx_key:

```rust
pub fn with_tx_key(&self, tx_key: TxKey) -> DB {
    DB {
        snapshot: self.snapshot.clone(),
        attribute_map: self.attribute_map.clone(),
        handle: self.handle.clone(),
        tx_key,
    }
}
```

### 3. Modify `DbCache::acquire` — `src/server.rs`

Change from exact tx_id lookup to "find any cached snapshot covering this tx_id."

The cache remains `HashMap<i64, DbCacheEntry>` but the key is now the **snapshot's own tx_id** (not the requested tx_id). Lookup becomes:

```rust
// Find any entry whose snapshot tx_id >= requested tx_id
// Prefer the smallest covering snapshot to minimize staleness
fn find_covering(&self, requested_tx_id: i64) -> Option<(&i64, &mut DbCacheEntry)> {
    entries.iter_mut()
        .filter(|(snap_tx_id, _)| **snap_tx_id >= requested_tx_id)
        .min_by_key(|(snap_tx_id, _)| **snap_tx_id)
}
```

The `acquire` signature changes to also accept the `TxKey` we want the returned DB to use:

```rust
async fn acquire<F, Fut>(&self, requested_tx_id: i64, wanted_tx_key: TxKey, create: F) -> Result<Arc<DB>>
```

On cache hit: bump refcount, return `Arc::new(cached_db.with_tx_key(wanted_tx_key))`.
On cache miss: call `create()`, insert keyed by the new snapshot's tx_id, return.

**Release** stays the same — keyed by snapshot tx_id.

### 4. Update `ConnectionState` — `src/server.rs`

The `handles` map currently stores `db_id → requested_tx_id` (which was the cache key). Now it needs to store the **snapshot's tx_id** (the actual cache key) so `release` works correctly:

```rust
// handles: db_id → snapshot_tx_id (for cache release)
handles: HashMap<u32, i64>,
```

Update `allocate_handle` to accept `snapshot_tx_id` (the cache key) separately from the DB's tx_key.

### 5. Update `handle_open_db` — `src/server.rs`

**Case `(None, None)` — latest:**
1. Call `node.indexer.read().await.latest_indexed_tx` (cheap, in-memory) to get the latest `TxKey`
2. Call `db_cache.acquire(latest.tx_id, latest_tx_key, || node.db())` — reuses a cached snapshot if one covers it, otherwise creates a new one
3. The `create` closure still calls `node.db()` as fallback

**Case `(Some(tid), Some(st))` — historical:**
1. Build the `TxKey { tx_id: tid, system_time }`
2. Call `db_cache.acquire(tid, tx_key, || node.db_as_of(tx_key))` — reuses cached snapshot if available
3. Fallback creates via `node.db_as_of()`

In both cases, `allocate_handle` stores the snapshot's tx_id (from the cache entry or the newly created DB) for later release.

## Files to modify

| File | Change |
|---|---|
| `src/indexer.rs` | Make `latest_indexed_tx` field `pub(crate)` |
| `src/node.rs` | Make `indexer` field `pub(crate)`, add `DB::with_tx_key()` |
| `src/server.rs` | Modify `DbCache::acquire` lookup logic, update `handle_open_db` |

## Verification

1. `cargo build` — compiles cleanly
2. `cargo test` — full test suite passes
3. Key behavioral check: two `OpenDb` calls for different tx_ids that are both <= the cached snapshot's tx_id should reuse the same snapshot (same refcount entry), not create two separate ones
