# Triplox Index Structure on SlateDB — Design Document

## Current State

Today, keys are encoded as:

```
[INDEX_PREFIX:1B][components...][OP_BYTE:1B]
```

with empty values. There is **no tx_id in the key**, so no way to distinguish versions.
The `_tx_key` parameter in `op_to_index_keys` (`indexer.rs:66`) is unused.

## The Central Tension

Triplox is event-sourced with time-travel queries (`basis_tx_id`). The join algorithms
(leapfrog, generic join) assume iterators that efficiently enumerate/seek over **live facts
only**. Adding versioning means every scan must somehow resolve which facts are alive at a
given point in time.

---

## Question 1: Separate vs. Unified Key Spaces

There are three viable approaches:

### Option A: Single space, tx_id in key (Datomic-style)

```
EAV key: [EAV][entity][attribute][value][tx_id:8B][op]
```

All versions of all facts live together. Iterators filter at read time.

**Pros:**
- Simple write path (pure append, no read-before-write)
- Naturally supports historical and current queries with the same index
- No data movement needed

**Cons:**
- **Every** query (including current) must scan through all versions of a fact to resolve
  its liveness
- Join algorithms need wrapping iterators that hide version resolution
- More data per scan = slower current queries (the 95% case)

### Option B: Two separate prefix spaces (current + history)

```
Current:  [INDEX_PREFIX][components...]          → value = latest_tx_id (or empty)
History:  [HIST_PREFIX][INDEX_PREFIX][components...][tx_id][op]
```

The current index holds only live facts (no versioning). The history index is append-only.

**Pros:**
- Current queries are as fast as today — zero version filtering
- Join algorithms work unchanged on the current index
- Clean separation: current is a materialized view, history is the source of truth

**Cons:**
- Retracts require deleting from current index (SlateDB delete = tombstone until
  compaction, but scan semantics are correct)
- ~2x write amplification (every op touches both spaces)
- Must keep both consistent (single WriteBatch handles this)

### Option C: Single space, tx_id in key, reverse-ordered for current optimization

```
EAV key: [EAV][entity][attribute][value][~tx_id:8B][op]
```

where `~tx_id = i64::MAX - tx_id` so newest sorts first.

**Pros:**
- Current queries can stop at first entry per (e,a,v) group
- Still a single key space

**Cons:**
- Historical queries at basis_tx T require skipping past newer entries
- Reverse ordering is confusing and error-prone
- Doesn't eliminate the filtering overhead, just reduces it for current

### Recommendation

**Option B** (two prefix spaces) is the strongest fit for Triplox because:

1. The join algorithms are designed around iterators that yield facts directly. Injecting
   version-resolution logic into `SlateIterator`, `GenericPrefixExtender`, `propose()`,
   `intersect()` etc. is invasive and error-prone.
2. Current queries dominate. Optimizing the common case at the cost of 2x writes is a
   good trade.
3. SlateDB's `WriteBatch` guarantees atomicity across both spaces.
4. The history index can use a simpler format since it's only traversed for time-travel
   queries (which tolerate higher latency).

---

## Question 2: How and When Keys Move from Current to Historic

With Option B, keys are **not moved** — they are **written to both spaces simultaneously**
in the same `WriteBatch`:

```
For Add(e, a, v) at tx T:
  WriteBatch:
    PUT [EAV][e][a][v]                         → value: empty or tx_id
    PUT [HIST][EAV][e][a][v][T][ADD]           → value: empty
    (+ same for AVE, AEV, AE, AV indices)

For Retract(e, a, v) at tx T:
  WriteBatch:
    DELETE [EAV][e][a][v]                      → removes from current
    PUT [HIST][EAV][e][a][v][T][RETRACT]       → appends to history
    (+ same for other indices)
```

Key points:

- The current index is a **materialized view** maintained inline with indexing. There's no
  background migration.
- Retracts on current use `WriteBatch.delete()`, which in SlateDB creates a tombstone
  that's resolved during compaction. Scans correctly exclude deleted keys.
- The op byte is no longer needed in the current index (presence = live, absence = not
  live). It remains in the history index to distinguish ADDs from RETRACTs.

### Implications for Delete and Erase

`Delete(entity)` and `Erase(entity)` are currently `todo!()` in the indexer.

- **Delete(entity)**: needs to scan the current EAV index for all keys with that entity,
  delete each from current, and write RETRACT entries to history. This is a read-then-write
  operation.
- **Erase(entity)**: same as delete from current, but also removes all history entries (or
  marks them as erased with a special op).

---

## Question 3: Filtering Historic Versions for Old Iterators

When querying at `basis_tx_id = T`, you create iterators over the **history** index with a
filtering wrapper:

```rust
struct HistoricIterator {
    inner: SlateIterator,  // scanning HIST prefix
    basis_tx: i64,
}
```

The algorithm:

1. **Scan** with prefix `[HIST][INDEX_PREFIX][attribute][...]`
2. **Group** consecutive keys by their fact identity (everything except tx_id and op). Keys
   with the same fact identity sort together because tx_id comes after the fact components.
3. **Filter within each group**: collect all `(tx_id, op)` pairs where `tx_id <= T`. The
   last such pair determines liveness.
4. **Yield or skip**: if the latest applicable op is ADD, yield the fact. If RETRACT, skip
   it.

In practice, this means the historic iterator:

- Reads all versions of a fact up to tx T
- Returns only the fact identity (not the tx_id or op) to the join algorithm
- Supports `seek()` by seeking in the underlying SlateDB iterator and then resolving the
  first live fact at or after the target

### Example Walk-Through

History contains:

```
[HIST][AV][name]["alice"][tx=1][ADD]
[HIST][AV][name]["alice"][tx=5][RETRACT]
[HIST][AV][name]["alice"][tx=8][ADD]
[HIST][AV][name]["bob"][tx=2][ADD]
```

- Query at `basis_tx=3`: alice is live (tx=1 ADD, retract at 5 not yet), bob is live →
  yields {alice, bob}
- Query at `basis_tx=6`: alice retracted at tx=5, bob live → yields {bob}
- Query at `basis_tx=9`: alice re-added at tx=8, bob live → yields {alice, bob}

### Performance Consideration

For facts with many versions, this scans linearly through the version chain. If this
becomes a bottleneck, you could add a secondary structure that stores per-fact version
summaries. But for most workloads (few retracts per fact), this is fine.

---

## Open Design Decisions

A few things to settle before implementation:

1. **Should the current index store the tx_id in the value?** Storing the latest tx_id as
   the value (instead of empty bytes) would allow answering "when was this fact last
   asserted?" without hitting the history index. Cost: 8 bytes per key.

2. **Prefix byte allocation.** Currently indices use prefixes 0–4, with 5 for
   ATTRIBUTE_TO_ID, 128 for STATS, 129 for META. The history variants need their own
   prefixes. Options:
   - Use a bit flag: `prefix | 0x40` for history (e.g., EAV=0x00, HIST_EAV=0x40)
   - Use a separate byte: `[HIST:1B][INDEX_PREFIX:1B][...]`
   - Use high prefix values: HIST_EAV=64, HIST_AVE=65, etc.

3. **tx_id encoding in history keys.** Should be big-endian `u64` (not bincode) for correct
   lexicographic sorting. The current entity/attribute encoding uses bincode which happens
   to work for fixed-size integers but is fragile.

4. **Do you need all 5 history index variants?** The AE and AV indices are projections that
   lose information. For history queries, you might only need EAV + AVE + AEV in the
   history space, and reconstruct AE/AV results by scanning the appropriate parent index.
