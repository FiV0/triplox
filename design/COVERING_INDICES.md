# Covering Indices: Approaches and Tradeoffs

This document explores how retract/delete operations interact with the index
design, and the tradeoffs between relying on SlateDB time-travel
([slatedb/slatedb#1263](https://github.com/slatedb/slatedb/issues/1263)) versus
Datomic-style covering indices that encode transaction time.

## Background

### Current Index Layout

Keys are structured as:

```
[index_prefix | component1 | component2 | component3 | op_byte]
```

Where `op_byte` is ADD (0x00), DELETE (0x01), or RETRACT (0x02). There is no
transaction-time component. An ADD and a RETRACT for the same (E, A, V) triple
produce two adjacent keys differing only in the final byte.

### The Problem

This layout creates two issues:

1. **Retract resolution at read time.** Queries must peek ahead to check whether
   an ADD key is followed by a RETRACT key for the same triple.

2. **No re-assertion.** If a triple is added, retracted, and re-added, the keys
   are byte-identical — SlateDB sees the same key written twice. The RETRACT key
   persists, so the triple appears retracted even after re-assertion.

3. **No history.** There is no way to answer "what was the state of the database
   at transaction T?" from the indices alone.

## Approach A: SlateDB Time-Travel (No Covering History)

Rely on [slatedb/slatedb#1263](https://github.com/slatedb/slatedb/issues/1263)
for point-in-time reads. Remove the OP suffix byte from index keys entirely.
Retract becomes a SlateDB `delete` of the ADD key.

### Key format

```
[index_prefix | component1 | component2 | component3]
```

- **Add** → `slatedb.put(key, [])`
- **Retract** → `slatedb.delete(key)`
- **As-of query** → `db.snapshot_with_options(SnapshotOptions { seqnum })`

### Pros

- Simplest key format — no OP byte, no read-time filtering.
- Re-assertion works naturally (put after delete restores the key).
- Point-in-time queries come for free via SlateDB snapshots with retention.
- No query-layer changes needed to resolve ADD/RETRACT pairs.

### Cons

- **No covering history.** A historical snapshot shows the *state* at a point in
  time, but cannot enumerate which datoms were asserted or retracted in a given
  transaction. Implementing Datomic's
  [`history`](https://docs.datomic.com/clojure/index.html#datomic.api/history)
  database — where you walk the indices and see `[e a v tx added]` tuples — is
  not possible from the indices alone.
- **Depends on unmerged SlateDB feature.** As of 2026-03-15, #1263 is still
  under discussion. The SlateDB maintainers have raised concerns about TTL
  interactions, compaction strategy coupling, and imprecise seqnum-to-timestamp
  mapping. The feature may land in a limited form or not at all.
- **History queries require the transaction log.** To answer "what changed in tx
  42?", you must read the serialised `Vec<TxOp>` from the transaction log and
  replay it — you cannot scan an index.
- **Retention window is imprecise.** SlateDB's sequence tracker maps timestamps
  to seqnums approximately. The further back in time, the less accurate.

## Approach B: Covering Indices with Transaction Time (T)

Encode the transaction ID in every index key, following Datomic's model. Each
assertion and retraction is a distinct key.

### Key format

```
[index_prefix | component1 | component2 | component3 | tx_id | added_flag]
```

Where `tx_id` is a big-endian i64 and `added_flag` is a single byte (1 = added,
0 = retracted).

Example for EAVT:

```
[EAV | entity | attribute | value | tx_id | added]
```

- **Add** → write key with `added = 1`
- **Retract** → write key with `added = 0` (different tx_id, so a distinct key)

### Current-state queries (as-of T)

Scan the index prefix `[EAV | entity | attribute | value]`. For each unique
(E, A, V) group, find the entry with the highest `tx_id <= T`. If `added = 1`,
the triple is present; if `added = 0`, it is absent.

### History queries

Scan the same prefix without filtering. Every entry is a `[e a v tx added]`
datom — the full assertion/retraction history is available directly from the
index.

### Pros

- **Full covering history.** Both current-state and history queries are answered
  from the indices. Datomic's `as-of`, `since`, and `history` database views are
  all implementable.
- **Re-assertion works.** Each add/retract has a distinct `tx_id`, so they never
  collide.
- **No dependency on SlateDB time-travel.** Works with SlateDB as a plain
  key-value store.
- **Audit and debugging.** "Show me everything that ever happened to entity 42"
  is a single prefix scan.

### Cons

- **Larger keys.** Every key grows by 9 bytes (8-byte tx_id + 1-byte flag).
  With five indices, this adds ~45 bytes per triple per transaction.
- **More complex read path.** Current-state queries must group by (E, A, V) and
  resolve the latest `tx_id <= T`. This is more work than a simple prefix scan,
  though the grouping is local (adjacent keys).
- **Write amplification.** Every retraction writes 5 new keys (one per index)
  rather than deleting existing ones.
- **Cannot leverage SlateDB snapshots for time-travel.** Even if #1263 lands,
  covering indices manage their own temporality — SlateDB snapshots would be
  redundant for query purposes (though still useful for crash recovery).

## Approach C: Hybrid

Use Approach A (simple keys, SlateDB time-travel) for current-state queries
within the retention window, and fall back to the transaction log for
historical/audit queries. If #1263 does not land, implement Approach B.

### How it would work

1. **Within retention window:** `snapshot_with_options(seqnum)` gives a
   point-in-time view. Indices have no OP byte — clean prefix scans.
2. **Beyond retention window or for history queries:** Read the transaction log,
   replay `Vec<TxOp>` entries, reconstruct the needed state.
3. **Migration path:** If SlateDB time-travel doesn't materialise, migrate to
   Approach B by re-indexing with tx_id-bearing keys.

### Pros

- Simplest possible index format while #1263 is pending.
- Full history available via the transaction log (slower but correct).
- Clean migration path to Approach B if needed.

### Cons

- History queries are slow (log replay vs index scan).
- Two code paths for temporal queries (snapshot vs log replay).
- If #1263 doesn't land, a re-index migration is required.

## Summary

| Property                        | A (SlateDB TT) | B (Covering T) | C (Hybrid)          |
|---------------------------------|-----------------|-----------------|---------------------|
| Key size                        | Smallest        | +9 bytes/key    | Smallest            |
| Read-path complexity            | Simplest        | Group + resolve | Simplest (current)  |
| Re-assertion                    | Works           | Works           | Works               |
| As-of queries                   | Via SlateDB     | Via index       | Via SlateDB         |
| History / audit from index      | No              | Yes             | No (use tx log)     |
| Depends on SlateDB #1263        | Yes             | No              | Partially           |
| Write amplification on retract  | Lowest (delete) | Highest (5 puts)| Lowest (delete)     |

## Open Questions

1. **Will SlateDB #1263 land?** As of 2026-03-15, discussion is active but
   unresolved. The feature may ship in a limited form (retention_duration only,
   no retention_versions).
2. **Is `history` database support a requirement?** If yes, Approach B is the
   only option that supports it efficiently from indices. If history queries are
   rare and can tolerate log replay, Approach A or C may suffice.
3. **tx_id encoding.** Approach B requires big-endian tx_id so byte order
   matches numeric order. This aligns with the existing TODO in indexer.rs
   (triplox-1vr) about switching to big-endian encoding.
