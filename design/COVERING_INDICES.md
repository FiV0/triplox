# Covering Indices

Triplox uses Datomic-style covering indices that encode the transaction entity
ID (tx_eid) in every key. Each assertion and retraction produces a distinct key,
so the full history of every triple is available directly from the indices — no
transaction log replay is needed for temporal queries.

## Index Key Layouts

Three general indexes contain all components of the (E, A, V) triple plus the
transaction entity ID T and an operation flag. VAE stores the same components
for unique attributes only. There are no stored AE or AV indexes.

| Index | Key components | Attributes |
|-------|----------------|------------|
| EAVT  | E, A, V, T, op | All        |
| AVET  | A, V, E, T, op | All        |
| AEVT  | A, E, V, T, op | All        |
| VAET  | V, A, E, T, op | Unique only |

- **T** is the transaction entity ID (tx_eid), stored using descending i64
  encoding so that **newest transactions sort first** within each logical key
  group. This uses the same encoding as entity IDs in the E position.
- **op** is a flag: *added* or *retracted*.

See `ENCODING.md` for byte-level encoding details.

### Write path

- **Assert (add)**: write to EAV, AVE, and AEV with `op = added`, and to VAE
  if the attribute is unique. For cardinality one attributes, additions also
  result in retractions of the superseded triples.
- **Retract**: write to the same indexes with `op = retracted`.

Each operation carries a distinct T in the full indices, so adds and retracts for
the same (E, A, V) triple are always distinct keys.

## Temporal Filtering (As-Of Queries)

### Full indices (EAVT, AVET, AEVT, VAET)

The **logical key** is everything minus T and op — the (E, A, V) components in
index order. Because T is stored in descending order, keys within a logical
group are sorted newest-first.

To resolve the current state at a given time `as_of`:

1. For each logical key group, find the first entry where `T <= as_of` (since
   newest comes first, this is the first entry that falls within the time
   window).
2. If `op = added`, the triple is present at `as_of`. If `op = retracted`, it
   is absent — skip the group.
3. Advance past the logical key group to the next distinct (E, A, V).

Grouping is local: all versions of the same triple are adjacent in the sorted
key space, so no buffering or hash tables are needed.

### Distinct candidate scans

When only the attribute is bound, a triple pattern needs distinct entities or
values for its first variable. These projections use the full indexes:

| Candidate | Source | Prefix skipped after emission |
|-----------|--------|-------------------------------|
| Entity    | AEV    | Index, A, E                   |
| Value     | AVE    | Index, A, V                   |

The iterator reads the first key under the attribute prefix and extracts its
first component. To advance, it seeks to the lexicographic successor of the
attribute-plus-component prefix. This skips all remaining triples and temporal
versions in the group without reading and deduplicating them individually.
Prefix-free component encodings make this safe for variable-length values too.

These scans are **atemporal**: they ignore T and op. Candidates may include
retracted facts, assertions newer than the query snapshot, and prefixes that
only occur in retractions. Partial validation uses the same scans. Once both
entity and value are known, the full index scan resolves temporal visibility
and removes absent triples. These extra candidates do not change final query
results.

Wildcard positions become hidden variables before planning, so patterns such
as `[?e :name _]` still receive full temporal validation before projection.

Candidate-count estimates come from the corresponding AEV or AVE range
statistics. They estimate physical keys, including versions and retractions,
not distinct candidates. This can affect proposer selection; exact distinct
counts would require additional statistics or scanning.

This replaces the former additive AE/AV indexes, reducing index writes and
storage in exchange for seeks during candidate enumeration. This experimental
change includes no migration or cleanup of old AE/AV entries. The remaining
index prefixes retain their byte values.

## History Queries

The full indices (EAVT, AVET, AEVT) support history queries directly. Scan the
logical key prefix without temporal filtering — every entry is a
`[e, a, v, t, added]` datom representing a single assertion or retraction. This
enables Datomic's `as-of`, `since`, and `history` database views.

## Seek Behaviour

Consider an attribute of an entity with cardinality one. That attribute sees 3 different values `v1`, `v2`
and `v3` over its lifetime.

```
(E1, A1, V1, ...)
(E1, A1, V2, ...)
(E1, A1, V3, ...)
(E2, A1, V1, ...)
```

When iterating over these values only one of them will be an assertion at the latest valid transaction.
The remaining ones will be retractions. So even if we iterate over these values in the joins, in an `as-of`
query only one value will appear. A similar pattern holds for temporal scans of the other full indexes. Distinct candidate scans do not resolve temporal visibility.

In case an attribute has cardinality many no retractions are added and all versions will be visible as required.
