# Covering Indices

Triplox uses Datomic-style covering indices that encode the transaction time in
every key. Each assertion and retraction produces a distinct key, so the full
history of every triple is available directly from the indices — no transaction
log replay is needed for temporal queries.

## Index Key Layouts

Five indices are maintained. The three **full indices** (EAVT, AVET, AEVT)
contain all components of the (E, A, V) triple plus the transaction time T and
an operation flag. The two **partial indices** (AE, AV) omit one triple
component and are atemporal — they carry neither T nor op.

| Index | Key components |
|-------|----------------|
| EAVT  | E, A, V, T, op |
| AVET  | A, V, E, T, op |
| AEVT  | A, E, V, T, op |
| AE    | A, E           |
| AV    | A, V           |

- **T** is stored in descending order so that **newest transactions sort first**
  within each logical key group.
- **op** is a flag: *added* or *retracted*.

See `ENCODING.md` for byte-level encoding details.

### Write path

- **Assert (add)**: write to all 5 indices. Full indices receive the key with
  `op = added`. Partial indices receive just the (A, E) or (A, V) key. For
  cardinality one attributes, additions also result in retractions of the
  superseded triples.
- **Retract**: write key with `op = retracted` to the 3 full indices only. The
  partial indices (AE, AV) are not updated.

Each operation carries a distinct T in the full indices, so adds and retracts for
the same (E, A, V) triple are always distinct keys.

## Temporal Filtering (As-Of Queries)

### Full indices (EAVT, AVET, AEVT)

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

### Partial indices (AE, AV)

AE and AV are **2-component** indices that omit V and E respectively. They are
atemporal: they carry no T or op, and retractions are never written to them.
Once an (A, E) or (A, V) pair is asserted, it remains in the index permanently.

This is necessary because per-entity retraction resolution is impossible in
these indices — the missing component (E or V) is needed to distinguish which
assertion a retraction cancels. For example, if E1 and E2 both have
`(:name, "alice")`, they map to the same AV key `(:name, "alice")`. A retraction
for E1 cannot be recorded without also affecting E2.

Because AE and AV are purely additive, they may **over-propose**, i.e. they can
report values or entities that have actually been retracted. This is safe
because every variable proposed by a 2-component index is validated by a
3-component index at a later join level. Each triple pattern `[?e :attr ?v]`
contributes a 2-component extender at the first variable's level (AE or AV) and
a 3-component extender at the second variable's level (AEV, AVE, or EAVT)
because one variable plus the constant attribute are now bound, all three key
components are determined. Any spurious proposal from AE/AV is filtered out when
the 3-component extender finds no matching entry.

#### Role of AE/AV in WCOJ

The Worst-Case Optimal Join (WCOJ) algorithm assigns one **join level** per
query variable. At each level, **extenders** propose candidate values for that
variable, and other extenders intersect (filter) the proposals.

Without AE/AV, the first-level proposal must scan a 3-component index and
deduplicate the extracted component across all trailing components. The partial
indices avoid this overhead by providing a pre-projected view.

#### Alternative: Eliminating Separate AE/AV Indices

Instead of maintaining 5 indices, AE scans can be performed against the **AEV**
index and AV scans against the **AVE** index by seeking over the underlying 3-component
indices. This is a tradeoff between space amplification and iteration speed.

This is viable and may be the right choice if write amplification proves to be
the bottleneck.

#### Wildcards

Wildcard patterns like `[?e :name _]` compile to a single-variable query. If the
wildcard position is discarded, AE (or AV) becomes the sole extender with no
3-component validation. This means wildcard patterns need to be compiled against
the full indices or wildcards be replaced with dummy variables.

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

When iterating over these values only one of them will be an assertion on the latest valid timestamp.
The remaining ones will be retractions. So even if we iterate over these values in the joins, in an `as-of`
query only one value will appear. A similar pattern holds for the other indices (forgetting partial indices AV/AE).

In case an attribute has cardinality many no retractions are added and all versions will be visible as required.
