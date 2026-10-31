# Covering Indices

Triplox uses covering indices in the style of Datomic. A covering index keeps all
the data in its keys. A triple is one (entity, attribute, value) fact. Each index
key contains the transaction entity ID (tx_eid), the entity ID of the transaction.

Each assertion (the addition of a triple) and each retraction (the removal of a
triple) produces a different key. Thus the indices contain the full history of
each triple. Temporal queries (queries about past states) do not need a replay of
the transaction log.

## Index Key Layouts

Triplox keeps five indices. The three full indices (EAVT, AVET, AEVT) contain all
components of the (E, A, V) triple, the transaction entity ID T, and an operation
flag (op). The two partial indices (AE, AV) omit one component of the triple. They
are atemporal, that is, they do not contain T or op.

| Index | Key components |
|-------|----------------|
| EAVT  | E, A, V, T, op |
| AVET  | A, V, E, T, op |
| AEVT  | A, E, V, T, op |
| AE    | A, E           |
| AV    | A, V           |

T is the transaction entity ID (tx_eid). Triplox stores T with a descending i64
encoding. Thus the newest transactions sort first in each logical key group (the
entries with the same E, A, and V). Entity IDs in the E position use the same
encoding. The op component is a flag with the value *added* or *retracted*.

Read `ENCODING.md` for the encoding at the byte level.

### Write path

An assertion writes to all 5 indices. The full indices get the key with
`op = added`. The partial indices get only the (A, E) or (A, V) key. An attribute
with cardinality one has one value for each entity at a time. For these
attributes, an assertion also causes retractions of the triples that it replaces.

A retraction writes a key with `op = retracted` to the 3 full indices only.
Triplox does not update the partial indices (AE, AV).

Each operation has a different T in the full indices. Thus an assertion and a
retraction for the same (E, A, V) triple always have different keys.

## Temporal Filtering (As-Of Queries)

An as-of query reads the state of the data at a given time `as_of`.

### Full indices (EAVT, AVET, AEVT)

The logical key is all of the key except T and op, that is, the (E, A, V)
components in index order. Triplox stores T in descending order. Thus the keys in
a logical key group sort newest first.

To resolve the state at a given time `as_of`, do these steps:

1. In each logical key group, find the first entry where `T <= as_of`. The newest
   entry comes first, so this entry is the first one in the time window.
2. If `op = added`, the triple is present at `as_of`. If `op = retracted`, the
   triple is absent. Skip the group.
3. Move past the logical key group to the next different (E, A, V).

Grouping is local. All versions of the same triple are adjacent in the sorted key
space. Thus buffers and hash tables are not necessary.

### Partial indices (AE, AV)

AE and AV are 2-component indices. AE omits V, and AV omits E. They are atemporal.
They carry no T or op, and Triplox never writes retractions to them. After an
assertion writes an (A, E) or (A, V) pair, the pair stays in the index permanently.

This design is necessary because these indices cannot resolve retractions for each
entity. To find which assertion a retraction cancels, it is necessary to know the
missing component (E or V). For example, if E1 and E2 both have
`(:name, "alice")`, they map to the same AV key `(:name, "alice")`. The index
cannot record a retraction for E1 without an effect on E2.

AE and AV only add entries. Thus they can over-propose, that is, they can report
values or entities that a retraction removed. This is safe because a 3-component
index validates each variable from a 2-component index at a later join level. A
join level is the join step for one query variable.

Each triple pattern `[?e :attr ?v]` gives two extenders. An extender proposes or
filters the values for one variable. At the level of the first variable, the
pattern gives a 2-component extender (AE or AV). At the level of the second
variable, it gives a 3-component extender (AEV, AVE, or EAVT). At that level, one
variable and the constant attribute are bound, so all three key components are
determined. If the 3-component extender finds no matching entry, it filters out
the false proposal from AE or AV.

#### Role of AE and AV in WCOJ

The Worst-Case Optimal Join (WCOJ) algorithm gives each query variable one join
level. At each level, extenders propose candidate values for that variable. Other
extenders intersect (filter) the proposals.

Without AE and AV, the proposal at the first level must scan a 3-component index.
It must also remove the duplicates of the extracted component across all trailing
components. The partial indices prevent this cost. They give a pre-projected view,
that is, a view that already holds only the two components.

#### Alternative: Remove the Separate AE and AV Indices

Triplox can do AE scans on the AEV index and AV scans on the AVE index, instead of
keeping 5 indices. These scans seek over the underlying 3-component indices. This
is a tradeoff between space amplification (extra storage for the same data) and
iteration speed.

This option is viable. If write amplification (extra writes for each change)
becomes the bottleneck, this option can be the right choice.

#### Wildcards

A wildcard (`_`) matches any value and binds no variable. A wildcard pattern such
as `[?e :name _]` compiles to a query with one variable. If the compiler discards
the wildcard position, AE (or AV) becomes the only extender. Then the query has no
3-component validation. Thus the compiler must compile wildcard patterns against
the full indices, or replace the wildcards with dummy variables.

## History Queries

The full indices (EAVT, AVET, AEVT) support history queries (queries for all past
changes) directly. To run a history query, scan the logical key prefix without
temporal filtering. Each entry is a datom, that is, one `[e, a, v, t, added]`
record for one assertion or retraction. Thus Triplox can support the `as-of`,
`since`, and `history` database views of Datomic.

## Seek Behavior

Look at an attribute with cardinality one on an entity. Over its lifetime, the
attribute gets 3 different values: `v1`, `v2`, and `v3`.

```
(E1, A1, V1, ...)
(E1, A1, V2, ...)
(E1, A1, V3, ...)
(E2, A1, V1, ...)
```

If we iterate over these values, only one of them is an assertion at the latest
valid transaction. The other values are retractions. Thus, even if we iterate over
these values in the joins, only one value appears in an `as-of` query. A similar
pattern holds for the other indices, except for the partial indices AV and AE.

If an attribute has cardinality many (many values for each entity), Triplox adds
no retractions. Thus all versions are visible, as required.
