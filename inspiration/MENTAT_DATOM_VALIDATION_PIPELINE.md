# Mentat Datom Validation Pipeline

From resolved datoms (no tempids) through validation to schema update.

---

## 1. Input

A flat list of `Term::AddOrRetract(op, KnownEntid(e), a, v)` — fully resolved
datoms with concrete entity IDs.

---

## 2. AEV Trie Construction

Terms are grouped into a trie that organizes by attribute, then entity, then
separates adds from retracts:

```
BTreeMap<
    (Entid, &Attribute),              -- attribute entid + its schema definition
    BTreeMap<
        Entid,                        -- entity
        AddAndRetract {
            add: BTreeSet<TypedValue>,
            retract: BTreeSet<TypedValue>,
        }
    >
>
```

Built by iterating each term: look up `&Attribute` from `Schema.attribute_map`
for the term's `a`, then insert `v` into the appropriate `add` or `retract` set
at `trie[(a, attribute)][e]`.

Grouping by `(a, &Attribute)` first means each attribute's constraints only need
to be checked once per entity group.

---

## 3. Validation (two passes over the trie)

### Pass 1 — Type checking

For every `(a, attribute)` group, for every entity, for every value in both
`add` and `retract`:

```
if attribute.value_type != v.value_type() -> error
```

Collects all mismatches into a `BTreeMap<(e, a, v), expected_ValueType>`.
Fails with all errors at once if non-empty.

### Pass 2 — Cardinality checking

For every `(a, attribute)` group where `attribute.multival == false`, for every
entity:

- If `add.len() > 1` -> `CardinalityOneAddConflict` (two distinct values for a
  cardinality-one attribute in the same transaction)
- If `add ∩ retract` is non-empty -> `AddRetractConflict` (same value both added
  and retracted in the same transaction)

Collects all conflicts. Fails with all errors at once if non-empty.

Both passes are exhaustive — they report every error, not just the first.

---

## 4. Metadata Detection

While draining the trie for storage insertion, each attribute entid `a` is
checked against a hardcoded list (`might_update_metadata`). If any `a` is a
schema-defining attribute (`:db/ident`, `:db/valueType`, `:db/cardinality`,
`:db/unique`, `:db/index`, `:db/fulltext`, `:db/isComponent`, `:db/noHistory`),
a flag `tx_might_update_metadata` is set.

If the flag is not set, the pipeline ends here. No schema update work is done.

---

## 5. Schema Update (only if flag is set)

After datoms are persisted, the committed `(e, a, v, added)` quadruples for
schema-related attributes are read back and processed through three layers.

### Layer 1 — Separate idents from attribute definitions

`update_schema_from_entid_quadruples` partitions quadruples into two
`AddRetractAlterSet`s:

**`ident_set`** — quadruples where `a == DB_IDENT`:
- Tracks `:db/ident` assertions, retractions, and alterations per entity.

**`attribute_set`** — quadruples where `a` is any other schema attribute
(`DB_VALUE_TYPE`, `DB_CARDINALITY`, `DB_UNIQUE`, etc.):
- Keyed by `(e, a)`, tracks the `TypedValue` asserted/retracted/altered.

`AddRetractAlterSet.witness(key, value, added)` folds an add+retract pair for
the same key into a single `altered` entry `(old_value, new_value)`. An add
alone stays in `asserted`, a retract alone stays in `retracted`. This produces
three disjoint sets: **asserted** (new), **retracted** (removed), **altered**
(changed from old to new).

### Layer 2a — Schema retractions

Retracted attribute triples are checked: if both `:db/valueType` and
`:db/cardinality` are retracted for entity `e`, AND `:db/ident` is also
retracted, then `attribute_map.remove(e)` — the attribute is fully removed.

Otherwise it is an error: you cannot partially retract a schema definition
without also retracting its `:db/ident`.

### Layer 2b — Attribute installation and alteration

Remaining assertions + alterations are grouped by entity into
`AttributeBuilder`s. Each `(e, a, v)` triple sets one field on the builder:

```
DB_VALUE_TYPE   -> builder.value_type(...)
DB_CARDINALITY  -> builder.multival(...)
DB_UNIQUE       -> builder.unique(...)
DB_INDEX        -> builder.index(...)
DB_FULLTEXT     -> builder.fulltext(...)
DB_IS_COMPONENT -> builder.component(...)
DB_NO_HISTORY   -> builder.no_history(...)
```

Then for each entity's builder, two cases:

**New attribute** (entity not in `attribute_map`):
1. `builder.validate_install_attribute()` — requires `:db/valueType` is set.
2. `builder.build()` -> `Attribute`.
3. `attribute.validate()` — enforces:
   - unique requires index
   - fulltext requires string + index
   - component requires ref
4. Insert into `attribute_map`.

**Existing attribute** (entity already in `attribute_map`):
1. `builder.validate_alter_attribute()` — rejects changes to `value_type` or
   `fulltext` (immutable after creation).
2. `builder.mutate(existing_attribute)` — applies changes in place, returns a
   list of `AttributeAlteration` variants (`Index`, `Unique`, `Cardinality`,
   `NoHistory`, `IsComponent`).

### Layer 3 — Ident map updates

From `ident_set`:

- **Asserted**: insert into both `schema.ident_map` (keyword -> entid) and
  `schema.entid_map` (entid -> keyword).
- **Altered**: remove old keyword, insert new keyword in both maps.
- **Retracted**: remove from both maps.

If any attributes changed, `schema.update_component_attributes()` rebuilds the
`component_attributes` vec by filtering `attribute_map` for `component == true`.

---

## 6. Result

A `MetadataReport` is returned:

```
MetadataReport {
    attributes_installed: BTreeSet<Entid>,
    attributes_altered: BTreeMap<Entid, Vec<AttributeAlteration>>,
    idents_altered: BTreeMap<Entid, IdentAlteration>,
}
```

The caller compares the mutated schema against the pre-transaction schema. If
different, it installs the new `Schema` and uses the `MetadataReport` to drive
any necessary storage-level updates (index flag changes, materialized view
rebuilds, cardinality constraint checks against existing data).

---

## Key Files

| File | Role |
|------|------|
| `db/src/internal_types.rs` | `AddAndRetract`, `AEVTrie` type definitions |
| `db/src/tx.rs` | `into_aev_trie()`, `extend_aev_trie()`, metadata detection, orchestration |
| `db/src/tx_checking.rs` | `type_disagreements()`, `cardinality_conflicts()` |
| `db/src/metadata.rs` | `update_schema_from_entid_quadruples()`, `update_attribute_map_from_entid_triples()`, `AttributeAlteration` |
| `db/src/schema.rs` | `AttributeBuilder`, `AttributeValidation` trait, `validate()` |
| `db/src/add_retract_alter_set.rs` | `AddRetractAlterSet` — folds add/retract pairs into asserted/retracted/altered |
| `core-traits/lib.rs` | `Attribute` struct, `ValueType` enum |
| `core/src/lib.rs` | `Schema` struct with `ident_map`, `entid_map`, `attribute_map` |
