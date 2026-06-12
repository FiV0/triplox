# Triplox Transaction Pipeline

Version 0.1

## Overview

The `transact_tx_inner` pipeline processes a set of transaction operations into committed datoms. A cloned `PartitionMap` isolates counter mutations so they are only applied on successful commit.

---

## Pipeline Stages

```
1. Clone PartitionMap           pending_pm = self.metadata.partition_map.clone()
   Allocate tx entity           tx_eid = pending_pm.allocate_entid(TX_PARTITION)
                                (pipeline reads go directly against slatedb;
                                 index writes are buffered in a WriteBatch
                                 committed atomically in step 9)
                                ↓
2. Expand TxOps                 tx::expand_tx_ops(ops, &schema) -> Vec<DatomExpanded>
   - TxOp::Put -> N DatomExpanded (one per attr)
   - TxOp::Add/Retract -> 1 DatomExpanded
   - Resolves EntityRef::Ident -> entid via schema.ident_map
   - EntityRef::Id -> EntityExpanded::Id (passthrough)
   - EntityRef::TempId -> EntityExpanded::TempId (deferred)
   - EntityRef::LookupRef -> EntityExpanded::LookupRef (attr ident resolved to entid)
   - Put without :db/id key -> generates internal tempid (__auto_N)
   - Ref-typed value DataType::Vector([Keyword, Value]) -> ValueExpanded::LookupRef
   - Ref-typed value DataType::String -> ValueExpanded::TempRef
   - Ref-typed value DataType::Keyword -> resolved ident -> ValueExpanded::Data(Long)
                                ↓
3. Resolve lookup refs          tx::resolve_lookup_refs(datoms, &schema, &slatedb)
   - Collects all unique (attr_entid, value) pairs from LookupRef variants
   - Batch-resolves via AVE prefix scans (no I/O if no lookup refs)
   - Converts DatomExpanded -> DatomWithTempids (eliminates LookupRef variants)
   - Errors if any lookup ref has no matching entity
                                ↓
4. Validate explicit IDs        tx::validate_allocated_entity_ids(...)
   - Concrete entity IDs must already be allocated in their partition
   - Ref-typed Long values must point to already allocated entity IDs
   - Tempids remain deferred and can still allocate within this transaction
                                ↓
5. Resolve tempids/upserts      tempids::resolve_tempids(...)
   - Splits DatomWithTempids into Mentat-style Generation populations
   - Resolves :db.unique/identity tempids against unique-only VAE
   - Evolves complex upserts until no simple upserts remain
   - Allocates remaining tempids, coalescing unresolved identity matches
   Build tx entity datoms       build_tx_entity_datoms(tx_eid, tx_key, ...)
                                ↓
6. Validate                     schema.validate_datoms(datoms)
   - Runs on the user's datoms before finalize, so intra-tx conflicts
     (add/retract overlap, card-one multi-assert) judge user intent
   - Type checking: value type matches attribute's value_type
   - Unknown attribute errors
   - Detects schema changes
                                ↓
7. Cardinality resolution       finalize_datoms_for_commit (EAV scan for card-one auto-retract)
   - Purely mechanical rewrite: cannot change tx semantics
                                ↓
8. Unique validation            validate_unique_constraints
   - Unique checks for :db.unique/value and :db.unique/identity
   - Runs post-finalize: must see auto-generated card-one retracts
                                ↓
9. Write indices + commit       let mut batch = WriteBatch::new();
                                write_index_entries(&mut batch, ...);
                                slatedb.write_with_options(batch, ...)
                                ↓
10. Apply on success only:
   - self.metadata.partition_map = pending_pm    (counter reservation committed)
   - self.metadata.schema.apply_schema_update(schema_update)  (infallible)
   - self.metadata.advance_generation()
```

Note: You might be wondering why lookup refs are fully typed in entity position, but not in value position (a string could a string or a tempid). We wanted to avoid moving Schema to the client, so that checking and resolving needs to happen server side.

## Type Pipeline

Each stage narrows the types, eliminating one class of unresolved references:

```
TxOp (EntityRef + DataType)
    ↓ expand_tx_ops (sync, schema-only)
DatomExpanded (EntityExpanded + ValueExpanded)   may contain LookupRef + TempId
    ↓ resolve_lookup_refs (async, unique-only VAE index)
DatomWithTempids (IdOrTempId + ValueWithTempIds) only TempId remains
    ↓ validate_allocated_entity_ids (sync, partition map)
DatomWithTempids                                explicit IDs checked
    ↓ tempids::resolve_tempids (async, VAE + partition map)
Datom (i64 + DataType)                           fully concrete
```

## Unique Attributes

Triplox supports two unique modes:

| Mode                  | Meaning                                                                           | Lookup refs | Tempid upsert |
|-----------------------|-----------------------------------------------------------------------------------|-------------|---------------|
| `:db.unique/value`    | At most one entity may have a given `[attribute value]`.                          | No          | No            |
| `:db.unique/identity` | Same uniqueness constraint, plus tempids can adopt existing entities by identity. | Yes         | Yes           |

VAE has layout `[VAE][value][attribute][entity][tx_eid][op]` and is written
only for attributes with `:db/unique`. It is used for unique checks, identity
lookup refs, and identity upsert resolution.

## Upsert Resolution

`DatomWithTempids` is Triplox's equivalent of Mentat's `TermWithTempIds`:

```
DatomWithTempids {
    entity: IdOrTempId,        // concrete entid or tempid
    attribute: Keyword,        // resolved to an entid during classification
    value: ValueWithTempIds,   // concrete value or tempid ref
    op: DatomOp,
}
```

The resolver uses the same terminology as Mentat:

```rust
struct UpsertE(String, Entid, DataType);   // [:db/add TEMPID unique-id-attr CONCRETE_V]
struct UpsertEV(String, Entid, String);    // [:db/add TEMPID unique-id-attr TEMPID2]

struct Generation {
    upserts_e: Vec<UpsertE>,
    upserts_ev: Vec<UpsertEV>,
    allocations: Vec<DatomWithTempids>,
    upserted: Vec<Datom>,
    resolved: Vec<Datom>,
}

struct FinalPopulations {
    upserted: Vec<Datom>,
    resolved: Vec<Datom>,
    allocated: Vec<Datom>,
}
```

Concrete datoms with no tempids are inert terms. They bypass evolution and are
merged back after tempid resolution.

### Initial Classification

Each staged datom goes into exactly one bucket:

| Shape                      | Attribute is `:db.unique/identity`? | Op     | Bucket        |
|----------------------------|-------------------------------------|--------|---------------|
| `(TempId e, a, Data v)`    | yes                                 | Assert | `upserts_e`   |
| `(TempId e, a, Data v)`    | no                                  | any    | `allocations` |
| `(TempId e, a, TempRef v)` | yes                                 | Assert | `upserts_ev`  |
| `(TempId e, a, TempRef v)` | no                                  | any    | `allocations` |
| `(Id e, a, TempRef v)`     | any                                 | any    | `allocations` |
| `(Id e, a, Data v)`        | any                                 | any    | inert         |

Retractions are never upserts. A retraction that still mentions an unresolved
tempid after evolution fails because the system cannot retract an entity that
was neither resolved nor allocated by an assertion.

### Evolution Loop

The driver loop is:

```rust
while generation.can_evolve() {
    let tempid_avs = generation.temp_id_avs();
    let temp_id_map = resolve_temp_id_avs(&tempid_avs, db).await?;
    record_resolutions_and_detect_conflicts(temp_id_map)?;
    generation = generation.evolve_one_step(&temp_id_map, &resolved_tempids)?;
}
```

`can_evolve()` is true only when `upserts_e` is non-empty. Each generation:

1. Collects candidate identity pairs from `UpsertE`: `(tempid, (attr, value))`.
2. Batch-resolves those `[attr value]` pairs through VAE.
3. Rewrites every population into a fresh `Generation`.

Rewrite rules:

- `UpsertE` that hits VAE moves to `upserted` as a concrete datom.
- `UpsertE` that misses VAE demotes to `allocations`.
- `UpsertEV` whose value tempid resolved promotes to `UpsertE` for the next
  generation. This happens even if the entity tempid also resolved, so the next
  VAE lookup can detect conflicts.
- `UpsertEV` whose entity tempid resolved but value tempid did not demotes to a
  partially concrete allocation.
- `UpsertEV` with neither side resolved remains in `upserts_ev`.
- Existing `allocations` substitute newly resolved tempids; fully concrete
  terms move to `resolved`.
- Existing `resolved` terms remain resolved.

The resolver tracks a global `resolved_tempids` map. If a tempid resolves to
different entids in different generations, the transaction aborts with a
conflicting-upsert error.

The VAE lookups performed during upsert resolution are intentionally not reused
by later validation in the initial implementation. A later optimization can
cache them for uniqueness validation, but cardinality old-value lookup will
still require its own `(entity, attribute) -> old value` EAV scan.

### Allocation After Evolution

When no `upserts_e` remain, unresolved `upserts_ev` are drained into
`allocations`. The resolver then:

1. Collects unresolved tempids in deterministic `BTreeSet` order.
2. Builds a multimap `(identity_attr, value_or_tempid) -> Vec<tempid>` for
   unresolved identity assertions.
3. Uses union-find to coalesce tempids that assert the same new identity.
4. Allocates one entid per union group.
5. Uses DB partition if any tempid in the group asserts `:db/ident`, otherwise
   USER partition.
6. Substitutes all remaining tempids and returns `FinalPopulations`.

### Worked Example

Assume `:person/email` is `:db.unique/identity` and the store has no
`"a@x"` entry:

```clojure
[{:db/id "alice"  :person/email "a@x" :person/name "Alice"}
 {:db/id "alice2" :person/email "a@x" :person/age 30}]
```

Initial generation:

- `upserts_e`: `("alice", :person/email, "a@x")`,
  `("alice2", :person/email, "a@x")`
- `allocations`: name and age datoms

The VAE lookup misses, so both upserts demote to allocations. Post-loop
union-find sees both tempids asserting the same unresolved identity and assigns
them one new entid. The final transaction writes one entity with email, name,
and age.

If the store already has entity `42` with `:person/email "a@x"`, both tempids
resolve to `42`; name and age move to `resolved`, and no new entity is allocated.

## User facing types

In the entity position we can use a strongly typed `EntityRef` (id, tempid, ident, lookup ref). The same thing doesn't
work for value positions as things like `[:attr value]` could be considered a vector or a lookup-ref in Clojure and I didn't
want to bring the schema to the client in non strongly typed languages. I think this is something to be revisited as it makes
the Rust API look less convincing. The other option is to use special syntax for lookup refs vs Clojure vectors.
