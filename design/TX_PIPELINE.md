# Triplox Transaction Pipeline

Version 0.1

## Overview

The `transact_tx_inner` pipeline changes a set of transaction operations into committed datoms. A datom is one entity-attribute-value fact. The pipeline changes the counters only in a clone of the `PartitionMap`. It applies the counter changes only if the commit succeeds.

## Threads

Indexing never waits on the shared `tpx-rt-worker` runtime. This runtime serves HTTP and incremental queries. The runtimes split the work as follows:

- `tpx-indexer` runs a current-thread runtime (all its tasks run on one thread). This runtime holds the log subscriber loop and the batch writer of SlateDB (`DbBuilder::with_write_runtime`). Thus a commit is a task switch on one thread. Every SlateDB write and `Db::close` go through this runtime. Thus `Node::close` stops it last.
- `tpx-io` drives all S3 HTTP I/O (`SpawnedReqwestConnector`). Thus a pooled connection does not tie the indexer to the runtime that opened the connection.
- SlateDB keeps recent writes in a memtable (a write buffer in memory). The memtable flusher, the compactor, and the garbage collector of SlateDB stay on the shared runtime. Writes wait on these tasks only under backpressure (SlateDB blocks writes until flushes catch up).

---

## Pipeline Stages

```
1. Clone PartitionMap           pending_pm = self.metadata.partition_map.clone()
   Derive tx entity id          tx_eid = tx_eid_from_tx_id(tx_key.tx_id)
                                pending_pm.set_tx_counter(tx_key.tx_id)?
                                (pipeline reads work directly against slatedb)
                                ↓
2. Expand TxOps                 tx::expand_tx_ops(ops, &schema, &slatedb)
                                -> Vec<DatomWithTempids>
   - TxOp::Put -> N DatomExpanded (one per attr)
   - TxOp::Add/Retract -> 1 DatomExpanded
   - TxOp::RetractEntity -> retractions for every active EAV datom
   - TxOp::Erase -> rejected as unsupported
   - RetractEntity rejects ident and tempid entity references
   - RetractEntity rejects DB_PARTITION and TX_PARTITION targets
   - An unknown user-partition target is a no-op
   - Resolves EntityRef::Ident -> entid via schema.ident_map
   - EntityRef::Id -> EntityExpanded::Id (passthrough)
   - EntityRef::TempId -> EntityExpanded::TempId (deferred)
   - Collects ordinary and RetractEntity lookup refs into one VAE batch
   - Resolves EntityRef::LookupRef -> EntityExpanded::Id
   - Put without :db/id key -> generates internal tempid (__auto_N)
   - Ref-typed lookup values resolve to ValueExpanded::Data(Long)
   - Ref-typed value DataType::String -> ValueExpanded::TempRef
   - Ref-typed value DataType::Keyword -> resolved ident -> ValueExpanded::Data(Long)
   - Sorts and deduplicates RetractEntity targets for one forward EAV scan
   - Narrows DatomExpanded to DatomWithTempids before returning
                                ↓
3. Validate explicit IDs        tx::validate_allocated_entity_ids(...)
   - Concrete entity IDs in ordinary datoms must already be allocated
   - Ref-typed Long values must point to already allocated entity IDs
   - Tempids remain deferred and can still allocate within this transaction
                                ↓
4. Resolve tempids/upserts      tempids::resolve_tempids(...)
   - Splits DatomWithTempids into Mentat-style Generation populations
   - Resolves :db.unique/identity tempids against unique-only VAE
   - Evolves complex upserts until no simple upserts remain
   - Allocates remaining tempids, coalescing unresolved identity matches
   Build tx entity datoms       build_tx_entity_datoms(tx_eid, tx_key, ...)
                                ↓
5. Validate                     schema.validate_datoms(datoms)
   - Runs on the user's datoms before finalize, so intra-tx conflicts
     (add/retract overlap, card-one multi-assert) judge user intent
   - Type checking: value type matches attribute's value_type
   - Unknown attribute errors
   - Detects schema changes
                                ↓
6. Cardinality resolution       finalize_datoms_for_commit (EAV scan for card-one auto-retract)
   - Purely mechanical rewrite: cannot change tx semantics
                                ↓
7. Unique validation            validate_unique_constraints
   - Unique checks for :db.unique/value and :db.unique/identity
   - Runs post-finalize: must see auto-generated card-one retracts
                                ↓
8. Write indices + commit       let mut batch = WriteBatch::new();
                                write_index_entries(&mut batch, ...);
                                slatedb.write_with_options(batch, ...)
                                ↓
9. Apply on success only:
   - self.metadata.partition_map = pending_pm    (counter reservation committed)
   - self.metadata.schema.apply_schema_update(schema_update)  (infallible)
   - self.metadata.advance_generation()
```

Note: Why do lookup refs (`[attribute value]` pairs that name an entity) have their own type in the entity position, but not in the value position? In the value position, a string can be a string or a tempid (a placeholder name for an entity). We did not want to move the schema to the client. Thus validation and resolution must happen on the server.

## Type Pipeline

Each stage makes the types narrower and removes one kind of unresolved reference:

```
TxOp (EntityRef + DataType)
    ↓ expand_tx_ops (async, unique-only VAE + EAV)
Vec<DatomWithTempids>                            only concrete IDs and tempids remain
    ↓ validate_allocated_entity_ids (sync, partition map)
Vec<DatomWithTempids>                            explicit IDs checked
    ↓ tempids::resolve_tempids (async, VAE + partition map)
Datom (i64 + DataType)                           fully concrete
```

## Unique Attributes

Triplox supports two unique modes:

| Mode                  | Meaning                                                                                                          | Lookup refs | Tempid upsert |
|-----------------------|------------------------------------------------------------------------------------------------------------------|-------------|---------------|
| `:db.unique/value`    | At most one entity can have a given `[attribute value]`.                                                         | No          | No            |
| `:db.unique/identity` | It has the same uniqueness constraint. Also, a tempid can upsert (adopt the existing entity with that identity). | Yes         | Yes           |

The VAE index (datoms sorted by value, attribute, entity) has the layout
`[VAE][value][attribute][entity][tx_eid][op]`. Triplox writes VAE entries only
for attributes with `:db/unique`. Triplox uses the VAE index for unique
validation, identity lookup refs, and identity upsert resolution.

## Upsert Resolution

`DatomWithTempids` in Triplox is the equivalent of `TermWithTempIds` in Mentat:

```
DatomWithTempids {
    entity: IdOrTempId,        // concrete entid or tempid
    attribute: Keyword,        // resolved to an entid during classification
    value: ValueWithTempIds,   // concrete value or tempid ref
    op: DatomOp,
}
```

The resolver uses the same terms as Mentat:

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

An inert term is a concrete datom with no tempids. Inert terms do not go
through evolution, the upsert loop in the section "Evolution Loop". The resolver
merges them back after tempid resolution.

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

Retractions are never upserts. After evolution, a retraction can still refer to
an unresolved tempid. In this case, the retraction fails. The reason is that
the system cannot retract an entity that an assertion did not resolve or
allocate.

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

`can_evolve()` is true only if `upserts_e` is not empty. A generation is one
set of buckets, and each pass of the loop makes a new generation. Each
generation has these steps:

1. The resolver collects the candidate identity pairs from `UpsertE`: `(tempid, (attr, value))`.
2. The resolver resolves those `[attr value]` pairs through VAE in one batch.
3. The resolver rewrites every bucket into a new `Generation`.

The rewrite rules are:

- If an `UpsertE` hits VAE, it moves to `upserted` as a concrete datom.
- If an `UpsertE` misses VAE, it demotes to `allocations`.
- If the value tempid of an `UpsertEV` resolved, the `UpsertEV` promotes to
  `UpsertE` for the next generation. This also occurs if the entity tempid
  resolved. Thus the next VAE lookup can detect conflicts.
- If the entity tempid of an `UpsertEV` resolved but the value tempid did not,
  the `UpsertEV` demotes to a partly concrete allocation.
- If neither tempid of an `UpsertEV` resolved, the `UpsertEV` stays in
  `upserts_ev`.
- In the existing `allocations`, the resolver substitutes the newly resolved
  tempids. Terms that become fully concrete move to `resolved`.
- Existing `resolved` terms stay resolved.

The resolver tracks a global `resolved_tempids` map. If a tempid resolves to
different entids (entity IDs) in different generations, the transaction aborts
with a conflicting-upsert error.

In the initial implementation, later validation does not reuse the VAE lookups
from upsert resolution. This is on purpose. A later optimization can cache
these lookups for unique validation. But the old-value lookup for card-one
attributes (one value per entity) will still need its own
`(entity, attribute) -> old value` EAV scan. EAV is the index sorted by entity,
attribute, value.

### Allocation After Evolution

When no `upserts_e` remain, the resolver moves the unresolved `upserts_ev` into
`allocations`. Then the resolver does these steps:

1. It collects the unresolved tempids in deterministic `BTreeSet` order.
2. It builds a multimap (a map from a key to many values)
   `(identity_attr, value_or_tempid) -> Vec<tempid>` for the unresolved
   identity assertions.
3. It uses union-find (a structure that merges items into groups) to coalesce
   the tempids that assert the same new identity.
4. It allocates one entid for each union group.
5. If a tempid in the group asserts `:db/ident`, it uses the DB partition.
   Otherwise, it uses the USER partition. A partition is a range of entity IDs.
6. It substitutes all remaining tempids and returns `FinalPopulations`.

### Worked Example

Assume that `:person/email` is `:db.unique/identity` and that the store has no
`"a@x"` entry:

```clojure
[{:db/id "alice"  :person/email "a@x" :person/name "Alice"}
 {:db/id "alice2" :person/email "a@x" :person/age 30}]
```

The initial generation has these buckets:

- `upserts_e`: `("alice", :person/email, "a@x")`,
  `("alice2", :person/email, "a@x")`
- `allocations`: the name datom and the age datom

The VAE lookup misses. Thus both upserts demote to `allocations`. After the
loop, union-find sees that both tempids assert the same unresolved identity.
It gives the two tempids one new entid. The final transaction writes one entity
with email, name, and age.

If the store already has entity `42` with `:person/email "a@x"`, both tempids
resolve to `42`. The name datom and the age datom move to `resolved`. The
resolver allocates no new entity.

## User-Facing Types

In the entity position, we can use a strongly typed `EntityRef` (id, tempid,
ident, lookup ref). An ident is a keyword that names an entity. A strongly
typed reference does not work in the value position. In Clojure, a value such
as `[:attr value]` can be a vector or a lookup ref. I did not want to bring the
schema to the client in languages that are not strongly typed. I want to look
at this decision again later, because it makes the Rust API look less
convincing. The other option is a special syntax that tells lookup refs apart
from Clojure vectors.
