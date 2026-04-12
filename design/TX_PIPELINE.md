# Triplox Transaction Pipeline

Version 0.1

## Overview

The `transact_tx_inner` pipeline processes a set of transaction operations into committed datoms. A cloned `PartitionMap` isolates counter mutations so they are only applied on successful commit.

---

## Pipeline Stages

```
1. Clone PartitionMap           pending_pm = self.metadata.partition_map.clone()
   Allocate tx entity           tx_eid = pending_pm.allocate_entid(TX_PARTITION)
   Begin SlateDB transaction    txn = slatedb.begin(Snapshot)
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
3. Resolve lookup refs          tx::resolve_lookup_refs(datoms, &schema, &txn)
   - Collects all unique (attr_entid, value) pairs from LookupRef variants
   - Batch-resolves via AVE prefix scans (no I/O if no lookup refs)
   - Converts DatomExpanded -> DatomWithTempids (eliminates LookupRef variants)
   - Errors if any lookup ref has no matching entity
                                ↓
4. Resolve tempids              tx::resolve_tempids(datoms, &mut pending_pm)
   - Pre-scan: tempids with :db/ident -> DB_PARTITION, others -> USER_PARTITION
   - Allocate entids from PartitionMap (same tempid string -> same entid)
   - Map DatomWithTempids -> Datom (IdOrTempId->i64, ValueWithTempIds->DataType)
   Build tx entity datoms       build_tx_entity_datoms(tx_eid, tx_key, ...)
                                ↓
5. Cardinality resolution       finalize_datoms_for_commit (EAV scan for card-one auto-retract)
                                ↓
6. Validate                     schema.validate_datoms(datoms)
   - Type checking: value type matches attribute's value_type
   - Unknown attribute errors
   - Detects schema changes
                                ↓
7. Write indices + commit       write_index_entries(), txn.commit()
                                ↓
8. Apply on success only:
   - self.metadata.partition_map = pending_pm    (counter reservation committed)
   - self.metadata.schema.apply_schema_update(schema_update)  (infallible)
   - self.metadata.advance_generation()
```

## Type Pipeline

Each stage narrows the types, eliminating one class of unresolved references:

```
TxOp (EntityRef + DataType)
    ↓ expand_tx_ops (sync, schema-only)
DatomExpanded (EntityExpanded + ValueExpanded)   may contain LookupRef + TempId
    ↓ resolve_lookup_refs (async, AVE index)
DatomWithTempids (IdOrTempId + ValueWithTempIds) only TempId remains
    ↓ resolve_tempids (sync, partition map)
Datom (i64 + DataType)                           fully concrete
```

## Key Types

### User-facing (src/ops.rs)

- `EntityRef` — how to identify an entity: `Id(i64)`, `TempId(String)`, `Ident(Keyword)`, `LookupRef(Keyword, DataType)`
- `TxOp` — transaction operation: `Put`, `Add`, `Retract`, `Delete`, `Erase`. Values are `DataType`; ref-typed attribute values are resolved server-side based on the schema.

### Stage 1 — After expansion (src/tx.rs)

- `EntityExpanded` — entity after ident resolution: `Id(i64)`, `TempId(String)`, or `LookupRef(i64, DataType)` (attribute entid resolved, value pending AVE lookup)
- `ValueExpanded` — value after ident resolution: `Data(DataType)`, `TempRef(String)`, or `LookupRef(i64, DataType)`
- `DatomExpanded` — datom-shaped tuple before lookup ref resolution

### Stage 2 — After lookup ref resolution (src/tx.rs)

- `IdOrTempId` — entity after lookup ref resolution: `Id(i64)` or `TempId(String)`
- `ValueWithTempIds` — value after lookup ref resolution: `Data(DataType)` or `TempRef(String)`
- `DatomWithTempids` — datom-shaped tuple before tempid allocation

### Resolved (src/ops.rs)

- `Datom` — fully resolved fact: `entity: i64`, `attribute: Keyword`, `value: DataType`, `op: DatomOp`
- Note: `tx_eid` is not stored on Datom — passed separately to `write_index_entries`

## Lookup Refs

Lookup refs allow referring to an entity by an attribute-value pair instead of by ID.

**Entity position**: Explicit via `EntityRef::LookupRef(keyword, value)`.

**Value position** (ref-typed attributes only): Implicit via `DataType::Vector([Keyword, Value])` — a 2-element vector where the first element is a keyword. This follows the same implicit-resolution pattern as tempids (`String`) and idents (`Keyword`) in value position.

Resolution is batched: all lookup refs across all datoms are collected, then resolved in one pass via AVE prefix scans.
