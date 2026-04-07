# Triplox Transaction Pipeline

Version 0.1

## Overview

The `transact_tx_inner` pipeline processes a set of transaction operations into committed datoms. A cloned `PartitionMap` isolates counter mutations so they are only applied on successful commit.

---

## Pipeline Stages

```
1. Clone PartitionMap           pending_pm = self.metadata.partition_map.clone()
   Allocate tx entity           tx_eid = pending_pm.allocate_entid(TX_PARTITION)
                                ↓
2. Expand TxOps                 tx::expand_tx_ops(ops, &schema) -> Vec<DatomWithTempids>
   - TxOp::Put -> N DatomWithTempids (one per attr)
   - TxOp::Add/Retract -> 1 DatomWithTempids
   - Resolves EntityRef::Ident -> entid via schema.ident_map
   - EntityRef::Id -> IdOrTempId::Id (passthrough)
   - EntityRef::TempId -> IdOrTempId::TempId (deferred)
   - EntityRef::LookupRef -> error (not yet supported)
   - Put without :db/id key -> generates internal tempid (__auto_N)
                                ↓
3. Resolve tempids              tx::resolve_tempids(datoms, &mut pending_pm)
   - Pre-scan: tempids with :db/ident -> DB_PARTITION, others -> USER_PARTITION
   - Allocate entids from PartitionMap (same tempid string -> same entid)
   - Map DatomWithTempids -> Datom (IdOrTempId->i64, ValueWithTempIds->DataType)
   Build tx entity datoms       build_tx_entity_datoms(tx_eid, tx_key, ...)
                                ↓
4. Validate + prepare           schema.validate_and_prepare(datoms) -> Result<SchemaUpdate>
   - Type checking: value type matches attribute's value_type
   - Unknown attribute errors
   - Witness pattern: split schema datoms, validate via AttributeBuilder
   - Produces SchemaUpdate (pending delta) — errors abort before commit
                                ↓
5. Cardinality resolution       (existing EAV scan for card-one auto-retract)
                                ↓
6. Write indices + commit       write_index_entries(), txn.commit()
                                ↓
7. Apply on success only:
   - self.metadata.partition_map = pending_pm    (counter reservation committed)
   - self.metadata.schema.apply_schema_update(schema_update)  (infallible)
   - self.metadata.advance_generation()
```

## Key Types

### User-facing (src/ops.rs)

- `EntityRef` — how to identify an entity: `Id(i64)`, `TempId(String)`, `Ident(Keyword)`, `LookupRef(Keyword, DataType)`
- `TxValue` — a value: `Data(DataType)` or `Ref(EntityRef)`
- `TxOp` — transaction operation: `Put`, `Add`, `Retract`, `Delete`, `Erase`

### Intermediate (src/tx.rs)

- `IdOrTempId` — entity after ident resolution: `Id(i64)` or `TempId(String)`
- `ValueWithTempIds` — value after ident resolution: `Data(DataType)` or `TempRef(String)`
- `DatomWithTempids` — datom-shaped tuple before tempid allocation

### Resolved (src/ops.rs)

- `Datom` — fully resolved fact: `entity: i64`, `attribute: Keyword`, `value: DataType`, `op: DatomOp`
- Note: `tx_eid` is not stored on Datom — passed separately to `write_index_entries`
