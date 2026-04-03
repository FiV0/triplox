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
2. Resolve entity IDs           resolve_entity_ids(ops, &mut pending_pm)
   - Missing db/id → allocate from USER_PARTITION (or DB_PARTITION if db/ident present)
   - Explicit db/id → pass through (TODO: validate via contains_entid once tempid pipeline exists)
                                ↓
3. Expand to datoms             tx_ops_to_datoms(resolved_ops, tx_eid)
   Build tx entity datoms       build_tx_entity_datoms(tx_eid, tx_key, ...)
                                ↓
4. Validate + prepare           schema.validate_and_prepare(datoms) → Result<SchemaUpdate>
   - Type checking: value type matches attribute's value_type
   - Unknown attribute errors
   - Witness pattern: split schema datoms, validate via AttributeBuilder
   - Produces SchemaUpdate (pending delta) — errors abort before commit
                                ↓
5. Cardinality resolution       (existing EAV scan for card-one auto-retract)
   // TODO: should likely merge into step 4 by using Attribute.multival
   // to detect card-one conflicts during validate_and_prepare, but
   // not in this change — keep the existing EAV scan logic for now.
                                ↓
6. Write indices + commit       write_index_entries(), txn.commit()
                                ↓
7. Apply on success only:
   - self.metadata.partition_map = pending_pm    (counter reservation committed)
   - self.metadata.schema.apply_schema_update(schema_update)  (infallible)
   - self.metadata.advance_generation()
```
