# Triplox Partition Specification

Version 0.1

## Overview

A partition is a logical grouping of entities that occupy a distinct region of the entity ID space. The partition is encoded directly in the upper bits of the entity ID, so entities in the same partition are contiguous in sorted order without any additional indirection.

Partitions serve three purposes:

1. **Index locality** — entities in the same partition are adjacent in E-leading indices (EAVT, AEVT), enabling efficient prefix scans in the storage layer.
2. **Automatic entity ID allocation** — clients do not assign entity IDs manually. They provide tempids, and the system allocates permanent IDs from a global counter within the appropriate partition.
3. **Namespace separation** — schema entities, transaction entities, and user data occupy non-overlapping regions of the ID space, preventing accidental collision.

### Guiding principles

- Entity IDs are i64 values everywhere: on the wire, in the indices, and in query results. The partition is not stored separately — it is the upper bits of the entity ID.
- Queries operate over the full index space. The query engine is partition-unaware; partitions affect physical layout, not logical semantics.
- There is a single, global allocation counter shared across all partitions (see Section 3).

---

## 1. Entity ID Bit Layout

An entity ID is a 64-bit signed integer with three fields:

```
 63  62   61                    42  41                              0
+----+----+---------------------+---+-------------------------------+
| S  | 0  | partition (20 bits) |          counter (42 bits)        |
+----+----+---------------------+---+-------------------------------+
```

| Field     | Bits  | Width | Range                      |
|-----------|-------|-------|----------------------------|
| Sign      | 63    | 1     | 0 (permanent), 1 (tempid)  |
| Reserved  | 62    | 1     | Always 0                   |
| Partition | 61–42 | 20    | 0 to 1,048,575             |
| Counter   | 41–0  | 42    | 0 to 4,398,046,511,103     |

### 1.1 Construction and Extraction

```rust
const COUNTER_BITS: u32 = 42;
const COUNTER_MASK: i64 = (1i64 << COUNTER_BITS) - 1;      // 0x3FF_FFFF_FFFF
const PARTITION_MASK: i64 = 0xFFFFF;                         // 20 bits

/// Extract the partition number from an entity ID.
pub fn partition(entity_id: i64) -> i64 {
    (entity_id >> COUNTER_BITS) & PARTITION_MASK
}

/// Extract the counter value from an entity ID.
pub fn counter(entity_id: i64) -> i64 {
    entity_id & COUNTER_MASK
}

/// Construct an entity ID from a partition number and counter.
pub fn make_entity_id(partition: i64, counter: i64) -> i64 {
    (partition << COUNTER_BITS) | counter
}
```

Note that `make_entity_id(0, n) == n` for any non-negative `n` that fits in 42 bits. This means entities in partition 0 have small, readable entity IDs.

### 1.2 Tempids

A negative entity ID (sign bit = 1) is a **tempid** — a client-side placeholder that is never stored persistently. Within a single transaction, two operations referencing the same negative value refer to the same entity. The system resolves each unique tempid to a fresh permanent ID before writing to the indices. See Section 4.

---

## 2. Built-in Partitions

Three partitions are installed at bootstrap:

| Partition | Number | Keyword           | Entity ID base    | Purpose                          |
|-----------|--------|-------------------|-------------------|----------------------------------|
| db        | 0      | `:db.part/db`     | `0`               | Schema, enums, system entities   |
| tx        | 1      | `:db.part/tx`     | `1 << 42`         | Transaction entities             |
| user      | 2      | `:db.part/user`   | `2 << 42`         | Default for application data     |

### 2.1 :db.part/db (partition 0)

Schema attributes, value type enums, cardinality enums, and partition entities live here. See `SCHEMA.md` for the bootstrap entity definitions.

Because `make_entity_id(0, n) == n`, entity IDs in this partition are small integers.

### 2.2 :db.part/tx (partition 1)

Each committed transaction is reified as an entity in this partition. The transaction entity carries at least one attribute:

| Attribute      | Value Type | Description                   |
|----------------|------------|-------------------------------|
| `db/txInstant`  | instant    | Wall-clock time of the commit |

A transaction with counter value `t` has entity ID `make_entity_id(1, t)`. Transaction entities appear in the E-leading indices like any other entity, enabling queries such as "find all transactions after time T" through the standard query engine.

### 2.3 :db.part/user (partition 2)

The default partition for application data. When a tempid does not specify a partition, the entity is allocated here.

---

## 3. The Global Counter

Entity IDs are allocated from a single, monotonically increasing counter called **T**. Every allocation — whether for a schema entity in `:db.part/db`, a transaction entity in `:db.part/tx`, or a user entity in `:db.part/user` — advances the same counter.

When an entity is allocated in partition P, its entity ID is `make_entity_id(P, T)`, and T advances to `T + 1`.

### 3.1 Counter Semantics

- T starts at 0 on a fresh database. The bootstrap transaction consumes the first batch of counter values for schema entities, enum entities, and partition entities.
- T never rewinds, even if a transaction fails after allocating IDs.
- Counter values may have gaps within a given partition. If T = 100 allocates a user entity and T = 101 allocates a tx entity, partition 2 (user) goes from counter 100 to counter 102.
- This is acceptable: entity IDs are opaque handles, not dense sequences. The gaps do not affect storage efficiency because the indices store only entities that exist.

### 3.2 Persistence

T is stored durably as part of the **database root** — a small metadata record that is updated atomically on each committed transaction. The root contains:

| Field    | Type    | Description                             |
|----------|---------|-----------------------------------------|
| `next_t` | i64     | Next counter value to allocate          |
| ...      |         | Index metadata (see `ENCODING.md`)      |

The commit path is:

1. Allocate counter values for all new entities in the transaction.
2. Write index entries to the storage layer.
3. Atomically update the root to reflect the new `next_t`.
4. Acknowledge the transaction.

If the system crashes before step 3 completes, the root still holds the old `next_t`. The failed transaction's counter values are lost (creating a small gap), and the next transaction resumes from the persisted `next_t`. No entity ID is ever reused.

---

## 4. Tempids and Allocation

### 4.1 Tempid Representation

A tempid is any negative i64. Clients use tempids in place of entity IDs when creating new entities:

```rust
// Two assertions about the same new entity
TxOp::Add { entity_id: EntityId(-1), attribute: "name",  value: "Alice" }
TxOp::Add { entity_id: EntityId(-1), attribute: "email", value: "alice@example.com" }

// A different new entity
TxOp::Add { entity_id: EntityId(-2), attribute: "name",  value: "Bob" }
```

Within a single transaction, all occurrences of the same negative value refer to the same to-be-allocated entity. Different negative values refer to different entities. The actual magnitude of the tempid is meaningless — it is only a correlation handle.

Tempids in `TxOp::Put` documents appear as the `db/id` field:

```rust
TxOp::Put(Document({
    "db/id": Long(-1),
    "name": String("Alice"),
}))
```

### 4.2 Partition Assignment

Each tempid is resolved to a permanent entity ID in one of the three built-in partitions. The partition is determined by what the entity represents:

| Condition | Target Partition |
|-----------|-----------------|
| Entity defines a schema attribute (`db/ident` + `db/valueType`) | `:db.part/db` (0) |
| Transaction entity (automatically created per transaction) | `:db.part/tx` (1) |
| Everything else | `:db.part/user` (2) |

### 4.3 Resolution Algorithm

Tempid resolution runs inside the transactor before datoms are written to the indices:

1. Scan all datoms in the transaction for negative entity values. Collect the set of unique tempids.
2. For each unique tempid, determine the target partition (see 4.2).
3. Allocate a permanent entity ID: `make_entity_id(partition, T)`, then `T += 1`.
4. Replace every occurrence of the tempid in the datom vector with its permanent ID.
5. Return the tempid → permanent ID mapping as part of the transaction result.

### 4.4 Transaction Result

The transaction result includes a map from tempids to resolved entity IDs, so clients can discover the permanent IDs of entities they created:

```rust
pub struct TransactionResult {
    pub tx_key: TxKey,
    pub tempids: HashMap<i64, i64>,  // tempid → permanent entity ID
}
```

---

## 5. Index Locality

Because entity IDs encode the partition in their upper bits, and because entity IDs are encoded with an order-preserving scheme (see `ENCODING.md`), all entities in the same partition are adjacent in the E-leading indices.

### 5.1 E-leading Indices (EAVT, AEVT)

Within EAVT, keys are sorted by entity ID first. The partition bits dominate the sort order, producing three contiguous regions:

```
[EAV prefix] [partition 0, counter 0] ...   ← schema entity
[EAV prefix] [partition 0, counter 1] ...   ← schema entity
...
[EAV prefix] [partition 1, counter 50] ...  ← tx entity (counter 50)
[EAV prefix] [partition 1, counter 53] ...  ← tx entity (counter 53, gap is fine)
...
[EAV prefix] [partition 2, counter 51] ...  ← user entity (counter 51)
[EAV prefix] [partition 2, counter 52] ...  ← user entity (counter 52)
...
```

### 5.2 Prefix Scans

A storage-layer prefix scan with the encoded entity ID prefix can efficiently enumerate all entities in a single partition. For example, scanning all user entities means scanning EAVT keys whose entity ID bytes fall in the partition-2 range. No full index scan is required.

### 5.3 Attribute-Leading Indices

The AVE, AEV, AE, and AV indices sort by attribute first. Partition encoding has no effect on their layout. Partitions benefit entity-centric access patterns (EAVT lookups, entity-range scans), not attribute-centric patterns.

---

## 6. User-Defined Partitions

_This section covers a future phase. The details of partition creation and assignment are TBD._

The 20-bit partition field supports up to 1,048,575 partitions beyond the three built-in ones. A future version will allow users to create named partitions and assign entities to them. This enables domain-specific locality — for example, grouping all entities related to a particular tenant or dataset so they cluster together in the E-leading indices.

Topics to be addressed:

- How user-defined partitions are created (likely via a `db.install/partition` transaction).
- How entities are assigned to a specific partition (explicit partition directives on tempids, e.g. `db/force-partition` or `db/match-partition`).
- Whether partition entities are modelled as regular entities in `:db.part/db` with a `db/ident`.

See [Datomic Partitions](https://docs.datomic.com/transactions/partitions.html) for reference.
