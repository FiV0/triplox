# Triplox Partition Specification

Version 0.1

## Overview

A partition is a logical grouping of entities that occupy a distinct region of the entity ID space. The partition is encoded directly in the upper bits of the entity ID, so entities in the same partition are contiguous in sorted order without any additional indirection.

Partitions serve two purposes:

1. **Index locality** — entities in the same partition are adjacent in E-leading indices (EAVT, AEVT), enabling efficient prefix scans in the storage layer.
2. **Namespace separation** — schema entities, transaction entities, and user data occupy non-overlapping regions of the ID space, preventing accidental collision.

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

Because partition 0 places no bits above the counter, entities in `:db.part/db` have small, readable entity IDs (the raw counter value).

### 1.1 Tempids

A negative entity ID (sign bit = 1) is a **tempid** — a client-side placeholder that is never stored persistently. Within a single transaction, two operations referencing the same negative value refer to the same entity. The system resolves each unique tempid to a fresh permanent ID before writing to the indices.

**Design note:** The primary tempid mechanism will be string identifiers (e.g. `"my-new-entity"`), following the common convention in Datomic-style systems. Negative integer tempids may be supported as an alternative in a later phase.

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

Because partition 0 places no bits above the counter, entity IDs in this partition are small integers.

### 2.2 :db.part/tx (partition 1)

Each transaction is reified as an entity in this partition. The transaction entity carries the following attributes:

| Attribute      | Value Type | Description                                                    |
|----------------|------------|----------------------------------------------------------------|
| `db/txInstant`  | instant    | Wall-clock time of the transaction                             |
| `db/txResult`   | ref        | `:db.tx/committed` or `:db.tx/aborted` (see `SCHEMA.md`)      |
| `db/txId`       | long       | The sequential tx_id assigned by the log                       |
| `db/txError`    | string     | Error message (present only when `db/txResult` is `:db.tx/aborted`) |

The transaction entity ID (`tx_eid`) is **not minted** by the partition map; it is a
pure function of the log-assigned `tx_id`:

```
tx_eid = make_entity_id(TX_PARTITION, tx_id) = (1 << 42) | tx_id   (== mask ^ tx_id)
tx_id  = extract_counter(tx_eid)
```

So a `TxKey { tx_id, system_time }` immediately identifies a database value: its
`tx_eid` (the temporal read coordinate) is recovered by masking, with no storage
lookup. A DB opened at a given `TxKey` sees datoms whose transaction entity is at or
before `tx_eid`. Transaction entities appear in the E-leading indices like any other
entity, enabling queries such as "find all transactions after time T" through the
standard query engine.

The indexer *sets* (rather than allocates) the `TX_PARTITION` counter from each
incoming `tx_id`, asserting it is strictly greater than the previous one so `tx_eid`
is strictly increasing. `tx_id` need not be dense — e.g. the file log uses byte
offsets, so `tx_eid`s have gaps. `db/txId` is stored for queryability but is
redundant with the entity id (`db/txId == extract_counter(tx_eid)`).

### 2.3 :db.part/user (partition 2)

The default partition for application data. When a tempid does not specify a partition, the entity is allocated here.

---

## 3. The Global Counter

Entity IDs are allocated from a single, monotonically increasing counter called **T**. Every allocation — whether for a schema entity in `:db.part/db`, a transaction entity in `:db.part/tx`, or a user entity in `:db.part/user` — advances the same counter.

When an entity is allocated in partition P, its entity ID is `(P << 42) | T`, and T advances to `T + 1`.

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
2. Write index entries to the storage layer and update the root to reflect the new `next_t`


### Alternatives

Another option is to have a counter per partition and just have these counters in memory. On initialization one
reads the highest entity id of every partition and extracts the counter part. A simple reverse scan of the EAV index
plus the partition bytes should be sufficient. Maybe 40 bits per partition makes more sense here.

---

## 4. Index Locality

Because entity IDs encode the partition in their upper bits, and because entity IDs are encoded with an order-preserving scheme (see `ENCODING.md`), all entities in the same partition are adjacent in the E-leading indices EAVT, and AEVT.

---

## 5. User-Defined Partitions

_This section covers a future phase. The details of partition creation and assignment are TBD._

The 20-bit partition field supports up to 1,048,575 partitions beyond the three built-in ones. In the future we will allow users to create named partitions and assign entities to them. This enables domain-specific locality. It allows for grouping all entities related to a particular tenant or dataset so they cluster together in the E-leading indices.

Topics to be addressed:

- How user-defined partitions are created (likely via a `db.install/partition` transaction).
- How entities are assigned to a specific partition (explicit partition directives on tempids, e.g. `db/force-partition` or `db/match-partition`).
- Whether partition entities are modelled as regular entities in `:db.part/db` with a `db/ident`.

See [Datomic Partitions](https://docs.datomic.com/transactions/partitions.html) for reference.
