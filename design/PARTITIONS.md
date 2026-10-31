# Triplox Partition Specification

Version 0.1

## Overview

An entity ID is the 64-bit number that identifies an entity. A partition is a group of entities with its own range of entity IDs. The upper bits of the entity ID hold the partition. Thus, entities in the same partition are next to each other in sorted order, and no extra level of indirection is necessary.

Partitions have two purposes:

1. Index locality. An E-leading index sorts its keys by entity first. Entities in the same partition are adjacent in the E-leading indices (EAVT, AEVT). Thus, the storage layer can do efficient prefix scans (reads of all keys with the same start).
2. Namespace separation. Schema entities, transaction entities, and user data use ranges of the ID space that do not overlap. This prevents accidental collisions.

### Guiding principles

- Entity IDs are i64 values everywhere: on the wire, in the indices, and in query results. The system does not store the partition separately. The partition is the upper bits of the entity ID.
- Queries operate over the full index space. The query engine does not know about partitions. Partitions change the physical layout, not the logical semantics.
- All partitions share one global allocation counter (see Section 3). This counter supplies the number for each new entity ID.

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

Because partition 0 sets no bits above the counter, entities in `:db.part/db` have small entity IDs that are easy to read. Each of these IDs is the raw counter value.

### 1.1 Tempids

A tempid is a temporary entity ID that the client supplies. A negative entity ID (sign bit = 1) is a tempid. It is only a placeholder, and the system never stores it persistently. In one transaction, two operations that use the same negative value refer to the same entity. Before the system writes to the indices, it resolves each unique tempid to a new permanent ID.

Design note: The primary tempid mechanism will be string identifiers (for example, `"my-new-entity"`). This follows the common convention in Datomic-style systems. Support for negative integer tempids as an alternative is possible in a later phase.

---

## 2. Built-in Partitions

At bootstrap (the setup of a new database), the system installs three partitions:

| Partition | Number | Keyword           | Entity ID base    | Purpose                          |
|-----------|--------|-------------------|-------------------|----------------------------------|
| db        | 0      | `:db.part/db`     | `0`               | Schema, enums, system entities   |
| tx        | 1      | `:db.part/tx`     | `1 << 42`         | Transaction entities             |
| user      | 2      | `:db.part/user`   | `2 << 42`         | Default for application data     |

### 2.1 :db.part/db (partition 0)

This partition holds the schema attributes, the value type enums, the cardinality enums, and the partition entities. For the definitions of the bootstrap entities, see `SCHEMA.md`.

Because partition 0 sets no bits above the counter, entity IDs in this partition are small integers.

### 2.2 :db.part/tx (partition 1)

The system reifies each transaction as an entity in this partition. To reify a transaction is to store it as a regular entity. The transaction entity has these attributes:

| Attribute      | Value Type | Description                                                         |
|----------------|------------|---------------------------------------------------------------------|
| `db/txInstant` | instant    | Wall-clock time of the transaction                                  |
| `db/txResult`  | ref        | `:db.tx/committed` or `:db.tx/aborted` (see `SCHEMA.md`)            |
| `db/txId`      | long       | The sequential tx_id that the log assigns                           |
| `db/txError`   | string     | Error message (only if `db/txResult` is `:db.tx/aborted`)           |

The partition map does not allocate the transaction entity ID (`tx_eid`). The `tx_eid` is a pure function of the `tx_id` that the log assigns. A pure function always gives the same output for the same input:

```
tx_eid = make_entity_id(TX_PARTITION, tx_id) = (1 << 42) | tx_id   (== mask ^ tx_id)
tx_id  = extract_counter(tx_eid)
```

This has a nice property. For a query, it is not necessary for us to look up the `tx_eid` of a given `TxKey`. We can derive it with a mask (a fixed bit pattern) instead.

The indexer does not allocate the `TX_PARTITION` counter. It sets the counter from each incoming `tx_key.tx_id`. The semantics of the log make sure that these IDs strictly increase. The system stores `db/txId` so that queries can use it.

### 2.3 :db.part/user (partition 2)

This is the default partition for application data. If a tempid does not specify a partition, the system allocates the entity in this partition.

---

## 3. The Global Counter

The system allocates entity IDs from one counter, called T. T is monotonically increasing, which means that it never decreases. Each allocation advances this same counter. This is true for a schema entity in `:db.part/db`, a transaction entity in `:db.part/tx`, and a user entity in `:db.part/user`.

When the system allocates an entity in partition P, the entity ID is `(P << 42) | T`. Then T advances to `T + 1`.

### 3.1 Counter Semantics

- T starts at 0 in a new database. The bootstrap transaction uses the first batch of counter values for the schema entities, the enum entities, and the partition entities.
- Even if a transaction fails after it allocates IDs, T never rewinds.
- In a given partition, the counter values can have gaps. For example, T = 100 allocates a user entity and T = 101 allocates a tx entity. Then partition 2 (user) goes from counter 100 to counter 102.
- The gaps are not a problem, because entity IDs are opaque handles, not dense sequences. An opaque handle is a value with no meaning of its own. The indices store only the entities that exist. Thus, the gaps do not affect storage efficiency.

### 3.2 Persistence

The database root is a small metadata record for the database. The system stores T durably in the database root. Each committed transaction updates the root atomically. The root contains these fields:

| Field    | Type    | Description                             |
|----------|---------|-----------------------------------------|
| `next_t` | i64     | Next counter value to allocate          |
| ...      |         | Index metadata (see `ENCODING.md`)      |

The steps to commit a transaction are:

1. Allocate counter values for all new entities in the transaction.
2. Write the index entries to the storage layer, and update the root with the new `next_t`.


### Alternatives

Another option is one counter per partition, with these counters only in memory. At initialization, the system reads the highest entity ID of each partition and extracts the counter part. A reverse scan (a read from the last key backward) of the EAV index with the partition bytes is probably sufficient. Possibly 40 bits per partition is a better choice for this option.

---

## 4. Index Locality

Entity IDs hold the partition in their upper bits. Entity IDs also use an order-preserving encoding (see `ENCODING.md`). Thus, the encoded bytes sort in the same order as the numbers. As a result, all entities in the same partition are adjacent in the E-leading indices EAVT and AEVT.

---

## 5. User-Defined Partitions

_This section is about a future phase. The details of how to create and assign partitions are TBD._

The 20-bit partition field supports up to 1,048,575 partitions in addition to the three built-in ones. In the future, we will let users create named partitions and assign entities to them. This gives locality that fits the domain of the application. For example, a user can group all entities of one tenant or one dataset. Then these entities cluster together in the E-leading indices.

These topics are still open:

- How a user creates user-defined partitions (probably with a `db.install/partition` transaction).
- How a user assigns entities to a specific partition (explicit partition directives on tempids, for example `db/force-partition` or `db/match-partition`).
- Whether we model partition entities as regular entities in `:db.part/db` with a `db/ident`.

For reference, see [Datomic Partitions](https://docs.datomic.com/transactions/partitions.html).
