# Mentat Internals: Partitions, Transactions, and Schema Validation

## Overview

Mentat maintains an in-memory model of partitions and schema that enables fast
entity ID allocation and schema validation without hitting SQLite on every
operation. This model is loaded from disk on startup, kept in sync during
transactions, and persisted back implicitly through the transaction log.

---

## 1. Partitions

### 1.1 What a Partition Is

A partition is a named, contiguous range of the entity ID space. Each partition
tracks its own allocation pointer (`next_entid_to_allocate`), making entity ID
allocation an O(1) bump-pointer operation with no database query required.

```rust
// db/src/types.rs
pub struct Partition {
    pub start: Entid,                         // first entid in range
    pub end: Entid,                           // maximum allowed entid
    pub allow_excision: bool,                 // can entities be excised?
    pub(crate) next_entid_to_allocate: Entid, // bump pointer
}
```

Invariant: `start <= next_entid_to_allocate <= end`. Allocating N entids
simply returns the range `[next..next+N)` and bumps the pointer forward.

```rust
pub fn allocate_entids(&mut self, n: usize) -> Range<i64> {
    let idx = self.next_entid();
    self.set_next_entid(idx + n as i64);
    idx..self.next_entid()
}
```

### 1.2 The Three Bootstrap Partitions

Defined in `db/src/bootstrap.rs`:

| Partition | Keyword | Range | Initial `next` | Excision | Purpose |
|-----------|---------|-------|-----------------|----------|---------|
| **db** | `:db.part/db` | `0` .. `65,535` | `41` (bootstrap idents count) | No | System schema entities |
| **user** | `:db.part/user` | `65,536` .. `268,435,455` | `65,536` | Yes | User-created entities |
| **tx** | `:db.part/tx` | `268,435,456` .. `i64::MAX` | `268,435,456` | No | Transaction entities |

Constants:
- `USER0 = 0x10000` (65,536)
- `TX0 = 0x10000000` (268,435,456)

### 1.3 PartitionMap

`PartitionMap` wraps a `BTreeMap<String, Partition>` and provides allocation
helpers:

```rust
// db/src/types.rs
pub struct PartitionMap(BTreeMap<String, Partition>);

// db/src/db.rs
impl PartitionMap {
    pub(crate) fn allocate_entid(&mut self, partition: &str) -> i64;
    pub(crate) fn allocate_entids(&mut self, partition: &str, n: usize) -> Range<i64>;
    pub(crate) fn contains_entid(&self, entid: Entid) -> bool;
}
```

### 1.4 How Partitions Are Persisted

**On-disk representation** (`db/src/db.rs`):

```sql
CREATE TABLE known_parts (
    part TEXT NOT NULL PRIMARY KEY,
    start INTEGER NOT NULL,
    end INTEGER NOT NULL,
    allow_excision SMALLINT NOT NULL
)
```

Note: `next_entid_to_allocate` is **not** stored as a column. It is
recomputed from the transaction log.

**The `parts` view** is created by `create_current_partition_view()`:

```sql
CREATE VIEW parts AS
    SELECT
        CASE
            WHEN e <= 65535 THEN ":db.part/db"
            WHEN e <= 268435455 THEN ":db.part/user"
            ...
        END AS part,
        min(e) AS start,
        max(e) + 1 AS idx     -- this becomes next_entid_to_allocate
    FROM timelined_transactions
    WHERE timeline = 0
    GROUP BY part
```

The view derives each partition's current allocation pointer (`idx`) by finding
the maximum entity ID that has been transacted in that partition.

### 1.5 Loading Partitions from Disk

`read_partition_map()` in `db/src/db.rs` reconstructs the full `PartitionMap`:

```sql
-- Part 1: partitions with transactions (get computed idx from parts view)
SELECT known_parts.part, known_parts.start, known_parts.end,
       parts.idx, known_parts.allow_excision
FROM parts
INNER JOIN known_parts ON parts.part = known_parts.part

UNION

-- Part 2: partitions with no transactions yet (use start as idx)
SELECT part, start, end, start, allow_excision
FROM known_parts
WHERE part NOT IN (SELECT part FROM parts)
```

This is run:
- On first connection open (`read_db()`)
- During timeline operations (rewinding/fast-forwarding)
- During sync

Each row is turned into a `Partition::new(start, end, idx, allow_excision)`.

### 1.6 In-Memory Lifecycle

The `PartitionMap` lives in two places:

1. **`DB` struct** (`db/src/types.rs`) - used during bootstrap and initial load:
   ```rust
   pub struct DB {
       pub partition_map: PartitionMap,
       pub schema: Schema,
   }
   ```

2. **`Metadata` struct** (`transaction/src/metadata.rs`) - the live runtime copy:
   ```rust
   pub struct Metadata {
       pub generation: u64,
       pub partition_map: PartitionMap,
       pub schema: Arc<Schema>,
       pub attribute_cache: SQLiteAttributeCache,
   }
   ```

During a transaction, the flow is:
1. `start_tx()` clones the `PartitionMap` and allocates a tx ID from `:db.part/tx`
2. Tempid resolution allocates user entity IDs from `:db.part/user` in batch
3. `conclude_tx()` returns the updated `PartitionMap` alongside the `TxReport`
4. The caller stores the updated `PartitionMap` back into `Metadata`

The partition state is never explicitly written back to `known_parts`. Instead,
the `parts` view automatically reflects the new allocation state because the
committed transaction rows in `timelined_transactions` include the newly
allocated entity IDs.

---

## 2. Transactions

### 2.1 Transaction Pipeline

The transactor (`db/src/tx.rs`) processes entities through four stages:

```
Entity (parsed EDN or builder terms)
    |
    v
Stage 1: entities_into_terms_with_temp_ids_and_lookup_refs()
    |  Converts Entity variants into Term structs.
    |  MapNotation is expanded into individual AddOrRetract terms.
    |  Produces: TermWithTempIdsAndLookupRefs
    v
Stage 2: resolve_lookup_refs()
    |  Resolves [a v] lookup refs to concrete entids via store.resolve_avs().
    |  Produces: TermWithTempIds
    v
Stage 3: Upsert resolution + entid allocation
    |  Multi-generational upsert evolution resolves tempids to existing entids
    |  for :db.unique/identity attributes.
    |  Remaining unresolved tempids get fresh entids from :db.part/user.
    |  Produces: terms with concrete entids only
    v
Stage 4: Insert into store
    |  Type checking, cardinality checking, SQL insertion.
    |  Schema metadata mutations detected and applied.
    |  Produces: TxReport
```

### 2.2 Transaction ID Allocation

Every transaction gets its own entity ID from `:db.part/tx`:

```rust
// db/src/tx.rs
fn start_tx(...) -> Result<Tx> {
    let tx_id = partition_map.allocate_entid(":db.part/tx");
    conn.begin_tx_application()?;
    Ok(Tx::new(conn, partition_map, ..., tx_id))
}
```

A `:db/txInstant` datom is automatically asserted for each transaction.

### 2.3 User Entity Allocation

After upsert resolution, all remaining unresolved tempids are allocated in a
single batch:

```rust
let unresolved_temp_ids: BTreeMap<TempIdHandle, usize> = generation.temp_ids_in_allocations(...)?;
let entids = self.partition_map.allocate_entids(":db.part/user", unresolved_temp_ids.len());

let temp_id_allocations = unresolved_temp_ids
    .into_iter()
    .map(|(tempid, index)| (tempid, KnownEntid(entids.start + (index as i64))))
    .collect();
```

This is O(1) regardless of batch size - a single bump of `next_entid_to_allocate`.

### 2.4 Transaction Result

```rust
// core/src/tx_report.rs
pub struct TxReport {
    pub tx_id: Entid,
    pub tx_instant: DateTime<Utc>,
    pub tempids: BTreeMap<String, Entid>,  // only external (user-named) tempids
}
```

### 2.5 Top-Level Transaction Function

```rust
// db/src/tx.rs
pub fn transact(conn, partition_map, schema_for_mutation, schema, watcher, entities)
    -> Result<(TxReport, PartitionMap, Option<Schema>, W)>
```

Returns the updated `PartitionMap` and optionally a new `Schema` if schema-
altering assertions were transacted. The caller is responsible for installing
these back into `Metadata`.

---

## 3. Schema Validation

### 3.1 The Schema Model

The in-memory `Schema` (`core/src/lib.rs`) holds three maps that enable O(1)
lookups in any direction:

```rust
pub struct Schema {
    pub entid_map: EntidMap,           // Entid -> Keyword  (e.g. 97 -> :person/name)
    pub ident_map: IdentMap,           // Keyword -> Entid  (e.g. :person/name -> 97)
    pub attribute_map: AttributeMap,   // Entid -> Attribute (e.g. 97 -> Attribute{...})
    pub component_attributes: Vec<Entid>,
}
```

Each attribute's properties are captured in the `Attribute` struct
(`core-traits/lib.rs`):

```rust
pub struct Attribute {
    pub value_type: ValueType,              // Ref, Boolean, Long, Double, String, Keyword, Uuid, Instant
    pub multival: bool,                     // cardinality one (false) vs many (true)
    pub unique: Option<attribute::Unique>,  // None, Value, or Identity
    pub index: bool,
    pub fulltext: bool,
    pub component: bool,
    pub no_history: bool,
}
```

### 3.2 Schema Loading from Disk

Schema is stored in two materialized-view tables:

```sql
CREATE TABLE idents (e INTEGER NOT NULL, a SMALLINT NOT NULL,
                     v BLOB NOT NULL, value_type_tag SMALLINT NOT NULL)

CREATE TABLE schema (e INTEGER NOT NULL, a SMALLINT NOT NULL,
                     v BLOB NOT NULL, value_type_tag SMALLINT NOT NULL)
```

On startup, `read_db()` reconstructs the full model:

```rust
// db/src/db.rs
pub(crate) fn read_db(conn: &rusqlite::Connection) -> Result<DB> {
    let partition_map = read_partition_map(conn)?;   // from known_parts + parts view
    let ident_map = read_ident_map(conn)?;           // from idents table
    let attribute_map = read_attribute_map(conn)?;    // from schema table
    let schema = Schema::from_ident_map_and_attribute_map(ident_map, attribute_map)?;
    Ok(DB::new(partition_map, schema))
}
```

During construction, all attributes are validated via `validate_attribute_map()`.

### 3.3 Attribute Validation Rules

The `AttributeValidation` trait (`db/src/schema.rs`) enforces these constraints:

| Rule | Constraint |
|------|-----------|
| Unique requires index | `:db/unique` (Value or Identity) requires `:db/index true` |
| Fulltext requires string | `:db/fulltext true` requires `:db/valueType :db.type/string` |
| Fulltext requires index | `:db/fulltext true` requires `:db/index true` |
| Component requires ref | `:db/isComponent true` requires `:db/valueType :db.type/ref` |

Validation runs:
- When a new attribute is installed (`validate_install_attribute()` + `validate()`)
- When an existing attribute is altered (`validate_alter_attribute()` + `validate()`)
- When the schema is loaded from disk (`from_ident_map_and_attribute_map()`)

### 3.4 Schema Alteration Rules

The `AttributeBuilder` (`db/src/schema.rs`) enforces alteration constraints:

**Immutable after creation** (enforced by `validate_alter_attribute()`):
- `:db/valueType` - cannot change
- `:db/fulltext` - cannot change

**Mutable** (tracked as `AttributeAlteration` in `db/src/metadata.rs`):
- `:db/index` -> `AttributeAlteration::Index`
- `:db/unique` -> `AttributeAlteration::Unique`
- `:db/cardinality` -> `AttributeAlteration::Cardinality`
- `:db/noHistory` -> `AttributeAlteration::NoHistory`
- `:db/isComponent` -> `AttributeAlteration::IsComponent`

**Retraction rules**:
- Only `:db/unique` and `:db/isComponent` can be retracted (with matching value)
- `:db/valueType`, `:db/cardinality`, `:db/index`, `:db/fulltext`, `:db/noHistory` cannot be retracted individually
- Full schema retraction requires retracting `:db/ident`, `:db/valueType`, and `:db/cardinality` together

### 3.5 Transaction-Time Validation

After tempid resolution and before SQL insertion, the transactor runs two
validation passes in `db/src/tx_checking.rs`:

**Type disagreements** - every value's type must match its attribute's `value_type`:

```rust
pub(crate) fn type_disagreements(aev_trie: &AEVTrie) -> TypeDisagreements {
    // For each (attribute, entity, values):
    //   if attribute.value_type != v.value_type() -> error
    // Returns ALL mismatches, not just the first.
}
```

**Cardinality conflicts** - for cardinality-one attributes:

```rust
pub(crate) fn cardinality_conflicts(aev_trie: &AEVTrie) -> Vec<CardinalityConflict> {
    // CardinalityOneAddConflict: 2+ distinct values for same (e, a) in one tx
    // AddRetractConflict: same value both added and retracted in one tx
}
```

Both functions collect **all** errors before returning, providing comprehensive
diagnostics rather than failing on the first error.

### 3.6 Schema Mutation During Transactions

When a transaction asserts or retracts schema attributes (`:db/valueType`,
`:db/cardinality`, etc.), the transactor:

1. Detects potential metadata mutations in stage 4
2. Calls `update_schema_from_entid_quadruples()` to process schema changes
3. Returns the new `Schema` as `Some(schema)` from `conclude_tx()`
4. The caller replaces the old `Arc<Schema>` in `Metadata` with the new one

For alterations that affect on-disk state (like adding an index), the transactor
also runs SQL updates:

```rust
// db/src/db.rs (update_metadata)
"UPDATE datoms SET index_avet = ? WHERE a = ?"    // when :db/index changes
"UPDATE datoms SET unique_value = ? WHERE a = ?"   // when :db/unique changes
```

---

## 4. The Fast-Path Model

The design enables fast transacting by keeping hot data in memory:

1. **Entity ID allocation** is a pointer bump on the in-memory `PartitionMap`.
   No SQL query needed.

2. **Schema validation** (type checks, cardinality checks) uses the in-memory
   `Schema.attribute_map` for O(1) attribute lookups by entid.

3. **Ident resolution** (keyword -> entid, e.g. `:person/name` -> `97`) uses
   the in-memory `Schema.ident_map` BTreeMap.

4. **Upsert resolution** for `:db.unique/identity` attributes requires a
   database query (`resolve_avs`), but this is batched across all tempids in
   one SQL call.

5. **Schema changes** are rare. The common path (`Cow::Borrowed`) avoids
   cloning the `Schema` at all. Only when schema mutations are detected does
   a `Cow::Owned` copy get created.

The `Schema` is wrapped in `Arc` in `Metadata`, so query threads can share
it without cloning. Only the transacting thread ever produces a new `Schema`.

---

## 5. Key Files

| File | Role |
|------|------|
| `db/src/types.rs` | `Partition`, `PartitionMap`, `DB` structs |
| `db/src/bootstrap.rs` | Bootstrap partitions (`V1_PARTS`), idents, and schema |
| `db/src/db.rs` | SQL schema creation, `read_partition_map()`, `read_db()`, `update_metadata()` |
| `db/src/tx.rs` | 4-stage transaction pipeline, `start_tx()`, `transact()` |
| `db/src/tx_checking.rs` | `type_disagreements()`, `cardinality_conflicts()` |
| `db/src/schema.rs` | `AttributeValidation`, `AttributeBuilder`, `SchemaBuilding` |
| `db/src/metadata.rs` | `AttributeAlteration`, `MetadataReport`, `update_attribute_map_from_entid_triples()` |
| `core-traits/lib.rs` | `Attribute`, `ValueType`, `TypedValue`, `AttributeBitFlags` |
| `core/src/lib.rs` | `Schema` struct, `HasSchema` trait |
| `transaction/src/metadata.rs` | `Metadata` struct (runtime connection state) |
| `db-traits/errors.rs` | `SchemaConstraintViolation`, `CardinalityConflict` |
