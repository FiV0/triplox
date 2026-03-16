# Triplox Schema Specification

Version 0.1

## Overview

Every user-defined attribute in Triplox must be declared before use. Schema defines the set of known attributes, their value types, and their cardinalities. The schema is enforced at transaction time — data that references an unknown attribute or provides a value of the wrong type is rejected.

Schema is stored as regular entity-attribute-value triples in the same indices as user data. The `SchemaCache` is an in-memory structure that mirrors this information for fast validation and attribute resolution.

---

## 1. Schema Attributes

A **schema attribute** defines a named attribute that can appear on data entities. It is itself an entity with three required properties:

| Property    | Key                | Value Type       | Description                                  |
|-------------|--------------------|------------------|----------------------------------------------|
| Ident       | `db/ident`         | Keyword          | The attribute's name, e.g. `:person/name`    |
| Value type  | `db/valueType`     | Keyword          | The type of values, e.g. `:db.type/string`   |
| Cardinality | `db/cardinality`   | Long (entity ref) | `30` (one) or `31` (many)                   |

Plus `db/id` (Long) — the entity's unique identifier.

An entity becomes a schema attribute when all three properties (`db/ident`, `db/valueType`, `db/cardinality`) are asserted for it **within the same transaction**. This can happen via:

- A single `Put` document containing all three keys
- Multiple `Add` triples targeting the same entity ID
- A mix of `Put` and `Add` operations

The operation type does not matter — what matters is that by the end of the transaction, the entity has all three properties. If an entity has `db/ident` and `db/valueType` but is missing `db/cardinality`, the transaction is **rejected**.

### 1.1 Supported Value Types

| Keyword              | Description                          |
|----------------------|--------------------------------------|
| `:db.type/keyword`   | EDN keyword                          |
| `:db.type/string`    | UTF-8 string                         |
| `:db.type/long`      | 64-bit signed integer                |
| `:db.type/ref`       | Entity reference (stored as Long)    |
| `:db.type/boolean`   | true / false                         |
| `:db.type/double`    | 64-bit IEEE 754 float                |
| `:db.type/float`     | 32-bit IEEE 754 float                |
| `:db.type/instant`   | Timestamp (microseconds since epoch) |
| `:db.type/uuid`      | 128-bit UUID                         |
| `:db.type/bytes`     | Arbitrary binary data                |
| `:db.type/bigint`    | Arbitrary precision integer          |
| `:db.type/tuple`     | Ordered heterogeneous collection     |
| `:db.type/vector`    | Ordered homogeneous collection       |
| `:db.type/map`       | String-keyed map                     |

### 1.2 Cardinality

| Entity ID | Keyword                | Meaning                                               |
|-----------|------------------------|-------------------------------------------------------|
| 30        | `:db.cardinality/one`  | An entity has at most one value for this attribute     |
| 31        | `:db.cardinality/many` | An entity may have multiple values for this attribute  |

---

## 2. Bootstrap Schema

A fresh database is initialized with a bootstrap transaction (tx_id=0) that installs the three meta-attributes that describe schema itself:

| Entity ID | Ident              | Value Type | Cardinality |
|-----------|--------------------|------------|-------------|
| 1         | `db/ident`         | keyword    | one         |
| 2         | `db/valueType`     | keyword    | one         |
| 3         | `db/cardinality`   | long       | one         |

These three attributes are self-referential: they describe themselves. They are the minimal set needed to define any further schema attributes.

The bootstrap transaction also installs enum entities for value types (IDs 10–23) and cardinalities (IDs 30–31). These are regular entities with `db/ident` but no `db/valueType` — they are **not** schema attributes.

Reserved entity ID ranges:

| Range | Purpose                      |
|-------|------------------------------|
| 1–3   | Bootstrap schema attributes  |
| 10–23 | Value type enum entities     |
| 30–31 | Cardinality enum entities    |

---

## 3. SchemaCache

`SchemaCache` is the in-memory representation of all known schema attributes. It is owned by the `Indexer` and used for two purposes:

1. **Transaction validation** — reject unknown attributes and type mismatches
2. **Attribute resolution** — map attribute names to entity IDs for index key construction

### 3.1 Data Structure

```
SchemaCache {
    by_ident: HashMap<String, SchemaAttribute>
}

SchemaAttribute {
    ident:       String        // e.g. "person/name"
    value_type:  ValueType     // e.g. String
    cardinality: Cardinality   // e.g. One
    entity_id:   i64           // e.g. 100
}
```

### 3.2 Attribute Map

`SchemaCache::attribute_map()` returns `HashMap<String, i64>` — a projection of ident to entity_id. This is the **only** interface the query engine uses. The query engine does not use value type or cardinality information; type checking is a transaction-time concern.

### 3.3 Lifecycle

1. **Fresh database**: `SchemaCache` is populated by processing the bootstrap transaction through `process_tx()`.
2. **Existing database**: `SchemaCache` is rebuilt from stored data by `load_schema_from_indices()`, which runs a Datalog query joining on `db/ident`, `db/valueType`, and `db/cardinality`.
3. **During operation**: Each transaction may define new schema attributes. After a transaction is written to the indices, `process_tx()` aggregates all ops by entity ID, detects entities that have `db/ident` + `db/valueType`, validates completeness (requires `db/cardinality`), and adds new schema attributes to the cache.

---

## 4. Transaction Rules

### 4.1 Validation Flow

In `Indexer::transact_tx()`:

```
1. validate_tx(ops)    — check all ops against current SchemaCache
2. write to indices    — persist the data
3. process_tx(ops)     — update SchemaCache with new attributes
```

This ordering means newly defined attributes are **not available within the same transaction** that defines them. A schema-defining document validates against the bootstrap schema (its keys are `db/ident`, `db/valueType`, `db/cardinality` — all known bootstrap attributes). The new attribute becomes available starting with the next transaction.

### 4.2 Data Validation

For `Put`, `Add`, and `Retract` operations on data entities:

- Every attribute key (except `db/id`) **must** exist in the schema cache
- Every value **must** match the attribute's declared `ValueType`
- `db/id` must be a Long

`Delete` and `Erase` operations are not validated against schema (they only reference entity IDs).

### 4.3 Cardinality Enforcement

When writing to the indices, the `SchemaCache` is consulted to determine how a `Put` or `Add` interacts with existing data for the same entity+attribute pair:

- **`:db.cardinality/one`** — An entity may hold at most one value for this attribute. When a `Put` or `Add` asserts a new value, the indexer must first look up the existing value (if any) and emit retraction index keys for it before writing the new assertion. This ensures the old value is no longer visible in queries.

- **`:db.cardinality/many`** — An entity may hold multiple values for this attribute. A `Put` or `Add` simply writes the new assertion without retracting anything. Multiple values coexist.

> **Note:** Cardinality enforcement is not yet implemented. Currently all writes behave as cardinality-many (no automatic retraction).

### 4.4 Schema Definition

Schema detection operates at the **transaction level**, not the individual operation level. After collecting all ops in a transaction, `process_tx()` aggregates the attributes asserted for each entity. An entity is a schema definition if it has both `db/ident` and `db/valueType` (from any combination of `Put` and `Add` ops).

The transaction is **rejected** if a schema-defining entity:

- Is missing `db/cardinality`
- Has a `db/ident` that is not a Keyword
- Has a `db/valueType` that is not a recognized value type keyword
- Has a `db/cardinality` that is not a valid cardinality entity ID (30 or 31)
- Is missing `db/id` or `db/id` is not a Long

### 4.5 Schema Immutability

Schema attributes are **immutable** once installed. The following operations are rejected:

| Operation  | Condition                                                     | Error                                |
|------------|---------------------------------------------------------------|--------------------------------------|
| `Put`      | `db/id` matches an existing schema entity                     | "Cannot redefine schema attribute"   |
| `Retract`  | attribute is `db/ident`, `db/valueType`, or `db/cardinality`  | "Cannot retract schema attributes"   |
| `Delete`   | entity ID belongs to a schema entity                          | "Cannot delete schema entity"        |
| `Erase`    | entity ID belongs to a schema entity                          | "Cannot erase schema entity"         |

---

## 5. Examples

Define a schema attribute via `Put`:

```edn
[{:db/id 100
  :db/ident :person/name
  :db/valueType :db.type/string
  :db/cardinality 30}]
```

Equivalently, via multiple `Add` triples in the same transaction:

```edn
[[:db/add 100 :db/ident :person/name]
 [:db/add 100 :db/valueType :db.type/string]
 [:db/add 100 :db/cardinality 30]]
```

Use it in a subsequent transaction:

```edn
[{:db/id 200
  :person/name "Alice"}]
```

Rejected — unknown attribute:

```edn
[{:db/id 200
  :person/email "alice@example.com"}]
;; Error: Unknown attribute: person/email
```

Rejected — type mismatch:

```edn
[{:db/id 200
  :person/name 42}]
;; Error: Type mismatch for attribute person/name: expected string, got Long
```

Rejected — missing cardinality:

```edn
[{:db/id 101
  :db/ident :person/age
  :db/valueType :db.type/long}]
;; Error: Schema attribute must have db/cardinality
```
