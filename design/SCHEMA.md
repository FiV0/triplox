# Triplox Schema Specification

Version 0.1

## Overview

Every user-defined attribute in Triplox must be declared before use. Schema defines the set of known attributes, their value types, and their cardinalities. The schema is enforced at transaction time — data that references an unknown attribute or provides a value of the wrong type is rejected.

Schema is stored as regular entity-attribute-value triples in the same indices as user data. The `SchemaCache` is an in-memory structure that mirrors this information for fast validation and attribute resolution.

### Guiding principles

Schema is not tracked across history. This follows Datomic's example. It is encouraged to deprecate attributes instead of renaming
them or changing their constraints. We call this deprecation guided schema evolution. Overall the schema for queries (no matter the T we query at) will use the schema at the head of the db. We roughly envision three stages of schema support:

1. In initial version supports additive schema only. Reasserting a schema attribute just fails.
2. In a second version schema can be changed, but the effects are only for incoming transactions. The existing indices are not checked for the newly defined schema.
3. Full schema migration support. One can change the schema constraints, but one has do the work so that the existing data fits the new schema. Schema is still always taken at head and resolved for head, but the schema can have a history.

---

## 1. Schema Attributes

A **schema attribute** defines a named attribute that can appear on data entities. It is itself an entity with three required properties:

| Property    | Key                | Value Type | Description                                        |
|-------------|--------------------|------------|----------------------------------------------------|
| Ident       | `db/ident`         | Keyword    | The attribute's name, e.g. `:person/name`          |
| Value type  | `db/valueType`     | Ref        | Entity reference, e.g. `11` for `:db.type/string`  |
| Cardinality | `db/cardinality`   | Ref        | Entity reference, `30` (one) or `31` (many)        |

An entity becomes a schema attribute when all three properties (`db/ident`, `db/valueType`, `db/cardinality`) are present.
Initially the 3 attributes need to be asserted in the same transaction. In later stages we might want to allow for more
granular schema evolvement. This will require more work in the indexer so likely needs more thought.

### 1.1 Supported Value Types

| Keyword              | Description                                                       |
|----------------------|-------------------------------------------------------------------|
| `:db.type/keyword`   | EDN keyword                                                       |
| `:db.type/string`    | UTF-8 string                                                      |
| `:db.type/long`      | 64-bit signed integer                                             |
| `:db.type/ref`       | Entity reference (same encoding as Long; schema-only distinction) |
| `:db.type/boolean`   | true / false                                                      |
| `:db.type/double`    | 64-bit IEEE 754 float                                             |
| `:db.type/float`     | 32-bit IEEE 754 float                                             |
| `:db.type/instant`   | Timestamp (microseconds since epoch)                              |
| `:db.type/uuid`      | 128-bit UUID                                                      |
| `:db.type/bytes`     | Arbitrary binary data                                             |
| `:db.type/bigint`    | Arbitrary precision integer                                       |
| `:db.type/tuple`     | Ordered heterogeneous collection                                  |
| `:db.type/vector`    | Ordered homogeneous collection                                    |
| `:db.type/map`       | String-keyed map                                                  |

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
| 2         | `db/valueType`     | ref        | one         |
| 3         | `db/cardinality`   | ref        | one         |
| 32        | `db.tx/instant`    | instant    | one         |
| 33        | `db.tx/result`     | keyword    | one         |
| 34        | `db.tx/id`         | long       | one         |
| 35        | `db.tx/error`      | string     | one         |

The first three attributes are self-referential: they describe themselves. They are the minimal set needed to define any further schema attributes. Attributes 32–35 are used on transaction entities (see `PARTITIONS.md`).

The bootstrap transaction also installs enum entities for value types (IDs 10–23) and cardinalities (IDs 30–31). These are regular entities with `db/ident` but no `db/valueType` — they are **not** schema attributes.

Reserved entity ID ranges:

| Range | Purpose                      |
|-------|------------------------------|
| 1–3   | Core schema attributes (db/) |
| 32–35 | Transaction schema attributes (db.tx/) |
| 10–23 | Value type enum entities     |
| 30–31 | Cardinality enum entities    |
| 40–41 | Transaction result enum entities |

### 2.1 Transaction Result Enums

The bootstrap transaction installs two enum entities representing transaction outcomes. These are used as values for the `db.tx/result` attribute on transaction entities (see `PARTITIONS.md`):

| Entity ID | Ident               |
|-----------|---------------------|
| 40        | `:db.tx/committed`  |
| 41        | `:db.tx/aborted`    |

### 2.2 Enums and References

`ValueType::Ref` is a supported schema type. Ref values are stored as `DataType::Long` with the same byte-level encoding (same tag). The schema is the sole authority on whether a Long value is an entity reference — this allows ref values and entity IDs to unify at the byte level in the generic join algorithm without special-casing.

`db/valueType` and `db/cardinality` are ref-typed attributes whose values are entity IDs pointing to the enum entities defined in the bootstrap transaction (e.g. `11` for `:db.type/string`, `30` for `:db.cardinality/one`). `db.tx/result` still uses keyword values; it could be migrated to ref in the future.

---

## 3. SchemaCache

`SchemaCache` is the in-memory representation of all known schema attributes. It is owned by the `Indexer` and used for three purposes:

1. **Transaction validation** - reject unknown attributes and type mismatches
2. **Attribute resolution** - map attribute names to entity IDs for index key construction
3. **Cardinality enforcement** - enforcing the cardinality constraints of the attributes in the indexer.


### 3.1 Lifecycle

1. **Fresh database**: `SchemaCache` is populated by processing the bootstrap transaction through `process_tx()`.
2. **Existing database**: `SchemaCache` is rebuilt from stored data by running a query against the indices.
3. **During operation**: Each transaction may define new schema attributes. After the triples have been written to the indices, the `SchemaCache` updates if the new data contains schema attributes.

### 3.2 Validation Flow

Newly defined attributes become available in the next transaction. Inside a transaction only the current schema is available and the data is checked against that schema.

### 3.3 Cardinality Enforcement

When writing to the indices, the `SchemaCache` is consulted to determine how a `Put` or `Add` interacts with existing data for the same entity+attribute pair:

- **`:db.cardinality/one`** — An entity may hold at most one value for this attribute. When a `Put` or `Add` asserts a new value, the indexer must first look up the existing value (if any) and emit retraction index keys for it before writing the new assertion.

- **`:db.cardinality/many`** — An entity may hold multiple values for this attribute. A `Put` or `Add` simply writes the new assertion without retracting anything. Multiple values coexist.


## 4. Schema Immutability

Schema attributes are **immutable** once installed. The following operations are rejected:

| Operation  | Condition                                                     | Error                                |
|------------|---------------------------------------------------------------|--------------------------------------|
| `Put`      | `db/id` matches an existing schema entity                     | "Cannot redefine schema attribute"   |
| `Retract`  | attribute is `db/ident`, `db/valueType`, or `db/cardinality`  | "Cannot retract schema attributes"   |
| `Delete`   | entity ID belongs to a schema entity                          | "Cannot delete schema entity"        |
| `Erase`    | entity ID belongs to a schema entity                          | "Cannot erase schema entity"         |

In its final form we likely reject any modifications to bootstrap schema attributes, but allow changes to user defined attributes.
We still strive for a deprecation guided schema evolution.

---

## 5. Schema Definition

### 5.1 Version 1
- A schema attribute needs to be fully defined within the same transaction. If `db/ident`, `db/valueType` or
  `db/cardinality` is missing, the transaction is rejected.
- These attributes can be defined with `Put` or `Add` statements (or combinations thereof) as long as the
  final set of required attributes is met.
- Updating or deleting a schema attribute is rejected.
- Attribute id resolution always works against the head of the db (basis-t in Datomic slang).
