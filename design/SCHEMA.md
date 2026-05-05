# Triplox Schema Specification

Version 0.1

## Overview

Every user-defined attribute in Triplox must be declared before use. Schema defines the set of known attributes, their value types, cardinalities, and optional uniqueness constraints. The schema is enforced at transaction time — data that references an unknown attribute, provides a value of the wrong type, or violates a uniqueness constraint is rejected.

Schema is stored as regular entity-attribute-value triples in the same indices as user data. The `SchemaCache` is an in-memory structure that mirrors this information for fast validation and attribute resolution.

### Guiding principles

Schema is not tracked across history. This follows Datomic's example. It is encouraged to deprecate attributes instead of renaming
them or changing their constraints. We call this deprecation guided schema evolution. Overall the schema for queries (no matter the T we query at) will use the schema at the head of the db. We roughly envision three stages of schema support:

1. In initial version supports additive schema only. Reasserting a schema attribute just fails.
2. In a second version schema can be changed, but the effects are only for incoming transactions. The existing indices are not checked for the newly defined schema.
3. Full schema migration support. One can change the schema constraints, but one has do the work so that the existing data fits the new schema. Schema is still always taken at head and resolved for head, but the schema can have a history.

---

## 1. Schema Attributes

A **schema attribute** defines a named attribute that can appear on data entities. It is itself an entity with three required properties and one optional uniqueness property:

| Property    | Key                | Value Type | Required | Description                                                        |
|-------------|--------------------|------------|----------|--------------------------------------------------------------------|
| Ident       | `db/ident`         | Keyword    | yes      | The attribute's name, e.g. `:person/name`                          |
| Value type  | `db/valueType`     | Ref        | yes      | Entity reference to a value type enum, e.g. `:db.type/string`      |
| Cardinality | `db/cardinality`   | Ref        | yes      | Entity reference to `:db.cardinality/one` or `:db.cardinality/many` |
| Unique      | `db/unique`        | Ref        | no       | Entity reference to `:db.unique/value` or `:db.unique/identity`     |

An entity becomes a schema attribute when all three properties (`db/ident`, `db/valueType`, `db/cardinality`) are present.
If a transaction asserts any of `db/valueType`, `db/cardinality`, or `db/unique` on an entity, it must also assert
`db/valueType` and `db/cardinality` in the same transaction — partial schema-attribute definitions are rejected.
Entities that only assert `db/ident` (e.g. enum entities like `:db.type/string` or user-defined idents) are accepted
without further validation. In later stages we might want to allow for more granular schema evolvement. This will
require more work in the indexer so likely needs more thought.

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
| `:db.type/vector`    | Ordered homogeneous collection                                    |
| `:db.type/map`       | String-keyed map                                                  |

### 1.2 Cardinality

| Keyword                | Meaning                                               |
|------------------------|-------------------------------------------------------|
| `:db.cardinality/one`  | An entity has at most one value for this attribute     |
| `:db.cardinality/many` | An entity may have multiple values for this attribute  |

### 1.3 Uniqueness

Unique attributes are indexed in VAE and checked during transaction processing.

| Keyword                | Meaning                                                                 |
|------------------------|-------------------------------------------------------------------------|
| `:db.unique/value`     | At most one entity may assert a given value. No lookup refs or upserts. |
| `:db.unique/identity`  | Same uniqueness constraint, plus lookup refs and identity upsert.       |

Only `:db.unique/identity` attributes participate in tempid upsert resolution. `:db.unique/value` is a constraint only: transactions may assert it, but they cannot use it to resolve lookup refs or adopt an existing entity for a tempid.

---

## 2. Bootstrap Schema

A fresh database is initialized with a bootstrap transaction (tx_id=0) that installs the four meta-attributes that describe schema itself, plus transaction metadata attributes:

| Ident              | Value Type | Cardinality | Unique   |
|--------------------|------------|-------------|----------|
| `db/ident`         | keyword    | one         | identity |
| `db/valueType`     | ref        | one         |          |
| `db/cardinality`   | ref        | one         |          |
| `db/unique`        | ref        | one         |          |
| `db.tx/instant`    | instant    | one         |          |
| `db.tx/result`     | ref        | one         |          |
| `db.tx/id`         | long       | one         |          |
| `db.tx/error`      | string     | one         |          |

The first four attributes are self-referential: they describe themselves. `db/ident`, `db/valueType`, and `db/cardinality` are the minimal required set needed to define any further schema attributes; `db/unique` adds optional uniqueness semantics. Attributes 32–35 are used on transaction entities (see `PARTITIONS.md`).

The bootstrap transaction also installs enum entities for value types (`:db.type/*`), cardinalities (`:db.cardinality/*`), and uniqueness (`:db.unique/*`). These are regular entities with `db/ident` but no `db/valueType` — they are **not** schema attributes.

### 2.1 Transaction Result Enums

The bootstrap transaction installs two enum entities representing transaction outcomes. These are used as values for the `db.tx/result` attribute on transaction entities (see `PARTITIONS.md`):

| Ident               |
|---------------------|
| `:db.tx/committed`  |
| `:db.tx/aborted`    |

### 2.2 Enums and References

`ValueType::Ref` is a supported schema type. Ref values are stored as `DataType::Long` with the same byte-level encoding (same tag). The schema is the sole authority on whether a Long value is an entity reference — this allows ref values and entity IDs to unify at the byte level in the generic join algorithm without special-casing.

`db/valueType`, `db/cardinality`, `db/unique`, and `db.tx/result` are ref-typed attributes whose values are entity IDs pointing to the enum entities defined in the bootstrap transaction (e.g. the entity for `:db.type/string`, `:db.cardinality/one`, `:db.unique/identity`, or `:db.tx/committed`).

---

## 3. SchemaCache

`SchemaCache` is the in-memory representation of all known schema attributes. It is owned by the `Indexer` and used for four purposes:

1. **Transaction validation** - reject unknown attributes and type mismatches
2. **Attribute resolution** - map attribute names to entity IDs for index key construction
3. **Cardinality enforcement** - enforcing the cardinality constraints of the attributes in the indexer.
4. **Uniqueness behavior** - identify attributes that require VAE writes, uniqueness checks, lookup refs, or identity upsert.


### 3.1 Lifecycle

1. **Fresh database**: `SchemaCache` is populated by processing the bootstrap transaction through `process_tx()`.
2. **Existing database**: `SchemaCache` is rebuilt from stored data by running a query against the indices.
3. **During operation**: Each transaction may define new schema attributes. After the triples have been written to the indices, the `SchemaCache` updates if the new data contains schema attributes.

### 3.2 Validation Flow

Newly defined attributes become available in the next transaction. Inside a transaction only the current schema is available and the data is checked against that schema. This includes uniqueness metadata: a newly defined unique attribute can be used as data only by a later transaction.

### 3.3 Cardinality Enforcement

When writing to the indices, the `SchemaCache` is consulted to determine how a `Put` or `Add` interacts with existing data for the same entity+attribute pair:

- **`:db.cardinality/one`** — An entity may hold at most one value for this attribute. When a `Put` or `Add` asserts a new value, the indexer must first look up the existing value (if any) and emit retraction index keys for it before writing the new assertion.

- **`:db.cardinality/many`** — An entity may hold multiple values for this attribute. A `Put` or `Add` simply writes the new assertion without retracting anything. Multiple values coexist.

### 3.4 Unique Enforcement

Unique attributes are the only attributes written to the VAE index. The VAE key order is value, attribute, entity, transaction entity, operation. It supports uniqueness checks and identity lookup without making all non-unique values globally searchable by value.

Before commit, the transaction pipeline checks each asserted unique `[attribute value]` pair. Two different entities cannot assert the same pair unless the old owner is retracted in the same transaction. For `:db.unique/identity`, tempid resolution first attempts to find an existing owner and adopts that entity ID. For `:db.unique/value`, no adoption happens; a collision is reported as a uniqueness violation.

Lookup refs are accepted only for `:db.unique/identity` attributes. Lookup refs on `:db.unique/value` or non-unique attributes are rejected.

## 4. Schema Immutability

Schema attributes are **immutable** once installed. The following operations are rejected:

| Operation  | Condition                                                     | Error                                |
|------------|---------------------------------------------------------------|--------------------------------------|
| `Put`      | `db/id` matches an existing schema entity                     | "Cannot redefine schema attribute"   |
| `Retract`  | attribute is `db/ident`, `db/valueType`, `db/cardinality`, or `db/unique` | "Cannot retract schema attributes"   |
| `Delete`   | entity ID belongs to a schema entity                          | "Cannot delete schema entity"        |
| `Erase`    | entity ID belongs to a schema entity                          | "Cannot erase schema entity"         |

In its final form we likely reject any modifications to bootstrap schema attributes, but allow changes to user defined attributes.
We still strive for a deprecation guided schema evolution.

---

## 5. Schema Definition

### 5.1 Version 1
- A schema attribute needs to be fully defined within the same transaction. The trigger is asserting any of
  `db/valueType`, `db/cardinality`, or `db/unique` on an entity: the transaction must then also assert
  `db/valueType` and `db/cardinality` on that entity, otherwise it is rejected.
- Entities asserting only `db/ident` (e.g. enum entities or user-defined idents) are accepted and are not
  treated as schema attributes.
- `db/unique` is optional. If present, it must be either `:db.unique/value` or `:db.unique/identity`.
- These attributes can be defined with `Put` or `Add` statements (or combinations thereof) as long as the
  final set of required attributes is met.
- Updating or deleting a schema attribute is rejected.
- Attribute id resolution always works against the head of the db (basis-t in Datomic slang).
