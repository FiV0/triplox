# Triplox Schema Specification

Version 0.1

## Overview

You must declare each user-defined attribute in Triplox before you use it. The schema is the set of known attributes, with the value type, the cardinality, and an optional uniqueness constraint of each attribute. The cardinality of an attribute is how many values one entity can have. Triplox enforces the schema at transaction time. If data references an unknown attribute, gives a value of the wrong type, or violates a uniqueness constraint, Triplox rejects it.

Triplox stores the schema as regular entity-attribute-value triples in the same indices as user data. The `SchemaCache` is an in-memory structure that holds a copy of this information. It makes validation and attribute resolution fast. Attribute resolution maps an attribute name to its entity ID.

### Guiding principles

Triplox does not keep a history of the schema. This follows the example of Datomic. We encourage you to deprecate an attribute instead of renaming it or changing its constraints. We call this deprecation-guided schema evolution. A query always uses the schema at the head of the database (its latest state), no matter at which T (point in time) the query reads. We roughly plan three stages of schema support:

1. The first version supports additive schema only. If you assert an existing schema attribute again, the assertion fails.
2. In the second version, you can change the schema, but the change applies only to incoming transactions. Triplox does not make sure that the existing indices agree with the newly defined schema.
3. The third version has full support for schema migration. You can change the schema constraints, but it is then necessary to do the work that makes the existing data fit the new schema. Triplox still always takes the schema at the head and resolves it for the head, but the schema can have a history.

---

## 1. Schema Attributes

A schema attribute defines a named attribute for data entities. A schema attribute is an entity itself. It has three required properties and one optional uniqueness property:

| Property    | Key                | Value Type | Required | Description                                                                                   |
|-------------|--------------------|------------|----------|-----------------------------------------------------------------------------------------------|
| Ident       | `db/ident`         | Keyword    | yes      | The name of the attribute as a keyword, for example `:person/name`                            |
| Value type  | `db/valueType`     | Ref        | yes      | Entity reference to an enum entity (a named constant entity) for the value type, for example `:db.type/string` |
| Cardinality | `db/cardinality`   | Ref        | yes      | Entity reference to `:db.cardinality/one` or `:db.cardinality/many`                           |
| Unique      | `db/unique`        | Ref        | no       | Entity reference to `:db.unique/value` or `:db.unique/identity`                               |

If an entity has all three properties (`db/ident`, `db/valueType`, `db/cardinality`), it becomes a schema attribute. If a transaction asserts any of `db/valueType`, `db/cardinality`, or `db/unique` on an entity, it must also assert `db/valueType` and `db/cardinality` in the same transaction. Triplox rejects a partial definition of a schema attribute.

An ident is the keyword name of an entity. Triplox accepts an entity that asserts only `db/ident` without more validation. Examples are enum entities such as `:db.type/string` and user-defined idents. In later stages, we possibly want to allow a more granular schema evolution. This requires more work in the indexer, so it likely needs more thought.

### 1.1 Supported Value Types

| Keyword              | Description                                                         |
|----------------------|---------------------------------------------------------------------|
| `:db.type/keyword`   | EDN keyword                                                         |
| `:db.type/string`    | UTF-8 string                                                        |
| `:db.type/long`      | 64-bit signed integer                                               |
| `:db.type/ref`       | Entity reference (same encoding as Long, only the schema tells them apart) |
| `:db.type/boolean`   | true / false                                                        |
| `:db.type/double`    | 64-bit IEEE 754 float                                               |
| `:db.type/float`     | 32-bit IEEE 754 float                                               |
| `:db.type/instant`   | Timestamp (microseconds since epoch)                                |
| `:db.type/uuid`      | 128-bit UUID                                                        |
| `:db.type/bytes`     | Arbitrary binary data                                               |
| `:db.type/bigint`    | Arbitrary precision integer                                         |
| `:db.type/vector`    | Ordered collection with all items of one type                       |
| `:db.type/map`       | String-keyed map                                                    |

### 1.2 Cardinality

| Keyword                | Meaning                                                |
|------------------------|--------------------------------------------------------|
| `:db.cardinality/one`  | An entity has at most one value for this attribute     |
| `:db.cardinality/many` | An entity can have multiple values for this attribute  |

### 1.3 Uniqueness

Triplox writes unique attributes to the VAE index (the index sorted by value first). During transaction processing, Triplox makes sure that the values of unique attributes stay unique. A tempid is a temporary ID for a new entity in a transaction. An upsert resolves a tempid to an existing entity. A lookup ref finds an entity by a unique attribute value.

| Keyword                | Meaning                                                                 |
|------------------------|-------------------------------------------------------------------------|
| `:db.unique/value`     | At most one entity can assert a given value. No lookup refs or upserts. |
| `:db.unique/identity`  | Same uniqueness constraint, plus lookup refs and identity upsert.       |

Only `:db.unique/identity` attributes take part in upsert resolution for tempids. `:db.unique/value` is only a constraint. Transactions can assert it, but they cannot use it to resolve lookup refs or to adopt an existing entity for a tempid.

---

## 2. Bootstrap Schema

Triplox initializes a fresh database with a bootstrap transaction (tx_id=0). The bootstrap transaction installs the four meta-attributes that describe the schema itself. It also installs the attributes for transaction metadata:

| Ident              | Value Type | Cardinality | Unique   |
|--------------------|------------|-------------|----------|
| `db/ident`         | keyword    | one         | identity |
| `db/valueType`     | ref        | one         |          |
| `db/cardinality`   | ref        | one         |          |
| `db/unique`        | ref        | one         |          |
| `db/txInstant`     | instant    | one         |          |
| `db/txResult`      | ref        | one         |          |
| `db/txId`          | long       | one         |          |
| `db/txError`       | string     | one         |          |

The first four attributes are self-referential: they describe themselves. `db/ident`, `db/valueType`, and `db/cardinality` are the minimum set that is necessary to define more schema attributes. `db/unique` adds optional uniqueness semantics. Transaction entities (entities that describe one transaction each) use attributes 32–35 (see `PARTITIONS.md`).

The bootstrap transaction also installs enum entities for value types (`:db.type/*`), cardinalities (`:db.cardinality/*`), and uniqueness (`:db.unique/*`). These are regular entities with `db/ident` but without `db/valueType`. They are not schema attributes.

### 2.1 Transaction Result Enums

The bootstrap transaction installs two enum entities for the outcome of a transaction. Transaction entities use them as values of the `db/txResult` attribute (see `PARTITIONS.md`):

| Ident               |
|---------------------|
| `:db.tx/committed`  |
| `:db.tx/aborted`    |

### 2.2 Enums and References

`ValueType::Ref` is a supported schema type. Triplox stores ref values as `DataType::Long`, with the same byte-level encoding (the same tag). Only the schema tells whether a Long value is an entity reference. Thus ref values and entity IDs unify at the byte level in the generic join algorithm, without special cases.

`db/valueType`, `db/cardinality`, `db/unique`, and `db/txResult` are attributes of type ref. Their values are entity IDs of the enum entities from the bootstrap transaction. Examples are the entities for `:db.type/string`, `:db.cardinality/one`, `:db.unique/identity`, and `:db.tx/committed`.

---

## 3. SchemaCache

`SchemaCache` is the in-memory representation of all known schema attributes. The `Indexer` owns it and uses it for four purposes:

1. Transaction validation, which rejects unknown attributes and type mismatches.
2. Attribute resolution, which maps attribute names to entity IDs to build index keys.
3. Cardinality enforcement, which enforces the cardinality constraints of the attributes in the indexer.
4. Uniqueness behavior, which identifies the attributes that need VAE writes, uniqueness checks, lookup refs, or identity upsert.


### 3.1 Lifecycle

1. For a fresh database, Triplox fills the `SchemaCache` when `process_tx()` processes the bootstrap transaction.
2. For an existing database, Triplox rebuilds the `SchemaCache` from the stored data with a query against the indices.
3. During operation, each transaction can define new schema attributes. If the new data contains schema attributes, Triplox updates the `SchemaCache` after it writes the triples to the indices.

### 3.2 Validation Flow

A newly defined attribute becomes available in the next transaction. Inside a transaction, only the current schema is available, and Triplox validates the data against that schema. This rule also applies to uniqueness metadata. Only a later transaction can use a newly defined unique attribute as data.

### 3.3 Cardinality Enforcement

When the indexer writes to the indices, it reads the `SchemaCache` to find how a `Put` or `Add` interacts with the existing data for the same entity-attribute pair.

For a `:db.cardinality/one` attribute, an entity can hold at most one value. If a `Put` or `Add` asserts a new value, the indexer must first look up the existing value, if there is one. The indexer must then emit retraction index keys for that value before it writes the new assertion.

For a `:db.cardinality/many` attribute, an entity can hold multiple values. A `Put` or `Add` writes the new assertion and retracts nothing. Multiple values coexist.

### 3.4 Unique Enforcement

Unique attributes are the only attributes that Triplox writes to the VAE index. The order of the VAE key is value, attribute, entity, transaction entity, operation. The index supports uniqueness checks and identity lookup, but it does not make all non-unique values globally searchable by value.

Before commit, the transaction pipeline makes sure that each asserted unique `[attribute value]` pair obeys the uniqueness constraint. Two different entities cannot assert the same pair, unless the same transaction retracts the old owner. For `:db.unique/identity`, tempid resolution first tries to find an existing owner and adopts that entity ID. For `:db.unique/value`, no adoption occurs. Triplox reports a collision as a uniqueness violation.

Triplox accepts lookup refs only for `:db.unique/identity` attributes. It rejects lookup refs on `:db.unique/value` attributes and on non-unique attributes.

## 4. Schema Immutability

A schema attribute is immutable after Triplox installs it. Triplox rejects these operations:

| Operation       | Condition                                                                 | Error                              |
|-----------------|---------------------------------------------------------------------------|------------------------------------|
| `Put`           | `db/id` matches an existing schema entity                                 | "Cannot redefine schema attribute" |
| `Retract`       | attribute is `db/ident`, `db/valueType`, `db/cardinality`, or `db/unique` | "Cannot retract schema attributes" |
| `RetractEntity` | entity ID belongs to a schema entity                                      | "Cannot retract schema entity"     |
| `Erase`         | entity ID belongs to a schema entity                                      | "Cannot erase schema entity"       |

In the final form, we likely reject all changes to bootstrap schema attributes, but allow changes to user-defined attributes. We still aim for deprecation-guided schema evolution.

---

## 5. Schema Definition

### 5.1 Version 1
- A transaction must define a schema attribute fully. If a transaction asserts any of `db/valueType`, `db/cardinality`, or `db/unique` on an entity, it must also assert `db/valueType` and `db/cardinality` on that entity. Otherwise, Triplox rejects the transaction.
- Triplox accepts an entity that asserts only `db/ident`, for example an enum entity or a user-defined ident. Triplox does not treat this entity as a schema attribute.
- `db/unique` is optional. If it is present, its value must be either `:db.unique/value` or `:db.unique/identity`.
- If the final set has all the required attributes, you can define these attributes with `Put` statements, `Add` statements, or both.
- Triplox rejects an update or a deletion of a schema attribute.
- Attribute ID resolution always uses the head of the database (basis-t in Datomic terms).
