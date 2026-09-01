---
title: Schema
description: Triplox schema.
---

Every Datom added to Triplox is validated by a schema. The schema itself is described by entities.
Schema is data and no different in terms of storage then standard entities.
A schema entity has the following attributes:

| Attribute         | Description                                                               | Required? |
| ----------------- | ------------------------------------------------------------------------- | --------- |
| `:db/ident`       | A unique name or identifier for an entity. Usually this is a schema entity. | Yes       |
| `:db/valueType`   | Defines the type of value associated with an attribute.                    | Yes       |
| `:db/cardinality` | Weather a attribute does have a single or multiple values.                 | Yes       |
| `:db/unique`      | Defines a uniqueness constraints for values associated with an attribute.  | No        |

### `:db/ident`

`:db/ident` is a unique keyword identifying an entity. In most cases it is used to identify attributes in
queries, but `:db/ident` can also be used to define an enum like collection of possible values. Examples
are the values of `:db/valueType` and `:db/cardinality` below or the value of [`:db/txResult`]()


For `:db/ident` it is recommended to prefix identifiers with the general entity class that an attribute is going to be used for.
Meaning it is discouraged to use `:name` for the name of people and the name of products and instead use two attributes
`:person/name` and `:product/name`. This avoids confusion and also gives better index locality when joining.


### `:db/valueType`

Allowed value types:

| ident                | Description                                                       |
| -------------------- | ----------------------------------------------------------------- |
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
| `:db.type/vector`    | Ordered collection of heterogeneous values                        |
| `:db.type/map`       | String-keyed map with heterogeneous values                        |

The composite types `vector` and `map` allow the same data types as described above. The recursive
values have no type checks to them, meaning a attribute of type vector can hold values of type long, strings, doubles
and nested vectors without schema-level checks. In particular refs are just longs, so
they will not get typed checked in the transaction pipeline if they sit in composite types.
In the worst case you end up with dangling references.

### `:db/cardinality`

Cardinality can take two values:

| ident                  | Description                                               |
| ---------------------- | ----------------------------------------------------- |
| `:db.cardinality/one`  | An entity has at most one value for this attribute.   |
| `:db.cardinality/many` | An entity may have multiple values for this attribute. |


### `:db/unique`

Unique attributes are indexed by value (VAE index) and checked during transaction processing.

| ident                 | Description                                                             |
| --------------------- | ----------------------------------------------------------------------- |
| `:db.unique/value`    | At most one entity may assert a given value. No lookup refs or upserts. |
| `:db.unique/identity` | Same uniqueness constraint, plus lookup refs and identity upsert.       |

Only `:db.unique/identity` attributes participate in tempid upsert resolution.
`:db.unique/value` is a constraint only: transactions may assert it, but they cannot use it to resolve lookup refs or adopt an existing entity for a tempid.
