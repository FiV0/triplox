# Triplox Glossary

This file gives short definitions of core concepts in Triplox. The list is not authoritative and can
be out of date.

## AEV

AEV is the Attribute-Entity-Value [index](#index). It sorts data by attribute, then entity, then
value.

## Assertion

An assertion is an operation that adds a fact. In [transaction data](#transaction-data), you write
it as `:db/add`. Each assertion becomes an asserted [datom](#datom). The opposite operation is a
[retraction](#retraction).

## Attribute

An attribute is something that you can say about an [entity](#entity). A [keyword](#keyword) such as
`:user/name` names each attribute.

## AVE

AVE is the Attribute-Value-Entity [index](#index). It sorts data by attribute, then value, then
entity.

## Basis

A basis is the point in time of a [database value](#database-value). A [TxKey](#txkey) identifies
each basis, so people sometimes use the two terms as if they mean the same thing. The difference is
in purpose. A basis identifies a stable database value. A TxKey identifies a transaction on the
[log](#log). Every TxKey gives you a stable basis.

## Connection

A connection is the handle that a client has to a [node](#node). The `connect` function returns it.
You submit [transactions](#transaction) through the connection. You call `db` on it to get a
[database value](#database-value) that you can query.

## Covering indexes

The covering indexes are the four primary [index](#index) permutations: EAV, AVE, AEV, and VAE. A
permutation is one order of entity, attribute, and value. Each permutation serves different access
patterns and joins.

## Datalog

Datalog is the declarative query language that you use to read data from Triplox. A declarative
language describes the result, not the steps to get it. You describe the data that you want with
[variables](#variable) and [patterns](#pattern). See [query](#query).

## Database

The word "database" has more than one meaning. In most database systems, it means the whole system.
Datomic first used the concept of a [database value](#database-value). There, "database" means an
immutable snapshot of facts at one point in time. Immutable means that it never changes. In Triplox,
"database" usually has this meaning, and we try to use [system](#system) for the traditional
meaning. In the future, "database" can also mean a database that maps to an object store bucket.

## Database value

A database value is an immutable snapshot of the [database](#database) at a [basis](#basis). You get
it from a [node](#node). Queries always run against a database value, never against a handle that
changes.

## Datom

A datom is the smallest unit of data. It is a fact of the form (entity, attribute, value, tx, op).
The op field marks the datom as an [assertion](#assertion) or a [retraction](#retraction). Triplox
tracks the [transaction](#transaction) separately and does not store it in the datom.

## EAV

EAV is the Entity-Attribute-Value [index](#index). It sorts data by entity, then attribute, then
value. Use it to look up the attributes of a known entity.

## EDN

EDN (Extensible Data Notation) is the text format for [queries](#query) and [transaction
data](#transaction-data) in Clojure. Triplox does not currently send EDN over the network.

## Entity

An entity is a thing that the database holds facts about. It is the first part of a [datom](#datom).
In [transaction data](#transaction-data), you refer to an entity by its [entity id](#entity-id), an
[ident](#ident), a temporary id, or a [lookup-ref](#lookup-ref).

## Entity id

An entity id is the 64-bit integer that identifies one [entity](#entity). Triplox allocates each
entity id from a [partition](#partition). The id encodes the partition and a counter.

## Find specification

The find specification is the part of a [query](#query) that names the [variables](#variable) to
return.

## Ident

An ident is a [keyword](#keyword) that names an [entity](#entity). You assign it with `:db/ident`.
Each ident resolves to its [entity id](#entity-id). With idents, you can refer to schema entities
and enumerated values by name.

## Index

An index is one of six sorted views of the same data. Each index serves a different query pattern.
The [indexer](#indexer) builds and maintains the indexes.

The four temporal indexes are [EAV](#eav), [AVE](#ave), [AEV](#aev), and [VAE](#vae). Their keys
contain the transaction, so they support [basis](#basis) and as-of queries. An as-of query reads the
data at a past basis. The two atemporal indexes are AE and AV, which are the [covering
indexes](#covering-indexes).

Triplox accepts more writes in exchange for faster reads.

## Indexer

The indexer is the write-side component that turns [transactions](#transaction) into data that you
can query. It [subscribes](#subscriber) to the [log](#log). It writes each transaction to every
[index](#index).

## Indexing

Indexing is the work that the [indexer](#indexer) does. It turns a [transaction](#transaction) into
entries in each [index](#index). Indexing is asynchronous. When you submit a transaction, you get a
[TxKey](#txkey) back before indexing finishes.

## Keyword

A keyword is a value that acts as a name. It starts with a colon and can have a namespace, for
example `:age` or `:user/name`. The namespace is the prefix before the slash. Keywords name
[attributes](#attribute). They also act as [idents](#ident) and enumerated values. Keyword is also a
[value type](#value-type).

## Log

The log is an append-only record of every [transaction](#transaction) in one total order.
Append-only means that new entries go only at the end. Each entry is an immutable record with a
[TxKey](#txkey). You can replay the log to rebuild any database state. Triplox has in-memory,
file-based, and Kafka-based implementations. Another name for the log is the [transaction
log](#transaction-log).

## Lookup-ref

A lookup-ref refers to an [entity](#entity) by a unique attribute and value, instead of by its
[entity id](#entity-id). You write it as an `[attribute value]` pair. The attribute must be
`:db.unique/identity`.

## Node

A node is the database instance that an application talks to. It accepts submitted
[transactions](#transaction) and appends them to the [log](#log). It also gives out [database
values](#database-value) for queries.

## Partition

A partition is a region of the [entity id](#entity-id) space. Triplox has three partitions: one for
[schema](#schema) entities, one for [transaction](#transaction) metadata, and one for user data.
Each entity id encodes its partition and a counter.

## Pattern

A pattern is an element of a [where clause](#where-clause) that matches entity-attribute-value
triples in the database.

## Query

A query is a declarative request for data. It has a [find specification](#find-specification) and
[where clauses](#where-clause). The [query compiler](#query-compiler) compiles and runs it.

## Query compiler

The query compiler is the read-side component that turns a [Datalog](#datalog) [query](#query) into
an execution plan. An execution plan is the list of steps that produce the result. The compiler
picks the best [index](#index) and sets a variable order for the [patterns](#pattern). Then it runs
the patterns with a multi-way Generic Join.

## Reference

A reference is an [attribute](#attribute) with the [value type](#value-type) `ref`. Its
[value](#value) is the [entity id](#entity-id) of another [entity](#entity). References connect
entities into a graph.

## Retraction

A retraction is an operation that removes one fact. In [transaction data](#transaction-data), you
write it as `:db/retract`. Each retraction becomes a retracted [datom](#datom). Current queries no
longer show the fact, but the history keeps it.

## Schema

The schema is the set of [attribute](#attribute) definitions that the database knows. It maps
[idents](#ident) to [entity ids](#entity-id). For each attribute, it records the [value
type](#value-type), the cardinality, and the uniqueness. Cardinality tells if an entity has one
value or many values for the attribute. Triplox creates the schema before the first user
[transaction](#transaction). Triplox uses the schema to make sure that each later transaction is
valid.

## Schema attribute

A schema attribute is a built-in attribute that defines other attributes. The schema attributes are
`:db/ident`, `:db/valueType`, `:db/cardinality`, and `:db/unique`. When you assert these on an
[entity](#entity), Triplox adds the entity to the [schema](#schema).

## Storage layer

The storage layer is the persistent key-value store that holds all [indexed](#index) data. It uses
SlateDB and supports in-memory and on-disk backends.

## Subscriber

A subscriber is a component that reads [transactions](#transaction) from the [log](#log). First, it
catches up on past transactions. Then it reads new transactions as they arrive. More than one
subscriber can read the same stream, and each one reads it independently. The [indexer](#indexer) is
one example of a subscriber.

## System

The system is the running set of components that make up a Triplox deployment. It contains a
[node](#node) with its [log](#log), [indexer](#indexer), and [storage layer](#storage-layer).

## Transaction

A transaction is an atomic batch of operations that changes data. Atomic means that all operations
succeed together or fail together. You submit a transaction to a [node](#node) as [transaction
data](#transaction-data). The node appends it to the [log](#log) with a [TxKey](#txkey). Then the
[indexer](#indexer) turns it into [datoms](#datom).

## Transaction data

Transaction data is the input that a user gives for a [transaction](#transaction). It is a list of
operations: [assertions](#assertion), [retractions](#retraction), and entity deletes. You write it
as [EDN](#edn). During [indexing](#indexing), Triplox resolves it against the [schema](#schema).

## Transaction log

See [Log](#log).

## Triple

A triple is the entity-attribute-value part of a [datom](#datom), without the operation or the
[transaction](#transaction). The word "datom" names the triple together with the transaction and the
operation. The word "triple" names only the entity-attribute-value structure. In queries, we usually
care only about the triple part of a datom.

## Tx

"Tx" is an abbreviation for transaction, and it has many meanings in Triplox. Depending on the
context, it can mean:

- A [transaction](#transaction).
- Its [TxKey](#txkey) or its numeric transaction id.
- The entity id of the transaction itself. Triplox uses this id for [basis](#basis) and as-of
  filtering.
- The `tx` module that expands transaction operations.
- The reserved `:db.tx/*` attributes that record the outcome of each transaction.

## TxKey

A TxKey is the unique identifier that Triplox gives to a [transaction](#transaction) when you submit
it. It contains a monotonic transaction id and a system timestamp. Monotonic means that each new id
is larger than the id before it. You get the TxKey back at once, before [indexing](#indexing)
completes.

## Upsert

An upsert happens when an [assertion](#assertion) uses a `:db.unique/identity` attribute with a
value that already exists in the database. Triplox then automatically resolves the temporary id to
the existing [entity](#entity). This prevents duplicate entities for the same unique value.

## VAE

VAE is the Value-Attribute-Entity [index](#index). Triplox writes it only for attributes with a
uniqueness constraint. Triplox uses it for uniqueness checks, [lookup-ref](#lookup-ref) resolution,
and [upserts](#upsert).

## Value

The value is the third part of a [datom](#datom). It is what the datom asserts about the
[entity](#entity) for an [attribute](#attribute). A value can be a scalar, a [reference](#reference)
to another entity, or a collection. A scalar is a single value: long, string, boolean, double,
instant, uuid, or keyword.

## Value type

The value type is the declared type of the values of an [attribute](#attribute). You set it with
`:db/valueType`. The value types include long, string, boolean, double, float, instant, uuid,
keyword, bytes, and [ref](#reference).

## Variable

A variable is a placeholder in a [query](#query) for an unknown value to find. It starts with `?`.
