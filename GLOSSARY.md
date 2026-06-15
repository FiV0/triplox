# Triplox Glossary

Short definitions of some core concepts in Triplox. This list is not authoritative and might be stale.

## AEV

The Attribute-Entity-Value [index](#index) view, ordered by attribute, then entity, then value.

## Assertion

An operation that adds a fact — a `:db/add` in [transaction data](#transaction-data) — that becomes an asserted [datom](#datom). Contrast [retraction](#retraction).

## Attribute

Something that can be said about an [entity](#entity), named by a [keyword](#keyword) such as `:user/name`.

## AVE

The Attribute-Value-Entity [index](#index) view, ordered by attribute, then value, then entity.

## Basis

A point in time a [database value](#database-value) is as-of. A basis is identified by a [TxKey](#txkey), so sometimes
these are used interchangeably. The basis is more about a stable database value identifier whereas the TxKey is
concerned with identifying a transaction on the log. Every TxKey gives you a stable basis.

## Connection

A client's handle to a [node](#node), returned by `connect`. You submit [transactions](#transaction) through it and call `db` on it to obtain a [database value](#database-value) to query.

## Covering indexes

The four primary index permuations EAV, AVE, AEV and VAE.

An [index](#index) — [AE](#ae) or [AV](#av) — that stores enough of the datom to answer certain queries directly, without consulting a temporal index. These omit the transaction suffix and record only [assertions](#assertion).

## Datalog

The declarative query language used to read from the system. You describe the data you want with [variables](#variable) and [patterns](#pattern) rather than how to fetch it. See [query](#query).

## Database

An overloaded term. In most database systems "database" refers to the system as a whole. Datomic pioneered the concept of
[database value](#database-value) where database means a immutable snapshot view of facts at a certain point in time. In most cases
we mean database in this sense, but try to keep the terminology system for the more traditional database meaning. Later we might
also add the concept of database mapping to a object store bucket. Here database than refers to the

## Database value

An immutable snapshot of the [database](#database) at a [basis](#basis), obtained from a [node](#node). Queries always run against a database value, never a mutable handle.

## Datom

The atomic unit of data: a fact of the form (entity, attribute, value, tx, op), where op marks it an [assertion](#assertion) or [retraction](#retraction). The [transaction](#transaction) is tracked separately, not stored in the datom itself.

## EAV

The Entity-Attribute-Value [index](#index) view, ordered by entity, then attribute, then value. Ideal for looking up the attributes of a known entity.

## EDN

Extensible Data Notation — the textual data format used for [queries](#query) and [transaction data](#transaction-data).

## Entity

A thing the database holds facts about; the first component of a [datom](#datom). Referenced in [transaction data](#transaction-data) by [entity id](#entity-id), [ident](#ident), temporary id, or [lookup-ref](#lookup-ref).

## Entity id

The 64-bit integer that uniquely identifies an [entity](#entity). Allocated from a [partition](#partition), which the id encodes alongside a counter.

## Find specification

The part of a [query](#query) that names which [variables](#variable) to return.

## Ident

A [keyword](#keyword) that names an [entity](#entity), assigned with `:db/ident`. Idents resolve to their [entity id](#entity-id), letting schema entities and enumerated values be referred to by name.

## Index

One of six ordered views of the same data — [EAV](#eav), [AVE](#ave), [AEV](#aev), and [VAE](#vae) (temporal, carrying the transaction in the key for [basis](#basis)/as-of queries), plus the atemporal [covering indexes](#covering-index) [AE](#ae) and [AV](#av) — each optimized for a different query pattern. The system trades write amplification for read performance. Built and maintained by the [indexer](#indexer).

## Indexer

The write-side component that turns [transactions](#transaction) into queryable structures. It [subscribes](#subscriber) to the [log](#log) and materializes each transaction across all the [index](#index) views.

## Indexing

The [indexer](#indexer)'s work of turning a [transaction](#transaction) into entries across the [index](#index) views. It is asynchronous: submitting a transaction returns a [TxKey](#txkey) before indexing finishes.

## Keyword

A name-like value, optionally namespaced, written with a leading colon — e.g. `:age` or `:user/name`. Keywords name [attributes](#attribute), serve as [idents](#ident) and enumerated values, and are themselves a [value type](#value-type).

## Log

An append-only, totally ordered record of every [transaction](#transaction). Each entry is an immutable record with a [TxKey](#txkey), and can be replayed to reconstruct any database state. Implementations include in-memory, file-based, and Kafka-based. Also called the [transaction log](#transaction-log).

## Lookup-ref

A reference to an [entity](#entity) by a unique attribute and value — an `[attribute value]` pair — instead of its [entity id](#entity-id). The attribute must be `:db.unique/identity`.

## Node

The database instance an application talks to. It accepts [transaction](#transaction) submissions, appends them to the [log](#log), and hands out [database values](#database-value) to query.

## Partition

A region of the [entity id](#entity-id) space. Triplox has three — one for [schema](#schema) entities, one for [transaction](#transaction) metadata, and one for user data — and each entity id encodes its partition plus a counter.

## Pattern

A [where clause](#where-clause) element that matches against entity-attribute-value triples in the database.

## Query

A declarative request for data, made up of a [find specification](#find-specification) and [where clauses](#where-clause). Compiled and executed by the [query compiler](#query-compiler).

## Query compiler

The read-side component that turns a [Datalog](#datalog) [query](#query) into an execution plan. It picks the best [index](#index) view, chooses join order, and uses multi-way join algorithms such as leapfrog trie-join.

## Reference

An [attribute](#attribute) whose [value type](#value-type) is `ref`: its [value](#value) is the [entity id](#entity-id) of another [entity](#entity). References connect entities into a graph.

## Retraction

An operation that removes a specific fact — a `:db/retract` in [transaction data](#transaction-data) — that becomes a retracted [datom](#datom). The fact stops appearing in current queries but remains in history.

## Schema

The set of [attribute](#attribute) definitions known to the database, mapping [idents](#ident) to [entity ids](#entity-id) and recording each attribute's [value type](#value-type), cardinality, and uniqueness. Bootstrapped before any user [transaction](#transaction) and used to validate every later one.

## Schema attribute

A built-in attribute used to define other attributes: `:db/ident`, `:db/valueType`, `:db/cardinality`, and `:db/unique`. Asserting these on an [entity](#entity) registers it in the [schema](#schema).

## Storage layer

The persistent key-value store holding all [indexed](#index) data, backed by SlateDB. Supports in-memory and on-disk backends.

## Subscriber

A component that consumes [transactions](#transaction) from the [log](#log). It first catches up on historical transactions, then streams live ones as they arrive. Multiple subscribers (e.g. the [indexer](#indexer)) consume the same stream independently.

## System

The running set of components — a [node](#node) with its [log](#log), [indexer](#indexer), and [storage layer](#storage-layer) — that together make up a Triplox deployment.

## Transaction

An atomic batch of operations that modifies data — all succeed or fail together. Submitted to a [node](#node) as [transaction data](#transaction-data), appended to the [log](#log) with a [TxKey](#txkey), then turned into [datoms](#datom) by the [indexer](#indexer).

## Transaction data

The user-facing input of a [transaction](#transaction): a list of operations — [assertions](#assertion), [retractions](#retraction), and entity deletes — written as [EDN](#edn) and resolved against the [schema](#schema) during [indexing](#indexing).

## Transaction log

See [Log](#log).

## Triple

The entity-attribute-value core of a [datom](#datom), without the operation or [transaction](#transaction).
"Datom" is the term used to identify the triple plus the tx and operation. "triple" refers to the underlying E-A-V structure.
In most cases we are interested in the triple part of a Datom when doing queries, so it can happen that

## Tx

A quite overloaded abbreviation for transaction in Triplox. Depending on context it can mean a
- [transaction](#transaction), its [TxKey](#txkey) or numeric transaction id, the entity id of the transaction itself (used for [basis](#basis) and as-of filtering), the `tx` module that expands transaction operations, or the reserved `:db.tx/*` attributes that record each transaction's outcome.

## TxKey

The unique identifier assigned to a [transaction](#transaction) when it is submitted: a monotonic transaction id plus a system timestamp. Returned immediately, before [indexing](#indexing) completes.

## Upsert

Automatic unification of a temporary id with an existing [entity](#entity) when an [assertion](#assertion) on a `:db.unique/identity` attribute matches a value already in the database. Prevents duplicate entities for the same unique value.

## VAE

The Value-Attribute-Entity [index](#index) view, written only for attributes with a uniqueness constraint. Used for uniqueness checks, [lookup-ref](#lookup-ref) resolution, and [upsert](#upsert).

## Value

The third component of a [datom](#datom) — what is asserted about the [entity](#entity) for an [attribute](#attribute). May be a scalar (long, string, boolean, double, instant, uuid, keyword), a [reference](#reference) to another entity, or a collection.

## Value type

The declared type of an [attribute](#attribute)'s values, set with `:db/valueType` — one of long, string, boolean, double, float, instant, uuid, keyword, bytes, [ref](#reference), and others.

## Variable

A placeholder in a [query](#query), prefixed with `?`, that represents an unknown value to find.
