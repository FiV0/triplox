---
title: Rationale
description: Why Triplox exists and the design choices behind it.
---

Triplox is a Datalog database for system-of-record applications. It keeps every fact you ever
wrote, lets you query any past state of your data, and can push you the changes to a query as they
happen. All of it runs on top of a bucket in object storage. This page explains the main ideas
behind Triplox and the trade-offs that come with them.

## Object storage as the source of truth

Triplox stores its indexes in [SlateDB](https://github.com/slatedb/slatedb), a key-value store
built on object storage (think [RocksDB](https://github.com/facebook/rocksdb) backed by S3). The
bucket is the database. Nodes are compute that you add and remove without moving data around.

- **Separation of storage and compute.** Data lives in the bucket, so compute scales
  independently of storage. A new node only needs access to the bucket.
- **Simple operations.** No replication protocol between nodes, no disks to manage. Reader nodes
  catch up by reading new WAL files from object storage.
- **No vendor lock-in.** Triplox is not tied to a particular cloud and aims to work with object
  storage from a range of providers.
- **Cheap durability.** Object storage is durable and inexpensive, and you only pay for compute
  you actually run.

The goal is a deployment that needs nothing but a bucket. Today Triplox still requires a separate
transaction log; see [Architecture](/getting-started/architecture/) and the
[open questions](/roadmap/open-questions/#log) for where this is heading.

## Facts, not rows

Triplox follows the [Datomic](https://www.datomic.com/) data model. The database is a set of
facts called [datoms](/getting-started/concepts/#datom): an entity has an attribute with a value,
asserted or retracted in a transaction.

- **Flexible schema.** Entities carry only the attributes they need. No sparse tables, no
  nullable columns for every possible subtype, and schema evolves by adding attributes.
- **Relationships are first class.** Reference attributes replace join tables, and the indexes let
  you navigate relationships in both directions.
- **Indexed for every access pattern.** Each datom is stored in four
  [covering indexes](/data-model/#indexes) (EAV, AVE, AEV, VAE), so lookups by entity, by value,
  by attribute or by reverse reference are all efficient.
- **The schema is data.** Schema, transactions and data are all stored as datoms and can be queried
  the same way.

See the [data model](/data-model/overview/) for the details.

## The database as a value

A [DB value](/getting-started/concepts/#db-value) is an immutable view of the database at a point in
time. Queries against it always return the same answer, no matter what gets written afterwards.

- **Consistent reads without coordination.** Run as many queries as you like against one db value
  and they all see the same state.
- **Full history.** Triplox is accumulate-only. Every assertion and retraction is kept, and you can
  open the database as of any earlier transaction.
- **Auditability.** Every transaction is itself an entity stored as datoms, so the history of
  changes is part of the data.

## Datalog as the query language

Queries are written in [EDN Datalog](/query-language/datalog/), the same dialect Datomic users
already know.

- **Declarative and composable.** Queries are data, not strings to concatenate. Clauses are
  patterns that join implicitly on shared variables.
- **Good at joins.** Datalog shines on many-way joins and graph-shaped data, where SQL tends to
  become verbose.
- **One language for one-off and live queries.** The same query can be run once against a db value
  or subscribed to as an incremental query.

## Incremental queries

Most databases answer a question once. Triplox can also keep answering it. An
[incremental query](/incremental-queries/overview/) is a Datalog query you subscribe to; Triplox then
streams you the rows that enter and leave the result set with every transaction.

- **Built on DBSP.** Incremental evaluation uses [DBSP](https://arxiv.org/abs/2203.16684), a
  theory of incremental computation with a solid formal foundation.
- **Dynamic subscriptions.** Subscribe and unsubscribe at runtime. Unlike systems that compile a
  new binary per query, an incremental query in Triplox is a lightweight circuit built on the fly.
- **Driven by storage.** Changes are picked up from SlateDB's change data capture, so incremental
  queries follow exactly what was indexed.
- **Views on your terms.** With a stream of deltas you can maintain materialized views, caches or
  derived data in your application without polling.

This is the most experimental part of Triplox and is under active development.

## Transactions with a total order

All transactions go through a log that gives them a
[total order](https://en.wikipedia.org/wiki/Total_order). A single indexer applies them one by one
against the state right before each transaction.

- **ACID.** A transaction's facts become visible together, or none of them do.
- **Schema-validated.** Types, cardinality and uniqueness are checked against the schema before
  anything is written. See the [transaction model](/transactions/transaction-model/).
- **Low-latency acknowledgement.** A transaction is durable once it is on the log, so clients can
  choose between waiting for the log (`submit_tx`) or for indexing (`execute_tx`).

## Client/server and language agnostic

Triplox is a client/server database. Queries run on the server, and clients are thin.

- **Any language.** Clients exist for [Rust](/apis/rust/), [Clojure](/apis/clojure/) and
  [Java](/apis/java/). The [protocol](https://github.com/FiV0/triplox/blob/main/design/PROTOCOL.md)
  is documented, so writing a new client is straightforward.
- **Beyond the JVM.** Datomic's model has mostly lived in the JVM world. Triplox brings it to other
  ecosystems.
- **Written in Rust.** The server is a single binary. The same binary will run as a writer or a
  reader node depending on its configuration (reader nodes are on the [roadmap](/roadmap/roadmap/)).

## What Triplox is not

Every design choice has a cost. Triplox is likely the wrong tool if you need:

- **Analytical scans.** The index layout targets OLTP workloads. Aggregates are supported, but a
  columnar engine like [DuckDB](https://github.com/duckdb/duckdb) will be much faster on large
  analytical queries.
- **High write throughput.** There is a single writer. Triplox favours read-heavy workloads.
- **Data you must forget.** History is kept by design. [Erasure](/transactions/transaction-data/#entity-erasure)
  exists, but it is the exception.
- **A production-hardened system today.** Triplox is alpha software. See the
  [roadmap](/roadmap/roadmap/) for what is planned.

## Next steps

- Try it in a few minutes with the [quick start](/getting-started/quick-start/).
- Read the [introduction](/getting-started/introduction/) and [architecture](/getting-started/architecture/).
- Join the [Discord](https://discord.gg/JSaGCaVre) or open an issue on
  [GitHub](https://github.com/FiV0/triplox/).
