---
title: Architecture
description: Triplox architecture.
---

Triplox uses [SlateDB](https://github.com/slatedb/slatedb) as its storage layer. SlateDB is a
key-value store built on top of object storage, think of it as
[RocksDB](https://github.com/facebook/rocksdb) backed by S3. By making object storage the
single source of truth, Triplox gets clean separation of storage and compute: the data
lives in a bucket, and any number of compute nodes can be added or removed without moving it.

SlateDB has a single writer, many readers architecture, and that property carries straight
through to Triplox: one node is the writer, and every other node is a reader.

Triplox is a client/server database. Your application connects to a node over the network
and queries run on the server, not inside your application process. This keeps clients thin and
opens the door to ecosystems outside the JVM.

The index layout and SlateDB's design both target [OLTP](https://en.wikipedia.org/wiki/Online_transaction_processing)
workloads. Triplox supports aggregates, but it is not a columnar
[OLAP](https://en.wikipedia.org/wiki/Online_analytical_processing) engine and will never beat
something like [DuckDB](https://github.com/duckdb/duckdb) on analytical scans. The architecture
also favors read-heavy workloads, as there is only a single writer.

## A typical setup

A typical deployment with three nodes, one primary writer plus two readers, looks roughly
like the diagram below. Transactions are sent to a log to obtain a
[total order](https://en.wikipedia.org/wiki/Total_order), and are then indexed by the primary
node into SlateDB. Reads are served from the primary node immediately, and from any reader node
as soon as it picks up the new WALs (Write-Ahead Logs) from object storage.

```
             ┌────────────────────────────────────────────────────────────┐
             │                    Object Storage (S3)                     │
             │  ┌───────────┐          ┌───────────┐       ┌───────────┐  │
             │  │  SlateDB  │          │  SlateDB  │       │  SlateDB  │  │
             │  │ (Writer)  │          │(Reader 1) │       │(Reader 2) │  │
             │  └─────┬─────┘          └─────┬─────┘       └─────┬─────┘  │
             └────────┼──────────────────────┼───────────────────┼────────┘
                      │                      │                   │
 Queries/Indices      ▲ read/write           ▼ read              ▼ read
                      │                      │                   │
      ┌───────────────┴───────────┐    ┌─────┴─────┐       ┌─────┴─────┐
      │        Writer Node        │    │ Reader 1  │       │ Reader 2  │
      │   ┌──────────┐            │    │           │       │           │
 ┌────┼──▶│ Indexer  │            │    │           │       │           │
 │    │   └──────────┘            │    │           │       │           │
 │    └───────────────┬───────────┘    └───────────┘       └───────────┘
 │                    │ write
 │ Transactions       ▼
 │  ┌─────────────────┴───────────────────────────────────────────────────┐
 │  │                     Log (Kafka, S2, WAL3, etc.)                     │
 │  │          ┌─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┐          │
 └──┼──────────┤ tx0 │ tx1 │ tx2 │ tx3 │ tx4 │ tx5 │ tx6 │ ... │          │
read│          └─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┘          │
    └─────────────────────────────────────────────────────────────────────┘
```

## Components

**Node.** A node is the database instance an application connects to, it exposes the API for
submitting transactions and running queries. A writer node submits transactions and serves
reads; a reader node only serves reads. The same binary fills both roles depending on its
configuration.

**Log.** The log is an append-only, totally ordered record of every transaction. Submitting a
transaction means appending it to the log, which hands back a `TxKey` (a `tx_id` offset plus a
`system_time`). At that point the transaction is durable but still unindexed. Keeping the log
separate from the writer lets Triplox acknowledge transactions with low latency even under
indexing back-pressure. The log is pluggable: in-memory for tests, file-based for a single
machine, and an external system such as Kafka for distributed setups. We also want
the external log to become backed by object storage to make the operational side very simple.
This is an active design discussion; see [Open questions → Log](/roadmap/open-questions/#log).

**Indexer.** The indexer is the write-side component on the primary node. It subscribes to the
log and, for each transaction, materializes the data into the
[covering indexes](/data-model/#indexes) in SlateDB. This is also where transaction semantics
are enforced: tempid and lookup-ref resolution, cardinality-one rewrites, uniqueness checks,
and schema validation. See the [transaction model](/transactions/transaction-model/) for the
full pipeline.

**Storage (SlateDB).** All indexed data lives in SlateDB, which persists it to object storage.
The writer reads and writes; readers read. Because object storage is the source of truth,
reader nodes converge on the writer's state by pulling new WALs with no direct node-to-node
communication.

**Query engine.** Queries are written in [EDN](https://github.com/edn-format/edn) Datalog and execute server-side against the
covering indexes. See the [query language](/query-language/datalog/) docs for details.

## Relationship to Datomic

Triplox is heavily inspired by [Datomic](https://www.datomic.com/), a database whose
data model (facts called *datoms*), transaction semantics, and Datalog query API Triplox
closely follows. If you have never used Datomic, you can safely skip this section.

For those coming from Datomic, the main differences are:

- **Deployment model.** Datomic comes in two flavors. Datomic Pro embeds a
  [peer library](https://docs.datomic.com/operation/peer-server.html) in your application, so
  queries run inside your process against a local cache.
  Datomic Cloud and Triplox are both client/server: a thin client talks to
  nodes over the network and queries run on the server.
  The remaining points below compare Triplox with Datomic Cloud.

- **Storage.** [Datomic Cloud](https://docs.datomic.com/operation/architecture.html) spreads
storage across multiple AWS services, DynamoDB for the transaction log, S3 for indexes,
  and EFS as a durable cache. Triplox keeps the indices in single object
  store bucket through [SlateDB](https://github.com/slatedb/slatedb) (with a separate log for now) and
  is not tied to a particular cloud. Be also aware that the way Datomic stores data
  is likely quite different to the simple indexes Triplox stores in SlateDB.
  Triplox does currently not have any version of immutable trees
  (see [Hitchhiker Trees](https://github.com/datacrypt-project/hitchhiker-tree)). As
  history grows this will create read amplification.

- **Infrastructure** Datomic Cloud is tailored towards AWS. Triplox is open
  source and aims to be compatible with object storage from a range of cloud providers.

## Deployment

The quickest way to try Triplox is the Docker image, which can run with an in-memory, local, or
object-storage backend. See the [quick start](/getting-started/quick-start/) to get a node
running and the [operations](/operations/overview/) section for distributed deployments.
