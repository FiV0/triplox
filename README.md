<p align="center">
  <img src="img/logo_thin.png" alt="Triplox logo" width="600">
  <br>
  <br>
  <a href="https://triplox.xyz">
    <img alt="Website" src="https://img.shields.io/badge/website-triplox.xyz-blue.svg?style=flat-square">
  </a>
  <a href="https://www.apache.org/licenses/LICENSE-2.0">
    <img alt="License" src="https://img.shields.io/github/license/FiV0/triplox?style=flat-square">
  </a>
  <a href="https://discord.gg/CYaAYFwC">
    <img alt="Discord" src="https://img.shields.io/badge/discord-join-7289DA.svg?style=flat-square&logo=discord&logoColor=white">
  </a>
  <!-- <a href="https://crates.io/crates/triplox-client"> -->
  <!--   <img alt="crates.io" src="https://img.shields.io/crates/v/triplox-client.svg?style=flat-square"> -->
  <!-- </a> -->
  <!-- <a href="https://clojars.org/xyz.triplox/triplox"> -->
  <!--   <img alt="Clojars" src="https://img.shields.io/clojars/v/xyz.triplox/triplox.svg?style=flat-square"> -->
  <!-- </a> -->
</p>

> 🚧 **WARNING: Alpha Software** 🚧
> Triplox is alpha software. The index encoding has not yet stabilized. There will be bugs. There will be ingestion and congestion issues. Incremental queries will likely be slow. Do **not** use this in production.

# Triplox

Triplox is a [Datomic](https://www.datomic.com/)-inspired general-purpose database on top of object storage. The backbone of the engine is [SlateDB](https://github.com/slatedb/slatedb/), a key-value store on top of object-storage. Think of SlateDB as [RocksDB](https://github.com/facebook/rocksdb) on top of object-storage. Datomic is the main inspiration and the data model, transaction semantics and query API closely follow Datomic.

The goals of Triplox are roughly the following (in no particular order):
- Object storage first. In it's final version Triplox should simply need a single S3 bucket for deployment. This is currently not the case. See [Architecture](###Architecture) below.
- The Datomic Data model and API as main inspiration. Datomic is awesome. Lets bring it directly to object storage.
- A Client/Server architecture. I hope that this will open the door to ecosystems outside of the JVM (where Datomic has had it's main success).
- Incremental Datalog queries. You should be able to dynamically subscribe and detach from incremental Datalog queries. This is the most experimental part of Triplox and will need quite a bit of engineering effort to get right, make fast, and fully support of all features of Datalog.

### Getting started

The easiest way to test Triplox is to just pull the docker image and start an in-memory or local node.
```bash
docker pull ghcr.io/fiv0/triplox:0.1.0-alpha.2
docker run -p 5490:5490 ghcr.io/fiv0/triplox:0.1.0-alpha.2
```
This will start a Triplox server with an in-memory DB to which you can connect at 5490. If you want an persistent local node, you start the image with
```bash
docker run -p 5490:5490 -e TRIPLOX_STORAGE=local -v triplox-data:/var/lib/triplox  ghcr.io/fiv0/triplox:0.1.0-alpha.2
```
In case you are already convinced and want to deploy Triplox in a distributed setting I suggest you have a look at the
[Operations](https://triplox.xyz/operations/overview/) section on the website.

There is also the option to run Triplox in `dev` mode which is particular useful for testing.
```bash
docker run -p 5490:5490 -e TRIPLOX_STORAGE=dev ghcr.io/fiv0/triplox:0.1.0-alpha.2
```
In this case a new in-memory DB is created on every connection.

Afterwards you connect with your favorite client.
```clj
(require '[io.triplox.api :as tc])

(def conn (tc/connect "localhost" 5490))

;; schema
(tc/transact conn [{:db/ident :name
                    :db/valueType :db.type/string
                    :db/cardinality :db.cardinality/one}
                   {:db/ident :age
                    :db/valueType :db.type/long
                    :db/cardinality :db.cardinality/one}])

;; data
(tc/transact conn [{:name "alice" :age 30}
                   {:name "bob" :age 25}])

;; query
(with-open [db (tc/db conn)]
  (tc/q db '{:find [?e ?name ?age]
             :where [[?e :name ?name]
                     [?e :age ?age]]}))
;; => [[8796093022209 "alice" 30] [8796093022208 "bob" 25]]
```

### Clients

- Rust
- Clojure
- Java
- Go (TODO)
- Python (TODO)
- TS/Cljs (TODO)

If you would like to create another client there is a [PROTOCOL](design/PROTOCOL.md) design doc that should help. Currently the
HTTP/2 server only supports a custom MessagePack encoding scheme. We plan to add support for `transit+json` and `transit+msgpack`
in the future.

### Architecture

By making object storage the single source of truth, you get separation of storage and compute. SlateDB has a single writer and many readers architecture and that directly translates to Triplox. In that sense it's similar to Datomic. Triplox sits more in the traditional client/server camp compared to Datomic where the [peer library](https://docs.datomic.com/operation/peer-server.html) gets embedded into the application code. Queries run on the server in Triplox and this makes certain query patterns different to Datomic. For example limiting your query results would be done with `:limit` in a EDN Datalog query instead of doing it application code side.

The layout of the indices and also SlateDB's focus are [OLTP](https://en.wikipedia.org/wiki/Online_transaction_processing) queries. If you expect columnar layout performance for [OLAP](https://en.wikipedia.org/wiki/Online_analytical_processing) style queries, Triplox is not the right fit. This doesn't mean Triplox doesn't support aggregates, it will just not beat something like [DuckDB](https://github.com/duckdb/duckdb) on these types of queries. Also keep in mind that the architecture favors read heavy workloads. Triplox is currently single writer.

A typical setup for Triplox with 3 nodes (1 primary writer node + 2 readers nodes) would then roughly look like the diagram below.
Transactions get send to the log to get a [total-order](https://en.wikipedia.org/wiki/Total_order) and then indexed through
the primary indexing node into SlateDB. Reads can then be served from the primary node immediately and form any reader node
as soon as the reader nodes receive new WALs (Write-Ahead Logs) from Object Storage.

There are some design decisions that still are up for grabs. In particular the external log has been a thorn in my side and I would like to get rid of it. See [Open Questions > Log](https://triplox.xyz/open-questions/log/) on the website.

```

                   ┌─────────────────────────────────────────────────────────────┐
                   │                   Object Storage (S3)                       │
                   │                                                             │
                   │  ┌─────────────┐      ┌─────────────┐      ┌─────────────┐  │
                   │  │   SlateDB   │      │   SlateDB   │      │   SlateDB   │  │
                   │  │  (Writer)   │      │  (Reader 1) │      │  (Reader 2) │  │
                   │  └──────┬──────┘      └──────┬──────┘      └──────┬──────┘  │
                   │         │                    │                    │         │
                   └─────────┼────────────────────┼────────────────────┼─────────┘
                             │                    │                    │
     Queries/Indices         ▲ read/write         ▼ read               ▼ read
                             │                    │                    │
        ┌────────────────────┴────────┐  ┌────────┴────────┐  ┌────────┴───────┐
        │         Writer Node         │  │  Reader Node 1  │  │  Reader Node 2 │
        │                             │  │                 │  │                │
        │      ┌──────────────┐       │  │                 │  │                │
  ┌─────┼────▶│   Indexer    │       │  │                 │  │                │
  │     │      └──────────────┘       │  │                 │  │                │
  │     │                             │  │                 │  │                │
  │     └─────────────┬───────────────┘  └─────────────────┘  └────────────────┘
  │                   │
  │  Transactions     │ write
  │                   ▼
  │     ┌──────────────────────────────────────────────────────────────────────┐
  │     │                                                                      │
  │     │                 Log (Kafka, S2, WAL3, etc.)                          │
  │     │                                                                      │
  │     │    ┌─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┐                 │
  └─────┼────┤ tx0 │ tx1 │ tx2 │ tx3 │ tx4 │ tx5 │ tx6 │ ... │                 │
   read │    └─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┘                 │
        │                                                                      │
        └──────────────────────────────────────────────────────────────────────┘
```

### Getting involved

This project is very much WIP and any help is appreciated. Discussions and feedback happen on [Discord](https://discord.gg/CYaAYFwC).

There is lots of things to work on. Feel free to open tickets for features, bugs or general ideas.
If you are building something more involved it's likely better to discuss it first
(either on Discord or in a ticket) and see if it fits the projects scope.

### Acknowledgements

The primary inspiration is Datomic and you will see it's impact throughout the project. [Mentat](https://github.com/mozilla/mentat) (from which the edn crate is copied) has been a great inspiration and help in designing the transaction pipeline of Triplox.

### Compatibility

The goal is not to have a 1-to-1 correspondence of features with Datomic. Datomic is the main inspiration and we'll strive to stay
close to the Datomic APIs, but don't guarantee feature parity nor identical behavior in all cases. The main differences will currently show up in the transaction pipeline. I am currently not dealing with schema updates. There are also some edge
cases with tempid + upsert resolution that I am currently not dealing correctly with.

There are some more open questions that I tried to write down on the [website](https://triplox.xyz/roadmap/open-questions/)

### Licence

Triplox is licensed under the Apache License, Version 2.0.

The [edn](edn/) crate was orginally copied from [mentat](https://github.com/mozilla/mentat) and is also licenced under Apache Licence, Version 2.0.
