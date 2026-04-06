<p align="center">
  <img src="img/logo_thin.png" alt="Triplox logo" width="600">
</p>

> 🚧 **WARNING: Alpha Software** 🚧
> Triplox is alpha software. The index encoding has not yet stabilized. There will be bugs. There will be ingestion and congestion issues. Incremental queries will likely be slow. Do **not** use this in production.

# Triplox

Triplox is a [Datomic](https://www.datomic.com/) inspired general-purpose database on top of object storage. The backbone of the engine is [SlateDB](https://github.com/slatedb/slatedb/), a key-value store on top of object-storage. Think of SlateDB as [RocksDB](https://github.com/facebook/rocksdb) on top of object-storage. Datomic is the main inspiration and the data model, transaction semantics and query API closely follow Datomic.

The goals of Triplox are roughly the following (in no particular order):
- Object storage first. In it's final version Triplox should simply need a single S3 bucket for deployment. This is currently not the case. See [Architecture](###Architecture) below.
- The Datomic Data model and API as main inspiration. Datomic is awesome. Let's bring it directly to object storage.
- A Client/Server architecture. I hope that this will open the door to ecosystems outside of the JVM (where Datomic has had it's main success).
- Incremental Datalog queries. You should be able to dynamically subscribe and detach from incremental Datalog queries. This is the most experimental part of Triplox and will need quite a bit of engineering effort to get right, make fast, and fully support of all features of Datalog (recursive rules being the most tricky part). We hook into SlateDB's [CDC](https://en.wikipedia.org/wiki/Change_data_capture) and produce new deltas for every WAL entry that comes through.


### Examples

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

### Acknowledgements

The primary inspiration is Datomic and you will see it's impact throughout the project. [Mentat](https://github.com/mozilla/mentat) (from which the edn crate is copied) has been a great inspiration and help in designing the transaction pipeline of Triplox.

### Compatibility

The goal is not to have a 1-to-1 correspondence of features with Datomic. Datomic is the main inspiration and we'll strive to stay
close to the Datomic APIs, but don't guarantee feature parity nor identical behavior in all cases. The main differences
will currently show up in the transaction pipeline. I am currently not dealing with schema updates. On the query side there is
a question of bag vs set semantics. Set semantics stays true to traditional Datalog and also avoids certain awkward
query patterns where variables otherwise "leak" into aggregates (see some thoughts in [semantics document](design/SEMANTICS.md).
On the other hand bags allow you to stream result sets in batches (no deduplication of the full result set) and in
theory also need less DBSP distinct operators (an operator that is expensive to maintain). I want to take this decision
carefully.

### Licence

Triplox is licensed under the Apache License, Version 2.0.

The [edn](edn/) crate was orginally copied from [mentat](https://github.com/mozilla/mentat) and is also licenced under Apache Licence, Version 2.0.
