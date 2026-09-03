---
title: Introduction
description: Introduction to Triplox.
---


Triplox is an object-storage-native Datalog database for system-of-record applications. Triplox is backed by [SlateDB](https://github.com/slatedb/slatedb/), a key-value store itself backed by object-storage. It is single writer and many readers. The database is a set of facts called Datoms. Transactions are totally ordered via a log and provide ACID guarantees. The name Triplox is portmanteau of triple and blocks.

The ideas of Triplox are roughly the following (in no particular order):

- Object storage first. In it's final version Triplox should simply need a single (or likely two) S3 bucket(s) for deployment. This is currently not the case. See the [Architecture](/getting-started/architecture) section.

- The [Datomic](https://docs.datomic.com/datomic-overview.html) data model and API as main inspiration.

- A Client/Server architecture. We hope that this will open the door to ecosystems outside of the JVM (where Datomic has had it's main success).

- Incremental Datalog queries. You should be able to dynamically subscribe and detach from incremental Datalog queries. This is the most experimental part of Triplox and will need quite a bit of engineering effort to get right, make fast, and to fully support all features of Datalog.

Many things that apply to Datomic, will also apply to Triplox, but there will be differences due to the architecture. We don't plan to be 1-to-1 compatible. There are also things still up for discussion, see the [Roadmap](/roadmap/roadmap) section. Many features are out-of-scope for now, as we want to make transactions, queries and incremental queries stable in a first version.

To further explore Triplox:
 - See the [quick start](/getting-started/quick-start) if you just want launch Triplox, ingest some data and query in back with your favourite API.
 - See [architecture](/getting-started/architecture) for an overview of the different components of Triplox.
 - See [transaction model](/transactions/transaction-model) or [life of a transaction](/transactions/life-of-a-transaction) to get an understanding of how transactions flow through a Triplox
 - See [query language](/query-language/overview) to understand and learn the syntax for EDN Datalog queries.
 - Have look at [incremental queries](/incremental-queries/overview/) overview if you want to learn about how to subscribe to changes in Triplox.
 - See [APIs](/apis/clojure/) to have a look at the available language bindings. We currently support Clojure, Rust and Java. If you wish to create a new client, have a look at the [protocol doc](https://github.com/FiV0/triplox/blob/main/design/PROTOCOL.md) in the repository.
 - Finally there is the [roadmap](/roadmap/roadmap) along with some open questions up for discussion. Feel free to join the [Discord](https://discord.gg/JSaGCaVre) or open a ticket in the [repo](https://github.com/FiV0/triplox/) if you have ideas or insights on those.
