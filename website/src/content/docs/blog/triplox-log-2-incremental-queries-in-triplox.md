---
title: "Triplox Log 2 - Incremental queries in Triplox"
date: 2026-11-24
authors:
  - name: Finn Völkel
---

The current implementation of Triplox contains two query engines. One for [standard Datalog](https://triplox.xyz/query-language/datalog/) queries and one for [incremental Datalog](https://triplox.xyz/incremental-queries/overview/) queries.  The post tries to give some insight into the state of incremental queries in Triplox and the problems and issues one encounters when trying to support many incremental queries at once.

Think of an incremental query as computing changes to the result set of a standard query. A standard query delivers a result set at a given point in time $t$. Let us call the database at that time $DB_t$. A transaction creates a new database $DB_{t+1}$. An incremental query gives you the changes of the standard query between $DB_t$ and $DB_{t+1}$ from a given $t$ onwards (this is sometimes written as $\Delta DB$).

Consider the query:
```clojure
{:find [?name ?residence]
 :where [[?p :person/name ?name]
         [?p :person/residence ?residence]]}
```
It computes the name and the residence of a person. A standard result would be:
```clojure
[["Ada Lovelace" "12 St. James's Square"]
 ["Alan Turing" "Bletchley Park"]]
``` 
If Ada Lovelace now moves from `12 St James's Square` to `Buckingham Palace` the change in the corresponding incremental query looks as follows. 
```clojure
[[["Ada Lovelace" "12 St. James's Square"] -1]
 [["Ada Lovelace" "Buckingham Palace"] 1]]
```
The first part of every pair is the tuple that changes and the latter number indicates whether the tuple was removed or added. 

Triplox exposes incremental query subscriptions as a primitive, so people can decide what to build on top of them, whether that is streaming, event processing, reactive UIs, or materialized views. Taking views as the example, one could save a mapping from tuples to counts on the client and update the mapping whenever changes arrive, dissociating a tuple whenever the count drops to 0.

Every client API of Triplox exposes some version of a `subscribe` method which registers the incremental query on the server. `subscribe` returns a stateful object that either needs to get closed or explicitly unregistered depending on the API. IQs (short for incremental queries) require resources on the server and the closing mechanics assure that these resources are properly cleaned up. 

Below is a rough sketch how a transaction affects incremental queries in Triplox:

![incremental_query_pipeline](/assets/incremental_query_pipeline.png)

Transaction data gets indexed and written to [SlateDB](https://github.com/slatedb/slatedb/). Triplox currently listens to the WAL-backed CDC stream of SlateDB and applies a batch of transactions sitting in one WAL file to the incremental queries. Deltas within the same WAL file that cancel each other out have no effect on the query. For Triplox nodes that are `in-memory` or `local`, the WAL flushing interval can be small as we are not paying for it. Flushing the WAL to object storage in a distributed setting becomes expensive when done too often, so there is a liveness/cost tradeoff to be made. Why do we not skip the Indexer/SlateDB/CDC route for IQs and go straight from `TxData` on the log to applying the changes to incremental queries? The indexer does a lot of checking on the transaction data like uniqueness constraints, auto cardinality one retractions, tempid and upsert resolution. Incremental queries should only see data that has been committed. So either the indexer passes the data it committed to the incremental query service directly or we do the full roundtrip as the system currently does. I mainly chose the latter approach because it also works on reader nodes. 

Triplox is using [Feldera's](https://www.feldera.com/) [DBSP crate](https://crates.io/crates/dbsp) under the hood for executing incremental queries. Datalog queries get compiled to "circuits", a DBSP terminology. Think of a circuit as kind of a dataflow graph with relational operators that hold state. Feldera is compiling a single binary with one circuit for every SQL pipeline they maintain. So they do full AOT compilation with monomorphised types and inlined expressions. Their engine sits very much on the [OLAP](https://en.wikipedia.org/wiki/Online_analytical_processing) end of the spectrum for incremental queries. Triplox does runtime circuit construction with precompiled operators and interpreted expressions. The idea is to quickly register a query and to serve many different (or rather parameterized) queries at once. The DBSP crate is not optimized for that use case (otherwise it is blazingly fast). It has a runtime that you can tune, but is all tied to single circuit construction. For the experiments below I used a modified version of the DBSP crate. I prototyped a shared runtime pool for circuits. In the modified version every IQ gets assigned to a single thread and background merger threads get shared across the different circuits, otherwise the crate is unmodified. I need to spend some more time on the details of these changes before fully relying on them. Certain circuits may require more than one thread, but doing dynamic resource allocation at runtime is even more complex. 
### What works
Assuming we have a small product inventory with coffee, milk and tea and a stock inventory per warehouse. The following incremental query finds items low on stock.
```clojure
{:find [?stock ?sku ?name ?location ?available ?threshold]
 :where [[?stock :stock/product ?product]
         [?stock :stock/warehouse ?warehouse]
         [?stock :stock/available ?available]
         [?product :product/sku ?sku]
         [?product :product/name ?name]
         [?product :product/reorder-at ?threshold]
         [?warehouse :warehouse/name ?location]
         [(< ?available ?threshold)]]}
```

The incremental query returns changes whenever stock at a warehouse is below the reorder threshold or gets refilled. Below is a little example view of all stock and items low on stock via a local [Clerk notebook](https://github.com/FiV0/triplox/tree/030ff140224b125cf293a28e5067c62f8e58de37/examples/clj) where we reconcile the changes client side and display the view. Every 500ms we submit a transaction that either reduces some stock (a sale) or does a refill.

![incremental_low_stock](/assets/incremental_low_stock.gif)

The reason the 500ms spaced transactions show up immediately, is that I am running the Triplox node locally and so can tune WAL flushing to a very low value (which would be too expensive in a distributed setting). 
Triplox incremental queries support all the standard query engine constructs (`not`, `not-join`, `or`, `or-join`, predicates, functions and aggregates), so you can write more involved queries than the above. A thing that is still on my TODO list are rules as they require fixed point calculations via DBSP which I haven't touched yet.
### Other solutions

Before we dive into the experiments, let's have a look at other solutions out there. Existing solutions that I would file under incremental queries are pitched quite differently. [Materialize](https://materialize.com/) is a streaming database which maintains SQL queries over changing inputs and [Feldera](https://www.feldera.com/) is a streaming SQL engine. They offer you to create heavy-duty pipelines (via SQL) which continuously update views or other forms of sinks and sit more on the OLAP end of the spectrum. There is also [Epsio](https://www.epsio.io/), which runs as a sidecar to Postgres and MySQL and incrementally maintains queries as tables in those systems.

The closest thing to [OLTP](https://de.wikipedia.org/wiki/Online_Transaction_Processing) type databases in the incremental query world are sync engines or database solutions offering sync. I split those largely into two camps. The first camp being hosted database solutions with a reactive backend. Projects like [ConvexDB](https://www.convex.dev/), [InstantDB](https://www.instantdb.com/), [Firebase](https://firebase.google.com/) and [SpacetimeDB](https://spacetimedb.com/) come to mind. All of them offer you a managed service + a client SDK for syncing efficiently.  What is curious about these solutions is that none of them offer you an unrestricted query language (for example no arbitrary number of joins), but rather restrict the expressiveness of the query language. Restricting the expressiveness helps with maintaining tens of thousands of subscriptions .

The second camp are sync engines that take an existing database and try to provide some solution for syncing on top of the database. They are purely a sync layer. [Electric](https://electric-sql.com/) with [pglite](https://pglite.dev/) and [livestore](https://livestore.dev/) with SQLite and [PowerSync](https://powersync.com/) are solutions in this space.  Recently [Neon](https://neon.com/blog/neon-now-has-realtime) (stateless postgres with disaggregated storage) released realtime. They seem to be first ones to really support client-facing streaming subscriptions over the full query language in this latter camp (please correct me if I am wrong on this). I suspect some form of DBSP under the hood as the ElectricSQL folks have been [experimenting](https://github.com/electric-sql/electric-circuits) with DBSP for a while. I admit I don't know exactly where Triplox will end up on this spectrum.
### The limits 

For the experiments I used a Hetzner machine with 8 vCPUs (AMD EPYC-Milan, KVM guest) and 30 GiB of memory. I ran two experiments with a Triplox file log and remote storage simulated via a local [RustFS](https://github.com/rustfs/rustfs) setup on the same machine. Both experiments showed the limits of what the system can currently do. The idea was to do some testing a bit more on the OLAP end of the spectrum and a bit on the OLTP end of the spectrum. As a first experiment I tried to run all queries from the [Join Order Benchmark](https://www.vldb.org/pvldb/vol9/p204-leis.pdf) at once incrementally while I ingested the data.  Not that I do any sophisticated join order planning in Triplox (there is no join order planning at all), but these are 113 queries more on the OLAP side of the spectrum (I know, also not full OLAP). Even though my indexing pipeline is currently quite slow (more on that in another post), after some time the first incremental queries started failing, because they couldn't keep up with ingest. What do I mean by failing? Every IQ in Triplox has some sort of inbox receiving the deltas relevant for the incremental query. The IQs are always lagging the indexer to some extent. Triplox bounds the size of every inbox and raises an error when the inbox overflows. The idea is that you should scale compute (to more reader nodes) if the queries can't keep up with ingest. Depending on how quick the circuits consume the inbox they are more or less behind the indexer. The 113 Queries ran on the 8 vCPUs with no circuit sharing, so 113 circuits fought for compute while processing deltas. As of the writing of this post, the first inboxes overflowed at ~13 minutes after starting ingest.

The second experiment I tried was to register a lot of queries with very little initial data in the database. I used the data model from [AuctionMark](https://hstore.cs.brown.edu/projects/auctionmark/) and created 5 parameterized point-lookup queries of which I tried to register a 1000 and 10k in separate runs. An example of such a query:

```clojure
'{:find [?id ?name ?price ?status]
  :in [?user-id]
  :where [[?u :user/id ?user-id]
          [?w :user-watch/user-id ?u]
          [?w :user-watch/item-id ?i]
          [?i :item/id ?id]
          [?i :item/name ?name]
          [?i :item/current-price ?price]
          [?i :item/status ?status]]}
```
The `:in [?user-id]` part is a parameter to the query. AuctionMark simulates an auction site and the query returns the auction price and status changes to items a user watches. In this scenario I only submitted around 5 transactions per second and configured the benchmark so that on average around 2 of the registered IQs were affected by a transaction. The SlateDB WAL flush interval was set to 500ms.  The 1000 subscription benchmark run ran fine for 30 minutes. With 10k queries I ran out of memory while registering queries. Every query still has some memory overhead, even if it is just single-digit MBs, 10k subscriptions eventually exhausted memory. Circuit sharing should help here.

Fundamentally DBSP is a memory/time tradeoff. One uses lots of memory, be that RAM or disk, and gets quick updates in return. All the problems that I am running into can be summarized as resource management problems. Writing the compiler from Datalog to DBSP was relatively straightforward. The part of making the engine work robustly for many queries seems actually more difficult. Another problem that surfaced during the incremental AuctionMark run was that DBSP background merger tasks (the state in the DBSP operators needs to get compacted from time to time) became saturated when the SlateDB flush interval was set to a low value and then applied backpressure to the IQs which started failing. 

The experiments showed that IQs can have a throughput bottleneck when you register a lot of them. Either there needs to be some circuit backpressure, which isn't really an option as the indexer needs to continue to ingest whenever transactions arrive, or automatic compute scaling on the node the IQs run on. Supporting tens of thousands of queries requires circuit sharing.

There are still a lot of other concerns to make incremental queries robust. Currently when a connection gets lost the IQs can't recover. I am considering using [durable streams](https://github.com/durable-streams/durable-streams).  The Durable Streams protocol provides a simple, production-proven protocol for creating and consuming ordered, replayable data streams over HTTP, with support for catch-up reads and live tailing.[^1] Unfortunately, [Electric]() doesn't seem to maintain the durable streams repository anymore, but there is a pure durable streams server that is available. A subscription could then immediately resume from a given `TxKey` onward whenever a connection gets lost. Another advantage is that the primary db server and the stream server would become independent. Building on top of an open protocol would also make it easier for people to write their own clients, which will likely become more and more common these days.

In a first version of shared circuits the queries that take `:in` parameters likely share the non-parameterized part. So for the parameterized AuctionMark query above, the system would compile the following query to a circuit 
```clojure
'{:find [?user-id ?id ?name ?price ?status]
  :where [[?u :user/id ?user-id]
          [?w :user-watch/user-id ?u]
          [?w :user-watch/item-id ?i]
          [?i :item/id ?id]
          [?i :item/name ?name]
          [?i :item/current-price ?price]
          [?i :item/status ?status]]}
```
I am calling this the "lifted" query (to be clear, this has nothing to do with lifting in DBSP, which is a different concept) because the `:in` parameters get lifted into the `:find` clause. It's important that this lifted query does NOT apply the filtering by `?user-id`, because the corresponding circuit needs to hold the state for all user ids. 
```
fact ──► shared circuit ──► rows [?user-id ?id ?name ?price ?status]
                                  │ index by ?user-id
                                  ▼
                                join ──► (sub-id, [?id ?name ?price ?status])
                                  ▲
subs ──► (sub-id, ?user-id) ─────┘ index by ?user-id
```
For every subscription the system then joins a subscription id + user id pair to the query stream of the lifted query circuit and routes the deltas accordingly. The `sub-id` is not strictly necessary if all the `?user-id` parameters are distinct, but it makes the routing simpler and always returns the initial priming result for a new subscription. In this setup, an application then might have 10k subscriptions, but only a dozen heavy circuits. 

The above approach doesn't work for all types of parameters. For example consider the query 
```clojure
{:find [?id ?price]
 :in [?max]
 :where [[?i :item/id ?id]
         [?i :item/current-price ?price]
         [(< ?price ?max)]]}
```
Here the "lifted" non-parameterized version would be
```clojure
{:find [?max ?id ?price]
 :where [[?i :item/id ?id]
         [?i :item/current-price ?price]
         [(< ?price ?max)]]}

```
but `?max` is not groundable (the facts in the database cannot give a finite set values to `?max`). The approach of applying the filter last doesn't work. The parameter needs to be groundable by the "lifted" query.

Multiple features of the DBSP crate are not used (yet). For example checkpoint saving of the state in the DBSP operators can help with failure recovery. This likely only makes sense for quite data-heavy long running circuits. The DBSP circuit checkpoints could also get synced to object storage. Currently all relevant data is read from the indexes at once on circuit initialization and applied as one delta. Batched backfilling should be done when a circuit requires a large initial delta, which the DBSP crate supports. 

Durable streams are good for client recovery, connection issues and separating node failover from incremental query failover. DBSP checkpointing helps with restarting incremental queries from a given point onwards. 

The above sounds a bit cautionary, but incremental queries, within the limits outlined above, work. Incremental queries support `not` and `or` scopes, predicates and aggregates. You subscribe and receive the changes as they flow through the indexer and CDC pipeline. If you are not hammering the indexer at full throttle, a few hundred not too heavy incremental queries should be fine. If you need thousands of subscriptions then you need to wait for partial circuit sharing to come to Triplox.

Happy Datalogging!

---

*Written by hand. An LLM was used for spelling, tense corrections and some diagrams*

[^1]: Quoted directly from <https://github.com/durable-streams/durable-streams/blob/461b40267aabd644558f9b19dbb9507dd5f691cf/README.md>