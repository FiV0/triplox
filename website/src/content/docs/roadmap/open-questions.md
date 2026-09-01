---
title: Open questions
description: Open questions about the architecture of Triplox.
---

### Log

As you can see in the [Architecture](/getting-started/architecture/) section, Triplox currently uses an external log to serialize the transaction history.
The external log is currently the main wrinkle for me in the architecture. It adds extra complexity that we would like to avoid.
[AutoMQ](https://github.com/automq/automq) is Kafka backed by S3. This would probably give good latency but is another service in the architecture.
Some kind of log component that writes directly to object storage to which the writer node just appends data would be preferable.
You will likely get higher latencies (although you could use something like S3 Express), but operationally this is likely the best option.
Triplox cannot use SlateDB's WAL to order transactions, because they need a
[total order](https://en.wikipedia.org/wiki/Total_order) before they are indexed into SlateDB. The indexer must process each transaction
against the SlateDB state produced by its predecessor. Something like [wal3](https://www.trychroma.com/engineering/wal3) would be something to consider, but it is currently not available as standalone dependency.
We have also discussed creating a standalone slatedb-wal, extracting the wal component of SlateDB into a standalone dependency and simply use it as a log.
So far this seems to be the best option to me, but I am happy to hear other ideas.


### Bag vs set semantics

On the query side there is a question of bag vs set semantics. Currently we are implementing bag semantics for query results
and this differs to traditional Datomic. Set semantics would remain closer to the traditional Datalog literature and
also avoids certain awkward query patterns where variables otherwise "leak"
into aggregates (see some thoughts in the [semantics document](https://github.com/FiV0/triplox/blob/main/design/SEMANTICS.md)).
On the other hand, bags allow you to stream result sets in batches (no deduplication of the full result set) and in
theory also need less DBSP distinct operators (an operator that is expensive to maintain). The decision on this
needs some more thought. If you have strong opinions regarding the set vs bag question, feel free to raise them on Discord
or in a ticket.

### Tx pipeline

To me the transaction pipeline feels currently like the most brittle part of the system. There are lots of assumptions made and if
an assumption is wrong the system might diverge from a correct state. This for example includes schema. On schema updates,
an in memory structure gets updated, if the data structure and actual data on disk diverge we are in a bad place. One solution
is to always re-query the schema (after the transaction has been submitted) when a schema update is detected.
This might be slowing down the hot ingestion path a bit, but schema updates don't happen too often.
One aspect which can not be solved via a query is uniqueness constraints. [Mentat](https://github.com/mozilla/mentat) solved this by using `CREATE UNIQUE INDEX` in SQLite, there is no equivalent in SlateDB, the storage layer of Triplox,
so we can kind of need to guarantee correctness of the tx pipeline.


### WAL tailing and Change Data Capture

As described above, Triplox uses an external log, so at first glance there is no need for enabling WAL replay on SlateDB level.
Triplox can read from SlateDB without waiting for durability, as we can always replay transactions from the external log if a node dies.
On the other hand Change Data Capture (CDC) done by tailing the WAL is quite handy for incremental queries.
The WAL files contain the new state in the indexes after having gone through the indexer, so they reflect what actually
made it into the database without doing all the verification that has happened in the indexer once more on a different path.
That is why we are currently using CDC from SlateDB for incremental query support. There is of course a cost associated
with streaming the WAL. More object storage put requests and listing operations for listening to WAL changes.

### Historical indices

Currently every datom, be that an assertion or a retraction, gets indexed into the same global unique indexes. We resolve the
"current" values of some db at read time. As history grows this will cause read amplification and if entities have a lot of
versions will not scale well. My current thinking is that I will likely split the covering indexes into a "current" (for some
reasonable definition of current) and a historic version. The idea would be that most performance critical queries
run close to the "head" of the indexer. If one is interested in truly historic queries (like last quarter), than it's fine
to take the extra performance hit.

Another option is of course to take a complete different route and look into things like
[Hitchhiker trees](https://github.com/datacrypt-project/hitchhiker-tree) and also consider the path copying approach with some
version of immutable trees that Datomic likely uses. This would be a complete overhaul of the storage layer,
so not something I am currently considering.
