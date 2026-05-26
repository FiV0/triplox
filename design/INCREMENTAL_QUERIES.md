# Triplox Incremental Queries

Version 0.1

## Overview

Incremental queries let a caller register a query once and then receive result
deltas as new transactions are indexed. The current implementation is the first
writer-node version: it supports a small subset of Datomic-style queries,
constructs one DBSP circuit per registered query, and drives those circuits from
SlateDB WAL change data capture.

The feature is intentionally crate-internal for now. It is meant to establish
the execution model and correctness boundaries before exposing incremental
query subscriptions over the public client protocol.

---

## 1. Current Scope

The current engine supports conjunctive triple-pattern queries on writer nodes.
Queries are registered through `Node<L: TxLog>` and return a subscription that
emits future deltas only. The registration basis is returned so a caller can
combine a separately obtained snapshot at that basis with the stream of later
deltas.

Currently supported:

- Triple patterns in `:where`.
- Constant attributes, written as idents or entids.
- Variables, constants, and placeholders in entity and value positions.
- Joins through shared variables across patterns.
- Disconnected pattern groups, using Cartesian-product semantics like the
  standard query engine.
- `register_incremental_query` and `unregister_incremental_query` on writer
  nodes.

Currently rejected:

- Variable or placeholder attributes.
- Repeated variables inside one triple pattern.
- `or`, `not`, predicates, functions, aggregates, rules, pull expressions,
  `:in`, `:order`, and `:limit`.
- Public client or JVM subscription APIs.

The one-shot query engine remains the semantic reference for the supported
subset. Incremental query planning lives separately from `query.rs`.

---

## 2. Architecture

The incremental query path has four main parts:

1. **Planner** - validates the supported query subset and lowers triple patterns
   into an incremental query plan.
2. **DBSP circuit** - owns the per-query dataflow graph and emits result deltas.
3. **Incremental query service** - owns registered query handles, channels, and
   per-query circuit instances.
4. **CDC loop** - reads SlateDB WAL files, decodes transactions into datoms, and
   applies weighted triple updates to all registered circuits.

Each registered query owns a DBSP circuit. The circuit input is a weighted set
of encoded triples:

```text
EncodedTriple {
    entity: encoded DataType::Long(entity_id),
    attribute: attribute entid,
    value: encoded DataType,
}
```

Values are encoded bytes rather than `DataType` keys because DBSP Z-set keys
need ordering, and `DataType` contains values such as floats, maps, and vectors
that do not form a simple total order. After pattern filtering, operators pass
rows of encoded values between patterns and joins.

The DBSP state is trace-backed and configured with file-backed storage. Triplox
does not keep full accumulated relation Z-sets in ordinary Rust memory as the
query state. Per-query trace files are derived state: they can be deleted when a
query is unregistered, and future restart support should be able to rebuild them
from a snapshot plus WAL replay or restore them from DBSP checkpoints.

---

## 3. Runtime Flow

The main runtime boundary is `IncrementalQueryService`. It is a cloneable handle
used by async code, but it does not own the mutable query state directly.
Instead, it sends `IncrementalCommand`s to one dedicated service thread. That
thread owns the registry of active queries and every query's DBSP circuit.

Registration flows through the system as follows:

```text
Node::register_incremental_query
    captures a TxBasis and WAL cursor
    plans the query with inc_query.rs
    scans initial triples with incremental/cdc.rs
    sends Register to IncrementalQueryService

triplox-incremental-query thread
    builds one QueryCircuit for the plan
    primes it with the initial triples
    stores the circuit, basis, WAL cursor, and subscription sender
    returns an IncrementalQuerySubscription
```

The split exists because DBSP circuit handles are mutable state that should be
stepped serially. The service thread gives the async node and CDC tasks a small
message-passing API while keeping circuit mutation, query registration, and
query cleanup in one place.

Live WAL application follows the same boundary:

```text
Tokio CDC task
    reads one WAL transaction
    decodes it to datoms using the node schema
    converts datoms to weighted EncodedTriple tuples
    sends ApplyTriples to IncrementalQueryService

triplox-incremental-query thread
    loops over registered queries
    skips transactions at or before each query basis
    steps each relevant QueryCircuit
    sends non-empty IncrementalQueryDelta values to that query's receiver
```

There are two channel directions:

- `std::sync::mpsc` carries commands into the service thread. It fits the
  blocking `receiver.recv()` loop used by the dedicated thread.
- `tokio::sync::mpsc` carries `IncrementalQueryDelta` values back to async
  subscribers. The service thread uses `blocking_send`, so a slow subscriber
  applies backpressure instead of losing deltas.

`IncrementalQueryDelta` is therefore not the command protocol. It is the
subscriber-facing result batch emitted after a circuit step. The command
protocol exists to serialize mutations to the query registry and DBSP circuits.

Registration is serialized against CDC application by a node-level registration
gate. The gate covers the snapshot/register cutover on the registration path and
the `apply_triples` call on the CDC path. This prevents a transaction after the
registration basis from being consumed by the global CDC loop before the new
query is present in the service registry.

---

## 4. Registration Basis

Registration creates a cutover point:

1. Capture the latest indexed transaction basis from SlateDB.
2. Plan the query against the current schema.
3. Scan the current EAV index up to the captured transaction entity.
4. Prime the DBSP circuit with positive triples from that snapshot.
5. Insert the registered query into the incremental service.
6. Start the CDC loop if it is not already running.

The subscription does not emit initial snapshot rows. It emits only transactions
after the returned basis.

Registration is serialized against CDC application. This prevents a race where
the global CDC loop consumes a transaction after the new query's snapshot basis
but before that query has been inserted into the incremental service. Without
that barrier, the new query could miss a future transaction.

The transaction basis may be in the middle of WAL availability. This is normal:
the CDC loop may later read a WAL file containing both transactions at or before
the registration basis and transactions after it. Each registered query filters
CDC transactions by transaction basis so it skips entries at or before its
registration basis and applies later ones.

---

## 5. CDC Flow

The writer node has one CDC loop for all incremental queries. It uses SlateDB
`WalReader` through Triplox's `CdcStream` helper. The loop decodes WAL entries
into transaction-sized batches, extracts EAV datoms using the current node
schema, and then applies the weighted triple delta to the incremental query
service. It does not wait on the writer indexer before applying a WAL entry;
in the current writer-node path, a WAL entry observed here has already passed
through the write/indexing path that produced it. The schema still comes through
the node boundary because CDC decoding needs the current ident and attribute
maps.

The CDC cursor is the WAL read-position marker. It contains the WAL file id
where reading should resume and the last SlateDB row sequence that should be
skipped because it has already been read. In the current writer-node
implementation this cursor is in-memory state owned by the live `CdcStream`;
it lets the stream park on `next_transaction().await`, reopen the current WAL
file when needed, and continue without rereading earlier rows.

This cursor is not yet a durable query checkpoint. Future crash recovery needs
a durable applied watermark that advances only after a WAL transaction has been
decoded, indexed, applied to DBSP, and delivered to subscriptions. That
watermark can then be used to restart without replay gaps; duplicate replay
handling also needs to be defined at that point.

For each transaction:

1. `CdcStream` yields one grouped WAL transaction.
2. EAV entries are decoded into datoms.
3. Transaction metadata is converted into a `TxBasis` when possible.
4. Datoms become a weighted batch of encoded triples.
5. The incremental service steps each registered query circuit that has not
   already advanced past this transaction.
6. Non-empty result deltas are sent on each subscription channel.
7. The live stream cursor advances as rows are read.

The current CDC state is in memory. This is sufficient for the writer-node-only
prototype, but it is not enough for crash recovery or reader nodes.

---

## 6. Subscription Lifecycle

Each registration returns:

- a query handle,
- the registration `TxBasis`, and
- a bounded Tokio channel of result deltas.

The channel capacity is 128. Deltas are ordered and lossless from the receiver's
point of view: dropping a delta would make the integrated result wrong. The
bounded channel therefore applies backpressure instead of silently discarding
updates.

Queries can be explicitly unregistered. They are also cleaned up when their
receiver is dropped. Cleanup removes the in-memory query state and deletes the
query's per-query DBSP storage directory.

---

## 7. Threading and Tuning Knobs

The current implementation has one incremental service thread per
`IncrementalQueryService`, not one thread per query. All registered queries are
stored in one registry and are stepped sequentially on that thread when an
`ApplyTriples` command arrives. Each registered query still owns a separate DBSP
circuit and separate DBSP storage directory.

The CDC reader is a Tokio task, not a dedicated OS thread. It parks on
`CdcStream::next_transaction().await`, decodes one transaction at a time, and
sends work to the service thread. Regular node APIs also run on Tokio runtime
threads.

Current tuning knobs are intentionally small and mostly hard-coded:

- `SUBSCRIPTION_CAPACITY` controls each result channel's bounded capacity.
  Raising it absorbs longer subscriber pauses at the cost of memory and lag;
  lowering it applies backpressure sooner.
- `CDC_POLL_INTERVAL` controls how often the CDC stream polls for new WAL
  transactions when no transaction is immediately available.
- `CircuitConfig::with_workers(1)` makes each query circuit single-worker
  today. Increasing DBSP workers would require checking circuit handle
  ownership, storage layout, and whether one service thread should still drive
  all query steps.
- The current service has one command loop for all queries. Future scaling
  options include sharding registered queries across service threads, grouping
  queries by database or workload, or introducing a worker pool for circuit
  stepping.
- Each query has its own circuit. Future sharing could reuse circuits,
  arrangements, or pattern streams across equivalent or overlapping queries.
- The initial scan currently scans EAV and filters needed attributes in memory.
  Attribute-specific scans are a straightforward read-path optimization.
- CDC currently applies one WAL transaction at a time. Future batching could
  coalesce multiple WAL transactions, but that would change delta granularity
  and basis reporting semantics.
- DBSP storage is file-backed per query. Cache sizing, storage roots,
  compaction, and checkpoint/restore policy are future operational controls.

The backpressure behavior is deliberate: result deltas are lossless state
changes. If a subscriber cannot keep up, the service thread blocks while sending
that query's delta instead of dropping it and corrupting the receiver's
integrated view.

---

## 8. What Is Missing

The current implementation is useful as the first execution path, but it is not
the final incremental query system.

Missing query features:

- `or` and `or-join`.
- `not` and `not-join`.
- Predicates and function expressions.
- Aggregates.
- `:in` bindings and query arguments.
- Ordering, limits, pull expressions, and public protocol support.

Missing operational features:

- Persistent CDC cursor state.
- Query checkpoint and restore.
- Durable recovery after process restart.
- Reader-node support.
- Public subscription API over the wire protocol.
- WAL retention coordination for incremental consumers.
- Detection and reporting of missing WAL file ranges.
- Restart or retry policy for CDC task errors.
- Observability for CDC lag, per-query lag, channel backpressure, and trace
  storage size.

Missing performance work:

- Attribute-specific initial scans instead of broad EAV scans.
- Shared circuits or shared arrangements across equivalent queries.
- Better planning for join order and disconnected groups.
- Batching and scheduling policies for many active queries.
- Storage cleanup and compaction policies for long-running query traces.

---

## 9. Direction

The next major step is to make incremental queries robust across restarts. That
requires a durable CDC cursor, a clear WAL-retention contract, and either DBSP
checkpoint restore or deterministic rebuild from a registration snapshot plus
WAL replay.

After restart behavior is defined, the public API can expose incremental query
subscriptions to clients. Until then, keeping the API crate-internal avoids
locking in wire semantics before the lifecycle and recovery model are stable.
