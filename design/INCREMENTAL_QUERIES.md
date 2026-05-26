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

## 3. Registration Basis

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

## 4. CDC Flow

The writer node has one CDC loop for all incremental queries. It uses SlateDB
`WalReader` through Triplox's `CdcStream` helper. The loop decodes WAL entries
into transaction-sized batches, extracts EAV datoms, waits for the writer node's
memory indexes to contain the corresponding transaction, and then applies the
weighted triple delta to the incremental query service.

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
4. The CDC task waits until that transaction has been indexed.
5. Datoms become a weighted batch of encoded triples.
6. The incremental service steps each registered query circuit.
7. Non-empty result deltas are sent on each subscription channel.
8. The live stream cursor advances as rows are read.

The current CDC state is in memory. This is sufficient for the writer-node-only
prototype, but it is not enough for crash recovery or reader nodes.

---

## 5. Subscription Lifecycle

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

## 6. What Is Missing

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

## 7. Direction

The next major step is to make incremental queries robust across restarts. That
requires a durable CDC cursor, a clear WAL-retention contract, and either DBSP
checkpoint restore or deterministic rebuild from a registration snapshot plus
WAL replay.

After restart behavior is defined, the public API can expose incremental query
subscriptions to clients. Until then, keeping the API crate-internal avoids
locking in wire semantics before the lifecycle and recovery model are stable.
