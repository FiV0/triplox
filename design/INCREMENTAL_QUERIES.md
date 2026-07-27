# Triplox Incremental Queries

Version 0.1

## Overview

Incremental queries let a caller register a query once and then receive result
deltas as new transactions are indexed. An incremental query does not return
a static result set, but rather a stream of changes between two consecutive
db values. The difference between these two db values is what affects the query
(in case it affects the query at all).

Every client should support some version of `subscribe(node, query)` which
registers an incremental query on the server. `subscribe` returns a stateful
object from which deltas can be retrieved and which can be closed. The closing
is important because an incremental query binds resources on the server that
otherwise won't get released.

Whenever the term query by itself appears in the following, it most likely means a incremental
query unless explicitly stated otherwise.

---

## Current Scope

Incremental queries use the shared query validation and an additional
incremental-query shape check. The supported query shape is:

- a relational `:find` containing only variables;
- fixed-attribute triple patterns;
- implicit `or` clauses, including nested `or` clauses; and
- `and` branches inside an `or`.

Patterns may use variables or constants in entity and value positions.
Attributes must be constant idents or entids. Source variables, transaction
positions, placeholders, and repeated variables within a pattern are not
supported. Explicit `or-join`, `not`, predicates, functions, `:with`, `:in`,
`:limit`, and `:order` are also not supported.

A query starts at the latest visible database value captured during
registration. The snapshot is the circuit's first batch. A non-empty snapshot
result is emitted as the first delta at the registration basis; later deltas
describe changes from subsequent transactions.

---

## Architecture

Incremental query planning and circuit construction are split across four
modules:

- `src/inc_query.rs` validates the incremental query shape and coordinates
  query planning.
- `src/inc_query/descriptor.rs` describes the semantic structure and binding
  properties of the `:where` tree.
- `src/inc_query/planner.rs` orders descriptors and lowers them to physical
  relation plans.
- `src/incremental/circuit.rs` assembles the relation plans into a per-query
  DBSP circuit.

`src/incremental.rs` owns registered query handles, channels, per-query circuit
instances, and the dedicated service thread. `src/incremental/cdc.rs` scans the
registration snapshot and feeds decoded SlateDB WAL transactions to the
service.

### Fact input and relation streams

Each registered query owns a DBSP circuit with one weighted fact input:

```text
EncodedTriple {
    entity: encoded DataType::Long(entity_id),
    attribute: attribute entid,
    value: encoded DataType,
}
```

Values are encoded bytes rather than `DataType` keys because DBSP Z-set keys
need ordering, and `DataType` contains values such as floats, maps, and vectors
that do not form a simple total order.

The fact input and an incoming relation are different inputs to plan-node
assembly. The fact input is the shared stream of `EncodedTriple` changes. An
incoming relation is the running `EncodedRow` stream produced while evaluating
the enclosing `:where` scope. It is internal to circuit assembly and is
unrelated to query `:in` bindings.

Every relation stream carries a variable layout alongside its rows. The layout
maps each variable to its positional encoded value in `EncodedRow`.

### Semantic descriptors

The descriptor tree mirrors nested query scopes:

```text
ScopeDescriptor {
    descriptors: [Descriptor],
    variables: [Variable],
    groundable: [Variable],
}

Descriptor {
    variables: [Variable],
    groundable: [Variable],
    kind: Pattern | Or { branches: [ScopeDescriptor] },
}
```

The top-level `:where` clauses form a scope. Each `or` is a descriptor whose
branches are scopes. An `and` branch is represented by a scope containing its
child descriptors, so nested scopes do not need global positions or flattened
identifiers.

`variables` lists the variables mentioned by a descriptor in stable semantic
order. `groundable` lists the variables that the descriptor can produce
without receiving them from an incoming relation:

| Descriptor | Variable order | Groundable variables |
| --- | --- | --- |
| Triple pattern | Entity variable, then value variable | All pattern variables |
| Scope / `and` | Ordered union of child variables | Ordered union of child groundable variables |
| `or` | First branch order | Variables groundable in every branch, in first branch order |

The planner derives a descriptor's required bindings as
`variables - groundable`. A descriptor is eligible once all required variables
are present in the running layout. Among eligible descriptors, the planner
prefers the descriptor sharing the most variables with the running relation
and uses scope order as the deterministic tie-breaker.

### Physical relation plans

Every physical relation plan records:

```text
RelPlan {
    incoming_vars: optional [Variable],
    output_vars: [Variable],
    kind: Pattern | Chain | Union,
}
```

`incoming_vars` is absent when the node starts a relation and present when it
extends a running relation. This distinction also preserves a present
zero-column relation.

- `Pattern` filters the fact input by attribute and constants. With an incoming
  relation, circuit assembly joins the filtered pattern rows to that relation
  using the plan's incoming, pattern, and output layouts.
- `Chain` represents a scope with multiple descriptors and passes each child's
  output relation to the next child.
- `Union` passes the same incoming relation to every branch and declares one
  output layout for all branch results.

`Chain` is a physical plan shape rather than a DBSP operator. Circuit assembly
uses the existing `flat_map`, join, projection, sum, and distinct operators.

### Row layouts

A standalone pattern outputs its pattern variable order. A node receiving an
incoming relation preserves that layout and appends only newly produced
variables in semantic order. Join keys are the variables shared by both sides,
in incoming-layout order. A join without shared variables is a Cartesian
product over the empty key.

A chain's output layout is its last child's output layout. A standalone union
uses its descriptor variable order. A union with an incoming relation preserves
the incoming layout and appends the variables groundable by the union. Because
branches may naturally produce their columns in different orders, every branch
is projected to the union's declared output layout before the branches are
summed.

The circuit verifies that the running relation layout matches each plan node's
declared incoming layout. It does not derive a different variable order during
assembly.

### Circuit assembly

Circuit assembly recursively consumes the shared fact input, a relation plan,
and the optional incoming relation:

- a pattern creates matching rows with `flat_map` and joins them to the
  incoming rows when present;
- a chain folds the running relation through its children; and
- a union evaluates each branch with the same incoming relation, projects the
  branch results to the union layout, sums them, and applies `distinct`.

The completed `:where` relation is projected to the query's `:find` variable
order. DBSP then maintains this circuit and emits signed result deltas for each
applied transaction.

The DBSP state is trace-backed and configured with file-backed storage.
Triplox does not keep full accumulated relation Z-sets in ordinary Rust memory as the
query state. The trace files get currently deleted when a query gets unregistered.
Future work should consider query restart and DBSP checkpointing.

---

## Runtime Flow

The main runtime boundary from a node to incremental queries is `IncrementalQueryService`.
The node holds an `IncrementalQueryService`. It is the coordination boundary between the
node and the threads/runtime/executor dealing with the state and the execution of incremental queries.

Registration flows through the system as follows:

```text
Node::register_incremental_query
    delegates to IncrementalQueryService::register_query

IncrementalQueryService::register_query
    captures the latest indexed TxKey and schema from the indexer
    plans the query
    scans the initial triples
    captures the WAL cursor
    sends Register to the dedicated service thread
    starts the CDC loop after Register returns, if not already running

triplox-incremental-query thread
    builds one QueryCircuit for the plan
    primes it with the initial triples
    queues the non-empty priming result at the registration basis
    stores the plan, circuit, basis, WAL cursor, and subscription sender
    returns an IncrementalQuerySubscription
```

Node should stay relative clean and should not know anything about internal circuit maintenance and
initialization logic. The `IncrementalQueryService` should deal with the lifecycle of incremental
queries and their state. The dedicated service thread is where things get executed. This is currently
single threaded which won't scale we add incremental queries to the service and we should pass to some
executor service in the future. The `IncrementalQueryService`
sends `IncrementalCommand`s to the one dedicated service thread. That thread owns the registry
of active queries and every query's DBSP circuit.

Live WAL application reaches the dedicated service thread through the same
command channel, but it enters from the internal side rather than the node side.
A spawned CDC task forwards triples as they arrive through WAL decoding.
It drives circuits by calling `apply_triples` on a clone of the same
`IncrementalQueryService` handle, which forwards an `ApplyTriples` command over
the same channel:

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
  subscribers. The service thread retries bounded sends, so a slow subscriber
  applies backpressure instead of losing deltas; node cancellation breaks that
  wait during shutdown.

In the future the server should drain these subscribers eagerly server side and send
the appropriate deltas to the corresponding clients. The clients then need to deal
with the backpressure themselves, depending on the client implementation.

`IncrementalQueryDelta` is a subscriber-facing result batch emitted after a
circuit step. The first delta is the non-empty priming result at the registration
basis; later deltas describe transactions after that basis. The command protocol
exists to serialize mutations to the query registry and DBSP circuits onto the
single service thread.

Registration is serialized against the application of CDC changes by a registration gate owned
by `IncrementalQueryService`. `register_query` holds the gate across its whole
body (basis capture, initial db capture, and the `Register` round-trip), and the
CDC loop holds the same gate around each `apply_triples`. This makes the
snapshot/register cutover atomic with respect to CDC application: a transaction
after the registration basis cannot be consumed by the global CDC loop before the
new query is present in the service registry. The cutover boundary itself is the
registration `TxKey` — the global CDC loop reads the WAL from the beginning and
each query skips transactions at or before its own `tx_id`. I think in the future
this serialization across WAL application, basis capture and circuit initialization
is too restrictive and we need to built something more multi-threaded.

---

## Registration Basis

Registration creates a cutover point:

1. Capture the latest indexed transaction basis from SlateDB.
2. Plan the query against the current schema.
3. Scan the current EAV index up to the captured transaction entity.
4. Prime the DBSP circuit with positive triples from that snapshot.
5. Insert the registered query into the incremental service.
6. Start the CDC loop if it is not already running.

If the initial query result is non-empty, registration queues it as the first
delta with the registration `TxKey`. Empty priming results are omitted. Later
deltas describe transactions after the returned basis.

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

## CDC Flow

The node has one CDC loop for all incremental queries. It uses SlateDB
`WalReader` through Triplox's `CdcStream` helper. The loop decodes WAL entries
into transaction-sized batches, extracts EAV datoms using the current node
schema (future optimizations should apply decoding more fine-grained per query),
and then applies the weighted triple delta to the incremental query
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

This CDC cursor is currently not saved anywhere persistently. If a node
crashes the query needs to be registered anew and populated anew.

For each transaction:

1. `CdcStream` yields one grouped WAL transaction.
2. EAV entries are decoded into datoms.
3. Transaction metadata is converted into a `TxKey` when possible.
4. Datoms become a weighted batch of encoded triples.
5. The incremental service steps each registered query circuit that has not
   already advanced past this transaction.
6. Non-empty result deltas are sent on each subscription channel.
7. The live stream cursor advances as rows are read.

---

## Subscription Lifecycle

Each registration returns:

- a query handle,
- the registration `TxKey`, and
- a bounded Tokio channel of result deltas.

Queries can be explicitly unregistered. They are also cleaned up when their
receiver is dropped. Cleanup removes the in-memory query state and deletes the
query's per-query DBSP storage directory.

---

## Threading and Tuning Knobs

The current tuning knobs:

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
  options include sharding registered queries across service threads
  or introducing a worker pool for circuit stepping.

---

## Direction

Currently the incremental query API is very bare bones. There is no way to
specify the `TxKey` that a query should start at. There is now way
to stop and restart a query without reinitializing the circuit. All these
things are possible, but need more thought and careful analysis for the
state management.

I have also been exploring an incremental join algorithm incorporating aspects
of WCOJ in Hooray. I want to bring these ideas to Triplox to see if
the incremental join on graph patterns could be improved at scale and if
there is a need for this kind of algorithms.

### Further optimizations

There are quite a few optimizations that can be done for incremental queries.
These should be tracked in the issue Tracker. In no particular order, they are:
- Filter relevant triples via fixed attributes and other constants for circuit initialization. Use the AVE index.
- Initialize the circuits in batches. At scale the current approach won't work.
- Make use of Triplox temporal indexes in base triple patterns. This will avoid
save a lot of space in the incremental circuits.
- Shared circuits or shared arrangements across equivalent queries.
- Cost-based join ordering beyond the shared-variable heuristic.
- Batching and scheduling policies for many active queries.
- Storage cleanup and compaction policies for long-running query traces.
- CDC currently applies one WAL transaction at a time. Future batching could
  coalesce multiple WAL transactions, but that would change delta granularity
  and basis reporting semantics.
- DBSP storage is file-backed per query. Cache sizing, storage roots,
  compaction, and checkpoint/restore policy are future operational controls.

### Cleanup

Currently the cleanup of queries happens when the incremental query gets unregistered
or the node shuts down. There is no cleaner process. I think we should consider
a client heartbeat (which is also useful for other purposes) and close incremental
queries if we have the impression the client is dead.
