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

The incremental query engine will lag behind the standard query engine in features in most
cases. The idea is to use the same validation logic for standard queries also for
incremental queries and a further more retraining check for incremental queries
that at some point should get removed when parity between the two query paths
is reached.

Any approach for incremental query compilation should be first tested in hooray
(https://github.com/FiV0/hooray2), it is the test bed for new join algorithms
in Triplox.

Initially we are focusing on queries that are started at the newest (as in most recently visible)
DB value. In theory nothing prevents us from replaying transactions from an arbitrary DB
value in the past as all the old data sits in the covering indexes,
but this requires a different approach of piping the old transactions through
the circuits compared to just tailing the WAL files of SlateDB. I am not
saying it is out of scope, it is just out of scope for now.

A query starts at the latest visible database value captured during
registration. A non-empty delta is emitted if the priming of the circuit produces results;
later deltas describe changes from subsequent transactions.

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
  DBSP circuit. `src/incremental/circuit/aggregates.rs` builds aggregate result
  streams.

`src/incremental.rs` owns registered query handles, channels, per-query circuit
instances, and the dedicated service thread. `src/incremental/cdc.rs` scans the
triples at the registration tx-key and feeds decoded SlateDB WAL transactions to the
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
    kind: Pattern
        | Predicate { expr: Expr }
        | Function { expr: Expr, output_var: Variable }
        | Not { scope: ScopeDescriptor }
        | Or { branches: [ScopeDescriptor] },
}
```

The top-level `:where` clauses form a scope. Each `or` is a descriptor whose
branches are scopes. An `and` branch is represented by a scope containing its
child descriptors and has no descriptor representation of its own. An implicit
`not` is a descriptor whose body is another scope.

`variables` lists the variables mentioned by a descriptor in stable semantic
order. `groundable` lists the variables that the descriptor can produce
without receiving them from an incoming relation. A pattern can ground all of
its variables. A function grounds its result variable unless the expression
also reads that variable. An `or` can ground the intersection of variables
groundable by all branches. Predicates and `not` ground no variables, so every
variable they mention must already be available from the enclosing positive
relation.

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
    kind: Pattern | Filter | Function | Chain | Difference | Union,
}
```

`incoming_vars` is absent when the node starts a relation and present when it
extends a running relation. This distinction also preserves a present
zero-column relation.

- `Pattern` filters the fact input by attribute and constants. With an incoming
  relation, circuit assembly joins the filtered pattern rows to that relation
  using the plan's incoming, pattern, and output layouts.
- `Filter` evaluates one compiled predicate against each incoming row and
  preserves the row and its weight only when the predicate returns true.
- `Function` evaluates one compiled expression against each incoming row. It
  appends a new result column or filters against an existing result binding.
- `Chain` represents a scope with multiple descriptors and passes each child's
  output relation to the next child.
- `Difference` preserves the incoming relation and removes rows whose selected
  key occurs in its negative subplan. The negative scope is seeded by projecting
  the incoming relation to that key.
- `Union` passes the same incoming relation to every branch and declares one
  output layout for all branch results.

`Chain` is a physical plan shape rather than a DBSP operator. Circuit assembly
uses the existing `flat_map`, filter, join, projection, antijoin, sum, and
distinct operators.

### Row layouts

A standalone pattern outputs its pattern variable order. A node receiving an
incoming relation preserves that layout and appends only newly produced
variables in semantic order. Join keys are the variables shared by both sides,
in incoming-layout order. A join without shared variables is a Cartesian
product over the empty key.

A chain's output layout is its last child's output layout. A standalone union
uses its descriptor variable order. A union with an incoming relation preserves
the incoming layout and appends its remaining descriptor variables; descriptor
ordering guarantees that variables the union cannot ground are already part of
the incoming layout. Because branches may naturally produce their columns in
different orders, every branch is projected to the union's declared output
layout before the branches are summed.

The circuit verifies that the running relation layout matches each plan node's
declared incoming layout. It does not derive a different variable order during
assembly.

### Circuit assembly

Circuit assembly recursively consumes the shared fact input, a relation plan,
and the optional incoming relation:

- a pattern creates matching rows with `flat_map` and joins them to the
  incoming rows when present.
- a filter decodes its referenced columns, evaluates the compiled expression,
  and filters the incoming rows without changing their layout or weights.
- a function decodes its inputs and either appends the encoded result with
  `flat_map` or filters rows whose existing result binding differs. Failed
  expression evaluation drops the row.
- a chain folds the running relation through its children.
- a difference projects the incoming rows to the negative key, evaluates the
  negative scope from that raw projection, and antijoins the original incoming
  rows against the resulting keys.
- a union evaluates each branch with the same incoming relation, projects the
  branch results to the union layout, sums them, and applies `distinct`.

The negative seed is not made distinct before evaluation. Its weights remain
correlated with the positive input, while DBSP antijoin treats the resulting
negative key relation by presence. This gives the expected add/retract behavior
when a key has multiple negative matches.

For a non-aggregate query, the completed `:where` relation is projected to the
query's `:find` variable order. An aggregate query passes that relation to the
aggregate assembly described below.

### Aggregate assembly

The find plan identifies the non-aggregate find variables as the group key and
records all find elements in projection order. Circuit assembly indexes the
completed `:where` rows by that group key and builds one independent result
stream for each aggregate expression. It then joins those streams by group and
restores group values and aggregate values to find-clause order. Equivalent
aggregate expressions are not deduplicated.

A global aggregate has an empty group key. The circuit seeds that group so an
empty input produces `0` for `count`, `count-distinct`, and `sum`. `avg`, `min`,
and `max` produce no row for an empty input.

`min` and `max` currently use a rather convoluted way to produce a result. The
main reason is duplicated behaviour for dealing with different non-comparable
types (i.e. "string" vs "int") and that the existing Min implementations from
the DBSP crate do not carry errors through the circuits, because `min`/`max`
can only produce a value from either left or right, without producing a
new value (error) and the signatures for the standard implementation does not
produce a `Result` value.

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
    stores the basis, routed WAL cursor, inbox sender, and subscription liveness
    spawns a per-query worker task that owns the circuit
    returns an IncrementalQuerySubscription
```

`IncrementalQueryService` owns circuit initialization and query lifecycle. Its dedicated
router thread owns the registry and handles `IncrementalCommand`s in order. Building and
priming a circuit still happen synchronously on that thread, preserving registration
errors. After registration, one task per query owns and steps its circuit on the
service's own Tokio runtime.

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
    try_sends the same Arc-backed batch to each relevant query's bounded inbox
    acknowledges fan-out without waiting for circuit steps or delivery

per-query worker task
    consumes its inbox in FIFO order
    acquires a shared semaphore permit
    copies and applies the batch inside spawn_blocking
    releases the permit
    sends non-empty IncrementalQueryDelta values to that query's receiver
```

The channels have distinct roles:

- `std::sync::mpsc` carries commands into the service thread. It fits the
  dedicated thread, which periodically checks a separate retirement channel.
- Bounded `tokio::sync::mpsc` inboxes carry shared transaction batches to workers.
  One producer and one consumer per inbox preserve WAL order for each query.
- Bounded `tokio::sync::mpsc` result channels carry `Result<IncrementalQueryDelta>`
  to subscribers. Delivery awaits channel capacity outside the semaphore permit,
  so a stalled receiver occupies neither a step permit nor a runtime thread.
- A separate `std::sync::mpsc` channel reports completed worker teardown. Workers
  hold no command senders, so dropping the last service handle can stop the router.

A slow subscriber receives every delta while its worker inbox stays within capacity.
Overflow terminates that subscription with a typed `SubscriptionLagError`; other
queries and CDC continue. Sending the terminal error has a timeout because the result
channel may also be full. If it times out, buffered deltas are followed by end-of-stream
without an error frame. The server forwards deltas subject to HTTP/2 flow control.

Batches are shared via `Arc` across inboxes. Each inbox holds at most its configured
number of pending batches, plus one transaction can be in flight per worker. Each
step makes its own copy because DBSP's input API requires an owned buffer. Result
channels and circuit state consume additional memory; queue capacities bound batch
counts, not bytes.

`IncrementalQueryDelta` is a subscriber-facing result batch emitted after a
circuit step. The first delta is the non-empty priming result at the registration
basis; later deltas describe transactions after that basis. An aggregate failure
is preserved as a typed error through the circuit and service channel. A priming
failure rejects registration. A live failure removes only the affected query,
sends an error subject to the terminal-send timeout, and closes that subscription.
The server encodes terminal errors, including lag and worker panics, as `QueryError`
(2001); no new wire error code is required. A supervisor isolates worker panics and
completes retirement. Registry mutations are serialized by the router, while each
worker serializes its own circuit steps.

The internal `Flush` command captures each active query's routed transaction count.
It waits for workers to report delivery through that count, or to retire. Barriers
use delivery progress notifications rather than inbox slots, so flushing a full inbox
does not itself trigger lag termination. A flush does not wait for future transactions.

Registration is serialized against the application of CDC changes by a registration gate owned
by `IncrementalQueryService`. `register_query` holds the gate across its whole
body (basis capture, initial db capture, and the `Register` round-trip), and the
CDC loop holds the same gate around each `apply_triples`. This makes the
snapshot/register cutover atomic with respect to CDC application: a transaction
after the registration basis cannot be consumed by the global CDC loop before the
new query is present in the service registry. The cutover boundary itself is the
registration `TxKey` — the global CDC loop reads the WAL from the beginning and
the router skips transactions at or before each query's basis `tx_id` before enqueueing.
Filtering before enqueueing prevents historical WAL replay from overflowing a new
query's inbox. The gate still covers the initial scan, circuit build, and priming;
moving registration work off the router is deferred. Re-registering a terminated
query therefore still pauses CDC routing during its scan and initialization.

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
5. The router enqueues the batch for queries whose registration basis precedes it.
6. Workers independently step circuits and deliver non-empty result deltas in order.
7. The live stream cursor advances as rows are read; the service acknowledges enqueueing,
   so cursor progress does not imply that all subscribers have received the transaction.

---

## Subscription Lifecycle

Each registration returns:

- a query handle,
- the registration `TxKey`, and
- a bounded Tokio channel of result deltas.

DBSP query state is trace-backed and stored in a per-query directory instead of
being accumulated in ordinary Rust memory. Registration failure removes its directory
synchronously. After registration, receiver closure, circuit failure, inbox overflow,
worker panic, explicit unregistration, and shutdown all retire the affected query.

Retirement drops the circuit, joining its DBSP threads, before removing its directory.
Normal teardown runs in `spawn_blocking`. Only then does the worker report retirement
to the router. Cleanup errors reach a pending unregister caller, or are logged for
asynchronous retirement; they do not fail unrelated commands. An unregister reply waits
for that query's teardown while the router continues processing other commands.

Receiver closure is detected even without another transaction. The router keeps a weak
subscriber sender so it can inspect liveness without delaying end-of-stream. Query IDs
are monotone within a service, so retiring queries cannot remove a newer query's storage.

Shutdown cancels workers and waits up to `RETIRE_TIMEOUT` for retirements. Dropping the
service uses the shorter `DROP_TIMEOUT`, and runtime shutdown also has a bounded wait.
A DBSP step or teardown cannot be forcibly interrupted. Workers still running at the
deadline are logged with their storage paths; their directories are left intact until
safe cleanup is possible. A later build reclaims stale storage at its query path.

---

## Threading and Tuning Knobs

The current tuning knobs:

- `SUBSCRIPTION_CAPACITY` defaults to 128 and controls each result channel's capacity.
  Raising it absorbs longer subscriber pauses at the cost of memory and lag;
  lowering it makes the worker wait on delivery sooner.
- `QUERY_INBOX_CAPACITY` defaults to 256 pending transactions per query. Overflow
  terminates that subscription instead of blocking the router.
- `MAX_CONCURRENT_STEPS` defaults to 4. A FIFO-fair semaphore bounds concurrent
  `spawn_blocking` circuit steps. The service has two async runtime threads and
  a blocking pool sized to the step limit plus four teardown slots.
- `RETIRE_TIMEOUT` defaults to ten seconds for terminal error delivery and shutdown
  retirement. `DROP_TIMEOUT` limits best-effort drop cleanup to one second.
- `IncrementalServiceConfig` supplies these service limits at construction time;
  tests use small capacities. They are not TOML configuration options.
- `CDC_POLL_INTERVAL` controls how often the CDC stream polls for new WAL
  transactions when no transaction is immediately available.
- `CircuitConfig::with_workers(1)` makes each query circuit single-worker
  today. Increasing DBSP workers would require checking circuit handle
  ownership and storage layout. Each circuit still owns its DBSP threads; the
  service runtime multiplexes driver tasks and does not reduce DBSP thread count.

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

Cleanup is currently event-driven; there is no separate process for detecting
clients that are no longer responsive. A client heartbeat could provide that
liveness signal and allow the service to close abandoned incremental queries.
