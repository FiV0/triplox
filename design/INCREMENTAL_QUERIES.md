# Triplox Incremental Queries

Version 0.1

## Overview

An incremental query is a query that reports changes to its result. A caller
registers the query one time. After that, the caller receives result deltas as
the indexer indexes new transactions. A delta is a set of added and retracted
result rows. An incremental query does not return a static result set. It
returns a stream of changes between two consecutive database values. A database
value is the state of the database at one transaction. The difference between
the two database values is the part that affects the query, if it affects the
query at all.

Every client must support some version of `subscribe(node, query)`. This
function registers an incremental query on the server. `subscribe` returns a
stateful object. The caller can get deltas from this object and can close it.
The caller must close the object, because an incremental query holds resources
on the server. Without the close, the server does not release these resources.

In the rest of this document, the word "query" alone usually means an
incremental query. Where a passage means a different query, it says so.

---

## Current Scope

In most cases, the incremental query engine will have fewer features than the
standard query engine. The plan is to use the same validation logic for
standard queries and for incremental queries. Incremental queries also get a
second, more restrictive validation. When the two query paths have the same
features, we will remove this second validation.

We must test each approach to incremental query compilation in Hooray
(https://github.com/FiV0/hooray2) first. Hooray is the test bed for new join
algorithms in Triplox.

At first, we focus on queries that start at the newest database value, that is,
the most recently visible one. In theory, we can also replay transactions from
any database value in the past. All the old data stays in the covering indexes
(indexes that keep all the data in their keys). But this replay requires a
different approach than tailing the WAL (write-ahead log) files of SlateDB. A
circuit is the DBSP dataflow graph that computes the deltas of one query. The
replay must send the old transactions through the circuits in a different way.
I do not say that it is out of scope. It is only out of scope at this time.

A query starts at the latest visible database value that registration captures.
Priming is the first run of the circuit on the existing data. If the priming of
the circuit produces results, the query emits a non-empty delta. Each later
delta describes the changes from the `TxKey` of the previous delta up to its
own `TxKey`. A later delta can cover several transactions.

---

## Architecture

Four modules share the incremental query planning and the circuit construction:

- `src/inc_query.rs` validates the incremental query shape and coordinates
  query planning.
- `src/inc_query/descriptor.rs` describes the semantic structure and binding
  properties of the `:where` tree.
- `src/inc_query/planner.rs` orders descriptors and lowers them to physical
  relation plans. A descriptor is the semantic summary of one `:where`
  clause. A relation plan is the plan for one stream of rows.
- `src/incremental/circuit.rs` assembles the relation plans into a DBSP circuit
  for each query. `src/incremental/circuit/aggregates.rs` builds aggregate
  result streams.

`src/incremental.rs` owns the registered query handles, the channels, the
circuit instance of each query, and the dedicated service thread.
`src/incremental/cdc.rs` scans the triples at the registration `TxKey`. It also
feeds the decoded SlateDB WAL files to the service.

### Fact input and relation streams

A weight is a signed count of a fact. Each registered query owns a DBSP
circuit with one weighted fact input:

```text
EncodedTriple {
    entity: encoded DataType::Long(entity_id),
    attribute: attribute entid,
    value: encoded DataType,
}
```

A Z-set is a collection of keys with weights. DBSP Z-set keys need an order.
In a total order, every two values compare. `DataType` contains values such as
floats, maps, and vectors that do not form a simple total order. Thus the
values are encoded bytes, not `DataType` keys.

The fact input and an incoming relation are different inputs to plan-node
assembly. The fact input is the shared stream of `EncodedTriple` changes. An
incoming relation is the running `EncodedRow` stream that the evaluation of the
enclosing `:where` scope produces. It is internal to circuit assembly. It has no
relation to the query `:in` bindings.

Each relation stream carries a variable layout together with its rows. The
layout maps each variable to the position of its encoded value in `EncodedRow`.

### Semantic descriptors

The descriptor tree has the same shape as the nested query scopes:

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

The top-level `:where` clauses form a scope. Each `or` is a descriptor, and its
branches are scopes. An `and` branch is a scope that contains its child
descriptors. An `and` branch has no descriptor of its own. A `not` or
`not-join` is a descriptor, and its body is another scope.

`variables` lists the variables that are visible at a descriptor boundary.
`groundable` lists the variables that the descriptor can produce without
receiving them from an incoming relation. A pattern can ground all of its
variables. A function grounds its result variable, unless the expression also
reads that variable. An `or` can ground the intersection of the variables that
all branches can ground. Predicates and `not` ground no variables. Thus each
variable that they expose must already come from the enclosing positive
relation. A `not` exposes the variables that its body mentions. A `not-join`
exposes only its declared variables, and its body binds the other variables
locally.

The planner derives the required bindings of a descriptor as
`variables - groundable`. When all required variables are in the running
layout, the descriptor is eligible. Among the eligible descriptors, the planner
selects the descriptor that shares the most variables with the running
relation. If two descriptors are equal, the scope order decides.

This eligibility rule does not resolve the dependencies inside OR branches
before it selects the OR. Thus the planner can select an OR with circular
function dependencies before an outside clause that supplies a necessary
binding. The result is that the planner rejects a query that can execute.

### Physical relation plans

Each physical relation plan records:

```text
RelPlan {
    incoming_vars: optional [Variable],
    output_vars: [Variable],
    kind: Pattern | Filter | Function | Chain | Difference | Union,
}
```

If the node starts a relation, `incoming_vars` is absent. If the node extends a
running relation, `incoming_vars` is present. This difference also keeps a
present relation with zero columns.

- `Pattern` filters the fact input by attribute and constants. If an incoming
  relation exists, circuit assembly joins the filtered pattern rows to that
  relation. The join uses the incoming, pattern, and output layouts of the plan.
- `Filter` evaluates one compiled predicate against each incoming row. If the
  predicate returns true, the filter keeps the row and its weight.
- `Function` evaluates one compiled expression against each incoming row. It
  appends a new result column or filters against an existing result binding.
- `Chain` represents a scope with more than one descriptor. It passes the
  output relation of each child to the next child.
- `Difference` keeps the incoming relation and removes the rows whose selected
  key occurs in its negative subplan. A projection of the incoming relation to
  that key is the seed of the negative scope.
- `Union` records its join variables separately from its full output layout.
  The branches receive only the incoming columns for those variables. Before
  the union, a projection removes the local output columns. Both `or` and
  `or-join` use this operator.

`Chain` records the evaluation order of its child plans. Circuit construction
connects the output stream of each child to the input of the next child. It
uses the existing `flat_map`, filter, join, projection, antijoin, sum, and
distinct operators. An antijoin keeps the left rows that have no right match.

### Row layouts

A standalone pattern outputs the variables in pattern order. A node that
receives an incoming relation keeps that layout. It appends only the newly
produced variables, in descriptor variable order. The join keys are the
variables that both sides share, in incoming-layout order. A join without
shared variables is a Cartesian product over the empty key. A Cartesian product
pairs each left row with each right row.

The output layout of a chain is the output layout of its last child. A
standalone union uses its descriptor variable order. A union with an incoming
relation keeps the incoming layout and appends its remaining descriptor
variables. The descriptor order guarantees that the incoming layout already
contains the variables that the union cannot ground. The branches can produce
their columns in different orders. Thus each branch gets a projection to the
join variables of the union before the sum of the branches. Explicit `or-join`
branches can also have local columns that this projection removes.

The circuit makes sure that the running relation layout matches the declared
incoming layout of each plan node. It does not derive a different variable
order during assembly.

### Circuit assembly

Circuit assembly recursively consumes the shared fact input, a relation plan,
and the optional incoming relation:

- A pattern creates matching rows with `flat_map`. If incoming rows exist, it
  joins the matching rows to them.
- A filter decodes its referenced columns and evaluates the compiled
  expression. It filters the incoming rows and does not change their layout or
  weights.
- A function decodes its inputs. It appends the encoded result with
  `flat_map`, or it filters out the rows where the existing result binding is
  different. If the expression evaluation fails, the function drops the row.
- A chain folds the running relation through its children.
- A difference projects the incoming rows to the negative key. It evaluates the
  negative scope from that raw projection. Then it antijoins the original
  incoming rows against the resulting keys. The local `not-join` columns are
  not part of the key.
- A union projects the incoming rows to the join variables that are already
  bound, and keeps their weights. It evaluates each branch with those rows and
  projects the results to all join variables. Then it applies `distinct` to the
  sum of the results. Then it joins back to the original incoming stream on the
  shared variables, to keep the outer columns and weights. If no incoming
  relation exists, it returns the distinct union directly.

The negative seed is the input of the negative scope. It is not made distinct
before the evaluation. Its weights stay correlated with the positive input. But
the DBSP antijoin looks only at whether a key is present in the negative key
relation. When a key has more than one negative match, this gives the expected
add and retract behavior.

For a non-aggregate query, a projection puts the completed `:where` relation
into the `:find` variable order of the query. An aggregate query passes that
relation to the aggregate assembly, which the next section describes.

### Aggregate assembly

The find plan uses the non-aggregate find variables as the group key. It
records all find elements in projection order. Circuit assembly indexes the
completed `:where` rows by that group key. It builds one independent result
stream for each aggregate expression. Then it joins those streams by group. It
puts the group values and aggregate values back into find-clause order.
Circuit assembly does not remove duplicates of equal aggregate expressions.

A global aggregate is an aggregate without group variables. It has an empty
group key. The circuit seeds that group, so an empty input produces `0` for
`count`, `count-distinct`, and `sum`. `avg`, `min`, and `max` produce no row
for an empty input.

`min` and `max` currently use a complex and indirect way to produce a result.
There are two main reasons. First, the behavior for different types that do
not compare (for example, "string" and "int") is duplicated. Second, the
existing Min implementations of the DBSP crate do not carry errors through the
circuits. `min` and `max` can only select a value from the left or the right
side. They cannot produce a new value, such as an error. Also, the signatures of
the standard implementation do not produce a `Result` value.

---

## Runtime Flow

The main runtime boundary from a node to the incremental queries is
`IncrementalQueryService`. The node holds an `IncrementalQueryService`. This
service is the coordination boundary between the node and the threads, runtime,
and executor that hold the state of incremental queries and execute them.

Registration goes through the system as follows:

```text
Node::register_incremental_query
    delegates to IncrementalQueryService::register_query

IncrementalQueryService::register_query
    captures the latest indexed TxKey and schema from the indexer
    plans the query
    scans the initial triples
    sends Register to the dedicated service thread
    starts the CDC loop after Register returns, if not already running

tpx-iq-service thread
    builds one QueryCircuit for the plan
    primes it with the initial triples
    queues the non-empty priming result at the registration basis
    creates a bounded input queue and starts an async worker owning the circuit
    stores the basis, routing state, and worker controls
    returns an IncrementalQuerySubscription
```

`IncrementalQueryService` owns query registration and lifecycle management.
Its service thread runs a dispatcher on a Tokio runtime that the service owns.
The dispatcher is the task that routes commands to the query workers. The
dispatcher owns the registry, and each async query worker owns its circuit.
Circuit build and priming use a blocking job, that is, a job on the blocking
thread pool. But the dispatcher still waits for the registration to finish
before it accepts the next command.

Live WAL application gets to the dedicated service thread through the same
command channel. But it enters from the internal side, not from the node side.
CDC (change data capture) is the process that reads new changes from the WAL. A
spawned CDC task forwards the triples as the WAL decoding produces them. The
task drives the circuits through calls to `apply_batch` on a clone of the same
`IncrementalQueryService` handle. This handle forwards an `ApplyBatch` command
over the same channel:

```text
Tokio CDC task
    reads one WAL file
    decodes it to datoms using the node schema
    converts datoms to weighted EncodedTriple tuples tagged with their tx id
    sends ApplyBatch to IncrementalQueryService

tpx-iq-service thread
    loops over registered queries
    drops the transactions at or before each query basis
    enqueues the shared batch (or its trimmed remainder) into each relevant query's input queue
    acknowledges ApplyBatch after fan-out

async query worker
    receives batches in FIFO order
    acquires a shared execution permit
    copies only triples with its patterns' attributes on the runtime's blocking pool
    steps the circuit unless none of those attributes are present
    releases the permit
    sends non-empty IncrementalQueryDelta values to its subscription
```

Tokio channels carry the commands into the dispatcher, the WAL batches into the
workers, and the results back to the subscribers. The workers report the
completed cleanup on a separate channel. Thus they do not keep the service
command channel alive.

`ApplyBatch` acknowledges the routing, not the completed circuit step. Fan-out
is the step that sends a batch to each relevant query. Each query has one FIFO
(first in, first out) consumer and at most one apply in flight. Different
queries advance independently. The registration basis is the last transaction
in the snapshot of a query. The dispatcher filters by registration basis
before it enqueues a batch. Thus WAL history that a snapshot already includes
cannot overflow the input queue of a new query.

Shared `Arc` batches prevent deep copies during fan-out. Each batch is sorted
by attribute one time. Each blocking apply copies only the triples with
the attributes of its query. If a query has none of these attributes, the query
skips the step. Priming always steps the circuit. After the priming, an empty
input cannot change the result.

An execution permit is a token that limits parallel circuit steps. If the
result channel of a query is full, only the worker of that query pauses. The
worker pauses after it releases its execution permit. If the input queue of a
query fills, the dispatcher stops the routing to that query. Then it
terminates the query with a lag error, an error for a query that falls
behind. The dispatcher discards the pending inputs.

`IncrementalQueryDelta` is a result batch for the subscriber. It comes after a
circuit step. The first delta is the non-empty priming result at the
registration basis. Later deltas describe the WAL batches after that basis.
Each later delta carries the `TxKey` of the last transaction that it covers. If
the priming fails, the service rejects the registration. A live failure
removes only the affected query.

A terminal channel is a separate channel for the final error of a query. This
channel prevents a full output queue from hiding an error. Ordinary query errors
come after the results that the service queued before them. Lag errors come
before the buffered results. Both become terminal `QueryError` frames on the
wire.

`IncrementalQueryService` owns a registration gate, a lock that registration
and CDC share. This gate serializes the registration against the application
of CDC changes. `register_query` holds the gate for its whole body. The body
includes the basis capture, the initial database capture, and the `Register`
round-trip. The CDC loop holds the same gate around each `apply_batch`. The
cutover is the switch from snapshot data to live WAL data. The gate makes the
snapshot/register cutover atomic with respect to the CDC application. Thus the
global CDC loop cannot consume a transaction after the registration basis before
the new query is in the service registry. The registration `TxKey` is the
boundary of the cutover. The global CDC loop reads the WAL from the beginning.
Each query drops the transactions at or before its own `tx_id`.

---

## Registration Basis

Registration creates a cutover point:

1. Capture the latest indexed transaction basis from SlateDB.
2. Plan the query against the current schema.
3. Scan the current EAV (entity, attribute, value) index up to the captured
   transaction entity.
4. Prime the DBSP circuit with positive triples from that snapshot.
5. Add the registered query to the incremental service.
6. If the CDC loop is not already running, start it.

If the initial query result is not empty, registration queues it as the first
delta with the registration `TxKey`. Registration omits empty priming results.
Later deltas describe the transactions after the returned basis.

Registration is serialized against the CDC application. This prevents a race
condition. In this race, the global CDC loop consumes a transaction after the
snapshot basis of the new query, but before the service adds that query.
Without that barrier, the new query can miss a future transaction.

The transaction basis can be in the middle of the available WAL. This is
normal. Some transactions are indexed, but not yet flushed. These transactions
share a WAL file with the transactions that follow them. Thus the CDC loop can
later read a WAL file with transactions at or before the registration basis and
transactions after it. The dispatcher trims such a batch for that query. The
query skips the entries at or before its registration basis and applies the
later ones.

---

## CDC Flow

The node has one CDC loop for all incremental queries. The loop uses the SlateDB
`WalReader` through the `CdcStream` helper of Triplox. The loop turns each WAL
file into one batch. A datom is one fact with its transaction. The loop extracts
EAV datoms with the current node schema. A future optimization is to decode
with a finer grain, for each query. Then the loop routes the weighted triple
delta through the incremental query service.

The loop does not wait for the writer indexer before it applies a WAL entry. In
the current writer-node path, a WAL entry that the loop sees here already went
through the write and indexing path that produced it. The schema still comes
through the node boundary, because CDC decoding needs the current ident and
attribute maps.

The CDC cursor is the marker for the WAL read position. It contains the WAL
file id where reading continues. It also contains the last SlateDB row sequence
that the stream must skip, because the stream already read it. In the current
writer-node implementation, this cursor is in-memory state that the live
`CdcStream` owns. The cursor lets the stream wait on `next_batch().await`. Then
the stream continues with the next WAL file and does not read earlier rows
again.

Currently, the node does not save this CDC cursor to persistent storage. If a
node crashes, it is necessary to register the query again and populate it again.

For each WAL file:

1. `CdcStream` yields all rows of the file after the cursor as one batch.
2. The CDC loop decodes the EAV entries into datoms.
3. The metadata of the last transaction becomes the batch `TxKey`.
4. The datoms become a weighted batch of encoded triples. Each triple has a tag
   with its transaction id.
5. The incremental service enqueues the batch for each query. It trims the
   batch to the transactions after the registration basis of the query. It
   terminates each query whose input queue overflows.
6. CDC continues after the routing. The workers apply their queued batches
   independently and send non-empty result deltas.
7. The live stream cursor advances as the stream reads rows. It does not depend
   on the progress of the workers.

---

## Subscription Lifecycle

Each registration returns:

- A query handle
- The registration `TxKey`
- A result receiver that combines a bounded delta channel with terminal
  notifications.

A trace is the stored, indexed state of a DBSP operator. DBSP keeps the query
state in traces, in a directory for each query. It does not collect this state
in ordinary Rust memory. The service deletes that directory in these cases:

- The registration fails.
- A caller explicitly unregisters the query.
- The receiver of the query is dropped.
- The circuit of the query returns an error.
- The input queue of the query overflows.
- The service shuts down.

Circuit destruction and directory removal run on blocking threads, in that
order. Unregister waits for the cleanup, but it does not block the dispatcher.
The service logs failures of automatic cleanup. Even if no more transactions
arrive, the service detects dropped receivers.

Retirement is the shutdown and cleanup of a query worker. Unregister and
shutdown wait up to 10 seconds. If the retirement did not finish, they return an
explicit `RetirementTimeout` error. The deadline includes the wait for the
dispatcher. Shutdown uses one deadline for both the CDC cleanup and the worker
cleanup. If CDC uses all of that deadline, shutdown still requests the cleanup.

Timeouts stop only the wait of the caller. The service runtime and the retiring
workers stay alive until the in-flight applies and the cleanup finish. The
storage stays in place until circuit destruction is done. The service logs late
cleanup failures. There are no execution timeouts and no forced circuit kills.
A circuit that never returns keeps its runtime resources and storage.

---

## Threading and Tuning Knobs

`IncrementalQueryOptions` gives the internal defaults. There are no new TOML
settings:

- Each input queue holds 256 WAL batches, plus at most one batch that its worker
  already took. An overflow terminates that subscription.
- `SUBSCRIPTION_CAPACITY` stays at 128 result batches. Backpressure makes a
  producer wait until the queue has space. A full result queue applies
  backpressure to its own worker.
- `max_concurrent_steps` defaults to half of `available_parallelism()`, rounded
  up, with a minimum of one. A semaphore is a counter that limits concurrent
  jobs. A shared semaphore limits the outstanding live apply jobs. A worker
  that waits for output holds no permit.
- `retire_timeout` limits the unregister and shutdown waits to 10 seconds.
- The Tokio runtime that the service owns has two pools. The async pool has 2
  threads (`worker_threads(2)`), and all query workers share it. A wait for
  input, a permit, or an apply result frees the thread to run other workers.
- The blocking (sync) pool has up to 512 threads, the default limit of Tokio.
  It creates threads on demand to run the synchronous jobs that
  `spawn_blocking` submits. If all 512 threads are busy, the jobs wait in a
  queue.
- WAL polling uses `cdc_poll_interval_us` for remote nodes (default 200ms). It
  uses a fixed 10ms for local and memory nodes.

Apply scheduling works as follows. The shared semaphore provides
`max_concurrent_steps` permits. Each worker acquires a permit. Then it calls
`spawn_blocking` to submit an apply job to the blocking pool. If the job is
still pending, the `.await` that follows suspends the query worker. This frees
the current async thread of the worker to run other tasks. When the job
finishes, Tokio schedules the query worker to resume on either async thread.
The job holds its permit through the queueing and the execution. Then it
releases the permit, so another worker can continue.

DBSP also owns one runtime worker thread for each circuit. The semaphore limits
the outstanding apply jobs, not the total thread count.

These limits are count limits, not byte limits. Shared transaction payloads
decrease the copying. But queue references, variable batch sizes, in-flight
copies, and result buffers still use memory. Registration stays serialized
under the existing CDC gate. Special scheduling for expensive circuits and
execution timeouts are outside this change.

---

## Direction

Currently, the incremental query API is very basic. There is no way to specify
the `TxKey` where a query starts. There is no way to stop and restart a query
without a new initialization of the circuit. All these features are possible.
But the state management for them needs more thought and careful analysis.

In Hooray, I also explore an incremental join algorithm that uses aspects of
WCOJ. WCOJ (worst-case optimal join) is a family of multi-way join algorithms.
I want to bring these ideas to Triplox. Then I can see whether they improve the
incremental join on graph patterns at scale. I can also see whether there is a
necessity for this kind of algorithm.

### Further optimizations

Many optimizations are possible for incremental queries. We must track them in
the issue tracker. In no particular order, they are:

- Filter the relevant triples for circuit initialization by fixed attributes
  and other constants. Use the AVE (attribute, value, entity) index.
- Initialize the circuits in batches. At scale, the current approach will not
  work.
- Use the temporal indexes of Triplox in base triple patterns. Temporal indexes
  keep the full history of each triple. This will save a lot of space in the
  incremental circuits.
- Share circuits or arrangements across equivalent queries. An arrangement is
  an indexed form of a stream.
- Order the joins by cost, not only by the shared-variable heuristic.
- Add batching and scheduling policies for many active queries.
- Add storage cleanup and compaction policies for long-running query traces.
- Add future operational controls for DBSP storage, which is file-backed for
  each query. These controls are cache sizing, storage roots, compaction, and
  checkpoint/restore policy.

### Cleanup

Currently, the cleanup is event-driven. No separate process detects the clients
that are no longer responsive. A client heartbeat is a periodic signal that
the client is alive. A heartbeat can provide that liveness signal. Then the
service can close the abandoned incremental queries.
