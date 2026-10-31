# Triplox Incremental Queries

Version 0.1

## Overview

An incremental query is a query that a caller registers once. After
registration, the caller gets result deltas as the node indexes new
transactions. A delta is the change in the query result between two consecutive
database values. A transaction that does not change the result produces no
delta.

Each client supports some form of `subscribe(node, query)`. This call registers
an incremental query on the server. It returns a stateful object that gives the
deltas and that the caller can close. The caller must close it, because an
incremental query holds server resources until it closes.

In this document, "query" means an incremental query, unless the text says
otherwise.

---

## Current Scope

The incremental query engine supports fewer features than the standard query
engine. Incremental queries use the same validation as standard queries. An
additional rule rejects the query shapes that the incremental engine does not
support yet. When the two engines have the same features, we will remove this
rule.

Test each new approach for incremental query compilation in Hooray
(https://github.com/FiV0/hooray2) first. Hooray is the test bed for new join
algorithms in Triplox.

A query starts at the latest visible database value at registration time. The
covering indexes keep all old data, so in theory a query can also start at an
older value. But an older start needs a different way to feed old transactions
through the circuits. The current design only tails the SlateDB WAL files. Thus,
older start points are out of scope at this time.

Priming is the first step of a circuit, on the data at the start of the query.
If the priming result is not empty, it is the first delta. Each later delta
describes the changes from the `TxKey` of the previous delta up to its own
`TxKey`. One delta can cover several transactions.

---

## Architecture

Four modules do the query planning and the circuit construction:

- `src/inc_query.rs` makes sure that the query shape is valid for incremental
  queries. It also coordinates the query planning.
- `src/inc_query/descriptor.rs` describes the semantic structure and the binding
  properties of the `:where` tree.
- `src/inc_query/planner.rs` orders the descriptors and lowers them to physical
  relation plans.
- `src/incremental/circuit.rs` assembles the relation plans into one DBSP
  circuit for each query. `src/incremental/circuit/aggregates.rs` builds the
  aggregate result streams.

`src/incremental.rs` contains the service handle, the dispatcher, and the
registry of queries. `src/incremental/worker.rs` contains the query workers.
`src/incremental/cdc.rs` contains the snapshot scan and the CDC task.

### Fact input and relation streams

Each registered query owns a DBSP circuit with one weighted fact input:

```text
EncodedTriple {
    entity: encoded DataType::Long(entity_id),
    attribute: attribute entid,
    value: encoded DataType,
}
```

A DBSP Z-set is a collection in which each element has an integer weight. A
positive weight adds the element, and a negative weight retracts it. Z-set keys
must have an order. `DataType` contains floats, maps, and vectors, and these
values do not have a simple total order. Thus, the values in the fact input are
encoded bytes, not `DataType` keys.

The fact input and an incoming relation are different inputs to the assembly of
a plan node. The fact input is the shared stream of `EncodedTriple` changes. An
incoming relation is the running `EncodedRow` stream that the enclosing `:where`
scope produces during evaluation. It is internal to circuit assembly. It has no
relation to the `:in` bindings of the query.

Each relation stream has a variable layout together with its rows. The layout
maps each variable to the position of its encoded value in `EncodedRow`.

### Semantic descriptors

The descriptor tree has the same nesting as the query scopes:

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
descriptors. It has no descriptor of its own. A `not` or a `not-join` is a
descriptor, and its body is another scope.

`variables` lists the variables that are visible at the boundary of a
descriptor. `groundable` lists the variables that the descriptor can produce
without an incoming relation. The descriptor kinds ground variables as follows:

- A pattern can ground all of its variables.
- A function grounds its result variable, unless the expression also reads that
  variable.
- An `or` can ground the variables that all of its branches can ground.
- Predicates and `not` ground no variables. Each variable that they expose must
  come from the enclosing positive relation.
- A `not` exposes the variables that its body mentions. A `not-join` exposes
  only its declared variables, and its body binds the other variables locally.

The planner calculates the required bindings of a descriptor as `variables -
groundable`. When the running layout contains all required variables of a
descriptor, the descriptor is eligible. The planner selects the eligible
descriptor that shares the most variables with the running relation. If two
descriptors share the same number of variables, the planner selects the
descriptor that comes first in the scope.

This selection does not resolve the dependencies inside OR branches before it
selects the OR. Thus, the planner can select an OR with circular function
dependencies before an outside clause that supplies a necessary binding. As a
result, the planner rejects a query that the engine can run.

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
present relation that has zero columns.

The plan kinds do these operations:

- `Pattern` filters the fact input by attribute and by constants. If an incoming
  relation is present, circuit assembly joins the filtered pattern rows to that
  relation. The join uses the incoming, pattern, and output layouts of the plan.
- `Filter` evaluates one compiled predicate on each incoming row. If the
  predicate returns true, the filter keeps the row and its weight.
- `Function` evaluates one compiled expression on each incoming row. It adds a
  new result column, or it filters on an existing result binding.
- `Chain` is a scope with more than one descriptor. It gives the output relation
  of each child to the next child.
- `Difference` keeps the incoming relation and removes the rows whose selected
  key occurs in its negative subplan. The negative scope starts from the
  incoming relation, projected to that key.
- `Union` records its join variables separately from its full output layout. The
  branches get only the incoming columns for those variables. The plan projects
  away the local output columns before the union. Both `or` and `or-join` use
  this operator.

`Chain` records the evaluation order of its child plans. Circuit construction
connects the output stream of each child to the input of the next child. It uses
the existing DBSP operators `flat_map`, filter, join, projection, antijoin, sum,
and distinct. An antijoin keeps the rows of one input that have no match in the
other input.

### Row layouts

A standalone pattern outputs its variables in pattern order. A node with an
incoming relation keeps that layout. It adds only the new variables, in
descriptor variable order. The join keys are the variables that both sides
share, in the order of the incoming layout. If a join has no shared variables,
it is a Cartesian product over the empty key.

The output layout of a chain is the output layout of its last child. A
standalone union uses its descriptor variable order. A union with an incoming
relation keeps the incoming layout and adds its other descriptor variables. The
descriptor order makes sure that the incoming layout already contains each
variable that the union cannot ground.

Branches can produce their columns in different orders. Thus, the circuit
projects each branch to the join variables of the union before it sums the
branches. Explicit `or-join` branches can also have local columns, and this
projection removes them.

The circuit makes sure that the running relation layout matches the declared
incoming layout of each plan node. It does not calculate a different variable
order during assembly.

### Circuit assembly

Circuit assembly is recursive. Its inputs are the shared fact input, a relation
plan, and the optional incoming relation. Each plan kind assembles as follows:

- A pattern creates matching rows with `flat_map`. If incoming rows are present,
  it joins the matching rows to them.
- A filter decodes the columns that it references and evaluates the compiled
  expression. It filters the incoming rows, and it does not change their layout
  or their weights.
- A function decodes its inputs. It adds the encoded result with `flat_map`, or
  it removes the rows whose existing result binding is different. If the
  evaluation of the expression fails, the function removes the row.
- A chain folds the running relation through its children.
- A difference projects the incoming rows to the negative key and evaluates the
  negative scope from that raw projection. Then it antijoins the original
  incoming rows against the resulting keys. Local `not-join` columns are not
  part of the key.
- A union projects the incoming rows to the join variables that are already
  bound, and it keeps their weights. It evaluates each branch with those rows,
  projects the results to all join variables, and applies `distinct` to their
  sum. Then it joins back to the original incoming stream on the shared
  variables, to keep the outer columns and the weights. If there is no incoming
  relation, the union returns the distinct union directly.

The circuit does not apply `distinct` to the negative seed before evaluation.
The weights of the seed stay correlated with the positive input. DBSP antijoin
uses only the presence of a key in the negative relation. If a key has more than
one negative match, this gives the correct add and retract behavior.

For a query without aggregates, the circuit projects the full `:where` relation
to the order of the `:find` variables. For an aggregate query, the circuit gives
that relation to the aggregate assembly in the next section.

### Aggregate assembly

The find plan uses the find variables that are not aggregates as the group key.
It records all find elements in projection order. Circuit assembly indexes the
full `:where` rows by that group key and builds one independent result stream
for each aggregate expression. Then it joins those streams by group. It puts the
group values and the aggregate values back in find-clause order. The circuit
does not deduplicate equivalent aggregate expressions.

A global aggregate has an empty group key. The circuit seeds that group, so an
empty input produces `0` for `count`, `count-distinct`, and `sum`. For an empty
input, `avg`, `min`, and `max` produce no row.

`min` and `max` use a complex method to produce a result. There are two reasons.
First, the implementation duplicates the handling of values with types that
cannot be compared, for example a string and an integer. Second, the `Min`
implementations of the DBSP crate do not carry errors through the circuit. They
can only select the left value or the right value, and they cannot produce a new
value such as an error. Their signatures also do not return a `Result`.

---

## Runtime Flow

`IncrementalQueryService` is the boundary between a node and its incremental
queries. The node holds one service handle. The handle sends commands to the
dispatcher. The diagram that follows shows the tasks and the channels between
them:

```text
┌─ Tokio runtime (node or dedicated) ────────────────────────────────────────────────────────┐
│                                                                                            │
│  ┌─ register_query(query) ─────────────┐                ┌─ CDC task ─────────────────────┐ │
│  │ plan_query                          │                │ loop:                          │ │
│  │ send Register{plan}                 │                │  WAL next_batch → Batch        │ │
│  │ await response                      │                │  send ApplyBatch ⇄ routed ack  │ │
│  └──────┬─────────────────▲────────────┘                └───────────────┬────────────────┘ │
│         │ Register        │ Ok(subscription) / Err                      │ ApplyBatch       │
│         │ Unregister      │ (sent by the worker after priming)          │                  │
│         ▼                 │                                             ▼                  │
│  ┌─ Dispatcher task ──────┼────────────────────────────────────────────────────────────┐   │
│  │ registry: handle → { inbox, basis, control }                                        │   │
│  │ Register:   basis = latest indexed TxKey                                            │   │
│  │             new inbox ← Prime{basis, response}; insert; spawn Worker                │   │
│  │             (first query: start RuntimePool + CDC task)                             │   │
│  │ ApplyBatch: per query batch.after(basis) → inbox ← Apply(batch)   (full ⇒ Lagged)   │   │
│  │ Unregister: control.terminate; reply on Completion                                  │   │
│  └───────────────┬────────┼───────────────────────────────────────────▲────────────────┘   │
│                  │ inbox  │ [Prime, Apply, Apply, …], bounded FIFO    │ Completion         │
│                  │        │ + control: stop, out of band              │                    │
│                  ▼        │                                           │                    │
│  ┌─ Worker task × N ──────┴───────────────────────────────────────────┴────────────────┐   │
│  │ build circuit on the pool                       (spawn_blocking)                    │   │
│  │ loop inbox:                                                                         │   │
│  │   Prime{basis, response}  scan at basis → step → emit(basis) → response             │   │
│  │   Apply(batch)            matches? → step → emit(batch.tx_key)                      │   │
│  │ exit: drop circuit, rm storage (spawn_blocking) → Completion                        │   │
│  └──────┬───────────────────────────────────┬──────────────────────────────────────────┘   │
└─────────┼───────────────────────────────────┼──────────────────────────────────────────────┘
          │ deltas: priming first, then WAL   │ transaction_async
          ▼                                   ▼
  ┌─────────────────────┐          ┌─ DBSP RuntimePool (shared, started once) ──┐
  │ SubscriptionDeltas  │          │ `threads` foreground threads, each circuit │
  └─────────────────────┘          │ pinned to one; `merger_threads` compaction │
                                   └────────────────────────────────────────────┘
```

The runtime has these parts:

- `register_query` runs on the task of the caller. It plans the query, sends
  `Register{plan}` to the dispatcher, and waits for the response.
- The dispatcher is one Tokio task that owns the registry of queries. It handles
  one message at a time. It never builds a circuit, scans data, or steps a
  circuit.
- The CDC (change data capture) task reads the SlateDB WAL. It turns each WAL
  file into one batch and sends the batch to the dispatcher.
- Each query has one worker task that owns the circuit of the query. The inbox
  of the worker is a bounded FIFO (first in, first out) queue of `Prime` and
  `Apply` messages.
- The DBSP `RuntimePool` runs the steps of all circuits on one shared set of
  threads.

At this time, the tasks run on the Tokio runtime of the node. A dedicated
runtime for incremental queries is also possible.

### Registration

The registration basis is the `TxKey` of the latest indexed transaction at
registration. Registration has these steps:

1. `register_query` plans the query with the current schema and sends
   `Register{plan}` to the dispatcher.
2. The dispatcher reads the latest indexed `TxKey` under the indexer read lock.
   This `TxKey` is the basis.
3. The dispatcher creates the inbox and puts `Prime{basis, response}` in it as
   the first message.
4. The dispatcher adds the query to the registry and starts the worker. For the
   first query, it also starts the `RuntimePool` and the CDC task.
5. The worker builds the circuit on the pool, on a blocking thread.
6. The worker takes `Prime` from the inbox. It scans the snapshot at the basis
   and steps the circuit with the snapshot triples.
7. If the priming result is not empty, the worker sends it as the first delta.
8. The worker sends `Ok(subscription)` on the response channel, and
   `register_query` returns the subscription.

The response channel goes inside `Prime`. Thus, the worker replies to
`register_query` directly, and the dispatcher does not wait for priming. If
priming fails, the worker sends the error on the response channel, and the
registration fails.

`register_query` plans with the schema at call time, and the dispatcher reads
the basis later. Thus, the schema for the plan can be older than the basis. This
order is correct on one condition: an attribute id does not change after the
schema assigns it.

### Live batches

A batch is the set of weighted triples from one WAL file. Each triple has the id
of its transaction. A batch goes through these steps:

1. The CDC task reads one WAL file and decodes it to datoms with the current
   schema. A datom is one asserted or retracted fact about an entity.
2. The CDC task converts the datoms to a batch of weighted `EncodedTriple`
   tuples.
3. The CDC task sends `ApplyBatch` to the dispatcher and waits for the routing
   acknowledgement.
4. For each query, the dispatcher removes the transactions at or before the
   basis of the query. If transactions remain, it puts `Apply(batch)` in the
   inbox of the query.
5. The worker takes the next message from its inbox. If the batch has no triples
   with the attributes of the query, the worker skips the step.
6. Otherwise, the worker copies those triples into the circuit input and awaits
   the step.
7. If the step result is not empty, the worker sends it as a delta.

The acknowledgement of `ApplyBatch` tells the CDC task that the dispatcher
routed the batch. It does not tell the CDC task that the circuits applied the
batch. The acknowledgement paces the CDC task. Each query has one FIFO consumer
and at most one step in progress. Different queries advance independently.

All queries share one `Arc` batch, so the dispatcher makes no deep copies. The
CDC task sorts each batch by attribute once. Each step copies only the triples
with the attributes of its query. After priming, an empty input cannot change
the result of a circuit. Thus, a query without matching triples skips the step.

### Ordering

The dispatcher does three actions in one step: it reads the basis, it creates
the inbox with `Prime`, and it adds the query to the registry. The dispatcher
handles one message at a time. Thus, the dispatcher cannot route a batch between
these three actions. No lock between registration and the CDC task is necessary.

A batch that the dispatcher routed before this step does not go to the new
query. This is correct, because the batch contains only transactions at or
before the basis. The indexer writes a transaction to SlateDB and publishes its
`TxKey` under one write lock. Thus, a basis that the dispatcher reads under the
read lock covers each transaction in the WAL. The snapshot scan at the basis
contains these transactions.

The dispatcher trims each batch that it routes after this step against the
basis. The remainder goes into the inbox behind `Prime`. The worker handles its
inbox in FIFO order, and it sends all deltas of its query. Thus, the priming
delta always comes before the deltas of WAL batches.

The basis can be in the middle of a WAL file. Transactions that are indexed but
not yet flushed share a WAL file with later transactions. Thus, the CDC task can
read a WAL file with transactions on both sides of the basis. The dispatcher
trims such a batch for the query, and the query applies only the later
transactions.

### Backpressure and errors

A full result channel stops only the worker of that query. If the inbox of a
query is full, the dispatcher stops routing to that query and stops the query
with a lag error. The dispatcher discards the pending messages.

During the circuit build and the snapshot scan, WAL batches wait in the inbox.
If the priming is slow, the inbox can fill. The query then stops with a lag
error before its first delta.

`IncrementalQueryDelta` is a result batch for the subscriber. The worker sends
it after a circuit step. Each delta has the `TxKey` of the last transaction that
it covers. The priming delta has the basis as its `TxKey`.

A priming failure makes the registration fail. A failure during a live step
stops only the affected query.

A separate terminal channel makes sure that a full output queue does not hide an
error. An ordinary query error comes after the results that are already in the
queue. A lag error comes before the buffered results. Both become terminal
`QueryError` frames on the wire.

The dispatcher stops a worker through its `Control`, not through the inbox. The
dispatcher uses `Control` for unregister, lag, and shutdown. A full inbox is the
condition for a lag stop. Thus, the stop signal cannot go through the inbox.

---

## CDC Flow

The node has one CDC task for all incremental queries. The dispatcher starts it
with the first query. The task uses the SlateDB `WalReader` through the
`CdcStream` helper of Triplox. It reads the WAL from the beginning and turns
each WAL file into one batch. It extracts EAV (entity, attribute, value) datoms
with the current node schema. A future optimization can decode separately for
each query.

The task does not wait for the writer indexer before it routes a WAL entry. On a
writer node, each WAL entry that the task reads already went through the write
and index path. The task gets the schema from the indexer, because decoding
needs the current ident and attribute maps.

The CDC cursor is the read position in the WAL. It contains the id of the WAL
file where reading continues. It also contains the last SlateDB row sequence
number that the stream already read. On the writer node, the cursor is in-memory
state of the live `CdcStream`. With the cursor, the stream can wait in
`next_batch().await` and continue with the next WAL file. The stream does not
read earlier rows again.

The node does not persist the cursor. If a node crashes, each query must
register and prime again.

For each WAL file, the CDC flow has these steps:

1. `CdcStream` gives all rows of the file after the cursor as one batch.
2. The task decodes the EAV entries into datoms.
3. The metadata of the last transaction becomes the `TxKey` of the batch.
4. The datoms become a weighted batch of encoded triples. Each triple has the id
   of its transaction.
5. The task sends `ApplyBatch` to the dispatcher and waits for the routing
   acknowledgement.
6. The dispatcher trims the batch for each query and puts the remainder in each
   inbox. If an inbox overflows, the dispatcher stops that query.
7. The task continues with the next WAL file. The workers apply their messages
   independently and send the deltas that are not empty.

The cursor advances as the task reads rows. It does not depend on the progress
of the workers. If the CDC task fails, the task stops, and shutdown reports the
error.

---

## Subscription Lifecycle

Each registration returns:

- A query handle.
- The registration `TxKey`.
- A result receiver, which combines a bounded delta channel with terminal
  notifications.

DBSP keeps the query state in traces. A trace is the indexed history of updates
that a DBSP operator keeps. The traces are in one directory for each query, not
in ordinary Rust memory. When its query stops, the worker removes that
directory. A query stops for these reasons:

- The registration fails.
- The caller unregisters the query.
- The caller drops the receiver.
- The circuit returns an error.
- The inbox overflows.
- The service shuts down.

The worker destroys the circuit and then removes the directory, on a blocking
thread. Then it sends `Completion` to the dispatcher. Unregister waits for this
`Completion`, and the dispatcher does not block during the wait. The dispatcher
logs the cleanup failures of queries that stopped for other reasons. If no new
transactions arrive, the worker still detects a dropped receiver.

Retirement is the stop and the cleanup of a query. `Shutdown` retires all
queries in these steps:

1. The dispatcher stops the CDC task and waits for it.
2. The dispatcher stops all workers through their `Control`.
3. The dispatcher waits for a `Completion` from each worker.
4. The dispatcher shuts down the `RuntimePool` and sends the result.

A worker cannot stop a circuit build that started. The build finishes, and then
the worker stops. If a worker stops before it takes `Prime`, the response
channel closes, and the registration fails.

Unregister and shutdown wait up to 10 seconds. If retirement does not finish in
that time, they return a `RetirementTimeout` error. Shutdown uses one deadline
for all of its steps.

A timeout only stops the wait of the caller. The dispatcher and the retiring
workers continue until the steps in progress and the cleanup finish. The storage
stays until the circuit destruction finishes. The dispatcher logs late cleanup
failures. There are no execution timeouts and no forced stops of circuits. A
circuit that never returns keeps its runtime resources and its storage.

---

## Threading and Tuning Knobs

The `[incremental]` configuration section sets the DBSP pool:

- `threads` sets the number of foreground threads. The default is half of
  `available_parallelism()`, rounded up.
- `merger_threads` sets the number of merger threads, which merge traces in the
  background. The default is 1.
- `cache_mib` sets the size of the cache in MiB. The default is 256.

`IncrementalQueryOptions` gives the other internal defaults:

- Each inbox holds 256 messages. The worker can also hold one message that it
  already took. Overflow stops the query.
- `SUBSCRIPTION_CAPACITY` is 128 result batches. A full result queue applies
  backpressure to its own worker.
- `retire_timeout` limits the waits of unregister and shutdown to 10 seconds.

The dispatcher, the CDC task, and the workers are Tokio tasks. The service owns
no threads of its own. All circuits run on one shared DBSP `RuntimePool`, which
the dispatcher starts with the first query. Each circuit is pinned to one
foreground thread of the pool. The foreground threads queue the circuits that
are ready. Thus, the number of foreground threads alone limits the number of
concurrent steps.

For a step, a worker adds the triples to the circuit input and awaits
`DBSPHandle::transaction_async`. The await does not hold a thread while the pool
runs the step. Then the worker decodes the output on the Tokio runtime. The
priming step uses the same path.

Blocking threads (`spawn_blocking`) run only the lifecycle work that waits on
the pool. This work is the pool start, the circuit build, the circuit
destruction with storage removal, and the pool shutdown.

Each snapshot scan runs at most 8 prefix scans at the same time
(`MAX_CONCURRENT_INITIAL_SCANS`). There is no shared limit across queries. Thus,
many registrations at the same time put a high read load on SlateDB.

For remote nodes, WAL polling uses `cdc_poll_interval_us`, with a default of 200
ms. For local and memory nodes, the interval is a fixed 10 ms.

These limits are counts, not bytes. Shared batches reduce copies. But queue
references, variable batch sizes, copies in progress, and result buffers still
use memory. Special scheduling for expensive circuits and execution timeouts are
out of scope.

---

## Direction

The incremental query API is minimal at this time. A caller cannot specify the
`TxKey` where a query starts. A caller cannot stop and restart a query without a
new circuit. These features are possible, but the state management needs more
analysis.

I am also exploring an incremental join algorithm in Hooray. It uses parts of
worst-case optimal joins (WCOJ). I want to bring these ideas to Triplox. The
goal is to find out whether the incremental join on graph patterns improves at
scale. A second goal is to find out whether Triplox needs this kind of
algorithm.

### Further optimizations

Many optimizations are possible for incremental queries. The issue tracker
records them. The optimizations, in no specific order, are:

- Initialize the circuits in batches. The current approach does not scale.
- Use the temporal indexes of Triplox in base triple patterns. This saves a lot
  of space in the circuits.
- Share circuits or arrangements across equivalent queries.
- Use cost-based join ordering beyond the shared-variable heuristic.
- Add batching and scheduling policies for many active queries.
- Add storage cleanup and compaction policies for long-running query traces.
- Add operational controls for the file-backed DBSP storage of each query: cache
  size, storage roots, compaction, and checkpoint and restore policy.
- If the CDC task fails, stop all subscriptions with the CDC error. At this
  time, the subscriptions get no more deltas, and only shutdown reports the
  error.
- Prevent lag errors during slow priming, for example with a larger inbox or a
  local buffer in the worker.
- Add a shared limit for snapshot scans across queries.

### Cleanup

Cleanup is event-driven at this time. No separate process detects clients that
do not respond. A client heartbeat can give that liveness signal. Then the
service can close abandoned incremental queries.
