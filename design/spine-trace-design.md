# Spine and Trace Architecture in DBSP

This design note describes the `Trace` and `Spine` architecture in the DBSP
crate. It is intended for engineers familiar with incremental computation,
incremental view maintenance, database indexing, and log-structured storage.

The short version:

* A **batch** is an immutable, sorted collection of weighted updates.
* A **trace** is an appendable, readable collection of batches that represents
  the accumulated state of a stream.
* A **spine** is DBSP's production trace implementation: an asynchronous,
  log-structured collection of immutable batches, maintained by background
  merges and optionally backed by storage.
* Stateful operators use traces as arrangements: indexed state that can be
  searched by key and value when new deltas arrive.

## Implementation Map

Primary source files:

* `crates/dbsp/src/trace.rs`: core `BatchReader`, `Batch`, and `Trace` traits.
* `crates/dbsp/src/trace/spine_async.rs`: asynchronous `Spine<B>`.
* `crates/dbsp/src/trace/spine_async/snapshot.rs`: `SpineSnapshot<B>` and
  `WithSnapshot`.
* `crates/dbsp/src/operator/trace.rs`: typed APIs such as `trace`,
  `integrate_trace`, `delay_trace`, and retention methods.
* `crates/dbsp/src/operator/dynamic/trace.rs`: dynamic implementation of timed
  and untimed trace operators.
* `crates/dbsp/src/operator/accumulate_trace.rs` and
  `crates/dbsp/src/operator/dynamic/accumulate_trace.rs`: transaction-flushed
  accumulated trace variants.
* `crates/dbsp/src/typed_batch.rs`: typed wrappers and aliases for `Vec*`,
  `File*`, `Fallback*`, `Spine`, and `SpineSnapshot`.
* `crates/dbsp/src/trace/ord/{vec,file,fallback}`: concrete in-memory,
  file-backed, and runtime-selected batch implementations.

## Conceptual Model

DBSP computes over streams of changes. Relational changes are represented as
Z-sets or indexed Z-sets: rows or key-value pairs carry integer-like weights.
Positive weights insert, negative weights retract, and equal items with weights
that sum to zero cancel.

The trace layer generalizes this to tuples:

```text
(key, value, time, weight)
```

For an unindexed Z-set, `value` is `()`. For an untimed integral, `time` is
also `()`. For timed traces, `time` is a DBSP logical timestamp.

The core abstractions are layered:

* `BatchReader` is a read-only sorted view. It exposes cursor access by key and
  value.
* `Batch` is a mostly immutable set of updates. It can be built, batched,
  merged, persisted, and converted to timed batches.
* `Trace` extends `BatchReader` with state maintenance: append new batches,
  compact logical time, track dirty state, apply retention, consolidate, and
  checkpoint.
* `Spine<B>` is the main `Trace` implementation. It stores immutable batches of
  type `B`, groups them by approximate size, and merges them asynchronously.

The design is log-structured. DBSP does not update a single large mutable map on
every input. It appends sorted immutable runs, then merges runs later to reduce
read amplification, cancel weights, compact timestamps, apply retention, and
move data between memory and storage.

## Fit in the Stream-Based Architecture

A DBSP circuit is a graph of operators connected by streams. Most streams carry
delta batches, one per clock cycle or transaction flush. Stateful operators need
the integral of one or more streams, usually as indexed state. `Trace` supplies
that integral as a stream value.

At the typed API level:

```rust
stream.integrate_trace() -> Stream<C, Spine<B>>
stream.trace()           -> Stream<C, TimedSpine<B, C>>
trace.delay_trace()      -> Stream<C, SpineSnapshot<B>>
```

`integrate_trace` lowers to a feedback loop:

```text
delta stream
    |
    v
UntimedTraceAppend <---- delayed trace
    |                         ^
    v                         |
current trace --------------> Z1Trace
```

`Z1Trace` is a strict operator. It outputs the previous trace value early in the
clock cycle and accepts the updated trace later. `UntimedTraceAppend` receives
the previous trace and the current delta batch, inserts the batch into the
trace, and returns the updated trace. The feedback edge stores trace state
across clock cycles.

`trace` is similar, but it labels each untimed input batch with the current
clock timestamp before insertion. `integrate_trace` constructs an untimed trace
whose contents are the materialized integral of the stream.

The circuit cache is part of the architecture. Repeated calls that logically ask
for the same trace of the same stream reuse the same physical trace. This avoids
duplicated state and ensures retention policies apply to all consumers of that
logical trace.

## Indexing Model

The trace is an indexed representation of accumulated weighted data. The index
order is:

```text
key -> value -> time/weight list
```

A batch is sorted by key and value. Time is not the primary ordering dimension.
For each `(key, value)` pair, a cursor can enumerate the associated
`(time, weight)` pairs. The trace itself is a logical merge of several sorted
batches, so a trace cursor is a multiway cursor over batch cursors.

This means a trace is closer to a database secondary index or differential
dataflow arrangement than to a heap table. It is designed for questions like:

```text
Find all values for key k.
Find whether key/value pair (k, v) has non-zero accumulated weight.
Find all records in a key range.
Scan keys in sorted order.
Fetch a set of keys from storage-backed batches.
```

The physical batches support cursor operations such as:

* seek to a key;
* seek to a value within the current key;
* step through keys;
* step through values;
* map over time-weight pairs for the current key/value;
* produce merge cursors with optional key and value filters.

For indexed Z-sets, the `key` is the join or grouping key and `value` is the
payload. For ordinary Z-sets, `value` is `()`, so the trace is effectively an
ordered weighted set keyed by row.

## How Queries Find Data

Operators do not query traces through SQL-style predicate evaluation. They use
specialized cursor access patterns compiled from the relational operator.

### Joins

For an equijoin, DBSP arranges one or both inputs as indexed Z-sets. The join
key becomes the trace key. When a delta arrives on one side:

1. Iterate over keys touched by the delta.
2. Seek the same key in the other side's delayed trace or snapshot.
3. Iterate matching values under that key.
4. Multiply/combine weights and produce output updates.

This is analogous to an indexed nested-loop join where the inner side is an
arrangement. Because both batches and traces are sorted, it can also behave like
a sort-merge lookup over the relevant keys.

### Aggregates

An incremental aggregate uses a trace to find prior values for groups touched by
the delta. The group key is the trace key. For each changed group, the operator
can seek the group in the input trace, recompute or adjust the aggregate for
that group, and emit the difference between the old and new aggregate output.

Some aggregate implementations maintain a trace of aggregate outputs instead of
or in addition to the full input trace. This reduces state when the aggregate
state is smaller than the input history.

### Distinct

Distinct needs to know whether an accumulated row has non-zero weight before and
after a delta. The row is the key. The trace cursor finds the row's weights,
sums them, and determines whether the distinct output should insert, retract, or
stay unchanged.

### Time-Series and Window Operators

Time-series operators rely on ordered keys or values plus waterline-derived
retention. The trace still indexes by key/value, but the key or value often
contains an event-time component. Operators use bounds to restrict the retained
and searched portion of the trace.

### Recursive and Iterative Computation

Recursive circuits use traces with feedback and dirty flags to accumulate
intermediate results and detect fixed points. The trace provides both the stored
state and the ability to replay or inspect the accumulated relation.

## Batches, Cursors, and Read Amplification

A spine may contain many batches. To answer a lookup for key `k`, the cursor may
need to search every batch that could contain `k`. File-backed batches can use
their internal indexes and filters to avoid reading irrelevant data, but the
trace-level cost still depends heavily on the number of visible batches.

This is why the spine merges batches. Merging reduces the number of sorted runs
that every trace cursor must consider. Merging can also reduce total tuple count
when weights cancel or retention filters drop obsolete data.

The logical read path is:

```text
operator
  -> SpineSnapshot or Spine cursor
  -> CursorList / merge cursor over batch cursors
  -> Vec/File/Fallback batch cursor
  -> key/value/time-weight traversal
```

For storage-backed batches, the file format is built for range and key access,
not arbitrary predicate search. Query planning and operator lowering must choose
trace keys that make the required lookups efficient.

## Spine Physical Layout

`Spine<B>` stores `Arc<B>` batches. Its asynchronous merger groups batches into
levels by approximate size. Each level has a slot:

```text
Slot[level] = loose batches + optional in-progress merge
```

Loose batches are visible to readers. Input batches of an in-progress merge
remain visible too. A completed merge atomically replaces its input runs with a
new output run at the appropriate level.

The structure is similar to an LSM tree:

* New batches append as small sorted runs.
* Runs are grouped into size levels.
* A level starts a merge when it has enough loose runs.
* A merge reads runs sequentially and writes one new sorted run.
* The new run may move to a higher level.

Unlike a general-purpose LSM tree, a spine is specialized for DBSP data:
weights, logical timestamps, cursor semantics, retention filters, checkpointing,
and batch serialization are all part of the same abstraction.

## Asynchronous Merge Strategy

The spine merge scheduler tries to keep read amplification low without wasting
too much work on partially completed merges.

It runs up to one Tokio merge task per level, started lazily. Level 0 merges run
to completion. Higher-level merges are given fuel based on the average level 0
merge cost, then yield back to the merger runtime. This gives higher levels
regular progress while making small merges complete quickly.

Merging applies maintenance:

* sum equal `(key, value, time)` weights;
* drop zero-weight entries;
* compact timestamps below the frontier;
* apply key and value retention filters;
* choose memory or storage for the output batch.

If a spine accumulates too many unmerged batches, insertion can wait for
backpressure relief. This prevents read amplification from growing without
bound.

## Memory and Storage

The trace is generic over batch type. The main batch families are:

* `Vec*`: in-memory sorted batches.
* `File*`: file-backed sorted batches.
* `Fallback*`: a runtime choice between `Vec*` and `File*`.

`FallbackWSet` and `FallbackIndexedWSet` contain either a vector batch or a file
batch. Builders choose where to build based on runtime storage thresholds,
estimated size, merge destination hints, and memory pressure. This lets operator
types remain fixed while individual batches can live in memory or storage.

The result is that a trace can serve simultaneously as:

* an in-memory arrangement for small or hot state;
* a disk-backed index for large state;
* a checkpointable durable state object;
* a stream value in the DBSP circuit graph.

## Snapshots

`SpineSnapshot<B>` is a read-only view of a spine's current batch set. It stores
`Arc<B>` references to batches, so creating a snapshot is cheap. The snapshot is
itself a `BatchReader`, so operators can use the same cursor code against a
batch, a spine, or a snapshot.

Snapshots are used when:

* an operator needs a stable view while background merges continue;
* a delayed trace exposes previous-cycle state;
* a retention policy needs to inspect the current trace state while deciding
  what a merge may discard.

## Time, Integrals, and Compaction

`trace` and `integrate_trace` differ in their treatment of time:

* `trace` records input batches with the current logical timestamp.
* `integrate_trace` maintains the untimed integral of a stream.

Timed traces preserve historical distinctions needed by some incremental
operators. Untimed traces store only the accumulated collection.

Compaction is controlled by frontiers. A frontier states that times below it are
no longer distinguishable to downstream computation. At merge time, the trace
can replace old timestamps with their join with the frontier, which enables more
updates to combine or cancel. This is lazy logical compaction, similar in spirit
to differential dataflow compaction.

## Retention and Bounds

Retention removes keys or values that cannot affect future output. There are two
mechanisms:

* Lower bounds on ordered keys or values. Multiple consumers can register
  bounds, and the effective bound is the minimum.
* General filters, including value policies such as last-N, top-N, and
  bottom-N.

The correctness condition is global. If multiple operators share a trace, the
retention policy must retain everything needed by all of them. The dynamic
implementation keeps `TraceBounds` in the circuit cache so source, sharded, and
spilled versions of a stream coordinate through one logical bounds object.

Retention is naturally enforced during merges. A trace implementation may apply
it eventually rather than immediately.

## Checkpointing and Replay

Traces are durable operator state. `Z1Trace` checkpoints and restores by
delegating to the underlying trace. The spine records its batch metadata and
uses batch-level file committers for durable storage.

Replay bootstraps downstream state from a restored trace. During normal
execution, the original delta stream is active. During replay, `Z1Trace` emits
chunked delta batches from a cursor over the restored trace into a replay
stream. At most one of the normal stream and replay stream is active at a time.

This allows downstream operators to reconstruct state from the restored integral
without relying only on external input replay.

## Sharding and Ownership

DBSP workers use shared-nothing data parallelism. Streams can be marked sharded,
usually by key. Trace construction preserves sharding metadata:

* If the input stream is sharded, delayed and current trace streams are marked
  sharded.
* Some accumulated trace paths shard before tracing when required.
* Bounds are associated with the logical source stream to avoid divergence
  between source, sharded, and spilled versions.

Append operators strongly prefer owned trace inputs because inserting a batch
mutates the trace value inside the feedback loop. Passing the trace only by
reference is not a valid mutation path.

## Performance Model

The spine has three central costs:

* Write cost: append a batch, maybe spill it, maybe wait for backpressure.
* Read cost: search or merge across all visible batches that may contain a key
  or range.
* Maintenance cost: background CPU and IO for merges, compaction, retention, and
  storage conversion.

The design trades write amplification for read efficiency. Small updates are
cheap to ingest. Background merges produce larger sorted runs. The number of
visible runs controls read amplification for trace lookups.

Important invariants:

* Batches are immutable once visible.
* In-progress merge inputs stay visible until the merge output is installed.
* Merge output is semantically equivalent to its inputs after valid compaction
  and retention.
* New trace tuples must not have times earlier than existing trace tuples.
* Retention predicates must be monotone with respect to the progress signal that
  drives them.
* Shared traces require retention safe for all consumers.

## What Else One Must Understand

To fully understand Trace and Spine, one also needs:

* Z-sets and indexed Z-sets: weights, cancellation, negative updates, and why
  DBSP uses the same representation for data and changes.
* DBSP logical time: nested timestamps, frontiers, joins, clock boundaries, and
  timed versus untimed traces.
* Batches and cursors: sorted layout, seeking, stepping, `map_times`, merge
  cursors, and builders.
* Circuit execution: streams, strict operators, feedback connectors, replay
  streams, dirty flags, and fixed-point detection.
* Arrangement-based incremental algorithms: how joins, distinct, aggregates,
  recursive operators, and windows use indexed state.
* Query lowering to indexes: why the compiler/runtime must choose keys that
  match the operator's lookup pattern.
* Trace sharing and caching: why repeated trace requests for the same stream
  should share one physical trace.
* Retention analysis: waterlines, lateness, lower bounds, and the correctness
  risk of dropping state too early.
* Sharding: key partitioning, exchange operators, and distributed state layout.
* Storage internals: vector batches, file batches, fallback batches, buffer
  cache, file committers, and checkpoint layout.
* Merge scheduling: levels, fuel, memory pressure, backpressure, and foreground
  latency interaction.
* Type erasure and typed wrappers: dynamic runtime machinery versus strongly
  typed operator APIs.

## Design Tradeoffs

The architecture chooses immutable sorted batches plus asynchronous merging over
mutable B-trees or hash tables for operator state.

Advantages:

* efficient bulk ingestion of sorted batches;
* sequential merge IO in memory and storage;
* cheap snapshots through `Arc` sharing;
* natural cancellation and timestamp compaction during merge;
* one abstraction for in-memory and disk-backed state;
* compatibility with checkpointing, replay, and deterministic incremental
  semantics.

Costs:

* read amplification when many batches accumulate;
* background CPU and IO for merges;
* latency interaction between foreground lookups and background maintenance;
* global correctness requirements for retention;
* complexity from typed wrappers, dynamic batch traits, sharding, replay, and
  storage policies.

## Summary

`Trace` is DBSP's physical abstraction for the integral of a stream of weighted
changes. `Spine` is the log-structured implementation that makes traces
practical: it stores immutable indexed batches, finds data through sorted
cursors, merges batches asynchronously, compacts time, applies retention, spills
to storage, snapshots cheaply, checkpoints durably, and participates directly in
the stream graph through feedback around `Z1Trace`.

The most important indexing point is that a trace is not a general predicate
store. It is an arrangement keyed by the tuple order chosen by the operator or
compiler. Query efficiency comes from arranging data under the keys that future
deltas will seek.
