# Z-Sets and DBSP Internals in Feldera

This document explains how Feldera implements the DBSP (Database Stream
Processing) theory from the paper *"DBSP: Automatic Incremental View
Maintenance for Rich Query Languages"* (Budiu, McSherry, Ryzhyk, Tannen, 2022).
It covers Z-set representation, consolidation and distinct, the data structures
used for large Z-sets, circuit initialization from disk, dataflow through
circuits, and what the DBSP crate provides versus what the broader Feldera
platform adds.

---

## 1. What Is a Z-Set?

### 1.1 The Theory (from the Paper)

A **Z-set** over a set *A* is a function `f : A -> Z` with finite support --
i.e., a mapping from elements to integer **weights**, where only finitely many
elements have non-zero weight. The notation is `Z[A]`.

Z-sets form an **abelian group** under pointwise addition:
`(f + g)(x) = f(x) + g(x)` for all `x in A`. This group structure is the
algebraic foundation of DBSP: it lets us talk about "adding changes" (deltas)
to datasets. A positive weight means an element is present (possibly with
multiplicity); a negative weight means a deletion.

A concrete Z-set `R in Z[string]` might be:
```
R = { "joe" -> 1, "anne" -> -1 }
```
This has two elements: `joe` with weight +1, and `anne` with weight -1.

The **`distinct`** function projects a Z-set to a set:
```
distinct(m)[x] = 1   if m[x] > 0
                 0   otherwise
```

This seemingly trivial definition is carefully chosen so that all SQL
relational operators (union, intersection, difference, join, projection,
selection) can be expressed as compositions of linear Z-set operators plus
`distinct` (see Paper, Table 1).

### 1.2 Indexed Z-Sets

An **indexed Z-set** is `Z[A][K]`, i.e., a map from keys *K* to Z-sets over
values *A*. Equivalently, a set of `(key, value, weight)` tuples with unique
`(key, value)` pairs. This models SQL tables with a primary key, or
key-value-indexed structures.

### 1.3 Feldera's Rust Types

**File:** `crates/dbsp/src/algebra/zset.rs`

```rust
/// The default weight type: a signed 64-bit integer.
pub type ZWeight = i64;

/// A non-indexed Z-set: (key, weight) pairs.
pub type OrdZSet<K>          = OrdWSet<K, DynZWeight>;

/// An indexed Z-set: (key, value, weight) tuples.
pub type OrdIndexedZSet<K,V> = OrdIndexedWSet<K, V, DynZWeight>;

/// In-memory variants:
pub type VecZSet<K>            = VecWSet<K, DynZWeight>;
pub type VecIndexedZSet<K, V>  = VecIndexedWSet<K, V, DynZWeight>;
```

The weight type `R` is generic throughout the batch system (the `W` in `WSet`
stands for "weighted"), but Z-sets specialize it to `ZWeight = i64`.

**Trait hierarchy:**

```
BatchReader             -- read-only access via cursors
  -> Batch              -- mutable, can be built, merged
    -> IndexedZSet      -- Batch<Time=(), R=DynZWeight> + algebra ops
      -> ZSet           -- IndexedZSet<Val=DynUnit>  (non-indexed)
```

The `IndexedZSet` trait adds the `distinct()` method:

```rust
// crates/dbsp/src/algebra/zset.rs:168-191
fn distinct(&self) -> Self {
    let mut builder = Self::Builder::with_capacity(&factories, self.key_count(), self.len());
    let mut cursor = self.cursor();
    while cursor.key_valid() {
        let mut n_updates = 0;
        while cursor.val_valid() {
            if cursor.weight().ge0() {
                builder.push_val_diff(cursor.val(), ZWeight::one().erase());
                n_updates += 1;
            }
            cursor.step_val();
        }
        if n_updates > 0 { builder.push_key(cursor.key()); }
        cursor.step_key();
    }
    builder.done()
}
```

This walks the cursor, keeping only values with positive weight and resetting
them to weight 1 -- exactly matching the paper's definition.

---

## 2. How Distinct and Consolidation Work

### 2.1 Non-Incremental Distinct (`stream_distinct`)

**File:** `crates/dbsp/src/operator/distinct.rs`

The simplest form operates on a single batch: for each `(key, value, weight)`
with `weight > 0`, emit `(key, value, 1)`. This is the `Distinct` operator,
which just calls `input.distinct()`.

### 2.2 Incremental Distinct

The paper's key insight (Proposition 4.7) is that `distinct` can be computed
**incrementally**: given the current delta `d` and the previous integrated state
`z^-1(I(d))`, we only need to check elements in the support of `d` to see if
their weight crossed the zero boundary.

Feldera implements two incremental distinct operators:

#### DistinctIncrementalTotal (root scope, `crates/dbsp/src/operator/dynamic/distinct.rs:399-594`)

For the common case of a flat (non-nested) circuit:

```
stream --> accumulate --> DistinctIncrementalTotal --> output
                    \
                     --> accumulate_integrate_trace --> delay --> (second input)
```

The operator takes:
- `delta`: the accumulated input spine (all changes this step)
- `delayed_integral`: the previous step's full integrated state

For each `(key, value)` in the delta:
1. Look up the old weight in the delayed integral
2. Compute `new_weight = old_weight + delta_weight`
3. If old was non-positive and new is positive: emit `+1`
4. If old was positive and new is non-positive: emit `-1`
5. Otherwise: no output (weight didn't cross the boundary)

```rust
// Simplified from distinct.rs:530-593
while delta_cursor.key_valid() {
    if integral_cursor.seek_key_exact(delta_cursor.key(), None) {
        while delta_cursor.val_valid() {
            let w = **delta_cursor.weight();
            let old_weight = /* lookup in integral */;
            let new_weight = old_weight + w;

            if old_weight <= 0 && new_weight > 0 {
                builder.push_val_diff(v, +1);
            } else if old_weight > 0 && new_weight <= 0 {
                builder.push_val_diff(v, -1);
            }
            delta_cursor.step_val();
        }
    }
    delta_cursor.step_key();
}
```

This is `O(|delta|)` -- work proportional to the size of the **change**, not the
full dataset.

#### DistinctIncremental (nested scope, `distinct.rs:603-1198`)

For nested circuits (used in recursive queries), timestamps are
multi-dimensional. The operator must compute a **partial derivative** across all
timestamp dimensions. It maintains a `keys_of_interest` map to track which
`(key, value)` pairs might need re-evaluation at future timestamps due to
interactions between updates at different times.

The partial derivative computation (lines 882-892) recursively splits a
`2^n`-element array of accumulated weights (one per timestamp corner of the
n-dimensional cube), applies `distinct` to each, then computes finite
differences.

#### HashDistinct (`hash_distinct`)

An alternative that re-indexes the input by the hash of the key before
computing distinct. Useful when keys are large, because the hash provides
better data locality for the cursor seeks.

### 2.3 Distinct Consolidation Optimization

The paper identifies two key optimization rules (Propositions 4.5 and 4.6):

- **Pushing distinct through linear operators:** For filtering, projection,
  join, or Cartesian product `Q`: `Q(distinct(i)) = distinct(Q(i))` when
  inputs are positive. This allows delaying `distinct` to the end of a chain.

- **Collapsing multiple distincts:** `distinct(Q(distinct(i))) = distinct(Q(i))`
  for the same class of operators. This allows eliminating intermediate
  `distinct` calls.

Feldera tracks this in the circuit cache with `mark_distinct()` and
`is_distinct()` methods on streams (`distinct.rs:123-166`), preventing redundant
distinct operations.

### 2.4 Consolidation in Batches

Consolidation at the batch/trace level means **merging multiple batches**,
combining weights for identical `(key, value)` pairs and dropping entries where
weights cancel to zero.

**File:** `crates/dbsp/src/utils/consolidation/utils.rs`

The `dedup_payload_starting_at()` function performs in-place consolidation of
sorted key/payload vectors by summing weights of adjacent duplicate keys and
removing zero-weight entries.

The `Trace::consolidate(self) -> Option<Self::Batch>` method (on the `Trace`
trait) merges all batches in a trace into a single batch, fully consolidating
weights.

---

## 3. Data Structures for Large Z-Sets

### 3.1 The Batch Hierarchy

Feldera provides three tiers of batch storage:

| Tier | Type Prefix | Storage | Use Case |
|------|-------------|---------|----------|
| In-memory | `Vec*` | RAM (vectors) | Small batches, hot data |
| File-based | `File*` | Disk (layer files) | Large persistent data |
| Fallback | `Fallback*` | Hybrid | Auto-spills from RAM to disk |

Each tier implements `Batch` (and thus `BatchReader`), so operators are
agnostic to storage location.

### 3.2 In-Memory Batches (Vec*)

**File:** `crates/dbsp/src/trace/ord/vec/`

Uses a **trie-based** columnar structure:

```
VecIndexedWSet<K, V, R>
  └── Layer<K, Leaf<V, R>, O>
       ├── keys: Vec<K>         -- sorted key column
       ├── offs: Vec<O>         -- offsets into child layer
       └── child: Leaf<V, R>
            ├── vals: Vec<V>    -- sorted value column
            └── diffs: Vec<R>   -- weight column
```

`Layer` (`crates/dbsp/src/trace/layers.rs`) is a generic trie node: sorted keys
with offsets into a child layer. `Leaf` is the terminal layer holding
value-weight pairs. The offset type `O` implements `OrdOffset` (either `u32` or
`usize`).

For non-indexed Z-sets (`VecWSet`), `Val = ()` and the trie degenerates to a
single `Leaf<K, R>`.

### 3.3 File-Based Batches (File*)

**File:** `crates/dbsp/src/trace/ord/file/`

When Z-sets grow beyond available RAM, they are stored as **layer files** on
disk.

**Layer file format** (`crates/dbsp/src/storage/file/format.rs`):

```
┌───────────────────────────────────────────┐
│ Data Block(s)          [LFDB magic]       │
│   ├─ Items serialized with rkyv           │
│   ├─ Value map (byte offsets)             │
│   └─ CRC32C checksum                     │
├───────────────────────────────────────────┤
│ Index Block(s)         [LFIB magic]       │
│   ├─ Min/Max bounds per child (rkyv)      │
│   ├─ Row totals (cumulative counts)       │
│   ├─ Child offsets (in 512-byte blocks)   │
│   └─ CRC32C checksum                     │
├───────────────────────────────────────────┤
│ Bloom Filter(s)                           │
│   (tracking bloom filter for key lookups) │
├───────────────────────────────────────────┤
│ File Trailer (512 bytes) [LFFT magic]     │
│   ├─ Version (v5)                         │
│   ├─ Compression (optional Snappy)        │
│   ├─ Column metadata                      │
│   └─ Filter offset/size                   │
└───────────────────────────────────────────┘
```

Key features:
- **Rkyv serialization:** Zero-copy deserialization for fast reads
- **Snappy compression:** Optional, reduces I/O
- **Bloom filters:** Fast probabilistic key lookups to avoid unnecessary I/O
- **Hierarchical indexing:** Index blocks enable binary search without reading
  all data
- **512-byte alignment:** Efficient disk I/O (matches common sector sizes)
- **Buffer cache:** LRU cache for file blocks
  (`crates/dbsp/src/storage/buffer_cache/`)

### 3.4 Fallback (Hybrid) Batches

**File:** `crates/dbsp/src/trace/ord/fallback/`

Fallback batches start in memory and **automatically spill to disk** when they
exceed a configurable threshold:

```rust
enum Inner<K, R> {
    Vec(VecWSet<K, R>),     // Small: in-memory
    File(FileWSet<K, R>),   // Large: on-disk
}
```

The builder tracks byte usage and spills:

```rust
enum BuilderInner<K, R> {
    Vec(VecWSetBuilder<K, R>),        // Always in-memory
    File(FileWSetBuilder<K, R>),      // Always on-disk
    Threshold {                       // Memory with auto-spill
        vec: VecWSetBuilder<K, R>,
        size: usize,
        threshold: usize,
    },
}
```

When `size >= threshold`, the builder calls `spill()` which copies all
accumulated data from the vec builder to a file builder. From that point on,
all new data goes directly to disk.

**Decision function** (`pick_merge_destination`):
```rust
fn pick_merge_destination(batches, dst_hint) -> BatchLocation {
    match Runtime::min_index_storage_bytes() {
        0          => Storage,    // Always persist to disk
        usize::MAX => Memory,     // Never use disk
        threshold  => {
            let size = sum of batch sizes;
            if size >= threshold { Storage } else { Memory }
        }
    }
}
```

Two runtime thresholds control spilling:
- `min_index_storage_bytes`: Controls merge results (default: 10 MiB)
- `min_step_storage_bytes`: Controls within-step builders (default: disabled)

### 3.5 The Spine: A Multi-Level Trace

**File:** `crates/dbsp/src/trace/spine_async.rs`

The **`Spine`** is Feldera's primary `Trace` implementation. It is a collection
of batches organized into **9 levels** (roughly log10 of batch size), with
**background asynchronous merging**.

```
Level 0:  [b] [b] [b] [b] [b] [b] [b] [b]   <- smallest batches
Level 1:  [B] [B] [B]
Level 2:  [BB] [BB]
...
Level 8:  [BBBBBBBBB]                          <- largest batches
```

Each level has a **slot** containing:
- `loose_batches`: Batches waiting to be merged (VecDeque)
- `merging_batches`: Batches currently being merged (background thread)

**Merge policy:** Each level merges when it accumulates enough batches:
```
Levels 0-1:  merge when >= 8 loose batches (up to 64)
Level 2:     merge when >= 3 loose batches
Levels 3-5:  merge when >= 3 loose batches
Levels 6-8:  merge when >= 2 loose batches
```

**Backpressure:** When total loose batches across all levels reaches 128, the
main thread blocks until the background merger catches up. This prevents
unbounded memory growth.

**Background merger (`AsyncMerger`):** A dedicated thread per spine continuously:
1. Checks each level for merge opportunities
2. Picks the least recently added batches
3. Merges them using either `ArcPushMerger` or `ArcListMerger`
4. Inserts the result (which may land in a higher level)

**Snapshots:** The spine supports lock-free reads via `SpineSnapshot`, which
captures a consistent view of all batches (both loose and merging) using
`Arc<B>` reference counting. Operators read from snapshots while merging
continues in the background.

---

## 4. Circuit Initialization from Disk (Checkpointing)

### 4.1 Checkpoint Architecture

**File:** `crates/dbsp/src/circuit/checkpointer.rs`

The `Checkpointer` manages save/restore of circuit state:

```rust
pub struct Checkpointer {
    backend: Arc<dyn StorageBackend>,    // S3, local disk, etc.
    checkpoint_list: VecDeque<CheckpointMetadata>,
}

pub struct CheckpointMetadata {
    uuid: Uuid,
    fingerprint: u64,   // Circuit structure hash for compatibility
    size: u64,
    instant: Instant,
}
```

At least 2 checkpoints are retained (`MIN_CHECKPOINT_THRESHOLD`).

### 4.2 Saving a Checkpoint

Each operator implements `Operator::checkpoint()` and `Operator::restore()`.
For a `Spine`-based trace:

**Save process** (`spine_async.rs`):
1. **Pause merges:** Stop initiating new merges, wait for in-progress ones
2. **Persist in-memory batches:** Convert all `Vec*` batches to `File*` batches
   via `batch.persisted()`
3. **Collect file paths:** Extract the storage path from each file-based batch
4. **Serialize metadata:** Write a `CommittedSpine` (batch paths, dirty flag)
   using rkyv
5. **Register files:** Add all batch files to the checkpoint's file list

```rust
struct CommittedSpine {
    batches: Vec<String>,          // Paths to batch files
    merged: Vec<(String, String)>, // Merge references
    effort: u64,
    dirty: bool,
}
```

6. **Commit:** The `Checkpointer` writes the checkpoint marker file, garbage
   collects old checkpoints, and updates storage usage metrics

### 4.3 Restoring from a Checkpoint

**Restore process:**
1. **Read metadata:** Deserialize `CommittedSpine` from rkyv binary
2. **Restore flags:** Set dirty/filter state
3. **Reload batches:** For each batch path, call `B::from_path()` to create a
   file-based batch from the stored layer file
4. **Insert into spine:** Each restored batch is inserted into the spine's merge
   hierarchy, and background merging resumes

```rust
fn restore(&mut self, base: &StoragePath, persistent_id: &str) -> Result<(), Error> {
    let content = Runtime::storage_backend().unwrap().read(&pspine_path)?;
    let committed: CommittedSpine = /* rkyv deserialize */;
    self.dirty = committed.dirty;
    for batch_path in committed.batches {
        let batch = B::from_path(&self.factories, &batch_path.into())?;
        self.insert(batch);
    }
    Ok(())
}
```

### 4.4 Fingerprint Verification

Before restoring, the checkpointer verifies that the stored checkpoint's
**fingerprint** (a hash of the circuit structure) matches the current circuit.
This prevents restoring incompatible state when the SQL query has changed.

### 4.5 Garbage Collection

On startup, `gc_startup()` scans the storage directory, identifies files
referenced by valid checkpoints, and deletes everything else. This cleans up
incomplete checkpoints from crashes.

---

## 5. Data Flow Through Circuits

### 5.1 The Fundamental Operators

DBSP circuits are built from a small number of primitive stream operators,
directly corresponding to the paper:

| Paper Symbol | Feldera Method | File | Purpose |
|-------------|----------------|------|---------|
| `z^-1` | `stream.delay()` | `operator/z1.rs` | Unit delay (feedback) |
| `I` | `stream.integrate()` | `operator/integrate.rs` | Cumulative sum |
| `D` | `stream.differentiate()` | `operator/differentiate.rs` | Extract changes |
| `↑f` | `stream.map()` / `stream.filter()` | `operator/filter_map.rs` | Lift scalar functions |
| `+` | `stream.plus()` | `operator/plus.rs` | Pointwise addition |
| `distinct` | `stream.distinct()` | `operator/distinct.rs` | Set projection |

### 5.2 The Integration-Differentiation Duality

Integration accumulates all past inputs:

```
        ┌─────────────────► output
        │
input ──┤    ┌───┐
        └───►│ + ├──► integral
         ┌──►│   │
         │   └───┘
         │     │
         │   ┌───┐
         └───┤z-1│◄──┘
             └───┘
```

`integrate(s)[t] = s[0] + s[1] + ... + s[t]`

Differentiation extracts changes:

```
input ────────────►│     │
                   │  -  ├──► output
input ──► z^-1 ───►│     │
```

`differentiate(s)[t] = s[t] - s[t-1]`

They are inverses: `D(I(s)) = s` and `I(D(s)) = s`.

### 5.3 The Incrementalization Recipe

Given a non-incremental query `Q` on full database snapshots, the paper's
Algorithm 4.8 produces an incremental version:

1. **Translate** `Q` into a DBSP circuit of Z-set operators
2. **Consolidate** `distinct` applications (push through linear operators)
3. **Lift** the circuit to operate on streams: `↑Q`
4. **Incrementalize** by wrapping with `I` and `D`: surround with integration
   and differentiation
5. **Apply chain rule:** `(Q1 ∘ Q2)^Δ = Q1^Δ ∘ Q2^Δ`
6. **Simplify:** Linear operators are their own incremental versions; bilinear
   operators (joins) use Theorem 3.4

The result: a circuit that processes **only changes** (deltas) and produces
**only changes** to the output view.

### 5.4 Stream Execution Model

**File:** `crates/dbsp/src/circuit/circuit_builder.rs`

Each `Stream<C, D>` holds a `RefStreamValue<D>`:

```rust
struct StreamValue<D> {
    val: Option<D>,           // Current value (one per clock cycle)
    consumers: usize,         // Number of consumers
    tokens: RefCell<usize>,   // Remaining consumers this cycle
}
```

**Per-clock-cycle protocol:**
1. **Producer writes:** `put(value)` stores the value and sets token count
2. **Consumers read:** `peek()` returns a reference; `take()` gives ownership
   to the last consumer
3. **Token tracking:** Each `consume_token()` decrements; when zero, value can
   be reclaimed

**Operator types:**
- `SourceOperator<Out>`: Zero inputs, generates data (input handles)
- `UnaryOperator<In, Out>`: One input stream
- `BinaryOperator<In1, In2, Out>`: Two input streams
- `TernaryOperator<In1, In2, In3, Out>`: Three inputs
- `NaryOperator<Out>`: Variable number of inputs

### 5.5 Circuit Execution

**File:** `crates/dbsp/src/circuit/runtime.rs`

A typical execution cycle:

```
1. User pushes data via InputHandle
   └─► Creates a batch (VecIndexedWSet or similar)

2. circuit.step() or circuit.transaction()
   └─► Scheduler evaluates all operators in topological order

3. For each operator:
   a. Read input stream(s) via cursor
   b. Compute output batch
   c. Write to output stream

4. For stateful operators (traces, integrals):
   └─► Insert new batch into Spine
       └─► Background merger compacts batches asynchronously

5. Output operators collect results
   └─► User reads via OutputHandle
```

**Nested circuits** (for recursive queries) use an inner clock:
- `clock_start(scope)`: Begin inner iteration
- Evaluate inner operators until fixedpoint (`fixedpoint(scope) == true`)
- `clock_end(scope)`: Finalize and emit to outer circuit

### 5.6 Recursive Queries (from Paper Section 5-6)

Recursive queries use the `delta_0` (stream introduction) and `∫` (stream
elimination) operators to implement fixed-point iteration:

```
I ──► δ₀ ──► ↑R ──► ↑distinct ──► D ──► ∫ ──► O
        ▲                                │
        └──────────── z^-1 ──────────────┘
```

The inner loop iterates `R` (the recursive body) on accumulated changes until
no new facts are derived (fixedpoint). The `D` operator extracts new facts per
iteration; `∫` aggregates them.

In Feldera, this is realized with `circuit.iterate()` which creates a nested
`ChildCircuit` with its own clock and fixedpoint detection.

---

## 6. The DBSP Crate: What It Provides

**Location:** `crates/dbsp/`

The `dbsp` crate is a **self-contained** incremental computation engine. It
provides everything described in the paper plus substantial engineering for
production use:

### 6.1 From the Paper (Theory)

- **Algebraic types:** Z-sets, indexed Z-sets, groups, rings (`algebra/`)
- **Stream and circuit model:** `Stream<C, D>`, `RootCircuit`, `ChildCircuit`
  (`circuit/`)
- **Primitive operators:** `z^-1`, `I`, `D`, lifting, `distinct`
  (`operator/z1.rs`, `integrate.rs`, `differentiate.rs`, `distinct.rs`)
- **Relational operators:** join, semijoin, filter, map, project, aggregate,
  group-by (`operator/join.rs`, `filter_map.rs`, `aggregate.rs`, `group.rs`)
- **Recursive query support:** Nested circuits with fixedpoint iteration
  (`operator/recursive.rs`)

### 6.2 Beyond the Paper (Engineering)

- **Multi-tiered storage:** Vec/File/Fallback batch implementations with
  automatic spilling (`trace/ord/`)
- **Async background merging:** The `Spine` with 9-level merge hierarchy and
  backpressure (`trace/spine_async.rs`)
- **Checkpointing:** Full save/restore of circuit state (`circuit/checkpointer.rs`)
- **Multi-worker parallelism:** Sharded streams with cross-worker communication
  (`operator/communication/`)
- **Dynamic dispatch:** Type-erased operator API to reduce compilation time
  (`dynamic/`, `operator/dynamic/`)
- **Buffer cache and storage backends:** LRU caching, pluggable storage
  (`storage/`)
- **Bloom filters:** Probabilistic key existence checks for file-based batches
- **Time-series operators:** Rolling aggregates, windowing, waterline
  (`operator/time_series/`)
- **As-of joins:** Temporal joins for event-time processing
  (`operator/asof_join.rs`)
- **Streaming binary operators:** Async operators that yield results in chunks
  for long-running computations (`operator/async_stream_operators.rs`)

### 6.3 What You Can Do with Only the DBSP Crate

The DBSP crate is a fully functional library for building incremental dataflow
programs in Rust. You can:

```rust
use dbsp::*;

let (mut circuit, (input, output)) = Runtime::init_circuit(4, |circuit| {
    // Create input stream
    let (input, handle) = circuit.add_input_indexed_zset::<u64, String>();

    // Build an incremental query
    let filtered = input.filter(|k, _v| *k > 10);
    let distinct = filtered.distinct();
    let output = distinct.accumulate_output();

    Ok((handle, output))
}).unwrap();

// Feed data
input.push(42, "hello".into(), 1);
circuit.transaction().unwrap();

// Read results
let result = output.consolidate();
```

This includes:
- Creating input/output streams
- All relational operators (filter, map, join, aggregate, distinct, group-by)
- Recursive queries via `circuit.iterate()`
- Checkpointing and recovery
- Multi-threaded execution
- Persistent storage for datasets larger than RAM

**What you cannot do** with the DBSP crate alone:
- Parse SQL (you must build circuits programmatically in Rust)
- Connect to Kafka, S3, HTTP, or other external systems
- Manage pipeline lifecycle via REST API
- Run the web console UI

---

## 7. What the Feldera Platform Adds

The broader Feldera platform wraps the DBSP crate with production
infrastructure:

### 7.1 SQL-to-DBSP Compiler

**Location:** `sql-to-dbsp-compiler/` (Java, Apache Calcite)

Translates SQL DDL/DML into DBSP circuits:
```
SQL query
  → Calcite parse/validate/optimize
  → CalciteProgram (relational algebra)
  → Incrementalize (Algorithm 4.8 from paper)
  → Generate Rust code
  → Compile to binary
```

The generated Rust code uses the DBSP crate API to build circuits
programmatically.

### 7.2 Intermediate Representation (IR)

**Location:** `crates/ir/`

A multi-level IR for the compilation pipeline:
- **HIR** (CalcitePlan): High-level from Calcite
- **MIR** (Middle IR): Optimized dataflow graph
- **LIR** (Low-level IR): Near Rust-level representation

Supports diffing to detect changes to views/tables without full recompilation.

### 7.3 Connectors and Adapters

**Location:** `crates/adapters/`, `crates/adapterlib/`

Pluggable I/O for:
- **Transports:** Kafka, S3, HTTP, databases, Iceberg, Delta Lake
- **Formats:** CSV, JSON, Avro, MessagePack, Arrow/Parquet
- **Fault tolerance:** Step-based input replay + output deduplication

### 7.4 Pipeline Manager

**Location:** `crates/pipeline-manager/`

Orchestrates the full lifecycle:
- REST API for pipeline CRUD
- Compilation (SQL → Rust → binary)
- Execution management (start, stop, pause, checkpoint)
- Database-backed state (PostgreSQL)
- Ad-hoc query support on running pipelines
- Metrics and monitoring

### 7.5 Web Console

A browser UI for creating, managing, and monitoring pipelines.

---

## 8. Architecture Diagram

```
┌──────────────────────────────────────────────────────────┐
│                    Feldera Platform                       │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────┐  │
│  │ Web Console │  │ Pipeline Mgr │  │ SQL Compiler   │  │
│  │ (React UI)  │  │ (REST API)   │  │ (Calcite/Java) │  │
│  └──────┬──────┘  └──────┬───────┘  └───────┬────────┘  │
│         │                │                   │           │
│         └────────────────┴─────────┬─────────┘           │
│                                    │                     │
│  ┌─────────────────────────────────▼──────────────────┐  │
│  │                   Adapters                          │  │
│  │  Kafka · S3 · HTTP · Iceberg · Delta Lake · ...    │  │
│  └─────────────────────────┬──────────────────────────┘  │
│                            │                             │
│  ┌─────────────────────────▼──────────────────────────┐  │
│  │                    DBSP Crate                       │  │
│  │  ┌──────────┐  ┌───────────┐  ┌────────────────┐  │  │
│  │  │ Circuits │  │ Operators │  │   Traces        │  │  │
│  │  │ & Streams│  │ (50+ ops) │  │   & Batches     │  │  │
│  │  └──────────┘  └───────────┘  │  ┌───────────┐  │  │  │
│  │  ┌──────────┐  ┌───────────┐  │  │ Vec (RAM) │  │  │  │
│  │  │ Runtime  │  │Checkpoint │  │  │ File(Disk)│  │  │  │
│  │  │& Sched.  │  │& Recovery │  │  │ Fallback  │  │  │  │
│  │  └──────────┘  └───────────┘  │  │  (Hybrid) │  │  │  │
│  │                               │  └───────────┘  │  │  │
│  │                               │  ┌───────────┐  │  │  │
│  │                               │  │  Spine    │  │  │  │
│  │                               │  │ (9-level  │  │  │  │
│  │                               │  │ async     │  │  │  │
│  │                               │  │  merge)   │  │  │  │
│  │                               │  └───────────┘  │  │  │
│  │                               └────────────────┘  │  │
│  └────────────────────────────────────────────────────┘  │
│                            │                             │
│  ┌─────────────────────────▼──────────────────────────┐  │
│  │                 Storage Backend                     │  │
│  │       Local Disk (POSIX) · S3 · Memory             │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
```

---

## 9. Summary: Paper Theory to Running System

| Paper Concept | Feldera Implementation |
|--------------|----------------------|
| Z-set `Z[A]` | `OrdZSet<K>`, `VecZSet<K>` with `ZWeight = i64` |
| Indexed Z-set `Z[A][K]` | `OrdIndexedZSet<K,V>`, `VecIndexedZSet<K,V>` |
| Stream `s : N -> A` | `Stream<C, D>` carrying batches per clock cycle |
| Delay `z^-1` | `Z1<T>` operator, `stream.delay()` |
| Integration `I` | `stream.integrate()` (Plus + Z1 feedback loop) |
| Differentiation `D` | `stream.differentiate()` (Minus + Z1) |
| Lifting `↑f` | `stream.map()`, `stream.filter()` |
| `distinct` | `stream.distinct()` / `stream.stream_distinct()` |
| Incremental `Q^Δ` | Chain rule + operator-specific optimizations |
| Circuit composition | `RootCircuit::build()`, method chaining on streams |
| Recursive queries | `circuit.iterate()` with `ChildCircuit` |
| Fixed-point `∫` | Inner loop with `fixedpoint()` detection |

The DBSP crate faithfully implements the paper's mathematical framework while
adding the engineering needed for production use: persistent multi-tiered
storage, background compaction, checkpointing, and multi-worker parallelism.
The Feldera platform then adds SQL compilation, I/O connectors, and
orchestration to make it accessible as a streaming database.
