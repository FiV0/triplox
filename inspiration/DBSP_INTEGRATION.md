# DBSP Crate Integration for OLTP Incremental View Maintenance

This document evaluates using the `dbsp` crate as a library for incremental view maintenance (IVM) inside an OLTP engine. The OLTP engine handles transactions; DBSP maintains materialized views by processing deltas produced by those transactions.

## Executive Summary

The DBSP crate is a strong fit for this use case. Its core computational model — processing streams of changes (Z-sets) through circuits of incremental operators — maps directly to the problem of maintaining materialized views as an OLTP engine processes writes. The crate already uses extensive dynamic dispatch internally (`DynData`, `Box<dyn Node>`), limiting monomorphization costs and making runtime circuit construction viable.

Key strengths:
- Circuit topology is built at runtime via a builder callback — no compile-time graph structure
- Operators are stored as trait objects (`Box<dyn Node>`) — bounded monomorphization
- Z-sets natively represent insertions (+weight) and deletions (-weight) — natural fit for transaction deltas
- Upsert handles (`SetHandle`, `MapHandle`) support OLTP mutation patterns directly
- Checkpointing and fault tolerance are built in

Key concerns:
- Circuit topology is immutable after construction — adding a new view requires a new circuit
- Per-step overhead must be profiled at OLTP transaction rates
- Stateful operators (joins, aggregates) accumulate state that grows with data size
- Row types must implement `DBData` (requires `Eq + Ord + Hash + Clone + Send + Sync + rkyv` serialization)

---

## Architecture Overview

### How DBSP Constructs Circuits

Circuits are built at runtime inside a closure passed to `RootCircuit::build()`:

```rust
let (handle, (input, output)) = RootCircuit::build(|circuit| {
    let (input_stream, input_handle) = circuit.add_input_zset::<MyRow>();
    let filtered = input_stream.filter(|row| row.active);
    let output_handle = filtered.output();
    Ok((input_handle, output_handle))
})?;
```

The closure runs at runtime. You can branch, loop, and programmatically decide which operators to wire based on your query plan. The resulting circuit is then driven by calling `handle.step()`.

### Single-threaded vs Multi-threaded

| Mode | Constructor | Handle Type | Notes |
|------|------------|-------------|-------|
| Single-threaded | `RootCircuit::build()` | `CircuitHandle` | Runs on calling thread |
| Multi-threaded | `Runtime::init_circuit()` | `DBSPHandle` | Spawns N worker threads, each with own circuit replica |

Multi-threaded mode requires the constructor closure to be `Clone + Send + 'static`. Each worker independently constructs the same circuit. Input data is distributed across workers; output must be gathered.

```rust
let (dbsp_handle, (input, output)) = Runtime::init_circuit(
    CircuitConfig::with_workers(4),
    |circuit| {
        let (stream, handle) = circuit.add_input_zset::<MyRow>();
        let result = stream.filter(|r| r.value > 0);
        Ok((handle, result.output()))
    }
)?;
```

### Type System: Static API, Dynamic Internals

DBSP uses a two-layer type system:

**API layer (static):** Generic types like `Stream<C, OrdZSet<K>>` provide compile-time safety.

**Internal layer (dynamic):** All data is erased to `DynData` trait objects. `OrdZSet<K>` wraps `DynOrdZSet<DynData>` internally. Operators are stored as `Box<dyn Node>`. This means:
- Only ~2 monomorphizations per operator (for `DynData`, not per user type)
- Operator code is compiled once in the `dbsp` crate, not re-monomorphized per schema
- Runtime overhead is dynamic dispatch (vtable calls), not code bloat

---

## Input/Output Interface

### Input Handles

Data enters the circuit through handles returned by `add_input_*` methods:

| Handle | Created By | Partitioning | Use Case |
|--------|-----------|--------------|----------|
| `ZSetHandle<K>` | `add_input_zset::<K>()` | Round-robin | Raw change streams (insert/delete with weights) |
| `IndexedZSetHandle<K, V>` | `add_input_indexed_zset::<K, V>()` | Round-robin | Key-value change streams |
| `SetHandle<K>` | `add_input_set::<K>()` | Hash-based | Set membership (insert/delete without weights) |
| `MapHandle<K, V, U>` | `add_input_map::<K, V, U>()` | Hash-based | Key-value maps with upsert semantics |

**For OLTP integration, `MapHandle` is the most natural fit** — it supports `Insert(v)`, `Delete`, and `Update(u)` operations per key, matching standard OLTP mutation patterns.

#### Key Handle Methods

```rust
// ZSetHandle: push individual changes
handle.push(key, weight);       // weight: +1 for insert, -1 for delete
handle.append(&mut vec![...]);  // batch push

// MapHandle: upsert semantics
handle.push(key, Update::Insert(value));
handle.push(key, Update::Delete);
handle.push(key, Update::Update(patch));

// All handles support staging for latency hiding
handle.stage(&mut buffers);     // prepare off critical path
buffers.flush();                // inject into circuit
```

All handles are **thread-safe** (`Arc`-based) and cloneable. Multiple threads can push concurrently. Data is buffered in per-worker mailboxes until the next `step()` call.

**Concurrency caveat:** Pushes are atomic per-worker but not globally atomic. A concurrent `step()` may see partial data from an in-progress `append()`.

### Output Handles

```rust
let output_handle = stream.output();

// After step():
let batch: OrdZSet<K> = output_handle.consolidate();  // merged from all workers
let per_worker = output_handle.take_from_worker(0);    // single worker output
```

Output reads are **destructive** — `take_from_worker` replaces the mailbox with `None`. Unread values are overwritten on the next step.

---

## Available Operators

### Stateless Operators (no persistent state)

| Operator | Signature Pattern | Description |
|----------|------------------|-------------|
| `filter()` | `Stream<B> -> Stream<B>` | Element-wise predicate filtering |
| `map()` | `Stream<B> -> Stream<OrdZSet<K2>>` | Transform elements |
| `map_index()` | `Stream<B> -> Stream<OrdIndexedZSet<K2, V2>>` | Transform to key-value pairs |
| `flat_map()` | `Stream<B> -> Stream<OrdZSet<K2>>` | One-to-many transform |
| `plus()` | `Stream<D> x Stream<D> -> Stream<D>` | Z-set union (weight addition) |
| `minus()` | `Stream<D> x Stream<D> -> Stream<D>` | Z-set difference (weight subtraction) |
| `neg()` | `Stream<D> -> Stream<D>` | Negate all weights |
| `inspect()` | `Stream<T> -> Stream<T>` | Side-effecting observation |

### Stateful Operators (maintain state across steps)

| Operator | Signature Pattern | Description | State |
|----------|------------------|-------------|-------|
| `join()` | `IZS<K,V1> x IZS<K,V2> -> ZS<O>` | Incremental equi-join | Both input relations |
| `semijoin_stream()` | `IZS<K,V> x ZS<K> -> ZS<(K,V)>` | Filter by key existence | None (stream variant) |
| `asof_join()` | `IZS<K,V1> x IZS<K,V2> -> ZS<O>` | Temporal nearest-match join | Both input relations |
| `aggregate()` | `IZS<K,V> -> IZS<K,A>` | Group-by aggregation | Per-key accumulator |
| `aggregate_linear()` | `IZS<K,V> -> IZS<K,A>` | Linear aggregates (SUM, COUNT) | Per-key accumulator (lighter) |
| `distinct()` | `Stream<Z> -> Stream<Z>` | Multiset to set | Seen elements |
| `topk()` | `IZS<K,V> -> IZS<K,ZS<V>>` | Top-K per group | Per-key top elements |
| `lag()` | Window lookback within groups | Per-key value history |
| `integrate()` | `Stream<D> -> Stream<D>` | Cumulative sum over time | Running total |
| `differentiate()` | `Stream<D> -> Stream<D>` | Delta from previous step | Previous value |
| `delay()` / `z1()` | `Stream<D> -> Stream<D>` | Unit time delay | One element buffer |

### Recursive/Fixed-Point

```rust
circuit.recursive(|child, input: Stream<_, OrdZSet<Edge>>| {
    let reachable = // ... build recursive circuit
    Ok(reachable)
});
```

Iterates until convergence. Automatic `distinct()` on output ensures termination.

### Non-Incremental Variants

Most stateful operators have `stream_*` variants (e.g., `stream_aggregate()`, `stream_join()`, `stream_distinct()`) that process each batch independently without maintaining cross-step state. Useful for one-shot computations within a step.

---

## DBData Trait Requirements

Any type used as a key or value in DBSP must implement `DBData`:

```rust
// Automatically derived for types satisfying all bounds:
pub trait DBData:
    Default + Clone + Eq + Ord + Hash
    + SizeOf + Send + Sync + Debug
    + ArchivedDBData       // rkyv zero-copy serialization
    + IsNone               // null/none detection
    + 'static
{}
```

**Implications for dynamic row types:**
- You need `Eq + Ord + Hash` — deterministic comparison and hashing
- You need `rkyv` serialization — zero-copy archival for persistent storage
- You need `SizeOf` — memory tracking
- A dynamic row type (e.g., `Vec<ScalarValue>`) can satisfy these, but implementing `ArchivedDBData` (rkyv) for a fully dynamic schema requires care

**Weights** additionally require `MonoidValue` (addition with identity element). The standard weight type is `ZWeight` (i64).

---

## Circuit Execution Model

### The Step Loop

```rust
// 1. Push transaction deltas
input_handle.push(key, Update::Insert(value));

// 2. Execute one circuit step
handle.step()?;  // processes all buffered input, propagates through operators

// 3. Read output
let changes = output_handle.consolidate();
```

`step()` is a **blocking, synchronous** call that:
1. Reads all input mailboxes (consuming buffered data)
2. Evaluates every operator in topological order
3. Writes results to output mailboxes
4. Returns `Result<(), DbspError>`

Takes `&self` (interior mutability via `RefCell`). Not thread-safe — single-threaded within each worker.

### Transaction Support

```rust
handle.start_transaction()?;
// ... push data, step ...
handle.start_commit_transaction()?;
while !handle.is_commit_complete() {
    // wait or poll
}
```

Commit progress can be tracked via `handle.commit_progress()`.

### Checkpointing

```rust
// Create checkpoint
let builder = handle.checkpoint();

// List available checkpoints
let checkpoints = handle.list_checkpoints()?;

// Garbage-collect old checkpoints
handle.gc_checkpoint(keep_set)?;
```

Checkpoints persist operator state to a storage backend, enabling recovery after crashes.

---

## Integration Concerns for OLTP

### 1. Circuit Immutability

**Problem:** Once `RootCircuit::build()` returns, the circuit topology is frozen. You cannot add operators, streams, or new views to a running circuit.

**Impact:** `CREATE MATERIALIZED VIEW` at runtime requires constructing a new circuit. Dropping a view means tearing down a circuit.

**Mitigation strategies:**
- **One circuit per view** — Each materialized view is a separate `RootCircuit`. Simple but multiplies memory for shared base tables (each circuit maintains its own operator state).
- **Circuit rebuild** — When views change, build a new circuit containing all current views. Requires replaying state or restoring from checkpoint.
- **Pre-built circuit templates** — If the set of possible view shapes is bounded, pre-compile variants and select at runtime.

### 2. Per-Step Latency

**Problem:** Each `step()` call processes all operators in the circuit, even if most have empty input. For OLTP with high transaction rates, per-step overhead matters.

**What to measure:**
- Empty step cost (no input, all operators short-circuit)
- Per-operator fixed overhead (scheduler dispatch, async future poll)
- Cost scales with number of operators in the circuit

**Mitigation:**
- **Micro-batching** — Accumulate N transactions, then call `step()` once. Amortizes fixed overhead. Trades latency for throughput.
- **Circuit partitioning** — Separate circuits for independent views. Only step circuits whose input tables were modified.

### 3. State Growth for Stateful Operators

**Problem:** Operators like `join()`, `aggregate()`, and `distinct()` maintain the full state of their input relations. For an OLTP workload with large tables, this means DBSP holds a copy of relevant data in its operator state.

**State size by operator:**
| Operator | State Size |
|----------|-----------|
| `join(lhs, rhs)` | `O(\|lhs\| + \|rhs\|)` — both full relations |
| `aggregate()` | `O(num_groups)` — per-key accumulator |
| `aggregate_linear()` | `O(num_groups)` — lighter per-key state |
| `distinct()` | `O(num_distinct_elements)` |
| `topk()` | `O(num_groups * K)` |
| `filter()`, `map()` | `O(1)` — stateless |

**Mitigation:**
- Persistent storage backend — DBSP supports file-backed batches (`FileZSet`, `FileIndexedZSet`) with memory-to-disk spillover
- Spine merging — Background compaction of trace state (up to 9 levels, configurable)
- `retain_keys()` / waterline-based GC — Prune state below a temporal watermark

### 4. Dynamic Row Types

**Problem:** DBSP's typed API requires concrete Rust types implementing `DBData` for keys and values. An OLTP engine with dynamic schemas needs a row representation that satisfies these bounds at runtime.

**Options:**
- **Fixed schema structs** — Generate Rust structs per table schema (requires compilation, defeats dynamic goal)
- **Generic row type** — e.g., `Vec<ScalarValue>` or Arrow-based rows. Must implement `Eq + Ord + Hash + rkyv + SizeOf + Default + Clone + Debug + Send + Sync`. Feasible but requires careful implementation, especially for `rkyv`.
- **Enum-tagged columns** — Similar to above but with typed enum variants per SQL type

The `DynData` trait object layer means DBSP itself won't re-monomorphize per schema — the cost is in implementing the trait bounds for your row type, not in DBSP's internals.

### 5. Expression Evaluation

**Problem:** Operator closures like `filter(|row| row.age > 30)` are Rust closures monomorphized at compile time. For dynamic view creation, predicates and projections must be evaluated at runtime.

**Options:**
- **Interpreter** — Walk an AST for each row. Simple, ~10-100x slower per row than native code.
- **JIT compilation** — Cranelift or LLVM. Near-native speed, significant implementation complexity.
- **Pre-compiled operator kernels** — If your expression language is small (comparisons, arithmetic, field access), pre-compile a library of composable kernels and select at runtime.

This is the main engineering challenge for fully dynamic circuit construction. DBSP's operator framework is agnostic to how the closures work internally — it just calls `Fn(&Row) -> bool` etc.

### 6. Multi-Worker Input Distribution

**Problem:** In multi-threaded mode, `CollectionHandle` distributes input round-robin, while `UpsertHandle` uses hash-based partitioning. For OLTP where a single transaction touches multiple keys, you need to understand the consistency model.

**Key points:**
- Pushes are not globally atomic — a `step()` concurrent with `append()` may see partial data
- For correctness, ensure all data for a transaction is pushed before calling `step()`
- `UpsertHandle` guarantees all updates to the same key go to the same worker (hash partitioning)

### 7. Memory and Compilation Overhead

**Compilation:** The `mono.rs` module limits monomorphization to ~2 variants per operator inside the `dbsp` crate. Your integration adds monomorphizations only for your concrete row types, not per-operator-per-type combinations.

**Memory per circuit:**
- `CircuitInner` stores `Vec<Box<dyn Node>>` — one allocation per operator
- Each stateful operator maintains its own trace/spine
- Spine levels: up to 9, with background merging
- Buffer caches per worker thread (LRU/S3-FIFO policies)

---

## Recommended Integration Architecture

```
OLTP Engine
    |
    |-- Transaction Processing
    |       |
    |       |-- Write to base tables
    |       |-- Produce deltas (inserts/deletes/updates)
    |       |
    |       v
    |-- DBSP Circuit Manager
    |       |
    |       |-- Circuit per view (or group of views)
    |       |-- Push deltas via MapHandle/ZSetHandle
    |       |-- Call step() (per-transaction or micro-batched)
    |       |-- Read updated view state via OutputHandle
    |       |
    |       v
    |-- View Storage
            |
            |-- Materialized view results
            |-- Queryable by OLTP read path
```

### Suggested Approach

1. **Start single-threaded** — Use `RootCircuit::build()`. Simpler correctness model. Profile before adding workers.

2. **Define a dynamic row type** — Implement `DBData` for a generic row representation. This is the foundational piece.

3. **Build circuits from your query plan** — Translate your internal query representation to DBSP operator calls inside the `build()` closure. The circuit topology is determined at runtime.

4. **Use MapHandle for OLTP tables** — Upsert semantics match INSERT/UPDATE/DELETE patterns.

5. **Micro-batch steps** — Accumulate deltas from N transactions, then call `step()`. Tune N for your latency/throughput tradeoff.

6. **One circuit per view** — Simplest lifecycle management. Create on `CREATE MATERIALIZED VIEW`, destroy on `DROP`.

7. **Profile aggressively** — Measure empty-step cost, per-operator overhead, and state growth under your expected workload before committing to the architecture.

---

## Key Source Files

| Component | Path |
|-----------|------|
| Circuit builder API | `crates/dbsp/src/circuit/circuit_builder.rs` |
| Runtime / DBSPHandle | `crates/dbsp/src/circuit/dbsp_handle.rs` |
| Input operators & handles | `crates/dbsp/src/operator/input.rs` |
| Upsert handles | `crates/dbsp/src/operator/dynamic/input_upsert.rs` |
| Dynamic type system | `crates/dbsp/src/dynamic/data.rs` |
| Typed batch wrappers | `crates/dbsp/src/typed_batch.rs` |
| Monomorphization limits | `crates/dbsp/src/mono.rs` |
| Operator implementations | `crates/dbsp/src/operator/` |
| Trace / Spine | `crates/dbsp/src/trace/` |
| Storage backends | `crates/storage/` |
