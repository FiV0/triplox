# Spec: Incremental Triple-Pattern Queries

## Assumptions

1. This first implementation is internal to the Rust `triplox` crate and does not add a client protocol or JVM API.
2. "Writer node" maps to the current `Node<L: TxLog>` type when used through `SubmitNode`/`QueryNode`; no separate reader-node type exists yet.
3. The supported query subset is conjunctive Datomic-style triple patterns with constant attributes, matching the current `compile_pattern()` constraint that attribute position is a keyword or entid.
4. `or`, `not`, predicates, functions, aggregates, `:in`, `:order`, `:limit`, and pull-style outputs are out of scope for the first pass.
5. CDC deltas come from `src/slate/cdc.rs::CdcStream` and `datoms_from_cdc_transaction()`, introduced by PR 37, rather than from the Triplox `TxLog`.
6. The first implementation may do redundant I/O by reading SlateDB WAL from the writer node; this is acceptable until reader nodes exist.
7. `src/query.rs` is read-only for this work. Incremental-query parsing, validation, and planning lives in a new file such as `src/inc_query.rs`.

## Objective

Build the first Triplox incremental-query engine for writer nodes. A caller can register a supported triple-pattern query, receive an initial result basis, and then receive per-transaction result deltas as SlateDB WAL transactions are decoded into datoms.

The design follows the Hooray DBSP-standard direction from `FiV0/hooray2#6`: compile a query into a static DBSP circuit of ordinary unary/binary operators instead of a custom WCOJ delta engine. Unlike Hooray, Triplox should use the Rust `dbsp` crate directly for circuit construction and stepping.

Success means a query such as:

```clojure
[:find ?name ?age
 :where
 [?e :name ?name]
 [?e :age ?age]]
```

can be registered on a writer node, initialized from current SlateDB state, and updated by WAL CDC datoms so that the emitted deltas match full-query recomputation at each transaction basis.

## Tech Stack

- Rust workspace: `triplox`, `edn`, `triplox-client`
- Async runtime: `tokio`
- Storage and CDC: SlateDB `WalReader`, `CdcStream`, `datoms_from_cdc_transaction()`
- Query AST: `edn::query::ParsedQuery`, `WhereClause::Pattern`
- Query value encoding: Triplox `codec::Encode`/`Decode` over `DataType`
- DBSP dependency: `dbsp = "0.299.0"`; verified current crate metadata requires `rust-version = 1.93.1`, and this worktree currently uses `rustc 1.95.0`

## Commands

Build:

```bash
cargo build --workspace
```

Format:

```bash
cargo fmt --check
cargo fmt
```

Targeted tests while developing:

```bash
cargo test -p triplox incremental
cargo test -p triplox slate::cdc
cargo test -p triplox node
```

Full required verification:

```bash
cargo test --workspace
cargo clippy --workspace --all-targets --all-features
```

## Project Structure

```text
src/incremental.rs        -> Writer-node incremental query service, handles, state, and public crate-internal API
src/incremental/plan.rs   -> Query subset validation and DBSP plan construction data
src/incremental/circuit.rs -> DBSP circuit assembly and typed input/output handles
src/incremental/cdc.rs    -> WAL CDC task, cursor tracking, and datom-to-circuit delta conversion
src/inc_query.rs          -> Incremental-query AST helpers, subset validation, variable ordering, and pattern planning
src/slate/cdc.rs          -> Existing CDC stream and `datoms_from_cdc_transaction()` source
src/query.rs              -> Existing one-shot query compiler; semantic reference only, not modified by this work
src/node.rs               -> Attach incremental service to `Node<L: TxLog>` for writer nodes
tests/                    -> End-to-end node tests when behavior crosses crate boundaries
```

Start with a flat `src/incremental.rs` if the implementation is still small; split into submodules when the planner, circuit, and CDC loop become easier to review separately.

## Data Model

The DBSP circuit should not use `DataType` directly as a Z-set key because `DataType` has floats, maps, and vectors and does not implement `Ord`. Instead, use encoded Triplox values:

```rust
type EncodedValue = Vec<u8>;
type EncodedRow = Vec<EncodedValue>;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
struct EncodedTriple {
    entity: EncodedValue,
    attribute: i64,
    value: EncodedValue,
}
```

Entity positions are encoded as `DataType::Long(entity).encode()`. This keeps joins correct when an entity variable in one pattern is compared with a ref value in another pattern.

`EncodedTriple` is only the base fact input shape. After pattern filtering, DBSP operators should pass around rows of encoded values:

```text
OrdZSet<EncodedTriple>  -> base CDC/input relation
OrdZSet<EncodedRow>     -> pattern rows, join rows, and projected result rows
```

For example, `[?e :name ?name]` maps an `EncodedTriple` to an `EncodedRow` containing `[?e, ?name]`, and a join with `[?e :age ?age]` produces an `EncodedRow` containing `[?e, ?name, ?age]`.

CDC conversion should operate at the transaction/batch level, not one helper call per datom:

```rust
fn datoms_to_zset(datoms: &[Datom], schema: &Schema) -> Result<OrdZSet<EncodedTriple>>;
```

`datoms_to_zset()` translates every CDC datom in one transaction into weighted `EncodedTriple` records, using `+1` for `DatomOp::Assert` and `-1` for `DatomOp::Retract`. Duplicate triples in the same CDC transaction are combined by the Z-set representation before the circuit step.

Projection decodes final `EncodedValue`s back to `DataType` for `QueryResult`-compatible rows.

## Query Scope

Always supported in v1:

- `WhereClause::Pattern` only.
- Attribute must be `PatternNonValuePlace::Ident` or `PatternNonValuePlace::Entid`.
- Entity and value positions may be variables, constants, or placeholders.
- Multiple patterns may share variables and form joins.
- Registration validation rejects repeated variables inside one triple pattern.

Rejected in v1:

- Variable or placeholder attribute positions.
- Repeated variables inside one triple pattern.
- `OrJoin`, `NotJoin`, `Pred`, `WhereFn`, rules, aggregates, order, limit, and non-relational find specs.
- `:in` bindings and query args.
- Any query shape whose incremental result cannot be represented as a finite relation of projected rows.

## Circuit Shape

Each registered query owns a DBSP circuit:

1. Add one input Z-set stream of `EncodedTriple`.
2. For each triple pattern, derive a stream from the input using filters for attribute and constants.
3. Map each pattern stream from `EncodedTriple` into an `EncodedRow` in the variable layout chosen by the planner.
4. Build a deterministic left-deep chain of binary indexed joins over `EncodedRow` values using `map_index()` and `join_index()`.
5. Map the final `EncodedRow` into `:find` order.
6. Attach an output handle to the final delta stream.

The planner should be deterministic. Mirror the current one-shot query variable ordering semantics in `src/inc_query.rs`, including disconnected pattern groups as Cartesian products. Do not move or expose helpers from `src/query.rs`.

## Initialization

Registering a query must create a consistent starting point:

1. Capture the writer node's latest indexed transaction basis from the existing `Indexer` state or `latest_tx_basis_from_sdb()`.
2. Capture a SlateDB WAL cursor from the database status at the same point, using the durable sequence as `CdcCursor.last_seq`.
3. Scan current EAV index state up to the captured `tx_eid` and feed the circuit with positive triples to prime DBSP state.
4. Step the circuit once and discard the initial output.
5. Store the query with its starting `TxBasis` and CDC cursor.

The initial scan can be intentionally simple for v1: scan EAV and filter in memory by the query's needed attributes. Attribute-specific scans are a later optimization.

Registration returns only future deltas after the registration basis. Initial snapshot rows can be added later as an explicit option.

## CDC and Watermark State

The incremental service owns one WAL reader task per writer node, not one per query. It fans decoded datom deltas out to registered query circuits.

State to track:

```rust
#[derive(Debug, Clone)]
struct IncrementalWalState {
    wal_cursor: CdcCursor,
    last_seen_seq: u64,
    last_indexed_basis: Option<TxBasis>,
}
```

Meanings:

- `wal_cursor`: persisted/resumable position used to construct `CdcStream`.
- `last_seen_seq`: latest WAL transaction sequence observed by the CDC task.
- `last_indexed_basis`: latest transaction basis known to be visible in the writer node's SlateDB memory indexes.

Do not persist a separate `last_applied_seq` in v1. In the writer-node-only design, the single ordered CDC apply loop defines applied progress: after a CDC transaction has been appended to DBSP, stepped, and sent to subscriptions, it is applied. If future observability needs a "DBSP has caught up to WAL seq N" metric, add it as in-memory instrumentation rather than correctness state.

For writer nodes, `last_indexed_basis` should normally be ahead of the WAL reader. Still, the service should only apply a CDC transaction after the corresponding write has reached memory indexes. In v1 this can be implemented by extracting the transaction entity datoms from the CDC transaction, deriving the `TxKey`, and using the existing `TxWaiter::await_indexed()` path before circuit application. If the tx entity cannot be derived, apply the CDC transaction but mark its output basis as sequence-only and keep this as a test-covered fallback.

Applying one CDC transaction:

1. `CdcStream::next_transaction()` yields a WAL transaction.
2. `datoms_from_cdc_transaction()` decodes EAV index entries and filters tombstones/non-EAV keys.
3. Extract transaction metadata from `:db/txId`, `:db/txInstant`, and the tx entity id when present.
4. Wait until the writer node has indexed that transaction.
5. Convert datoms to one weighted `EncodedTriple` Z-set update.
6. Append the delta batch to the DBSP input handle.
7. Step each registered query circuit.
8. Store each non-empty output delta with its `TxBasis` and WAL sequence.
9. Advance the CDC cursor after the transaction has been processed successfully.

## API Shape

Keep the first API crate-internal until behavior stabilizes:

```rust
pub(crate) struct IncrementalQueryHandle {
    id: IncrementalQueryId,
}

pub(crate) struct IncrementalQuerySubscription {
    pub handle: IncrementalQueryHandle,
    pub deltas: tokio::sync::mpsc::Receiver<IncrementalQueryDelta>,
}

pub(crate) struct IncrementalQueryDelta {
    pub basis: Option<TxBasis>,
    pub wal_seq: u64,
    pub rows: Vec<(Vec<DataType>, isize)>,
}

impl<L: TxLog> Node<L> {
    pub(crate) async fn register_incremental_query(
        &self,
        query: ParsedQuery,
    ) -> Result<IncrementalQuerySubscription, Error>;

    pub(crate) async fn unregister_incremental_query(
        &self,
        handle: IncrementalQueryHandle,
    ) -> Result<(), Error>;
}
```

Use a bounded `tokio::sync::mpsc` channel for the subscription. Incremental deltas are an ordered log: dropping one delta corrupts the receiver's integrated result. Bounded `mpsc` makes backpressure explicit by letting the WAL/DBSP task lag rather than silently losing updates.

Do not use `tokio::sync::broadcast` for v1 exact subscriptions; lagging receivers can miss messages and would need a resync protocol. Do not use `tokio::sync::watch`; it only retains the latest value and is suited to state snapshots, not deltas.

The internal execution model still follows DBSP handles, not channels:

1. The CDC task converts one WAL transaction to an `OrdZSet<EncodedTriple>`.
2. The task appends it to the DBSP input handle.
3. The task steps the circuit.
4. The task immediately drains/consolidates the DBSP output handle.
5. The task sends one `IncrementalQueryDelta` over the subscription channel.

This mirrors Feldera's layering. The `dbsp` crate exposes input/output handles (`ZSetHandle::append`, `OutputHandle::take_from_all`/`consolidate`) and overwrites unread output on the next step. Feldera's adapter/controller layer then reads those handles after `circuit.step()`, enqueues per-step output batches, and wakes output threads. Triplox should keep that same separation: DBSP handles inside the incremental service, Tokio channels at the Triplox subscription boundary.

Call `unregister_incremental_query()` when the caller wants to stop receiving updates before dropping the node or subscription. If a receiver is dropped without explicit unregistering, the service should unregister the query and stop sending to that subscription. If a sender fails because the receiver was dropped, treat that as normal cleanup, not a transaction failure.

This is deliberately not added to `triplox-client` yet. A later protocol design can decide whether clients subscribe over streams, polling, or server-side cursors.

## Code Style

Prefer explicit small types at the boundaries between parsing, CDC, and DBSP:

```rust
struct PatternPlan {
    attribute: i64,
    entity: PatternSlot,
    value: PatternSlot,
    output_vars: Vec<Variable>,
}

enum PatternSlot {
    Variable(Variable),
    Constant(EncodedValue),
    Placeholder,
}
```

Keep comments short and attached to non-obvious invariants, especially cursor and basis ordering. Avoid encoding query semantics in stringly typed maps when a small struct is clearer.

## Testing Strategy

Unit tests:

- Planner rejects unsupported query forms with stable error messages.
- Planner accepts single-pattern and multi-pattern fixed-attribute queries.
- Datom-batch-to-Z-set conversion maps assert/retract to `+1`/`-1` and consolidates duplicate triples.
- Repeated variable patterns are rejected during registration validation.

Circuit tests:

- Single pattern: add and retract one fact.
- Two-pattern join on entity.
- Join through ref value, where one pattern's value equals another pattern's entity.
- Three-pattern chain.
- Cartesian product when patterns share no variables.
- Constant entity/value filtering.

Writer-node CDC tests:

- Register query, execute transaction, receive expected delta.
- Cardinality-one overwrite emits retract of old result and assert of new result.
- Multi-entity transaction emits one grouped delta at one basis.
- CDC cursor starts after initialization and does not replay existing facts as live deltas.
- The CDC cursor advances only after DBSP step and subscription delivery succeed.

Equivalence tests:

- After each transaction in a test sequence, integrate emitted deltas and compare to `DB::query()` on the same `TxBasis`.

## Boundaries

- Always: use `datoms_from_cdc_transaction()` for CDC decoding instead of duplicating EAV key parsing.
- Always: preserve existing one-shot query semantics for supported query shapes.
- Always: keep writer-node incremental state cancellation-aware through `CancellationToken`.
- Always: run `cargo fmt`, `cargo test --workspace`, and `cargo clippy --workspace --all-targets --all-features` before declaring implementation complete.
- Ask first: adding public protocol/client APIs, persisting incremental query registrations, changing SlateDB write durability, or introducing reader-node behavior.
- Ask first: supporting `or`, predicates, functions, aggregates, or variable attributes.
- Never: replace the existing one-shot `execute_query()` path as part of this first implementation.
- Never: edit `src/query.rs` for this work; create or extend `src/inc_query.rs` for incremental-query-specific namespaces and helpers.
- Never: read from the Triplox `TxLog` to synthesize query deltas when SlateDB CDC is available.
- Never: commit or push unless explicitly requested.

## Success Criteria

- A writer node can register at least one fixed-attribute triple-pattern incremental query.
- Initial circuit state is built from the current database without replaying existing rows as live deltas.
- Registration emits only future deltas after the registration basis.
- Subsequent writer-node transactions produce per-transaction result deltas from SlateDB CDC.
- Integrated result deltas match one-shot `DB::query()` results at each tested basis.
- CDC cursor and indexed-basis state are observable in tests and advance monotonically.
- Unsupported query forms fail during registration, before any circuit is installed.
- Full workspace tests and clippy pass.

## Resolved Decisions

1. Registration returns only future deltas after the registration basis. Initial snapshot delivery can be added later as an explicit option.
2. V1 supports disconnected pattern groups as Cartesian products, matching the standard query engine.
3. V1 does not persist `last_applied_seq`; applied progress is implicit in the ordered CDC apply loop and can become in-memory instrumentation later if useful.
4. The CDC task relies on normal SlateDB WAL-file availability and does not force WAL flushes after writer-node commits.
5. Repeated variables inside one triple pattern are rejected by registration validation.
