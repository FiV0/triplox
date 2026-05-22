# Implementation Plan: Incremental Triple-Pattern Queries

## Overview

Implement the first writer-node incremental query path for fixed-attribute triple-pattern queries. The implementation compiles supported `ParsedQuery` values into DBSP circuits, initializes each circuit from the current SlateDB indexes, then feeds future SlateDB WAL CDC datoms through the circuit and publishes exact per-transaction result deltas over bounded Tokio channels. `src/query.rs` remains a semantic reference only and is not modified.

## Architecture Decisions

- Use `dbsp` directly for circuit construction. Each registered query owns one DBSP circuit with an `OrdZSet<EncodedTriple>` input and `EncodedRow` streams between operators.
- Keep query-subset validation and variable layout in `src/inc_query.rs`, separate from the existing one-shot query compiler.
- Convert CDC datoms to DBSP input at transaction granularity with `datoms_to_zset(datoms, schema)`, mapping assertions to `+1` and retractions to `-1`.
- Support disconnected pattern groups as Cartesian products, matching the standard query engine.
- Expose crate-internal registration through `Node<L>::register_incremental_query()` and `Node<L>::unregister_incremental_query()`.
- Use bounded `tokio::sync::mpsc` receivers as the subscription boundary. DBSP handles remain internal to the incremental service.
- Keep v1 state in memory. Do not persist query registrations or a separate `last_applied_seq`.

## Dependency Graph

```text
dbsp dependency and module scaffolding
    |
    +-- encoded triple/row data model
    |       |
    |       +-- CDC datom batch to input Z-set conversion
    |
    +-- incremental query validation and pattern plan
            |
            +-- DBSP pattern streams and row layout
                    |
                    +-- joins and Cartesian products
                            |
                            +-- output projection and decoding
                                    |
                                    +-- writer-node service API
                                            |
                                            +-- initialization scan
                                            |
                                            +-- WAL CDC apply loop
                                                    |
                                                    +-- end-to-end and equivalence tests
```

## Task List

### Phase 1: Foundation

#### Task 1: Add DBSP Dependency and Incremental Module Shell

**Description:** Add the `dbsp` dependency and introduce the incremental-query module boundary without changing runtime behavior. This should prove the selected crate version compiles with the workspace before any feature logic is built on it.

**Acceptance criteria:**
- [x] `Cargo.toml` includes `dbsp = "0.299.0"` for the `triplox` crate.
- [x] `src/lib.rs` declares `mod inc_query;` and an incremental service module, either `mod incremental;` or `mod incremental { ... }` through files.
- [x] No existing query behavior changes.

**Verification:**
- [x] `cargo check -p triplox`
- [x] `cargo fmt --check`

**Dependencies:** None

**Files likely touched:**
- `.cargo/config.toml`
- `Cargo.toml`
- `Cargo.lock`
- `src/lib.rs`
- `src/incremental.rs`

**Estimated scope:** S

#### Task 2: Add Encoded Data Model and Datom Batch Conversion

**Description:** Define the encoded input and row types used by DBSP and implement conversion from one transaction's CDC datoms into a weighted `OrdZSet<EncodedTriple>` input batch.

**Acceptance criteria:**
- [x] `type EncodedValue = Vec<u8>` and `type EncodedRow = Vec<EncodedValue>` exist in the incremental module boundary.
- [x] `EncodedTriple` derives the ordering and hashing traits required for DBSP Z-set keys.
- [x] `datoms_to_zset(datoms, schema)` encodes entity as `DataType::Long(entity)`, resolves attributes to entids, and maps assert/retract weights correctly.
- [x] Duplicate triples in one transaction consolidate through the chosen Z-set representation.

**Verification:**
- [x] Unit tests for assert, retract, unknown attribute, and duplicate consolidation.
- [x] `cargo test -p triplox incremental`

**Dependencies:** Task 1

**Files likely touched:**
- `Cargo.toml`
- `Cargo.lock`
- `src/incremental.rs`
- `src/incremental/cdc.rs`

**Estimated scope:** M

#### Task 3: Add Incremental Query Validator and Planner

**Description:** Create `src/inc_query.rs` with validation for the v1 query subset and a deterministic pattern plan that describes pattern filters, variable layout, join keys, Cartesian products, and final projection order.

**Acceptance criteria:**
- [x] Accepts `WhereClause::Pattern` queries with constant ident/entid attributes.
- [x] Accepts disconnected pattern groups and records them as Cartesian-product joins.
- [x] Rejects variable/placeholder attributes, repeated variables inside one pattern, `:in`, non-relational find specs, `OrJoin`, `NotJoin`, predicates, functions, rules, aggregates, order, and limit.
- [x] Produces stable variable ordering for supported queries without calling or modifying `src/query.rs`.

**Verification:**
- [x] Planner tests for single pattern, entity join, ref-value join, three-pattern chain, constants, placeholders, rejected unsupported forms, and disconnected Cartesian products.
- [x] `cargo test -p triplox inc_query`

**Dependencies:** Task 1

**Files likely touched:**
- `src/inc_query.rs`
- `src/lib.rs`

**Estimated scope:** M

### Checkpoint: Foundation

- [x] `cargo fmt --check`
- [x] `cargo check -p triplox`
- [x] `cargo test -p triplox incremental`
- [x] `cargo test -p triplox inc_query`
- [x] Review the DBSP trait bounds before moving into circuit construction.

### Phase 2: DBSP Circuit

#### Task 4: Build Pattern Streams and Row Mapping

**Description:** Compile each planned triple pattern into a DBSP stream derived from the shared `EncodedTriple` input. Pattern streams filter by attribute and constants, then map matching triples into the pattern's `EncodedRow` layout.

**Acceptance criteria:**
- [x] Constant entity and value filters compare encoded values.
- [x] Placeholder positions do not appear in output rows.
- [x] Pattern rows use the planner's variable order, including entity/value variables encoded the same way.

**Verification:**
- [x] Circuit unit tests for single-pattern add, retract, constants, and placeholders.
- [x] `cargo test -p triplox incremental::circuit`

**Dependencies:** Tasks 2 and 3

**Files likely touched:**
- `src/incremental/circuit.rs`
- `src/incremental.rs`
- `src/inc_query.rs`

**Estimated scope:** M

#### Task 5: Implement Joins and Cartesian Products

**Description:** Build the deterministic left-deep DBSP operator chain over `EncodedRow` streams. Shared variables use indexed joins; disconnected groups use Cartesian-product semantics instead of rejection.

**Acceptance criteria:**
- [x] Joins preserve one row layout with no duplicated shared-variable columns.
- [x] Entity-to-value joins work because both sides use encoded Triplox values.
- [x] Disconnected patterns produce the same Cartesian-product multiplicities as the one-shot engine.

**Verification:**
- [x] Circuit tests for two-pattern entity join, ref-value join, three-pattern chain, and disconnected Cartesian product.
- [x] `cargo test -p triplox incremental::circuit`

**Dependencies:** Task 4

**Files likely touched:**
- `src/incremental/circuit.rs`
- `src/inc_query.rs`

**Estimated scope:** M

#### Task 6: Decode and Publish Circuit Output Rows

**Description:** Attach output handles to the final projected delta stream, drain each DBSP step, consolidate weights, and decode final `EncodedValue` rows back to `DataType` values for subscription delivery.

**Acceptance criteria:**
- [x] Output rows are ordered according to `:find`.
- [x] Positive and negative weights are preserved.
- [x] Empty output steps do not emit subscription messages.
- [x] Decode errors surface as query-service errors and do not silently corrupt output.

**Verification:**
- [x] Unit tests for output projection order, negative deltas, empty steps, and decode failure handling.
- [x] `cargo test -p triplox incremental::circuit`

**Dependencies:** Task 5

**Files likely touched:**
- `src/incremental/circuit.rs`
- `src/incremental.rs`

**Estimated scope:** M

### Checkpoint: Circuit

- [x] All circuit tests pass.
- [x] Integrated circuit outputs match expected rows for in-memory datom sequences.
- [x] `cargo fmt --check`
- [x] `cargo check -p triplox`

### Phase 3: Writer-Node Service

#### Task 7: Add Registration and Unregistration API

**Description:** Add crate-internal registration methods on `Node<L>` and an incremental service that owns query circuits, subscription senders, and cancellation-aware service state.

**Acceptance criteria:**
- [x] `register_incremental_query(query)` validates and installs a query circuit.
- [x] `unregister_incremental_query(handle)` removes the circuit and closes that subscription's sender.
- [x] Dropped receivers are treated as normal cleanup.
- [x] The API returns only future deltas, not an initial snapshot.
- [x] `IncrementalQuerySubscription` exposes the registration `TxBasis` as the cutover basis for those future deltas.

**Verification:**
- [x] Unit or node-level tests for register, unregister, duplicate unregister, and receiver-drop cleanup.
- [x] `cargo test -p triplox incremental`

**Dependencies:** Task 6

**Files likely touched:**
- `src/node.rs`
- `src/incremental.rs`
- `src/lib.rs`

**Estimated scope:** M

#### Task 8: Prime Query Circuits from Current Index State

**Description:** During registration, capture a consistent basis and WAL cursor, scan current EAV state for the query's needed attributes, feed positive triples into the circuit, step once, and discard the initial output.

**Acceptance criteria:**
- [x] The registration basis is captured from existing indexed state.
- [x] Initial rows are installed into DBSP state but not emitted as live deltas.
- [x] The WAL cursor starts after the captured basis so existing facts are not replayed.
- [x] The implementation can use simple EAV scans and in-memory filtering for v1.

**Verification:**
- [x] Node-level test: register after existing data, then assert no initial delta is received.
- [x] Node-level test: a future matching transaction emits exactly one live delta.
- [x] `cargo test -p triplox incremental`

**Dependencies:** Task 7

**Files likely touched:**
- `src/incremental.rs`
- `src/incremental/cdc.rs`
- `src/indexer.rs` only if a small accessor is required

**Estimated scope:** M

#### Task 9: Add WAL CDC Apply Loop

**Description:** Run one writer-node CDC task that reads SlateDB WAL transactions, decodes datoms with `datoms_from_cdc_transaction()`, waits for the corresponding transaction to be visible in memory indexes when possible, steps every registered circuit, and sends exact deltas to subscribers.

**Acceptance criteria:**
- [x] Uses `CdcStream::next_transaction()` and `datoms_from_cdc_transaction()`.
- [x] Uses the existing `TxWaiter` path when tx metadata can be derived from CDC datoms.
- [x] Falls back to sequence-only output basis if tx metadata cannot be derived.
- [x] Advances CDC cursor only after circuit stepping and subscription delivery succeed.
- [x] Relies on normal SlateDB WAL availability and does not force WAL flushes.

**Verification:**
- [x] Node-level tests for a single transaction, multi-entity transaction grouped into one delta, and cardinality-one overwrite producing retract/assert result changes.
- [x] `cargo test -p triplox incremental`
- [x] `cargo test -p triplox slate::cdc`

**Dependencies:** Task 8

**Files likely touched:**
- `src/incremental.rs`
- `src/incremental/cdc.rs`
- `src/node.rs`
- `src/slate/cdc.rs` only if an accessor is missing

**Estimated scope:** M

### Checkpoint: Writer Node

- [x] Registration returns future deltas only.
- [x] Unregistration stops delivery and releases service state.
- [x] CDC cursor state advances monotonically in tests.
- [x] `cargo fmt --check`
- [x] `cargo test -p triplox incremental`
- [x] `cargo test -p triplox slate::cdc`

### Phase 4: Equivalence and Hardening

#### Task 10: Add End-to-End Incremental Query Tests

**Description:** Cover the complete writer-node flow for the supported query subset, including joins and Cartesian products, using normal transaction submission and subscription delivery.

**Acceptance criteria:**
- [x] Tests cover single pattern, entity join, ref-value join, three-pattern chain, constants, placeholders, retracts, and disconnected Cartesian products.
- [x] Tests integrate deltas locally and assert the expected live result after each transaction.
- [x] Tests do not require client protocol changes.

**Verification:**
- [x] `cargo test -p triplox incremental`
- [x] `cargo test -p triplox node`

**Dependencies:** Task 9

**Files likely touched:**
- `src/incremental.rs`
- `tests/incremental_query_test.rs` or `src/node.rs` test module

**Estimated scope:** M

#### Task 11: Add Equivalence Tests Against One-Shot Query

**Description:** For representative supported queries, compare integrated incremental deltas after each transaction with `DB::query()` at the same basis.

**Acceptance criteria:**
- [x] Equivalence tests use the public `QueryNode`/`SubmitNode` flow where possible.
- [x] Each checked transaction basis compares the same query semantics between incremental and one-shot execution.
- [x] The Cartesian-product case is included.

**Verification:**
- [x] `cargo test -p triplox incremental_equivalence`

**Dependencies:** Task 10

**Files likely touched:**
- `tests/incremental_query_test.rs`
- `src/incremental.rs` test helpers only if needed

**Estimated scope:** M

#### Task 12: Final Validation and Documentation Cleanup

**Description:** Tighten errors, remove temporary scaffolding, update the design doc if implementation discovers a necessary deviation, and run the full workspace verification required by the repository.

**Acceptance criteria:**
- [x] Unsupported query errors are clear and stable enough for tests.
- [x] The design doc and implementation agree on API names, state, and semantics.
- [x] No public client or protocol API has been added.
- [x] `src/query.rs` remains untouched by this feature.

**Verification:**
- [x] `cargo fmt --check`
- [x] `cargo test --workspace`
- [x] `cargo clippy --workspace --all-targets --all-features`

**Dependencies:** Task 11

**Files likely touched:**
- `design/INCREMENTAL_QUERIES.md`
- Implementation files from earlier tasks as needed

**Estimated scope:** S

### Checkpoint: Complete

- [x] Writer nodes can register and unregister supported incremental queries.
- [x] Initial state is primed but not emitted as live deltas.
- [x] Future SlateDB CDC transactions produce exact per-transaction deltas.
- [x] Integrated deltas match the standard one-shot query engine, including disconnected Cartesian products.
- [x] Full workspace tests and clippy pass.

## Parallelization Opportunities

- Tasks 2 and 3 can proceed in parallel after Task 1 because encoded CDC conversion and query planning are independent.
- Planner rejection tests can be written in parallel with circuit work once Task 3 defines the plan API.
- End-to-end test cases can be drafted after Task 7, but they should not be finalized until Tasks 8 and 9 define initialization and CDC timing behavior.
- Tasks 8 and 9 should stay sequential because cursor initialization and WAL application share state invariants.

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| DBSP requires additional trait derives or archive traits for `EncodedTriple`/`EncodedRow`. | High | Fail fast in Task 1 or 2 with a minimal compiling circuit before building the service. |
| Cartesian-product DBSP implementation produces multiplicities that differ from the one-shot engine. | High | Include circuit tests and equivalence tests specifically for disconnected patterns. |
| Registration captures an inconsistent EAV snapshot and WAL cursor. | High | Keep Task 8 focused on the basis/cursor invariant and test that existing facts are not replayed as live deltas. |
| WAL CDC arrives before memory indexes are visible. | Medium | Use existing `TxWaiter::await_indexed()` when tx metadata can be derived, with a sequence-only fallback covered by tests. |
| Bounded subscription channels can lag the CDC task. | Medium | Treat backpressure as service lag, not data loss; avoid `broadcast` unless a future resync protocol exists. |
| Query semantics drift from `src/query.rs` because helpers cannot be reused directly. | Medium | Use one-shot equivalence tests as the semantic guardrail and keep `src/query.rs` read-only. |
| Pulling `dbsp` increases compile time or introduces workspace feature conflicts. | Medium | Add the dependency before other work and keep Task 1 as an explicit compile checkpoint. |

## Open Questions

- None.
