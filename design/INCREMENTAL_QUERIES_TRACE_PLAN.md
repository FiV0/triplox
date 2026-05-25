# Implementation Plan: Trace-Backed Incremental Query Circuits

## Overview

Update the current writer-node incremental query implementation so DBSP circuit state is trace-backed and file-backed instead of held as full accumulated `OrdZSet`s in memory. This is a follow-up to the existing incremental query branch: it keeps the current API, CDC path, query subset, and subscription semantics, but changes the circuit construction and runtime storage model.

## Architecture Decisions

- Do not use `input.integrate()` as the query state model. That accumulates the full base relation as an in-memory Z-set.
- Use DBSP trace-backed state for materialized relations: either explicit `integrate_trace()`/`Spine` operators or DBSP join operators such as `join()`/`join_index()` after verifying they use traces internally for this crate version.
- Configure file-backed DBSP runtime storage before constructing query circuits. Trace-backed state must be able to spill/checkpoint to files.
- Keep Triplox service metadata in memory for v1: active query handles, subscription senders, and CDC cursors do not need persistence in this pass.
- Keep SlateDB as the source of truth. DBSP trace files are derived query state and may be rebuilt from a registration snapshot plus WAL replay if restart support is deferred.

## Implemented DBSP Contract

- `dbsp = "0.299.0"` implements `Stream::join()`/`Stream::join_index()` through trace joins internally, so Triplox uses `join()` for row joins instead of explicitly exposing `Spine` values in the Triplox circuit API.
- Query circuits are built with `Runtime::init_circuit()` and a `CircuitConfig` containing `CircuitStorageConfig::for_config(StorageConfig, StorageOptions)`.
- `StorageOptions::min_storage_bytes` is set to `Some(0)` for the incremental-query runtime so tests and small queries exercise the file-backed path.
- Each registered query gets a `query-<id>` directory below the writer node's incremental DBSP storage root. Existing per-query directories are removed before construction because v1 does not restore persisted query registrations or DBSP traces.
- Per-query storage is removed when a query is explicitly unregistered or implicitly cleaned up after its subscription receiver is dropped.
- Stable persistent operator IDs are deferred until restart/restore support is added. In this pass, SlateDB remains the source of truth and query state can be rebuilt from a registration snapshot plus WAL replay.

## Non-Goals

- Do not add public client or wire protocol APIs.
- Do not implement reader-node behavior.
- Do not persist query registrations.
- Do not edit `src/query.rs`.
- Do not expand the supported query subset beyond fixed-attribute triple patterns.

## Dependency Graph

```text
DBSP storage/runtime contract
    |
    +-- file-backed circuit construction
            |
            +-- trace-backed pattern/join circuit shape
                    |
                    +-- priming and CDC delivery parity
                            |
                            +-- storage and equivalence tests
                                    |
                                    +-- docs cleanup
```

## Task List

### Phase 1: Storage Foundation

#### Task 1: Confirm DBSP Trace and Storage APIs

**Description:** Inspect the `dbsp = "0.299.0"` APIs used by this branch and decide the exact construction path for file-backed trace state. Confirm whether `join()`/`join_index()` provides the desired trace-backed implementation or whether the circuit must call `integrate_trace()` explicitly.

**Acceptance criteria:**
- [x] Document the chosen DBSP APIs in this plan or the main design doc.
- [x] Identify the runtime/storage configuration type and where it must be initialized.
- [x] Identify any persistent-id requirements for trace/checkpoint operators.

**Verification:**
- [x] Minimal compile-only experiment or targeted test proves a circuit can be built with file-backed DBSP storage enabled.
- [x] `cargo check -p triplox`

**Dependencies:** None

**Files likely touched:**
- `design/INCREMENTAL_QUERIES_TRACE_PLAN.md`
- `src/incremental.rs` or a small scratch test module

**Estimated scope:** S

#### Task 2: Add File-Backed DBSP Storage Configuration

**Description:** Thread a DBSP storage root through the writer-node incremental service and use it when constructing query circuits. Tests should use temporary directories, while real writer nodes should derive a stable storage directory from the node/database path.

**Acceptance criteria:**
- [x] Query circuits are constructed in a DBSP runtime with storage enabled.
- [x] Each registered query gets isolated storage namespace or stable persistent IDs to avoid trace-state collisions.
- [x] The implementation fails fast if production circuit construction would run with storage disabled.

**Verification:**
- [x] Unit or node-level test proves registration succeeds with a temporary file-backed storage root.
- [x] Test or assertion covers the storage-disabled failure path if the API permits detection.
- [x] `cargo test -p triplox incremental`

**Dependencies:** Task 1

**Files likely touched:**
- `src/incremental.rs`
- `src/node.rs`
- `src/incremental/circuit.rs`

**Estimated scope:** M

### Checkpoint: Storage Foundation

- [x] File-backed DBSP storage is configured before any query circuit is built.
- [x] Existing registration/unregistration tests pass.
- [x] `cargo fmt --check`
- [x] `cargo check -p triplox`

### Phase 2: Trace-Backed Circuit Shape

#### Task 3: Replace Full-ZSet Integration in Query Circuits

**Description:** Refactor `src/incremental/circuit.rs` so query semantics do not depend on `input.integrate()` plus `stream_join()` plus final `differentiate()`. Pattern streams should remain delta streams, and joins should use trace-backed DBSP operators.

**Acceptance criteria:**
- [x] `query_find_stream()` no longer calls `input.integrate()`.
- [x] The join path no longer uses `stream_join()` over fully integrated current relations.
- [x] The projected output is produced as a delta stream without differentiating a full materialized result relation.

**Verification:**
- [x] Circuit tests for single pattern, entity join, ref-value join, chain join, and Cartesian product pass.
- [x] `rg "integrate\\(|stream_join|differentiate\\(" src/incremental/circuit.rs` shows no full-Z-set query-state path remains, except comments/tests explicitly explaining why.
- [x] `cargo test -p triplox incremental::circuit`

**Dependencies:** Task 2

**Files likely touched:**
- `src/incremental/circuit.rs`

**Estimated scope:** M

#### Task 4: Preserve Priming Semantics with Trace State

**Description:** Update registration priming so the current EAV snapshot initializes DBSP traces without emitting initial rows as live subscription deltas. Keep the existing future-only subscription contract.

**Acceptance criteria:**
- [x] Snapshot triples are loaded into trace state during registration.
- [x] Initial output produced by priming is drained and discarded.
- [x] Transactions after the registration basis still emit exact future deltas.

**Verification:**
- [x] Existing node test for “register after existing data emits no initial delta” passes.
- [x] Existing node test for “future matching transaction emits one live delta” passes.
- [x] `cargo test -p triplox test_register_incremental_query_after_existing_data_emits_no_initial_delta`

**Dependencies:** Task 3

**Files likely touched:**
- `src/incremental.rs`
- `src/incremental/circuit.rs`
- `src/node.rs`

**Estimated scope:** M

#### Task 5: Recheck Join Multiplicities and Cartesian Products

**Description:** Verify that trace-backed joins preserve the same multiplicities as the one-shot query engine, including disconnected patterns represented as Cartesian products over a unit key.

**Acceptance criteria:**
- [x] Entity joins preserve shared-variable semantics.
- [x] Ref-value joins still compare encoded entity values and encoded ref values correctly.
- [x] Disconnected patterns produce the same Cartesian-product multiplicities as `DB::query()`.

**Verification:**
- [x] Circuit tests cover Cartesian products and ref-value joins.
- [x] End-to-end equivalence tests cover entity join and Cartesian product.
- [x] `cargo test -p triplox incremental_equivalence`

**Dependencies:** Task 3

**Files likely touched:**
- `src/incremental/circuit.rs`
- `src/node.rs` tests if additional coverage is needed

**Estimated scope:** M

### Checkpoint: Circuit Semantics

- [x] No full-Z-set `integrate()` query-state path remains.
- [x] Circuit and node-level incremental tests pass.
- [x] Integrated deltas still match one-shot query results.

### Phase 3: Hardening and Documentation

#### Task 6: Add Storage-Oriented Regression Coverage

**Description:** Add tests that would fail if circuit state were only kept as an in-memory full Z-set or if file-backed storage were not configured. Prefer direct assertions against the DBSP storage setup where available, plus behavioral tests across enough data to exercise trace state.

**Acceptance criteria:**
- [x] Tests prove query registration uses a file-backed DBSP storage root.
- [x] Tests cover multiple transactions after priming with trace-backed state.
- [x] Tests clean up temporary storage directories.

**Verification:**
- [x] `cargo test -p triplox incremental`
- [x] `cargo test -p triplox node`

**Dependencies:** Tasks 2-5

**Files likely touched:**
- `src/incremental.rs`
- `src/node.rs`
- `src/incremental/circuit.rs`

**Estimated scope:** M

#### Task 7: Update Design Documentation

**Description:** Update the main incremental query design doc after the implementation lands, replacing the current `integrate() + stream_join() + differentiate()` circuit description with the trace-backed/file-backed design.

**Acceptance criteria:**
- [x] `design/INCREMENTAL_QUERIES_SPEC.md` states that relation state is trace-backed and file-backed.
- [x] The circuit shape section matches the implemented operator graph.
- [x] The docs distinguish in-memory service metadata from file-backed DBSP relation state.

**Verification:**
- [x] `rg "input\\.integrate\\(|stream_join\\(\\)|differentiate a full" design/INCREMENTAL_QUERIES_SPEC.md` finds no stale prescribed circuit shape.
- [x] `cargo fmt --check`

**Dependencies:** Tasks 2-6

**Files likely touched:**
- `design/INCREMENTAL_QUERIES_SPEC.md`
- `design/INCREMENTAL_QUERIES_TRACE_PLAN.md`

**Estimated scope:** S

### Checkpoint: Complete

- [x] DBSP query relation state is trace-backed.
- [x] DBSP trace state is file-backed through configured runtime storage.
- [x] Registration still returns only future deltas after its basis.
- [x] CDC delivery remains exact and ordered.
- [x] One-shot equivalence tests still pass.
- [x] `cargo test --workspace`
- [x] `cargo clippy --workspace --all-targets --all-features`

## Parallelization Opportunities

- Task 1 can be done independently as source/API research.
- Task 6 test design can be drafted after Task 2 defines the storage contract, but final assertions should wait until Tasks 3-5 land.
- Tasks 3, 4, and 5 should stay mostly sequential because priming and multiplicity behavior depend on the final circuit shape.

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| `join_index()` is trace-backed but not file-backed without runtime storage. | High | Make storage configuration Task 2 and test it directly before circuit refactoring. |
| Explicit `integrate_trace()` APIs do not compose cleanly with the typed join shape needed here. | High | Confirm in Task 1 whether to use explicit traces or high-level trace-backed joins. |
| File-backed DBSP runtime setup conflicts with the current dedicated service thread. | Medium | Keep storage/runtime initialization inside the service thread where `CircuitHandle` already lives. |
| Priming trace state emits initial rows to subscribers. | Medium | Keep priming output drain/discard as a dedicated Task 4 invariant. |
| Cartesian-product multiplicities change when switching operators. | Medium | Keep existing equivalence tests and add focused trace-backed circuit tests if needed. |

## Open Questions

- Should DBSP trace files live under the Triplox database directory or under a separate configurable runtime directory?
- Do we want restart/restore from DBSP trace files in this pass, or only file-backed spill/checkpoint support with rebuild on restart?
- Should production registration reject storage-disabled DBSP runtimes, or should this be guarded at node startup?
