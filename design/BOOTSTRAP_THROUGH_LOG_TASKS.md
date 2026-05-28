# Implementation Plan: Bootstrap Through The Log

Status: Proposed
Date: 2026-05-28
Spec: [BOOTSTRAP_THROUGH_LOG.md](BOOTSTRAP_THROUGH_LOG.md)

## Overview

This plan breaks the bootstrap-through-log spec into small, reviewable
implementation slices. The goal is to move fresh database bootstrap from direct
SlateDB writes into the transaction log and indexer path while keeping each
step independently testable.

The implementation should preserve public client APIs, source the bootstrap
`TxKey` from the first log record, synchronously index bootstrap before node
startup returns, and write the low-level initialization marker atomically with
bootstrap index entries.

## Architecture Decisions

- Use a synchronous startup bootstrap path instead of the background subscriber
  for fresh bootstrap. This lets startup return decode, validation, schema, and
  write errors directly.
- Keep the low-level initialization marker as the startup completion gate.
  Queryable version facts may be added later, but startup cannot depend on
  queryable state before the database is known to be bootstrapped.
- Treat old direct-bootstrap marker values as unsupported during this bootstrap
  phase. Backwards compatibility and migration are explicitly out of scope.
- Validate bootstrap log records by decoding with the current bincode format and
  requiring exact `Vec<TxOp>` equality with `bootstrap_schema_tx()`.
- Extract shared transaction batch construction before wiring bootstrap startup.
  This keeps normal transaction behavior reviewable before the new bootstrap
  path depends on it.

## Task List

### Phase 1: Bootstrap Metadata Foundation

#### Task 1: Split Bootstrap Metadata And Marker Helpers

Description: Refactor `bootstrap::init_db` into helpers that can detect whether
the database is initialized, load metadata from indices, create fresh bootstrap
metadata, write the new low-level marker into a provided batch, and detect
marker/index mismatch.

Acceptance criteria:

- [ ] `load_metadata_if_initialized` returns `Ok(None)` for no marker and empty
      EAV state.
- [ ] Marker-present, marker-absent/EAV-non-empty, and unsupported-marker states
      are distinguishable.
- [ ] Existing direct-bootstrap initialization still works until later tasks
      remove the call path.

Verification:

- [ ] `cargo test -p triplox bootstrap`

Dependencies: None

Files likely touched:

- `src/bootstrap.rs`
- `src/codec.rs`

Estimated scope: Medium

#### Task 2: Add Bootstrap Log Record Validation

Description: Add a helper that validates a first log record as the bootstrap
transaction by decoding it with the current log serialization format and
comparing the decoded operations to `bootstrap_schema_tx()`.

Acceptance criteria:

- [ ] A decoded first log record equal to `bootstrap_schema_tx()` is accepted.
- [ ] Malformed bincode is rejected with a deterministic error.
- [ ] A valid non-bootstrap record and a semantically different bootstrap-shaped
      record are rejected.

Verification:

- [ ] `cargo test -p triplox bootstrap_log`

Dependencies: Task 1

Files likely touched:

- `src/bootstrap.rs`
- `src/log.rs`

Estimated scope: Small

### Checkpoint: Metadata Foundation

- [ ] Bootstrap helper tests pass.
- [ ] Existing bootstrap initialization tests still pass.
- [ ] No node startup behavior has changed yet.

### Phase 2: Shared Indexer Commit Path

#### Task 3: Extract Shared Transaction Batch Construction

Description: Extract the normal transaction path in `Indexer::transact_tx_inner`
so normal and bootstrap transactions can share expansion, lookup-ref
resolution, tempid resolution, validation, tx entity datom creation, index batch
construction, metadata update, latest-indexed update, and completion broadcast.

Acceptance criteria:

- [ ] Normal `transact_tx` behavior is unchanged.
- [ ] Semantic transaction failures still write aborted tx entities through the
      existing normal path.
- [ ] `latest_indexed_tx` and completion broadcasts still happen only after a
      successful write.

Verification:

- [ ] `cargo test -p triplox indexer`
- [ ] `cargo test -p triplox node::tests::test_transact`

Dependencies: None

Files likely touched:

- `src/indexer.rs`

Estimated scope: Medium

#### Task 4: Add Bootstrap Schema Derivation Check

Description: Add bootstrap-specific schema handling around the shared commit
path. Bootstrap operations are interpreted with `bootstrap_schema()`, schema
updates are prepared against `Schema::default()`, and the resulting schema must
match the hardcoded bootstrap schema.

Acceptance criteria:

- [ ] Bootstrap indexing produces the same `ident_map` as `bootstrap_schema()`.
- [ ] Bootstrap indexing produces the same `attribute_map` as
      `bootstrap_schema()`.
- [ ] A mismatched bootstrap record fails deterministically before marking the
      database initialized.

Verification:

- [ ] `cargo test -p triplox bootstrap_schema`
- [ ] `cargo test -p triplox indexer`

Dependencies: Task 3

Files likely touched:

- `src/indexer.rs`
- `src/schema.rs`
- `src/bootstrap.rs`

Estimated scope: Medium

#### Task 5: Add Marker-In-Batch Bootstrap Commit

Description: Add `transact_bootstrap_tx(record) -> Result<TxBasis>` on the
indexer. It must validate the bootstrap record, build bootstrap index entries,
add the low-level format marker to the same `WriteBatch`, commit once, update
metadata, set `latest_indexed_tx`, and broadcast completion.

Acceptance criteria:

- [ ] Bootstrap index entries and the low-level marker are committed in one
      SlateDB write batch.
- [ ] A bootstrapped indexer has `latest_indexed_tx = Some(bootstrap_basis)`.
- [ ] `tx_waiter().await_tx(bootstrap_tx_key)` resolves after bootstrap commit.

Verification:

- [ ] `cargo test -p triplox bootstrap_indexer`
- [ ] `cargo test -p triplox test_fresh_node_tx_waiter_knows_bootstrap_tx_is_indexed`
      remains red until Phase 3 wires node startup.

Dependencies: Tasks 2, 3, 4

Files likely touched:

- `src/indexer.rs`
- `src/bootstrap.rs`

Estimated scope: Medium

### Checkpoint: Indexer Bootstrap Path

- [ ] Normal transaction tests still pass.
- [ ] Bootstrap indexer tests pass.
- [ ] The new bootstrap commit path is unused by node startup until Phase 3.

### Phase 3: Startup Routing

#### Task 6: Add Synchronous Bootstrap Replay Helper

Description: Add a node startup helper that checks the log before normal
subscription starts, appends bootstrap only if the log is empty, reads the first
record back, indexes it synchronously through `transact_bootstrap_tx`, awaits
the bootstrap tx key through the waiter, and returns startup errors directly.

Acceptance criteria:

- [ ] Empty log appends exactly one bootstrap record and indexes it.
- [ ] Log with bootstrap as the first record replays that record instead of
      appending another bootstrap transaction.
- [ ] Log with a non-bootstrap first record fails startup.

Verification:

- [ ] `cargo test -p triplox bootstrap_startup`

Dependencies: Task 5

Files likely touched:

- `src/node.rs`
- `src/log.rs`
- `src/memory_log.rs`
- `src/file_log.rs`

Estimated scope: Medium

#### Task 7: Route Memory Node Startup Through Bootstrap Replay

Description: Change `Node::memory_node` to initialize fresh in-memory databases
through the synchronous bootstrap replay helper, then start normal subscription
after the bootstrap tx id.

Acceptance criteria:

- [ ] `Node::memory_node` returns only after bootstrap is indexed.
- [ ] `db().tx_basis()` is the bootstrap basis from the log-derived tx key.
- [ ] `tx_waiter().await_tx(db.tx_key())` resolves immediately.

Verification:

- [ ] `cargo test -p triplox test_fresh_node_tx_waiter_knows_bootstrap_tx_is_indexed`
- [ ] `cargo test -p triplox test_fresh_db_has_queryable_bootstrap_transaction_entity`

Dependencies: Task 6

Files likely touched:

- `src/node.rs`
- `src/bootstrap.rs`

Estimated scope: Small

#### Task 8: Route FileLog Startup Through Initialized Or Fresh Paths

Description: Change `Node<FileLog>::from_slate_and_log` to load the new marker
for initialized databases and otherwise bootstrap through synchronous log
replay. Initialized startup should seed the indexer from
`latest_tx_basis_from_sdb` and subscribe after that tx id.

Acceptance criteria:

- [ ] Fresh local nodes write bootstrap through `FileLog`.
- [ ] Reopening the same local node does not duplicate bootstrap.
- [ ] Already indexed user transactions are skipped on restart.
- [ ] Startup errors for marker/log/index mismatch are returned from
      `local_node` or `remote_node`.

Verification:

- [ ] `cargo test -p triplox local_node`
- [ ] `cargo test -p triplox restart`

Dependencies: Tasks 6, 7

Files likely touched:

- `src/node.rs`
- `src/file_log.rs`
- `src/bootstrap.rs`

Estimated scope: Medium

### Checkpoint: Startup Bootstrap

- [ ] Fresh memory node regression passes.
- [ ] Fresh local node writes bootstrap through the log.
- [ ] Restart tests prove bootstrap is not duplicated.
- [ ] Wrong first record and marker/index mismatch return startup errors.

### Phase 4: Cleanup And Semantics

#### Task 9: Remove New-Format Bootstrap Key Assumptions

Description: Remove new-format reliance on `BOOTSTRAP_TX_KEY`. The bootstrap
`TxKey` comes from the first log record. `BOOTSTRAP_TX_EID` may remain as the
expected first TX partition entity.

Acceptance criteria:

- [ ] Tests assert consistency with the DB/indexed basis, not equality with
      `BOOTSTRAP_TX_KEY`.
- [ ] Fresh DBs have exactly one transaction with `:db/txId 0`.
- [ ] The first user transaction after bootstrap has `tx_id > 0`.

Verification:

- [ ] `cargo test -p triplox tx_id`
- [ ] `cargo test -p triplox node`

Dependencies: Tasks 7, 8

Files likely touched:

- `src/bootstrap.rs`
- `src/node.rs`
- `src/indexer.rs`

Estimated scope: Small

#### Task 10: Remove Legacy Replay Cursor Workaround For New Format

Description: Replace the current `BOOTSTRAP_TX_EID`-based replay workaround in
`Node<FileLog>::from_slate_and_log` with new marker-driven startup behavior.
Unsupported old marker values should fail startup instead of silently opening.

Acceptance criteria:

- [ ] New-format initialized DBs subscribe after `latest_indexed_tx.tx_id`.
- [ ] Unsupported old direct-bootstrap marker values fail startup.
- [ ] The branch no longer needs bootstrap-specific tx-id ambiguity logic for
      new databases.

Verification:

- [ ] `cargo test -p triplox restart`
- [ ] `cargo test -p triplox bootstrap`

Dependencies: Tasks 8, 9

Files likely touched:

- `src/node.rs`
- `src/bootstrap.rs`

Estimated scope: Small

### Checkpoint: Complete

- [ ] `cargo fmt`
- [ ] `cargo test --workspace`
- [ ] `cargo clippy --workspace --all-targets --all-features`
- [ ] Bootstrap-through-log spec success criteria are all satisfied.

## Risks And Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Shared transaction extraction changes normal transaction semantics | High | Land extraction before startup routing and verify normal transaction tests before using the helper for bootstrap. |
| Startup errors remain hidden in the background subscriber | High | Keep fresh bootstrap synchronous and return `Result` before starting normal subscription. |
| Marker and bootstrap indices are not atomic | High | Only write the marker through the bootstrap indexer write batch. Do not mark initialized from node startup. |
| Old direct-bootstrap databases are opened as if they were new-format | Medium | Introduce a distinct new marker value and fail unsupported marker values during this bootstrap phase. |
| Concurrent fresh startup appends duplicate bootstrap records | Medium | Add an internal local startup lock before marker/log checks; keep remote multi-writer bootstrap out of scope. |
| Future `TxOp` serialization changes invalidate bootstrap validation | Medium | Require an explicit log/bootstrap migration strategy before changing the record shape. |

## Parallelization Opportunities

- Tasks 1 and 3 can proceed independently after agreeing on helper signatures.
- Task 2 can proceed after Task 1 exposes the validation location.
- Tests for Phase 3 failure cases can be drafted while Task 5 is underway, but
  startup wiring must wait for the bootstrap indexer path.
- Tasks 9 and 10 should remain sequential because both remove legacy identity
  and replay assumptions.

## Open Questions

- None for the initial bootstrap-phase implementation. Remote multi-writer
  bootstrap and migration of old direct-bootstrap databases remain out of scope.
