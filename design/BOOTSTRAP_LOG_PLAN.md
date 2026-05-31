# Implementation Plan: Bootstrap Transaction Through the Log

Spec: `design/BOOTSTRAP_LOG.md`

## Overview

Implement issue 278 by making bootstrap a real log transaction processed by the subscriber. The plan first creates a reusable bootstrap-aware indexer path, then rewires node startup so fresh stores append or replay bootstrap through the log, and finally removes the old direct-write constants and replay workaround.

## Architecture Decisions

- The log is authoritative for fresh bootstrap. Startup probes the first record with `read_txs_after(None, 1)`.
- Bootstrap indexing is subscriber-driven. Startup coordinates by creating a `TxWaiter`, starting `subscribe(..., None, ...)`, and waiting for the bootstrap `TxKey`.
- Bootstrap uses two schemas: `bootstrap_schema()` for processing/validation and `Schema::default()` as the mutation base used to prove the transaction installs the expected schema.
- No migration path exists for a missing-version store whose first log record is not bootstrap.
- `BOOTSTRAP_TX_EID` and `BOOTSTRAP_TX_KEY` are removed once startup no longer needs the direct-write path.

## Dependency Graph

```text
Bootstrap indexer entry point
    -> bootstrap version marker commit
    -> fresh startup bootstrap append/replay
        -> existing startup replay cursor simplification
            -> old constant/test cleanup
                -> workspace verification
```

## Task List

### Task 1: Extract Bootstrap State Helpers

**Description:** Split `bootstrap::init_db` into small helpers that can answer whether SlateDB is initialized and can load existing metadata without writing bootstrap indices.

**Acceptance criteria:**
- [ ] Existing DB metadata loading remains available as a standalone helper.
- [ ] Fresh DB detection is based only on the version marker.
- [ ] No fresh DB path writes bootstrap indices directly.

**Verification:**
- [ ] `cargo test -p triplox bootstrap::tests::test_init_db_existing`
- [ ] `cargo test -p triplox bootstrap::tests::test_init_db_preserves_old_version`

**Dependencies:** None

**Files likely touched:**
- `src/bootstrap.rs`

**Estimated scope:** S

### Task 2: Add Bootstrap Transaction Indexer Path

**Description:** Add an indexer method for committed bootstrap transactions that runs the existing transaction stages with `bootstrap_schema()` as the processing schema and `Schema::default()` as the schema mutation base.

**Acceptance criteria:**
- [ ] Bootstrap transaction allocates the first `TX_PARTITION` entity through `PartitionMap`.
- [ ] Bootstrap writes normal transaction entity datoms using the log-provided `TxKey`.
- [ ] The transaction-derived schema is compared to `bootstrap_schema()`.
- [ ] Normal user transactions still reject installed schema redefinition.

**Verification:**
- [ ] `cargo test -p triplox indexer::tests::test_bootstrap_transaction_installs_expected_schema`
- [ ] `cargo test -p triplox schema::tests::test_bootstrap_schema_consistency`

**Dependencies:** Task 1

**Files likely touched:**
- `src/indexer.rs`
- `src/bootstrap.rs`
- `src/schema.rs`

**Estimated scope:** M

### Task 3: Commit Version Metadata With Bootstrap Indexing

**Description:** Ensure the bootstrap indexer path writes the version marker in the same `WriteBatch` as the bootstrap index entries.

**Acceptance criteria:**
- [ ] Version marker is only written after bootstrap validation succeeds.
- [ ] Version marker and bootstrap index entries are committed atomically in one SlateDB write.
- [ ] Existing database loading still reads the same version marker key.

**Verification:**
- [ ] `cargo test -p triplox bootstrap`

**Dependencies:** Task 2

**Files likely touched:**
- `src/bootstrap.rs`
- `src/indexer.rs`

**Estimated scope:** S

### Checkpoint: Bootstrap Pipeline

- [ ] Bootstrap/indexer targeted tests pass.
- [ ] Fresh direct-write bootstrap path is no longer used by new helpers.
- [ ] Human review before startup rewiring.

### Task 4: Add Fresh Startup Bootstrap Coordination

**Description:** Rework `Node<MemoryLog>::memory_node` and the shared local startup path so an uninitialized empty log appends `bootstrap_schema_tx()` and waits for the subscriber to index it.

**Acceptance criteria:**
- [ ] Startup creates `TxWaiter` before appending bootstrap.
- [ ] Startup starts subscriber with `after_tx_id = None` before append.
- [ ] Fresh memory and local nodes expose exactly one tx entity with `:db/txId 0`.

**Verification:**
- [ ] `cargo test -p triplox test_db_on_fresh_node_returns_bootstrap_tx_key`
- [ ] `cargo test -p triplox test_fresh_db_has_queryable_bootstrap_transaction_entity`

**Dependencies:** Task 3

**Files likely touched:**
- `src/node.rs`
- `src/bootstrap.rs`

**Estimated scope:** M

### Task 5: Add Bootstrap Replay Crash Recovery

**Description:** Handle missing-version, non-empty logs by verifying the first record is `bootstrap_schema_tx()`, starting the subscriber from the beginning, and waiting for that record's `TxKey`.

**Acceptance criteria:**
- [ ] Missing version plus bootstrap first record replays and initializes successfully.
- [ ] Missing version plus non-bootstrap first record fails startup.
- [ ] Startup does not append a second bootstrap record during replay.

**Verification:**
- [ ] `cargo test -p triplox local_node_replays_bootstrap_when_version_missing`
- [ ] `cargo test -p triplox local_node_rejects_uninitialized_non_bootstrap_log`

**Dependencies:** Task 4

**Files likely touched:**
- `src/node.rs`
- `src/file_log.rs`
- `src/bootstrap.rs`

**Estimated scope:** M

### Task 6: Simplify Existing DB Replay Cursor

**Description:** Remove the `BOOTSTRAP_TX_EID` disambiguation from `Node<FileLog>::from_slate_and_log` and use `latest_tx_basis_from_sdb` directly for the replay cursor.

**Acceptance criteria:**
- [ ] Existing DB startup uses `after_tx_id = latest_indexed.tx_key.tx_id`.
- [ ] Restart after bootstrap-only state still indexes the first user tx.
- [ ] Restart after indexed user tx does not replay it.

**Verification:**
- [ ] `cargo test -p triplox local_node_restart`

**Dependencies:** Task 5

**Files likely touched:**
- `src/node.rs`
- `src/bootstrap.rs`

**Estimated scope:** S

### Checkpoint: Startup Flow

- [ ] Fresh memory node works.
- [ ] Fresh local node works.
- [ ] Crash recovery tests pass.
- [ ] Restart tests pass.

### Task 7: Remove Old Bootstrap Constants and Update Tests

**Description:** Delete `BOOTSTRAP_TX_EID`, `BOOTSTRAP_TX_KEY`, and tests that assert the temporary duplicate tx-id behavior.

**Acceptance criteria:**
- [ ] No production code references `BOOTSTRAP_TX_EID` or `BOOTSTRAP_TX_KEY`.
- [ ] The duplicate `:db/txId 0` test is replaced with a one-row assertion.
- [ ] Bootstrap tests assert log-backed behavior where practical.

**Verification:**
- [ ] `rg "BOOTSTRAP_TX_EID|BOOTSTRAP_TX_KEY|temporarily_shares_bootstrap" src tests`
- [ ] `cargo test -p triplox bootstrap`
- [ ] `cargo test -p triplox test_first_submitted_tx_has_distinct_tx_id`

**Dependencies:** Task 6

**Files likely touched:**
- `src/bootstrap.rs`
- `src/node.rs`
- `src/indexer.rs`

**Estimated scope:** S

### Task 8: Full Workspace Verification

**Description:** Run formatting, workspace tests, and clippy after the implementation is complete.

**Acceptance criteria:**
- [ ] Formatting is clean.
- [ ] All workspace tests pass.
- [ ] Clippy has no warnings.

**Verification:**
- [ ] `cargo fmt`
- [ ] `cargo test --workspace`
- [ ] `cargo clippy --workspace --all-targets --all-features`

**Dependencies:** Task 7

**Files likely touched:**
- None expected, except formatting changes from `cargo fmt`.

**Estimated scope:** S

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Subscriber race around bootstrap append | High | Create `TxWaiter` before append and start subscriber before append. |
| Bootstrap path accidentally weakens normal schema immutability | High | Keep bootstrap entry point separate; test normal schema redefinition rejection. |
| Version marker written without complete bootstrap indices | High | Put version marker in the same `WriteBatch` as bootstrap index entries. |
| FileLog byte-offset tx ids make assumptions brittle | Medium | Treat `TxKey` as opaque and use the log-returned key everywhere. |
| Existing tests rely on duplicate tx id behavior | Medium | Replace those tests with distinct tx id assertions in Task 7. |

## Parallelization Opportunities

- Tasks 1-6 should be sequential because each changes shared startup/indexer behavior.
- After Task 3, one person can draft tests for Tasks 4-6 while another implements startup wiring.
- Task 8 is always last.

## Open Questions

None for the current implementation plan.
