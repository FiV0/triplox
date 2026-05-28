# Spec: Bootstrap Through The Log

Version 0.2

## Objective

Fresh Triplox databases should install the bootstrap schema by writing the
bootstrap transaction to the transaction log and indexing that log record through
the same transaction/indexer pipeline used for later transactions.

This replaces the current direct SlateDB bootstrap write. It addresses:

- [Issue #278](https://github.com/FiV0/triplox/issues/278): bootstrap and the
  first user transaction can both appear with raw `tx_id = 0`.
- [Issue #134](https://github.com/FiV0/triplox/issues/134): bootstrap has a
  special transaction identity and does not seed indexer state through the same
  path as normal transactions.

Success means a fresh node has exactly one transaction with `:db/txId 0`, that
transaction is bootstrap, the first user transaction has a later log tx id, and
`latest_indexed_tx` contains the indexed bootstrap basis before the node is
returned.

Backwards compatibility with old direct-bootstrap databases is out of scope for
this bootstrap phase. Old initialization markers should be treated as an
unsupported format rather than silently accommodated.

## Design

Triplox should use the same high-level pattern Mentat uses for bootstrap:

1. Keep a prebuilt `bootstrap_schema()` in memory so bootstrap operations can be
   interpreted even though the schema is self-describing.
2. Apply `bootstrap_schema_tx()` through the transaction/index-writing path.
3. Build the schema produced by bootstrap datoms from `Schema::default()`.
4. Compare the produced schema with the prebuilt bootstrap schema before marking
   the database initialized.

The Triplox-specific difference is that bootstrap first goes through the
Triplox log. Startup then synchronously replays and indexes the bootstrap log
record before starting the normal background subscription.

Fresh startup flow:

```rust
let slate = local_slate(db_path).await;
let log = Arc::new(FileLog::new(log_path, Box::new(clock::SystemClock))?);

match bootstrap::load_metadata_if_initialized(&slate).await? {
    Some(metadata) => start_initialized_node(slate, log, metadata).await?,
    None => bootstrap_from_log_synchronously(slate, log).await?,
}
```

`bootstrap_from_log_synchronously` must:

1. Build a bootstrap-mode `Indexer`.
2. Read the first log record with `read_txs_after(None, 1)`.
3. If the log is empty, append serialized `bootstrap_schema_tx()`, then read
   the first log record back from the log.
4. Decode the first log record with the current bincode format and require exact
   decoded `Vec<TxOp>` equality with `bootstrap_schema_tx()`.
5. Create a waiter before indexing the bootstrap record.
6. Index the bootstrap record synchronously through `Indexer`, returning any
   decode, validation, schema, or write error directly from startup.
7. Await the bootstrap tx key through the waiter as a startup invariant.
8. Start the normal background subscription after bootstrap, using
   `after_tx_id = Some(bootstrap_tx_key.tx_id)`.
9. Return a node whose indexer has `latest_indexed_tx = Some(bootstrap_basis)`.

The low-level initialization marker must be written in the same SlateDB
`WriteBatch` as the bootstrap index entries. The marker is a startup completion
gate, not ordinary query data. Queryable database/schema version facts may be
added later as datoms, but they do not replace the low-level marker because
startup must be able to decide whether the schema/query layer is safe to use.

Initialized startup flow:

1. Read the low-level marker and require the new bootstrap-through-log format
   marker value.
2. Load schema from indices.
3. Scan partition counters from indices.
4. Recover latest indexed basis with `latest_tx_basis_from_sdb`.
5. Create the indexer seeded with that basis.
6. Start the normal subscription after that basis' `tx_id`.

Initialization recovery:

- Marker absent and log empty: append bootstrap, read it back, index it, and
  atomically write marker + bootstrap indices.
- Marker absent and log first record is bootstrap: replay that record through
  bootstrap mode and atomically write marker + bootstrap indices.
- Marker absent and log first record is not bootstrap: fail startup with an
  initialization error.
- Marker absent and SlateDB has any Triplox EAV index entry: fail startup with
  an initialization error. This concretely detects marker/index mismatch by
  checking whether the EAV index is non-empty.
- Marker present with any value other than the new bootstrap-through-log format:
  fail startup as unsupported initialization format.

Concurrent fresh startup against the same persistent database path is not
supported unless startup is serialized by an exclusive lock. Local/FileLog node
startup must take an internal startup lock before checking the marker/log state.
If the lock cannot be acquired, startup must return an error instead of racing
to append bootstrap. Remote multi-writer bootstrap remains out of scope.

## Tech Stack

- Rust workspace using Tokio async tests.
- SlateDB-backed indices via `slatedb`.
- Log implementations:
  - `MemoryLog` for in-memory nodes and tests.
  - `FileLog` for local/remote persistent nodes.
- Current transaction operation type: `triplox_client::ops::TxOp`.
- Current bootstrap data source: `schema::bootstrap_schema_tx()`.
- Current log serialization format: bincode. Bootstrap log validation is exact
  decoded equality against the current `bootstrap_schema_tx()`. Future TxOp/log
  serialization changes must add an explicit log/bootstrap migration strategy
  before changing this record shape.

## Commands

- Format: `cargo fmt`
- Triplox targeted tests: `cargo test -p triplox <test-filter>`
- Root crate tests: `cargo test -p triplox`
- EDN tests: `cargo test -p triplox-edn`
- Client tests: `cargo test -p triplox-client`
- Workspace tests: `cargo test --workspace`
- Clippy: `cargo clippy --workspace --all-targets --all-features`

## Project Structure

- `src/bootstrap.rs` owns initialization detection, format marker writes,
  metadata loading, bootstrap log validation, and corruption predicates.
- `src/node.rs` owns node startup orchestration, synchronous bootstrap replay,
  startup locking, and normal subscription startup.
- `src/indexer.rs` owns transaction indexing, `latest_indexed_tx`, and the
  bootstrap transaction indexing mode.
- `src/log.rs`, `src/memory_log.rs`, and `src/file_log.rs` keep the existing log
  traits and implementations unless implementation proves a small internal
  helper is needed.
- `src/schema.rs` keeps bootstrap schema constants and bootstrap schema
  consistency checks.
- `design/BOOTSTRAP_THROUGH_LOG.md` is the source of truth for this change.

## Code Style

Prefer explicit state names over overloading one variable for both indexed basis
and log replay position:

```rust
let latest_indexed = latest_tx_basis_from_sdb(slate.db.as_ref()).await?;
let subscription_after_tx_id = Some(latest_indexed.tx_key.tx_id);

let indexer = Indexer::new(slate.db.clone(), metadata, Some(latest_indexed));
let subscription = subscribe(log.clone(), subscription_after_tx_id, indexer.clone()).await;
```

Keep bootstrap-only branches narrow and named. Do not hide bootstrap behavior
inside generic helpers if the helper would silently change normal transaction
semantics.

## Implementation Plan

### 1. Split Bootstrap Metadata And Marker Helpers

Refactor `bootstrap::init_db` into explicit helpers:

- `load_metadata_if_initialized(slate) -> Result<Option<Metadata>>`
- `metadata_from_indices(slate) -> Result<Metadata>`
- `fresh_bootstrap_metadata() -> Metadata`
- `put_initialized_marker(batch: &mut WriteBatch)`
- `validate_bootstrap_log_record(record: &Record) -> Result<Vec<TxOp>>`
- `has_any_eav_index_entry(slate) -> Result<bool>`

Acceptance:

- Existing direct-bootstrap writes are still present until later tasks remove
  the call path.
- Helpers can distinguish: no marker/no EAV entries, no marker/EAV non-empty,
  supported marker, and unsupported marker.
- Bootstrap log validation uses decoded `Vec<TxOp>` equality to
  `bootstrap_schema_tx()`.

### 2. Extract Shared Transaction Batch Construction

Extract the normal transaction path so both normal and bootstrap transactions
can build datoms, tx entity datoms, validation reports, and index `WriteBatch`
without duplicating the entire pipeline.

Acceptance:

- Normal `transact_tx` behavior is unchanged.
- Shared code still updates `latest_indexed_tx` and broadcasts completion only
  after successful write.

### 3. Add Bootstrap Schema Derivation Check

Add bootstrap-specific schema handling around the shared pipeline:

- Interpret bootstrap tx ops with `bootstrap_schema()`.
- Build schema updates against `Schema::default()`.
- Compare produced `ident_map` and `attribute_map` to `bootstrap_schema()`.
- Fail bootstrap indexing if the produced schema differs.

Acceptance:

- A focused indexer test proves bootstrap indexing produces the hardcoded
  bootstrap schema.
- A mismatched bootstrap record fails deterministically.

### 4. Add Marker-In-Batch Bootstrap Commit

Add `transact_bootstrap_tx(record: Record) -> Result<TxBasis>` on the indexer.
It must:

- Validate the record as bootstrap.
- Build bootstrap index entries.
- Add the low-level format marker to the same `WriteBatch`.
- Commit once.
- Scan partition counters after the write.
- Set `metadata.schema`, `metadata.partition_map`, and `latest_indexed_tx`.
- Broadcast bootstrap completion.

Acceptance:

- Bootstrap index entries and marker are committed atomically.
- A bootstrapped indexer has `latest_indexed_tx = Some(bootstrap_basis)`.

### 5. Route Fresh Startup Through Synchronous Bootstrap Replay

Change `Node::memory_node` and `Node<FileLog>::from_slate_and_log` so fresh DBs
bootstrap by synchronous log replay before normal subscription starts.

Acceptance:

- Startup errors for wrong first record, decode failure, schema mismatch, and
  marker/index mismatch are returned from node startup instead of logged in a
  background task.
- Fresh node startup appends bootstrap only if the log is empty.
- If the log already contains bootstrap and the marker is absent, startup
  replays the existing record instead of appending a duplicate.

### 6. Remove Legacy Bootstrap Identity Assumptions

Remove new-format use of hardcoded `BOOTSTRAP_TX_KEY`. The bootstrap `TxKey`
comes from the first log record. `BOOTSTRAP_TX_EID` may remain as the expected
first TX partition entity.

Acceptance:

- Fresh DBs have exactly one transaction with `:db/txId 0`.
- The first user transaction after bootstrap has `tx_id > 0`.
- Tests assert consistency with the DB/indexed basis, not equality with
  `BOOTSTRAP_TX_KEY`.

## Testing Strategy

Use focused Tokio tests in `src/node.rs`, `src/bootstrap.rs`, `src/indexer.rs`,
and existing log tests.

Required tests:

- Bootstrap log validation:
  - decoded first log record equal to `bootstrap_schema_tx()` is accepted.
  - malformed bincode, non-bootstrap first record, and semantically different
    bootstrap ops are rejected.
- Bootstrap indexer:
  - bootstrap through the indexer produces the same `ident_map` and
    `attribute_map` as `bootstrap_schema()`.
  - marker and bootstrap indices are written in one successful bootstrap commit.
  - `latest_indexed_tx` is set to the bootstrap basis.
- Fresh memory node:
  - `db().tx_basis()` is the bootstrap basis.
  - `tx_waiter().await_tx(db.tx_key())` resolves immediately.
  - querying `[:find ?tx :where [?tx :db/txId 0]]` returns exactly one row.
- First user transaction:
  - after defining test schema, querying `:db/txId 0` still returns only
    bootstrap.
  - the first user transaction has `tx_id > 0`.
- Local restart:
  - a fresh local node writes bootstrap through `FileLog`.
  - reopening the same directory does not duplicate bootstrap.
  - already indexed user transactions are skipped on restart.
- Recovery and corruption:
  - log contains bootstrap but marker is missing: startup replays bootstrap and
    does not append duplicate bootstrap.
  - marker missing and first log record is not bootstrap: startup errors.
  - marker missing and EAV is non-empty: startup errors.
  - unsupported old marker value: startup errors.

Final verification must run:

```bash
cargo fmt
cargo test --workspace
cargo clippy --workspace --all-targets --all-features
```

## Boundaries

- Always:
  - Preserve existing public client APIs.
  - Source the bootstrap `TxKey` from the log record.
  - Wait for bootstrap indexing before returning a fresh node.
  - Write the low-level format marker atomically with bootstrap index entries.
  - Treat queryable version facts, if added, as ordinary data and not as the
    startup initialization gate.
  - Keep historical reads based on `tx_eid`.
  - Return deterministic startup errors for invalid bootstrap state.
- Ask first:
  - Supporting remote multi-writer bootstrap.
  - Changing the public log trait API.
  - Adding dependencies or changing workspace structure.
  - Changing public wire formats.
- Never:
  - Reintroduce direct fresh-bootstrap index writes as the primary path.
  - Mark a DB initialized outside the bootstrap indexer write batch.
  - Rely on hardcoded `BOOTSTRAP_TX_KEY` as the source of truth for new DBs.
  - Replace low-level startup detection with a query that requires the DB to
    already be bootstrapped.
  - Silently open unsupported old direct-bootstrap database formats.
  - Remove failing regression tests without replacing them with equivalent
    coverage.

## Success Criteria

- Fresh bootstrap enters through the log, is synchronously indexed by the
  indexer during startup, and is observable through the same waiter path as
  normal transactions.
- `latest_indexed_tx` is populated with the bootstrap basis immediately after
  fresh startup.
- New DBs no longer create duplicate raw `tx_id = 0` transaction entities.
- Bootstrap schema generated by the transaction path equals the hardcoded
  `bootstrap_schema()`.
- Invalid or unsupported initialization states return deterministic startup
  errors.
- The full workspace test and clippy commands pass.

## Open Questions

- None for the initial bootstrap-phase implementation.
