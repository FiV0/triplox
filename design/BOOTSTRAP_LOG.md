# Spec: Bootstrap Transaction Through the Log

Version 0.1

Issue: https://github.com/FiV0/triplox/issues/278

## Assumptions

1. The bootstrap transaction should remain the first transaction in a fresh database.
2. Fresh databases should index bootstrap by using the same transaction pipeline as user transactions, instead of direct SlateDB index writes.
3. The bootstrap transaction may keep `tx_id = 0`; the bug is that the first user transaction also gets indexed as `tx_id = 0`.
4. No migration path is required for pre-change stores whose log starts with the first user transaction instead of bootstrap.

## Objective

Route the bootstrap schema transaction through the log and indexer so transaction ids, tx entities, waiters, CDC, and restart replay all observe one transaction path.

Success means a fresh node has exactly one indexed transaction with `:db/txId 0`, that transaction installs the bootstrap schema, and the first user-supplied transaction has a later log-issued `tx_id`. Startup should no longer need to compare against `BOOTSTRAP_TX_EID` to avoid skipping the first user transaction.

## Tech Stack

- Rust 2021
- `tokio` async runtime
- `slatedb` for indexed storage
- `bincode`-encoded `Vec<TxOp>` records in `MemoryLog` and `FileLog`
- Triplox schema, tempid, validation, and indexing pipeline in `src/`

## Commands

Build:

```bash
cargo build --workspace
```

Format:

```bash
cargo fmt
```

Tests:

```bash
cargo test --workspace
```

Clippy:

```bash
cargo clippy --workspace --all-targets --all-features
```

Targeted tests during implementation:

```bash
cargo test -p triplox bootstrap
cargo test -p triplox local_node_restart
cargo test -p triplox test_execute_tx_returns_tx_key
```

## Project Structure

- `src/bootstrap.rs` owns fresh/existing database initialization, bootstrap constants, version metadata, and bootstrap-specific verification.
- `src/node.rs` owns node startup, log construction, subscription, catch-up, and public `SubmitNode`/`QueryNode` behavior.
- `src/indexer.rs` owns the transaction pipeline, transaction entity creation, index writes, schema updates, and tx completion notifications.
- `src/log.rs`, `src/memory_log.rs`, and `src/file_log.rs` own log append/read/subscribe behavior.
- `src/schema.rs` owns bootstrap schema constants, schema validation, schema update preparation, and schema loading from indices.
- `design/` contains architectural docs. This file is the design reference for issue 278.

## Current Behavior

Fresh database initialization currently bypasses the log:

1. `bootstrap::init_db` builds `bootstrap_schema()` and `bootstrap_schema_tx()`.
2. It expands, resolves, validates, and writes bootstrap datoms directly to SlateDB.
3. It appends tx entity datoms using `BOOTSTRAP_TX_KEY`, where `tx_id = 0`.
4. `Node<FileLog>::from_slate_and_log` then creates the log and replays user records.
5. The first `MemoryLog` transaction also gets `tx_id = 0`; the first `FileLog` transaction also gets `tx_id = 0` when the file is empty because its id is the starting byte offset.
6. Startup works around the collision by treating latest indexed tx `0` as replayable unless its `tx_eid` is beyond `BOOTSTRAP_TX_EID`.

This creates two indexed transaction entities with `:db/txId 0` after the first user transaction.

## Mentat Reference

Mentat bootstraps by passing bootstrap assertions through its regular transactor while using two schema views:

- a complete bootstrap schema for resolving and validating bootstrap assertions;
- an empty schema as the mutation base that the bootstrap transaction must populate.

After transacting, Mentat compares the produced schema against the expected bootstrap schema and fails startup if they differ. Triplox should use the same shape: bootstrap needs enough schema to validate itself, but correctness is checked by deriving the schema from the bootstrap transaction output.

## Proposed Design

### Decisions

- No migration path for pre-change uninitialized/local stores.
- Bootstrap is processed by the subscriber, not by a direct indexer call.
- Startup may use `read_txs_after(None, 1)` to inspect the first log record unless a stronger log introspection API becomes useful for other callers.
- Remove `BOOTSTRAP_TX_EID` after the replay workaround is gone.

### Bootstrap Record

For a fresh database with an empty log, startup should append `bincode::serialize(&bootstrap_schema_tx())` to the same `TxLog` used for user transactions. The log returns the authoritative `TxKey`.

For `MemoryLog`, this makes bootstrap `tx_id = 0` and the first user transaction `tx_id = 1`.

For `FileLog`, this makes bootstrap `tx_id = 0` in an empty file and the first user transaction a later byte offset.

### Bootstrap Transaction Entry Point

Add a bootstrap-specific transaction path in the indexer rather than special-casing direct writes in `init_db`.

The bootstrap path should:

1. Start from a pending partition map where the next `TX_PARTITION` entity is the bootstrap tx entity.
2. Use `bootstrap_schema()` as the processing schema for ident resolution, ref resolution, type checks, uniqueness checks, and index key construction.
3. Use `Schema::default()` as the schema mutation base.
4. Run the same expansion, lookup-ref resolution, tempid resolution, tx entity datom construction, cardinality finalization, validation, and index writing stages as normal transactions.
5. Prepare the schema update against the mutation base, apply it to an empty schema, and assert it equals `bootstrap_schema()`.
6. Commit index entries and the database version marker atomically in the same `WriteBatch`.
7. Update metadata, partition counters, `latest_indexed_tx`, and tx completion notifications just like normal committed transactions.

The normal transaction path should not be weakened to allow redefinition of existing schema entities. Only the bootstrap entry point gets the separate mutation schema.

### Startup Flow

Fresh database with empty log:

1. Create the log.
2. Create an indexer in bootstrap mode.
3. Create a `TxWaiter` before appending.
4. Start the subscriber with `after_tx_id = None`.
5. Append `bootstrap_schema_tx()` to the log.
6. Wait for the bootstrap tx completion.
7. Mark the database initialized as part of the bootstrap commit.
8. Keep the subscriber running for normal live transactions.

Fresh database with non-empty log and missing version marker:

1. Read the first log record with `read_txs_after(None, 1)`.
2. Deserialize the first record and require it to equal `bootstrap_schema_tx()`.
3. Create a `TxWaiter` for that record's `TxKey`.
4. Start the subscriber with `after_tx_id = None`.
5. Wait for bootstrap replay to complete.
6. Fail startup if the first record is not the bootstrap transaction.

Existing database:

1. Load schema from indices.
2. Scan partition counters.
3. Read latest indexed transaction basis from SlateDB.
4. Start normal log catch-up after that tx id.
5. Do not special-case `BOOTSTRAP_TX_EID` for new-format stores.

Crash recovery:

- If the log append succeeds but indexing does not, startup should replay the bootstrap record from the log and complete initialization.
- The version marker must not be written before bootstrap index entries are committed.
- If version metadata exists, startup must not append another bootstrap record.
- If version metadata is missing and the first log record is not bootstrap, startup fails instead of attempting legacy migration.
- The subscriber owns replay in all recovery cases; startup coordinates by subscribing before append or replay and waiting on the bootstrap tx key.

### Log Emptiness

The simplest implementation is to use `read_txs_after(None, 1)` as a first-record probe. It answers both required startup questions:

- empty result means startup should append bootstrap;
- one result gives the transaction that must be verified as bootstrap and replayed.

Alternatives are possible but add more surface area:

- add `TxLogReader::first_tx()` or `TxLogReader::is_empty()`;
- add a separate durable log metadata/header record;
- infer bootstrap state only from SlateDB metadata.

The metadata-only option is insufficient for crash recovery because it cannot distinguish "log append succeeded, indexing did not" from "nothing happened yet". A new log helper is reasonable if more callers need first-record inspection, but `read_txs_after(None, 1)` is enough for this implementation.

## Code Style

Prefer small helpers with explicit schema roles:

```rust
struct BootstrapSchemas {
    processing: Schema,
    mutation_base: Schema,
}

impl BootstrapSchemas {
    fn new() -> Self {
        Self {
            processing: bootstrap_schema(),
            mutation_base: Schema::default(),
        }
    }
}
```

Names should distinguish processing/validation schema from mutation schema. Avoid generic names like `schema2` or `bootstrap_schema_for_update`.

## Testing Strategy

Use unit tests for the bootstrap/indexer contract and integration tests for node restart behavior.

Required tests:

- Fresh `MemoryLog` node has one bootstrap tx entity with `:db/txId 0`.
- After the first user transaction, `[:find ?tx :where [?tx :db/txId 0]]` returns one row, not two.
- First user transaction result has `tx_key.tx_id > 0`.
- Fresh local node writes bootstrap through `FileLog`; after restart, startup does not replay already-indexed bootstrap or user transactions.
- A bootstrap transaction whose produced schema differs from `bootstrap_schema()` fails initialization.
- If a bootstrap record exists in the log but the version marker is absent, startup can replay it and finish initialization.
- Existing DB startup still loads schema from indices and catches up unindexed log records.

## Boundaries

- Always: keep bootstrap validation at least as strict as normal transaction validation.
- Always: compare the transaction-derived bootstrap schema to `bootstrap_schema()`.
- Always: keep the version marker write ordered with successful bootstrap indexing.
- Always: preserve one-line comments where possible.
- Ask first: compatibility behavior for pre-change stores whose log starts with the first user transaction rather than a bootstrap transaction.
- Ask first: changing the durable log record format.
- Ask first: changing public client protocol fields for transaction ids or bases.
- Never: silently direct-write bootstrap indices in new-format fresh databases.
- Never: migrate or reinterpret pre-change uninitialized logs whose first record is not bootstrap.
- Never: allow normal user transactions to redefine installed schema entities as a side effect of this change.

## Success Criteria

- Issue 278 is fixed for new databases: bootstrap and the first user transaction no longer share indexed `:db/txId 0`.
- Bootstrap data is present in the log for fresh databases.
- Bootstrap schema is proven by the transaction output, not trusted only because constants were loaded.
- `Node<FileLog>::from_slate_and_log` no longer needs the `BOOTSTRAP_TX_EID` replay workaround for new-format stores.
- `BOOTSTRAP_TX_EID` is removed unless another non-workaround use appears during implementation.
- `cargo fmt`, `cargo test --workspace`, and `cargo clippy --workspace --all-targets --all-features` pass before committing.

## Open Questions

None for the current implementation plan.
