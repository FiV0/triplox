# Spec: Bootstrap Transaction Through the Indexer

Issue: https://github.com/FiV0/triplox/issues/278

## Objective

Bootstrap should be the first durable log transaction and should be indexed by the same transaction pipeline used for user transactions. Startup should decide whether a store is initialized by checking whether SlateDB contains an indexed transaction entity in `TX_PARTITION`; there is no separate bootstrap version marker.

Success means a fresh node has exactly one indexed transaction with `:db/txId 0`, that transaction installs the bootstrap schema, and the first user transaction receives a later log-issued id.

## Design

Bootstrap differs from normal transactions only by schema context:

- Normal transaction: processing schema is current metadata schema, and schema update base is current metadata schema.
- Bootstrap transaction: processing schema is `bootstrap_schema()`, and schema update base is `Schema::default()`.

The processing schema is used for tx expansion, lookup-ref resolution, tempid resolution, cardinality-one finalization, uniqueness checks, datom validation, and index-key construction. The schema update base is used only when preparing schema updates from finalized datoms.

For bootstrap, the derived schema must equal `bootstrap_schema()`. This proves the bootstrap tx installed the expected schema instead of merely trusting prebuilt constants. Normal transactions continue to reject modification of already-installed schema entities.

## Startup Flow

Startup first checks for an indexed transaction entity using the `TX_PARTITION` scan used to load the latest transaction basis.

If a latest transaction exists:

1. Load schema and partition counters from indices.
2. Initialize the indexer with that metadata and latest basis.
3. Subscribe after the latest indexed tx id.

If no transaction exists:

1. Probe the first log record with `read_txs_after(None, 1)`.
2. If the log is empty, append serialized `bootstrap_schema_tx()` and use the returned `TxKey`.
3. If the log is non-empty, require the first record to equal `bootstrap_schema_tx()` and use that record's `TxKey`.
4. Index that record directly through the indexer pipeline in bootstrap schema context.
5. Subscribe after the bootstrap tx id.

If no transaction exists and the first log record is not bootstrap, startup fails. No compatibility path is required for old pre-change stores.

## Testing

Required coverage:

- Empty SlateDB reports no latest transaction basis.
- Bootstrap indexing installs the expected schema and creates the latest transaction basis.
- Fresh memory/local nodes expose exactly one tx entity with `:db/txId 0`.
- The first user transaction receives a later tx id.
- Local restart after bootstrap does not replay or skip transactions.
- Missing indexed tx plus bootstrap first log record replays and initializes.
- Missing indexed tx plus non-bootstrap first log record fails startup.
- Normal user schema updates still reject modifying installed schema entities.

Before handoff, run:

```bash
cargo test -p triplox bootstrap
cargo test -p triplox
cargo test --workspace
cargo clippy --workspace --all-targets --all-features
```
