# Spec: Reader Node

## Assumptions

1. A ReaderNode is a Triplox server process that serves read-only query traffic from a local filesystem SlateDB database.
2. ReaderNode must implement `QueryNode` only; it must not implement `SubmitNode`.
3. ReaderNode uses `slatedb::DbReader`, not `slatedb::Db`, for its read handle.
4. ReaderNode should follow new transactions by subscribing to `DbReader` status changes, then scanning Triplox indexes to refresh reader-visible schema and derive the latest indexed `TxBasis`.
5. ReaderNode may lag the writer by SlateDB's reader refresh and WAL replay behavior; it should report the latest transaction that is actually queryable on that reader.
6. The first version supports only local filesystem `DbReader` storage.

## Objective

Add ReaderNode functionality so Triplox can run query-only nodes backed by SlateDB read-only handles. A ReaderNode should expose the existing DB open/query/close server workflow, track the latest transaction available on its reader, and reject all transaction write endpoints.

Users should be able to point a ReaderNode at the same local SlateDB database path as a writer node and issue normal queries against the latest reader-visible state. `submit-tx` and `execute-tx` must fail with HTTP 405 without attempting to append to a log or index local writes.

## Tech Stack

- Rust workspace rooted at `Cargo.toml`
- Server: `axum` HTTP/2 handlers in `src/server.rs`
- Node traits: `QueryNode` and `SubmitNode` from `triplox-client/src/node.rs`, re-exported by `src/node.rs`
- Writable node: `Node<L: TxLog>` in `src/node.rs`
- Read-only storage: `slatedb::DbReader`
- Local object store: `slatedb::object_store::local::LocalFileSystem`
- SlateDB status: `DbReader::subscribe()` / `DbReader::status()` returning `DbStatus`
- Query engine: `src/query.rs` and `src/iterator/*`

## Commands

Build:

```bash
cargo check -p triplox
```

Focused tests:

```bash
cargo test -p triplox reader_node
cargo test -p triplox client_server
```

Workspace tests:

```bash
cargo test --workspace
```

Lint:

```bash
cargo clippy --workspace --all-targets --all-features
```

Format:

```bash
cargo fmt
```

## Project Structure

- `src/node.rs` - existing writable `Node<L>` and candidate home for `ReaderNode`
- `src/slate/mod.rs` - SlateDB construction helpers; add reader construction helpers here
- `src/server.rs` - HTTP server routing; adapt to support query-only nodes
- `src/config.rs` - add reader-node configuration shape
- `src/main.rs` - instantiate writable or reader server from config
- `src/query.rs` and `src/iterator/*` - adapt query execution away from `DbSnapshot`-only assumptions if needed
- `slatedb_estimates` - updated separately so range estimates can work from reader-visible manifests
- `design/READER_NODE.md` - this specification
- `tests/client_server_test.rs` or new integration tests - server behavior for reader tx rejection and query success

## Code Style

Keep read and write capabilities separated in types. ReaderNode should compile without a `SubmitNode` implementation.

Do not commit to the exact in-memory ReaderNode state shape before the SlateDB status observation path is implemented. See `design/READER_NODE_ANALYSIS.md` for the current analysis of `DbStatus`, `tokio::sync::watch`, and how status changes should drive derived Triplox state refresh.

Prefer explicit capability names over boolean mode checks where possible. If a router must expose transaction routes for wire compatibility, route handlers should reject them through server capability state rather than requiring `ReaderNode: SubmitNode`.

## Testing Strategy

Unit tests:

- ReaderNode does not implement `SubmitNode`; this is primarily enforced by Rust types.
- Latest transaction tracker updates after `DbReader::subscribe()` reports a newer visible state.
- Reader-visible schema refreshes after `DbReader::subscribe()` reports a newer visible state.
- Status-change refresh scans Triplox indexes using `load_schema_from_indices` and `latest_tx_basis_from_*`, then publishes the schema and latest queryable `TxBasis` together.

Server tests:

- A reader server responds successfully to `/db/open`, `/db/{db_id}/query`, and `/db/{db_id}` close.
- A reader server returns HTTP 405 for `/tx/submit`.
- A reader server returns HTTP 405 for `/tx/execute`.
- Writable node server behavior remains unchanged.

Integration tests:

- Start a writer backed by a shared local SlateDB path, commit a transaction, open a ReaderNode over the same path, wait for reader visibility, and query the committed data.
- Commit another writer transaction and verify ReaderNode eventually advances to the new latest transaction without restart.

## Boundaries

- Always: Keep ReaderNode read-only at the type level.
- Always: Derive ReaderNode's advertised/latest transaction from indexes that are visible to that DbReader, not from writer-local state.
- Always: Rebuild ReaderNode's schema from indexes visible to that DbReader during startup and after each observed status change.
- Always: Reject `submit-tx` and `execute-tx` with HTTP 405 from reader server mode.
- Always: Close `DbReader` on shutdown.
- Ask first: Changing SlateDB or `slatedb_estimates` APIs.
- Ask first: Adding new public wire-protocol messages or changing existing client APIs.
- Ask first: Changing query planner semantics or replacing approximate range statistics with exact scans.
- Never: Let ReaderNode open a writable `slatedb::Db`.
- Never: Implement `SubmitNode` for ReaderNode.
- Never: Accept tx requests and then fail later after partially decoding or applying write state.

## Success Criteria

- `ReaderNode` exists and implements `QueryNode`.
- `ReaderNode` does not implement `SubmitNode`.
- Reader construction uses `slatedb::DbReader`.
- ReaderNode tracks the latest reader-visible `TxBasis` and schema by subscribing to DbReader changes and scanning Triplox indexes after updates.
- Server configuration can select local reader-node mode.
- Reader server permits DB open/query/close and rejects tx submit/execute with HTTP 405.
- Existing writable `Memory`, `Local`, `Remote`, and `Dev` modes keep their current behavior.
- `cargo test --workspace` and `cargo clippy --workspace --all-targets --all-features` pass before declaring implementation complete.

## Current Findings

- `DbReader` exposes `get`, `scan`, `scan_prefix`, `subscribe`, `status`, `manifest`, and `close`.
- `DbReader::status()` exposes `durable_seq`, `current_manifest`, and `close_reason`, but not a Triplox `TxBasis`.
- `DbReader::subscribe()` is enough to observe that SlateDB's reader view changed, but ReaderNode still needs to query Triplox indexes afterwards to compute the latest indexed transaction and refresh schema.
- SlateDB provides `DbStatus`; Triplox derives schema and `TxBasis` from reader-visible indexes after observing `DbStatus` changes. The exact ReaderNode-owned state shape is deferred until the status-driven refresh path is implemented.
- Writable startup handles existing DBs by calling `bootstrap::init_db`, whose existing-DB path calls `load_schema_from_indices` before constructing `Metadata`.
- Writable nodes keep schema fresh after local writes through `Indexer::transact_tx_inner`, which applies schema deltas to in-memory metadata after a successful index write. ReaderNode will not execute that write path, so status-change refresh must reload schema from reader-visible indexes rather than relying only on startup schema.
- Triplox query execution currently stores `Arc<slatedb::DbSnapshot>` in `DB`, `execute_query`, `SlateIterator`, `TemporalFilterIterator`, and `GenericPrefixExtender`.
- `DbReader` does not appear to expose a `snapshot()` method. Query code should accept a `DbReaderLike` abstraction where it currently accepts `Db` or `DbSnapshot`.
- `slatedb_estimates::RangeStats` currently stores `Arc<slatedb::Db>` and reads `db.manifest()`. ReaderNode will rely on an adapted `slatedb_estimates` that can operate from reader-visible manifests.

## Decisions

1. First version supports local filesystem `DbReader` only.
2. Reader tx endpoints return HTTP `405 Method Not Allowed`.
3. Query and index code should take a `DbReaderLike` abstraction where `Db` or `DbSnapshot` is currently required.
4. `slatedb_estimates` will be adapted separately so ReaderNode can use range estimates with reader-visible manifests.
5. ReaderNode latest transaction tracking is internal only; no public health/status endpoint is required for the first version.
6. ReaderNode state refresh includes schema and latest `TxBasis` as one observed reader-visible state.

## Implementation Plan

1. Introduce `DbReaderLike`
   - Define a small read abstraction covering `get_with_options`, `scan_with_options`, `scan_prefix_with_options`, and manifest access needed by range estimates.
   - Implement it for `slatedb::Db`, `slatedb::DbSnapshot`, and `slatedb::DbReader` as needed by existing call sites.
   - Keep the abstraction internal unless public API pressure appears.

2. Generalize query/index reads
   - Change `DB`, `execute_query`, `SlateIterator`, `TemporalFilterIterator`, and prefix extenders to hold/use `Arc<dyn DbReaderLike>` or a generic equivalent.
   - Preserve current writable node behavior by wrapping `DbSnapshot` in the same read abstraction.
   - Keep temporal filtering and `as_of` semantics unchanged.

3. Add local ReaderNode construction
   - Add a local reader helper in `src/slate/mod.rs` using `LocalFileSystem` and `slatedb::DbReader::builder`.
   - Add `ReaderNode` in `src/node.rs` with `QueryNode` only.
   - Load schema and latest `TxBasis` from reader-visible indexes on startup.
   - Spawn a status subscription task that responds to `DbReader` changes by reloading schema, rescanning latest indexed tx, and atomically updating internal reader state.

4. Split server write capability
   - Adapt `src/server.rs` so query routes require only `QueryNode`.
   - Keep tx routes installed for protocol compatibility, but make reader mode return HTTP 405.
   - Preserve writable server routes for nodes implementing `SubmitNode`.

5. Add config/main wiring
   - Extend `StorageConfig` with a local reader mode.
   - Instantiate `ReaderNode` from `src/main.rs` for that mode.
   - Keep existing `Dev`, `Memory`, `Local`, and `Remote` modes unchanged.

6. Verify behavior
   - Add focused ReaderNode tests for query success, latest tx advancement, and 405 tx rejections.
   - Run `cargo check -p triplox`.
   - Run focused tests, then `cargo test --workspace`.
   - Run `cargo clippy --workspace --all-targets --all-features`.
