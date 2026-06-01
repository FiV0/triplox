# Implementation Plan: Bootstrap Transaction Through the Indexer

Spec: `design/BOOTSTRAP_LOG.md`

## Overview

Implement issue 278 by making bootstrap consume the first durable log key while indexing it through the normal indexer transaction pipeline. Initialization is based on indexed transaction presence, not a metadata version marker.

## Tasks

1. Add an optional latest-transaction helper that returns `None` when `TX_PARTITION` is empty and still errors on malformed tx entities.
2. Refactor the indexer transaction pipeline to accept a schema context: normal transactions use current schema for processing and schema updates, while bootstrap uses `bootstrap_schema()` for processing and `Schema::default()` for schema updates.
3. Rework fresh startup to append or replay the first bootstrap log record, index it directly through the bootstrap schema context, then start normal subscription after the bootstrap tx id.
4. Keep existing startup on initialized stores: load metadata from indices, load latest tx basis, and subscribe after that tx id.
5. Remove version-marker code, stale bootstrap subscriber mode, and tests/docs that assume version metadata.
6. Verify targeted bootstrap/restart tests, then the package/workspace checks.

## Acceptance Criteria

- Bootstrap consumes the first log-issued `TxKey`.
- Fresh nodes have one indexed `:db/txId 0` transaction entity.
- The first user transaction has a distinct later tx id.
- Startup treats an empty `TX_PARTITION` as uninitialized.
- Startup rejects an uninitialized store whose first log record is not bootstrap.
- Normal schema mutation restrictions remain unchanged.

## Verification

```bash
cargo test -p triplox bootstrap
cargo test -p triplox test_first_submitted_tx_has_distinct_tx_id
cargo test -p triplox local_node_replays_bootstrap_when_version_missing
cargo test -p triplox
cargo test --workspace
cargo clippy --workspace --all-targets --all-features
```
