# Triplox Metrics

Version 0.1

Status: design, not implemented.

## Overview

Triplox has no operational metrics today. An operator cannot see load, latency, indexing lag, memory, or how far incremental queries (subscriptions) fall behind. This design adds a `/metrics` endpoint that Prometheus can scrape. It covers queries in flight, incremental queries in flight, query and transaction latency, server memory, incremental query memory, incremental inbox lag, the latest indexed transaction and the latest submitted transaction.

## Design decisions

1. Use `metrics` and `metrics-exporter-prometheus`. `metrics` is a facade, like `log`. Library code records values with macros, and only the binary installs the exporter. Without an exporter the macros do nothing, so embedded nodes and tests are not affected.
2. Serve `/metrics` on a separate port. The main server speaks HTTP/2 only (`hyper`/`hyper-util` have only the `http2` feature, `src/server.rs:33`). Prometheus scrapes plain-text targets over HTTP/1.1.
3. Add `retrieve_profile_async` to the FiV0/feldera fork for the circuit memory numbers.
4. Export only aggregates for incremental queries. Do not add a `query_id` label. The facade cannot delete a series when a query retires, so per-query labels grow without limit.

All names use the `triplox_` prefix and Prometheus units (`_seconds`, `_bytes`, `_total` for counters). A value that goes down, such as "queries in flight", is a gauge, not a counter.

## Dependencies

Add these to `[dependencies]` in `Cargo.toml`. Use the newest compatible releases at implementation time (currently `metrics` 0.24 and exporter 0.17):

- `metrics = "0.24"`
- `metrics-exporter-prometheus = { version = "0.17", default-features = false, features = ["http-listener"] }`

Add `metrics-util` (the version that the exporter uses, with the `debugging` feature) to `[dev-dependencies]` for tests.

`dbsp` already pulls in `metrics` 0.23. It records its own numbers in static atomics (`dbsp/src/circuit/metrics.rs`), not through the facade, so the two versions do not conflict. Use the existing `sysinfo` dependency for process memory. Do not add `metrics-process`.

## New module: `src/metrics.rs`

Declare it in `src/lib.rs` as `pub mod metrics`. The module contains:

1. One `pub(crate) const` for each metric name, so that call sites do not repeat string literals.
2. `pub fn install(config: &MetricsConfig, token: CancellationToken) -> Result<()>`. It builds a `PrometheusBuilder` with `with_http_listener(addr)`, sets the buckets, calls `install()`, runs the `describe_*` calls, sets `triplox_build_info{version}` to 1, and starts the process memory sampler task.
3. Buckets for each histogram, with `set_buckets_for_metric(Matcher::Full(name), ...)`. Without buckets the exporter renders summaries, and Prometheus cannot add summaries across nodes.
   - Latency (`*_duration_seconds`, `*_wait_seconds`): 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30.
   - Lag (`*_lag_seconds`): 0.001 up to 300.
   - Depth: 0, 1, 2, 4, 8, 16, 32, 64, 128, 256.
   - Bytes: powers of 4 from 64 KiB to 16 GiB.
4. `InFlightGuard`. It increments a gauge in `new` and decrements it in `Drop`. Axum drops a handler future when the client disconnects, so a manual decrement at the end of the handler can be skipped.
5. The process memory sampler. Every 5 s it refreshes the current process with `sysinfo` (`get_current_pid`, `refresh_processes_specifics`) and sets the gauges. It stops when the token is cancelled.

Library code only calls the macros. Only `main.rs` calls `install`. Without an installed recorder the macros do nothing, so embedded nodes and tests are not affected.

## Configuration (`src/config.rs`, `src/main.rs`)

- Add `MetricsConfig { enabled: bool, host: String, port: u16 }` with defaults `true`, `127.0.0.1`, `9490`. Use `#[serde(deny_unknown_fields)]` and `#[serde(default)]`, like `ServerConfig`.
- Add `#[serde(default)] pub metrics: MetricsConfig` to `Config`.
- In `run()` in `src/main.rs`, if `config.metrics.enabled` is true, call `triplox::metrics::install(&config.metrics, token.clone())` before `run_server`. This requires `run()` to create the cancellation token and pass it to `run_server`.
- Add a commented `[metrics]` section to the files in `config/`.

## Metrics and hook points

### Queries (`src/server.rs`)

| Name | Type | Labels | Hook |
|---|---|---|---|
| `triplox_queries_in_flight` | gauge | | `InFlightGuard` at the top of `query` |
| `triplox_queries_total` | counter | `outcome=ok\|error` | `query` |
| `triplox_query_duration_seconds` | histogram | `outcome` | the full `query` handler, including decode and encode |
| `triplox_query_basis_wait_seconds` | histogram | | around `state.node.db_as_of(...)` in `query` |
| `triplox_query_index_wait_seconds` | histogram | | around `tokio::time::timeout(timeout, waiter.await_indexed(tx_key))` in `Node::db_as_of_with_timeout` (`src/node.rs:316`) |
| `triplox_query_index_wait_timeouts_total` | counter | | the `TxIndexingTimeout` branch in `Node::db_as_of_with_timeout` |
| `triplox_query_execution_seconds` | histogram | | around `db.query_with_args(...)` in `query` |

`db_as_of_with_timeout` does three things before a query can run:

1. It takes the indexer read lock for `tx_waiter()`. The log subscriber holds the indexer write lock while it indexes one transaction, so this lock wait is indexing contention even when the basis is already indexed.
2. It waits in `await_indexed` until the requested transaction is indexed. This is the indexing lag that the query sees.
3. It takes the read lock again and clones the `ident_map`.

`triplox_query_index_wait_seconds` measures only step 2. `triplox_query_basis_wait_seconds` measures all three steps, so the difference is lock contention plus the clone. `triplox_query_execution_seconds` measures only the query engine. The rest of `triplox_query_duration_seconds` is request decode, parse and response encode.

To record the outcome once, move the body of `query` into an inner async fn that returns `Result`, and record in the outer handler.

### Transactions (`src/node.rs`, `src/indexer.rs`)

| Name | Type | Labels | Hook |
|---|---|---|---|
| `triplox_tx_submitted_total` | counter | `route=submit\|execute` | after `log.append_tx` succeeds in `Node::submit_tx` and `Node::execute_tx` |
| `triplox_tx_append_duration_seconds` | histogram | | around `log.append_tx` |
| `triplox_tx_execute_duration_seconds` | histogram | `outcome=committed\|aborted\|error` | the full `Node::execute_tx` |
| `triplox_tx_indexed_total` | counter | `outcome=committed\|aborted\|failed` | `Indexer::transact_tx` and the deserialize failure in `Indexer::accept` |
| `triplox_tx_index_duration_seconds` | histogram | | `Indexer::transact_tx` |
| `triplox_tx_index_lag_seconds` | histogram | | `Indexer::transact_tx` on success: now minus `tx_key.system_time` |

Add a private helper `Node::append` that wraps `log.append_tx`, records the timer, the counter and the submitted gauge. `submit_tx` and `execute_tx` both call it.

### Latest transactions

| Name | Type | Hook |
|---|---|---|
| `triplox_latest_submitted_tx_id` | gauge | `Node::append`, after the append succeeds |
| `triplox_latest_indexed_tx_id` | gauge | the two places that set `self.latest_indexed_tx` (`src/indexer.rs:368`, `src/indexer.rs:570`) |
| `triplox_latest_indexed_tx_system_time_seconds` | gauge | same places, as Unix seconds |

Also set the indexed gauges once in `Node` construction from `latest_tx_key_from_sdb`, so they are correct after a restart. The node is the only writer today, so the submitted gauge covers all writes. If reader nodes come later, move the submitted gauge to the log side. The indexing backlog is `triplox_latest_submitted_tx_id - triplox_latest_indexed_tx_id` in PromQL. Gauges are `f64`, so transaction ids are exact up to 2^53.

### Incremental queries (`src/incremental.rs`, `src/incremental/worker.rs`)

| Name | Type | Labels | Hook |
|---|---|---|---|
| `triplox_incremental_queries_active` | gauge | | set from `queries.len()` after the insert in `start_query` and after the remove in `retire` |
| `triplox_incremental_queries_registered_total` | counter | | `start_query` |
| `triplox_incremental_queries_retired_total` | counter | | `retire` |
| `triplox_incremental_queries_terminated_total` | counter | `reason=lagged\|failed` | the `Termination::Lagged` branch in `apply_batch`, and `Termination::Failed` in `Worker::run` |
| `triplox_incremental_register_duration_seconds` | histogram | | `Worker::run`, from start of the build until the registration response is sent in `serve` |
| `triplox_incremental_step_duration_seconds` | histogram | | around `circuit.apply(&batch)` in `serve` |

Take the active count from the dispatcher, not from the `subscribe` handler. A dropped subscription body retires the query asynchronously, so only the dispatcher has the true number.

### Incremental lag

Each query has two bounded queues. The inbox fills when the worker is slower than CDC. A full inbox terminates the query as lagged. The output channel (`SUBSCRIPTION_OUTPUT_CAPACITY`) fills when the client reads too slowly. For a tokio `Sender`, the number of queued items is `max_capacity() - capacity()`.

| Name | Type | Hook |
|---|---|---|
| `triplox_incremental_inbox_depth_max` | gauge | `apply_batch`, the max over all queries after the sends. Also recompute in `retire`. |
| `triplox_incremental_inbox_depth` | histogram | `apply_batch`, one sample per query per routed batch |
| `triplox_incremental_inbox_capacity` | gauge | set once from `options.inbox_capacity` in `IncrementalQueryServiceInner::new` |
| `triplox_incremental_output_depth_max` | gauge | `apply_batch`, from `RegisteredQuery::sender` |
| `triplox_incremental_delta_lag_seconds` | histogram | `serve`, after each step: now minus `batch.tx_key.system_time` |

### Server memory

| Name | Type | Source |
|---|---|---|
| `process_resident_memory_bytes` | gauge | process memory sampler (`sysinfo` `Process::memory`) |
| `process_virtual_memory_bytes` | gauge | process memory sampler (`Process::virtual_memory`) |
| `triplox_incremental_rss_budget_bytes` | gauge | `MAX_RSS_BYTES` in `src/incremental/circuit.rs:468`, set once if it is `Some` |

The `process_*` names are the standard Prometheus names, so existing dashboards can use them.

### Incremental query memory

`DBSPHandle::retrieve_profile` is synchronous. It blocks the calling thread until the pool thread answers, so a worker cannot call it on the shared Tokio runtime.

Fork change: add `pub async fn retrieve_profile_async(&mut self) -> Result<DbspProfile, DbspError>` to `DBSPHandle` in FiV0/feldera. Model it on `transaction_async` (`crates/dbsp/src/circuit/dbsp_handle.rs:1878`). Make the change in a branch that starts from the rev that Triplox pins (`1d81c0323e96`). Then bump the `dbsp` and `feldera-macros` rev in `Cargo.toml`.

In Triplox:

1. Add `fn stats(&mut self) -> impl Future<Output = Result<CircuitStats>> + Send` to the `Circuit` trait (`src/incremental/worker.rs:88`). `CircuitStats` has `used_bytes`, `storage_bytes` and `state_records`.
2. Implement it on `QueryCircuit` with `retrieve_profile_async`, then `total_used_bytes()`, `total_storage_size()` and `attribute_total_as_count(&STATE_RECORDS_COUNT)` on `DbspProfile`. Forward it from `SnapshotCircuit`. Implement it on the test circuits in `src/incremental/worker.rs` as zeros.
3. In `serve`, add a `tokio::time::interval` arm (15 s, first tick skipped) to the `select!`. On each tick, call `circuit.stats()`. A failed sample logs a warning and does not stop the query.
4. Each worker keeps its last reported `CircuitStats`. It adds the difference to the aggregate gauges with `gauge.increment(new - old)`. In `Worker::run`, before the circuit is dropped, it subtracts its last values. With this pattern the dispatcher needs no shared map.

| Name | Type | Source |
|---|---|---|
| `triplox_incremental_memory_used_bytes` | gauge | sum of `total_used_bytes` over workers |
| `triplox_incremental_storage_bytes` | gauge | sum of `total_storage_size` |
| `triplox_incremental_state_records` | gauge | sum of `STATE_RECORDS_COUNT` |
| `triplox_incremental_query_memory_bytes` | histogram | one sample per worker per tick |

The runtime pool and its buffer cache are shared by all circuits, so this does not assign them to one query.

### Build info

`triplox_build_info{version}` is set to 1 in `install`, with `env!("CARGO_PKG_VERSION")`.

### DBSP statics and pool stats (optional)

The DBSP fork keeps process-wide counters in static atomics: `dbsp::circuit::metrics` (step count, step latency, operator commit latency, files created and deleted, compaction stall time) and `feldera_storage::metrics` (storage read, write and sync latency). `RuntimePool::stats()` (`dbsp/src/circuit/runtime_pool.rs:210`) returns registered circuits, completed steps, queue time and execution time without blocking. Queue time shows how long circuits wait for a pool thread, which is a second lag signal.

None of these values hold memory bytes or state records. They do not replace the metrics above. A sampler task can copy them into the facade every few seconds with `counter!(...).absolute(v)`. The DBSP histograms are already bucketed, and the facade cannot take bucketed data, so they come out only as a sum and a count.

## Implementation order

1. Make the fork change and bump the rev. Make sure that `cargo build` passes.
2. Add the dependencies, `src/metrics.rs`, `MetricsConfig` and the `main.rs` wiring. Curl the endpoint to make sure that `triplox_build_info` and the process gauges show.
3. Add the query and transaction metrics, and the latest-tx gauges.
4. Add the incremental counters, the depth gauges and the lag histogram.
5. Add the circuit stats and the memory gauges.
6. Document the metrics and the `[metrics]` configuration in `README.md` or `DEVELOPMENT.md`.

## Verification

Tests: use `metrics_util::debugging::DebuggingRecorder` with `metrics::with_local_recorder`, and read the values from its `Snapshotter`. `with_local_recorder` is thread-local. Use it with synchronous code or with a current-thread runtime and no spawned tasks, or read values at the end of the closure.

1. `InFlightGuard`: the gauge goes back to 0 when the guard drops, also when a future that holds it is dropped early.
2. `Node::execute_tx` on a memory node: `triplox_tx_submitted_total{route="execute"}` is 1, `triplox_tx_indexed_total{outcome="committed"}` is 1, and `triplox_latest_indexed_tx_id` equals the returned tx id.
3. An aborted transaction increments `triplox_tx_indexed_total{outcome="aborted"}`.
4. The existing test `history_before_registration_basis_does_not_fill_inbox` and the lag tests in `src/incremental.rs` use `inbox_capacity: NonZeroUsize::MIN`. Extend one lag test to assert `triplox_incremental_queries_terminated_total{reason="lagged"}` is 1.
5. Register and unregister one incremental query. `triplox_incremental_queries_active` goes 1 then 0, and `triplox_incremental_memory_used_bytes` returns to 0.

Manual check:

1. Run `cargo run -- config/triplox.toml`.
2. Submit transactions and queries with the client, and open one subscription.
3. Run `curl -s localhost:9490/metrics` and make sure that every metric above shows, that histograms have `_bucket` lines, and that the memory gauges change after 15 s.

Before you finish, run `cargo fmt`, `cargo test --workspace` and `cargo clippy --workspace --all-targets --all-features`, and fix all warnings.
