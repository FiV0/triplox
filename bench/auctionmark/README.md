# AuctionMark

This directory holds a Triplox port of the [BenchBase](https://github.com/cmu-db/benchbase) AuctionMark benchmark. AuctionMark is an OLTP workload that models an online auction site. The benchmark runs on [benchy](https://github.com/FiV0/benchy).

A run has two stages:

1. The load stage installs the schema and generates users, items, bids and watches. Each load step is a timed setup stage.
2. The OLTP stage runs a weighted mix of procedures on a pool of worker threads. A background job runs `check-winning-bid` and `post-auction` every 10 seconds.

All workers share one connection, because `TriploxNode` is thread-safe.

[Live-query workload](#live-query-workload) describes a separate workload for incremental queries (`clojure -M:sync`).

## Procedures

| Procedure | Weight |
|---|---|
| `get-item` | 40 |
| `new-bid` | 18 |
| `new-item` | 10 |
| `get-user-info` | 10 |
| `new-user` | 5 |
| `get-watched-items` | 5 |
| `new-feedback` | 3 |
| `new-purchase` | 2 |
| `new-comment` | 2 |
| `update-item` | 2 |
| `get-comment` | 2 |
| `new-comment-response` | 1 |

## Run with Docker Compose

Docker Compose starts RustFS, a Triplox server with remote storage, and the benchmark:

```bash
cd bench/auctionmark
docker compose up --build
```

The `benchmark` service reads `SCALE_FACTOR`, `THREADS` and `DURATION` from its environment.

## Run against an existing server

```bash
cd bench/auctionmark
clojure -M:run --host localhost --port 5490 --scale-factor 0.01 --threads 8 --duration 60
```

The scale factor sets the data size. A scale factor of 1 loads 1,000,000 users and 10,000,000 items. `--seed` sets the random seed. `--report PATH` appends the benchy events and the summary to PATH as JSON Lines. `--help` shows all options.

At the end of a run, the benchmark prints the status, the throughput and a table with latency percentiles for each procedure.

## Tests

The tests run against a Triplox dev node. A dev node gives each connection its own in-memory database.

```bash
docker run -p 5490:5490 -e TRIPLOX_STORAGE=dev ghcr.io/fiv0/triplox:0.1.0-alpha.8
cd bench/auctionmark
clojure -X:test
```

To use a different server, set `-Dtriplox.host` and `-Dtriplox.port`.

## Live-query workload

This experiment measures how many independent incremental queries Triplox can keep
fresh while AuctionMark transactions change their results. It models the small,
parameterized views used by syncing frameworks. It is not a traditional OLTP QPS
comparison or a full AuctionMark implementation.

`clojure -M:sync` runs the live-query workload. Use the disposable launcher below for a reproducible local run.

### Build and run

From the repository root:

```bash
cargo build --release --bin triplox
(cd triplox-jvm && ./gradlew publishMavenPublicationToMavenLocal \
  -PtriploxVersion=0.1.0-incremental-auctionmark-SNAPSHOT -PsignAllPublications=false)
python3 bench/auctionmark/run-sync.py
```

The launcher starts RustFS and a Triplox server with [`server.toml`](server.toml),
runs the driver and writes results to `target/auctionmark-sync/<timestamp>/`.
Arguments after `--` go to the Clojure driver.

### Workload

The driver loads a fixture (default 200 users, 1,000 items, seed 42) into a fresh
database and opens five live queries per user: item detail, watchlist, seller
listings, buyer bids and seller questions. It then writes AuctionMark transactions
at `--rate` (default 5/s) for `--warmup` + `--duration` seconds. The run fails if
results diverge from ordinary queries, a subscription fails, the achieved rate is
below 90% or no probe was taken. The queries and operation mix are in
[`live.clj`](src/auctionmark/live.clj), the driver is
[`sync.clj`](src/auctionmark/sync.clj), and `--help` lists the options.

### Scaling

Scale with users = queries / 5 and items = 5 × users. **A** is the current workload:
a transaction touches the buyer's and seller's queries only, so it changes about 2
queries at any scale. **B** is an unimplemented hot-item variant in which 4 hot
items get half of all bids and every user watches one. The numbers are estimates,
not measurements.

| Queries | Users | Items | Changed per tx, A | Changed per tx, B | Watchers per hot item |
|---|---|---|---|---|---|
| 1,000 | 200 | 1,000 | ~2 (0.2%) | ~25 | 50 |
| 10,000 | 2,000 | 10,000 | ~2 (0.02%) | ~250 | 500 |
| 100,000 | 20,000 | 100,000 | ~2 (0.002%) | ~2,500 | 5,000 |

### What the measurements mean

One Java 21 virtual thread reads each HTTP subscription and another consumes its
deltas. The client opts into virtual readers; the JVM library's default reader
behavior is unchanged. OS threads are sampled separately because virtual threads
can still pin carrier threads in third-party blocking code.

Result rows are checked against ordinary database queries twice, outside the timed
window. Each subscription's primed result must equal the query at its registration
tx-key. At the end of the run, the driver waits until every subscription has reached
the last transaction's tx-key, stops the consumers and compares every client result
with the query at the tx-key its subscription has reached. Deltas are not checked
individually while the run is in progress.

The server applies one SlateDB WAL batch of transactions at a time. One delta can
therefore cover several transactions and carries the tx-key of the last one; a batch
that leaves a result unchanged still sends an empty delta. Each consumer applies every
delta, empty ones included, and then publishes the tx-id it has reached.

Freshness is measured with probes. During the measurement window, the first transaction
committed at least `--probe-ms` (500 ms) after the previous probe becomes a probe. A
probe polls every millisecond until every subscription has reached the transaction's
tx-id, and records the time since the transaction was submitted. This is the catch-up
time of the slowest subscription. It starts immediately before the transaction API
call, so it includes serialization, network time and the wait for the acknowledgement.
The final catch-up, from the last transaction's submission until every subscription
has reached it, is recorded the same way as `final-catch-up-ms`. No other
per-subscription or per-delivery latency is tracked.

The timed write path does not issue verification queries. Procedure reads, HTTP
encoding, and client scheduling still consume the same CPU budget. Low achieved write
rate can therefore indicate a client bottleneck rather than server capacity.

Text output gives the verdict, achieved transaction rate, catch-up p95, max and
probe count, and final catch-up time. JSON additionally includes initialization times,
p50/p95/p99/max catch-up time, final result sizes and errors. Reports include
revisions, configured limits, and launcher metadata.

The launcher measures resources from outside the measured processes, as the JOB
benchmark does. Every 500 ms it reads `/proc` for the server and client process
trees: RSS, CPU time, OS threads and open file descriptors, summed over each tree.
About once a second it records `docker stats` for the RustFS container, which reports
the container's cgroup memory and CPU. Samples go to `processes.jsonl` and
`containers.jsonl`. After the client exits, the launcher writes peaks and average CPU
to `resources.json` and adds them to `report.json` as `resources`. Sampling covers
initialization and final verification, not just the measurement window.

The combined peak is the largest summed server and client RSS of one sample, plus the
RustFS peak. If it exceeds the threshold, the launcher marks the report as failed with
a `memory` check and exits nonzero. The check runs after the client exits, so it does
not stop a run early. Process RSS is not the cgroup accounting that limits enforce. A 300 second measurement has about 500 probes; inspect the count
before reading the tail percentiles.

### WAL flush versus CDC polling

The **flush interval** schedules SlateDB's writes of buffered WAL data to object
storage. The **poll interval** schedules Triplox's incremental CDC reader checks
for newly visible WAL data. They are separate stages of delivery, followed by
circuit execution and client consumption. The 200 ms setting is CDC polling, not
a 200 ms write interval.

Memory/local storage use SlateDB's default 100 ms flush interval. Remote storage
defaults to 200 ms. This RustFS demo sets `wal_flush_interval_us=500000`, a 500 ms
flush interval. A short interval writes one small WAL object per transaction. The
throttled RustFS container cannot list that many objects, and requests time out
after about 20 minutes. Actual writes take additional time. `src/incremental/cdc.rs` keeps its 200 ms polling interval, so polling phase
and object-store latency are material parts of the measured catch-up time.

### Validation

The live-query integration tests cover insertions, updates, retractions, duplicate
rows, initially empty views, irrelevant writes, reconnect/priming, and the existing
AuctionMark procedures. A driver test runs a short benchmark and checks its probes.
Use a pooled dev server and the locally published JVM artifact:

```bash
# In a separate terminal, add the [incremental] section of server.toml to a dev config:
cargo run -- config/triplox-dev.toml
# Then:
(cd bench/auctionmark && clojure -X:test)
```

The runtime has tests for pooled worker sharing, independent subscription
retirement, and error propagation. Ordinary query errors retire their subscription;
a panic in a shared pool can affect other subscriptions in that pool. This demo
reports failures rather than treating a restarted subscription as uninterrupted.

### Flush interval pressure

A short flush interval makes results fresher. SlateDB writes buffered WAL data to RustFS sooner, so the server sees each transaction sooner. The cost is more and smaller WAL objects, and that puts pressure on two places.

RustFS pressure: each flush writes one WAL object. The server finds new WAL data by listing the `triplox/wal/` prefix. RustFS reads the metadata of every live object for each listing, so a listing gets slower as the WAL grows. SlateDB deletes old WAL objects only after it flushes data to level 0. A 30 minute run held 1,500 to 4,000 live WAL objects. With a 100 microsecond interval and the 0.25-core RustFS quota, listings took more than 5 seconds. Then they timed out with HTTP 500, and the server stopped making progress.

Inbox pressure: every query has an inbox of 256 batches (`inbox_capacity` in `src/incremental.rs`). The server sends each WAL batch to the inbox of every query. A query that falls 256 batches behind ends with `Incremental query fell behind`. A shorter interval makes more batches each second, so the inbox fills sooner. At about 5 batches a second, 256 batches is about 51 seconds of lag. At about 2 batches a second, it is about 128 seconds.

These runs used 1,000 subscriptions, 5 transactions a second and 30 minutes of writes. The merger threads are the DBSP threads that merge query state in the background:

| Flush interval | Merger threads | RustFS cores | Result | Catch-up p50 | p95 | p99 | max |
|---|---|---|---|---|---|---|---|
| 100 microseconds | 2 | 0.25 | Failed, 801 queries fell behind | 3.4 s | 12.9 s | 45.5 s | 87.3 s |
| 500 ms | 2 | 0.25 | Passed | 2.3 s | 4.9 s | 12.9 s | 28.8 s |
| 500 ms | 6 | 0.25 | Passed | 2.4 s | 4.7 s | 16.9 s | 30.6 s |
| 100 ms | 2 | 2 | Passed | 0.8 s | 1.4 s | 1.7 s | 4.5 s |
| 500 ms | 6 | 2 | Passed | 1.0 s | 1.6 s | 1.8 s | 2.2 s |

The RustFS CPU quota has the largest effect on lag. With 0.25 cores, RustFS ran at its quota for the whole run, and p95 was about 5 s with a long tail. With 2 cores, RustFS used about half of its quota, and p95 was about 1.5 s. Six merger threads instead of two did not change the lag. The 100 ms and 500 ms intervals behaved alike with 2 cores. The run that failed used both the 100 microsecond interval and the 0.25-core quota. We did not test 100 microseconds with 2 cores.

Do not make the flush interval small without checking RustFS first. During a run, look at the RustFS CPU in `containers.jsonl` and at `retry.rs` lines in `server.log`. If RustFS sits at its CPU quota, raise the quota first. If it still sits at the quota, raise the flush interval. Each row is one run on one 8-core machine, so small differences are not reliable.
