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

Requirements: Linux with a systemd user session, Docker, Python 3.11+, Java 21,
Clojure CLI, and Rust. Run these commands from the repository root. Builds use at
most two CPU cores and 8 GiB; do not run them alongside the benchmark.

```bash
systemd-run --user --scope --quiet -p CPUQuota=200% -p MemoryMax=8G \
  nice -n 10 cargo build --release --locked -j1 --bin triplox

(cd triplox-jvm && systemd-run --user --scope --quiet \
  -p CPUQuota=200% -p MemoryMax=2G \
  ./gradlew --no-daemon --max-workers=1 -Dorg.gradle.jvmargs=-Xmx768m \
  test publishMavenPublicationToMavenLocal \
  -PtriploxVersion=0.1.0-incremental-auctionmark-SNAPSHOT \
  -PsignAllPublications=false)

python3 bench/auctionmark/run-sync.py
```

`CARGO_TARGET_DIR` or `--server-binary /path/to/triplox` selects an existing release
binary. The launcher records its SHA-256 and checkout revision; rebuild after Rust
changes. It creates a fresh RustFS container and local transaction log, starts the
server, runs the Clojure driver, and tears down the processes and container. The
server configuration is [`server.toml`](server.toml); the launcher fills in its ports
and paths and writes the result to the output directory. Logs,
configuration, JSON results, and local database files remain in
`target/auctionmark-sync/<timestamp>/`. `--output-dir` must name a new directory.
The default client timeout is 600 seconds, configurable with `--timeout`.

Launcher options come first; arguments after `--` go to the Clojure driver.

| Option | Default | Meaning |
|---|---|---|
| `--server-binary PATH` | `$CARGO_TARGET_DIR/release/triplox` | Triplox release binary |
| `--output-dir DIR` | `target/auctionmark-sync/<timestamp>` | New directory for artifacts |
| `--timeout SECONDS` | 600 | Client timeout |
| `-- --users N` | 200 | Fixture users |
| `-- --items N` | 1000 | Fixture items |
| `-- --subscriptions N` | 1000 | Live queries, five per user |
| `-- --seed N` | 42 | Workload seed |
| `-- --rate N` | 5 | Target committed transactions/second |
| `-- --warmup SECONDS` | 10 | Writes before measurement |
| `-- --duration SECONDS` | 300 | Measured writes |
| `-- --probe-ms MS` | 500 | Interval between catch-up probes |

The launcher sets `--host`, `--port` and `--output` itself and overrides any values
passed after `--`.

Triplox runs as a plain process with **no memory or CPU limit** from the launcher. The
JVM client has its own systemd scope with a two-core quota and 2 GiB memory limit; its
heap remains capped at 768 MiB. RustFS has a separate 0.25-core quota and 768 MiB memory
limit. The launcher's combined memory threshold is 75% of host RAM plus 2 GiB for
the client and 768 MiB for RustFS. Swapping is disabled in the client scope and the
RustFS container. Bucket creation uses a small short-lived `rc` container before the
server starts. The launcher requires the client scope to succeed. The direct Clojure
command neither measures resources nor installs these operating-system limits.

DBSP's process RSS pressure threshold is 75% of the smaller of host RAM and the
cgroup memory limit reported by sysinfo. With no limit on the
server, it is 75% of host RAM. This is a soft pressure threshold for the whole Triplox
process; nothing enforces a hard limit. The server log records the detected threshold
as `DBSP process RSS budget` in bytes.

The server uses eight pooled DBSP foreground workers, two merger workers, and a
**4 GiB shared cache**. This is a cache capacity, not preallocated memory or a cap
on total circuit state. The runtime comes from the `runtime-pooling` branch of
[FiV0/feldera](https://github.com/FiV0/feldera/tree/runtime-pooling), pinned at
`1d81c0323e96a0cbc9e50e584babc112e5cfdb16`. All incremental queries share this pool;
the benchmark sizes it explicitly in [`server.toml`](server.toml):

```toml
[incremental]
threads = 8
merger_threads = 2
cache_mib = 4096
```

### Workload

The default fixture has 200 users and 1,000 items, generated from seed 42 using the
AuctionMark branch's schema, generators, and write procedures. Initial views are
populated with bids, watches, and unanswered questions. The fixture needs a fresh
database. Wall-clock timestamps and random entity UUIDs are not byte-for-byte
reproducible.

| Query | Parameter | Result |
|---|---|---|
| Item detail | Item ID | Name, description, price, bid count, status |
| Watchlist | User ID | Watched items and current prices/statuses |
| Seller listings | Seller ID | Open items, prices, bid counts |
| Buyer bids | Buyer ID | Bids on currently open items |
| Seller questions | Seller ID | Unanswered item questions |

Each group of five subscriptions belongs to another user. Item-detail views use a
separate item per user. These are distinct, selective queries, with constants
embedded in Datalog because incremental query arguments are not supported by the
server. Results preserve row multiplicities; duplicate watch rows are meaningful.
Views are unpaginated, so fixture size and result sizes must accompany any claim.

Operation choices are 50% bids, 15% new listings, 15% watch changes, 10% questions
or answers, and 10% auction status transitions. This is an operation mix: the
existing new-item procedure writes twice, and some procedures can do nothing.
The rate limiter acts on **actual submitted transactions**, including both writes
of a multi-transaction procedure. It adds seeded 10% pacing jitter to avoid aligning
all writes with the CDC polling period. It does not generate catch-up bursts.

A run opens `--subscriptions` live queries (default 1,000, five for each of the 200
users), then writes at five committed transactions/second: 10 seconds of warmup
(`--warmup`), then 300 seconds of measurement (`--duration`). With the defaults, every
user has live queries, so most writes change at least one subscription. The run passes only if it has:

- at least one catch-up probe (see below);
- at least 90% of the requested transaction rate;
- correct primed and final results, and no failed subscriptions;
- combined peak memory within the launcher's threshold (launcher runs only).

A run with no probes cannot pass. Probes and the final catch-up have no timeout: however
late the subscriptions are, the driver waits for them and records the time, and only
the launcher's `--timeout` bounds the whole run. A failed run exits nonzero and retains its report. A
passing run establishes only the tested count for this fixture and machine, not a
maximum capacity. Subscriptions need at least subscriptions/5 users and at least as
many items as users:

```bash
python3 bench/auctionmark/run-sync.py --timeout 600 -- \
  --users 400 --items 2000 --subscriptions 2000 \
  --rate 5 --warmup 10 --duration 300
```

For a short functional check, use `--users 20 --items 100 --subscriptions 100
--warmup 1 --duration 3`; such a small sample is not a freshness or capacity
conclusion.

### Scaling

A transaction usually touches one or two users, such as the buyer and the seller. It
changes only their queries, so it changes about 2 queries at any scale. The changed
fraction is therefore about 1 / (2 × users). With 20 users, that is about 2.5%; with
the default 200 users, it is about 0.25%. Subscription and item counts have little
effect.

For larger runs, use users = queries / 5 and items = 5 × users, at five transactions
per second. Two workloads measure different costs:

- **A. Per-user** (current): each transaction changes about 2 queries. This measures
  the cost of idle queries, because every WAL batch sends a delta to every
  subscription.
- **B. Hot items** (not implemented): 4 hot items that never close get half of all
  bids. Each user watches one hot item, and item-detail queries point at hot items.
  A transaction changes about 2.5% of the queries at every scale. This measures the
  cost of many non-empty deltas.

| Queries | Users | Items | Changed per tx, A | Changed per tx, B | Watchers per hot item |
|---|---|---|---|---|---|
| 1,000 | 200 | 1,000 | ~2 (0.2%) | ~25 | 50 |
| 10,000 | 2,000 | 10,000 | ~2 (0.02%) | ~250 | 500 |
| 100,000 | 20,000 | 100,000 | ~2 (0.002%) | ~2,500 | 5,000 |

These numbers are estimates from the workload code, not measurements.

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
# In a separate terminal, add the incremental section above to a dev config:
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

These runs used 1,000 subscriptions, 5 transactions a second, 30 minutes of writes and 2 merger threads:

| Flush interval | RustFS cores | Result | Catch-up p50 | Catch-up p95 |
|---|---|---|---|---|
| 100 microseconds | 0.25 | Failed, 801 queries fell behind | 3.4 s | 12.9 s |
| 500 ms | 0.25 | Passed | 2.3 s | 4.9 s |
| 100 ms | 2 | Passed | 0.8 s | 1.4 s |

Do not make the flush interval small without checking RustFS first. During a run, look at the RustFS CPU in `containers.jsonl` and at `retry.rs` lines in `server.log`. If RustFS sits at its CPU quota, raise the quota or the flush interval. These numbers come from one 8-core machine.
