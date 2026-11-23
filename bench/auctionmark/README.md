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
server, runs the Clojure driver, and tears down the processes and container. Logs,
configuration, JSON results, and local database files remain in
`target/auctionmark-sync/<timestamp>/`. `--output-dir` must name a new directory.
The default client timeout is 600 seconds, configurable with `--timeout`.

Triplox runs as a plain process with **no memory or CPU limit** from the launcher. The
JVM client has its own systemd scope with a two-core quota and 2 GiB memory limit; its
heap remains capped at 768 MiB. RustFS has a separate 0.25-core quota and 768 MiB memory
limit. The launcher's combined sampled RSS threshold is 75% of host RAM plus 2 GiB for
the client and 768 MiB for RustFS. Swapping is disabled in the client scope and the
RustFS container. Bucket creation uses a small short-lived `rc` container before the
server starts. The launcher requires the client scope to succeed. The direct Clojure
command only samples RSS; it does not install these operating-system limits.

DBSP's process RSS pressure threshold is 75% of the smaller of host RAM and the
tightest detected cgroup memory limit, including parent cgroups. With no limit on the
server, it is 75% of host RAM. This is a soft pressure threshold for the whole Triplox
process; nothing enforces a hard limit. The server log records the detected threshold
as `DBSP process RSS budget` in bytes.

The server uses eight pooled DBSP foreground workers, two merger workers, and a
**4 GiB shared cache**. This is a cache capacity, not preallocated memory or a cap
on total circuit state. The runtime comes from the `runtime-pooling` branch of
[FiV0/feldera](https://github.com/FiV0/feldera/tree/runtime-pooling), pinned at
`1d81c0323e96a0cbc9e50e584babc112e5cfdb16`. All incremental queries share this pool;
the benchmark sizes it explicitly:

```toml
[incremental]
threads = 8
merger_threads = 2
cache_mib = 4096
```

### Workload

The default fixture has 20 users and 100 items, generated from seed 42 using the
AuctionMark branch's schema, generators, and write procedures. Initial views are
populated with bids, watches, and unanswered questions. Every stage starts from the
same logical fixture. Generated identities and procedure pools reset between stages;
wall-clock timestamps and random entity UUIDs are not byte-for-byte reproducible.

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

The default stages are 5, 20, 50, and 100 subscriptions, at five committed
transactions/second, with 10 seconds of warmup and 30 seconds of measurement each.
The next stage runs only if the current stage has:

- p95 transaction-submission-to-client-applied latency below 250 ms;
- at least 90% of the requested transaction rate;
- no missing, unexpected, incorrect, or failed deliveries;
- combined sampled RSS within the memory budget.

A stage with no measured deliveries cannot pass. Five seconds of delivery backlog
aborts the stage. A failed run exits nonzero and retains its report. A passing smoke
run establishes only the tested counts for this fixture and machine, not a maximum
capacity. Larger runs require explicit `--mode capacity` and subscription stages,
with enough users (at least subscriptions/5) and items (at least users):

```bash
python3 bench/auctionmark/run-sync.py --timeout 600 -- \
  --mode capacity --stages 100,200 --users 40 --items 200 \
  --rate 5 --warmup 10 --duration 30
```

Do not run larger stages automatically after a failed smoke test. For a short
functional check, override `--stages 5 --warmup 1 --duration 3`; such a small sample
is not a freshness or capacity conclusion.

### What the measurements mean

One Java 21 virtual thread reads each HTTP subscription and another consumes its
deltas. The client opts into virtual readers; the JVM library's default reader
behavior is unchanged. OS threads are sampled separately because virtual threads
can still pin carrier threads in third-party blocking code.

Each subscription's primed result is checked against an ordinary query at its
registration tx-key and starts that subscription's expected history. Before
submitting a transaction, a small in-process reference model computes which query
results should change and their new results. After the response, the transaction
ID is added to the history of each changed result.

The server applies one SlateDB WAL batch of transactions at a time. One delta can
therefore cover several transactions and carries the tx-key of the last one. Changes
within a batch can cancel out, and a batch that leaves a result unchanged still sends
an empty delta. The client consumes every delta, empty ones included, and applies its
signed row weights. The tracker then compares the whole resulting bag with the
expected result as of the delta's tx-key. A change counts as delivered when its
subscription first reaches a tx-key at or after the transaction, so an absent
delivery cannot disappear from the latency statistics. A delta that arrives before
its transaction's acknowledgement is parked until the acknowledgement arrives.
Unchanged query/transaction pairs are counted separately. Warmup changes are checked
but excluded from latency and throughput samples.

At the end of a stage, the driver drains expected deliveries and compares every
client result with both its model and an ordinary database query. The timed write
path does not issue one verification query per subscription. Oracle computation,
procedure reads, HTTP encoding, and client scheduling still consume the same CPU
budget. Low achieved write rate or scheduling delays can therefore indicate a
client bottleneck rather than server capacity. Procedure reads happen before the
submission timestamp; measured latency starts immediately before the transaction
API call and includes its serialization and network time.

Text output gives the stage verdict, achieved transaction rate, p95, missing
updates, and combined RSS. JSON additionally includes initialization times,
p50/p95/p99/max latency overall and by query family, acknowledgement latency,
scheduling delay, fanout, unchanged pairs, final result sizes, errors, and per-process
CPU, peak RSS, OS threads, and file descriptors. RSS is sampled every 500 ms and
includes initialization and final verification, not just the measurement window.
It is not the cgroup memory accounting used for enforcement. Unavailable process
fields are null or have an error; remote/direct runs without server PIDs do not
pretend to measure server resources. Reports include revisions, configured limits,
and launcher metadata. Short runs and low-fanout families may have few samples;
inspect each percentile's count.

### WAL flush versus CDC polling

The **flush interval** schedules SlateDB's writes of buffered WAL data to object
storage. The **poll interval** schedules Triplox's incremental CDC reader checks
for newly visible WAL data. They are separate stages of delivery, followed by
circuit execution and client consumption. The 200 ms setting is CDC polling, not
a 200 ms write interval.

Memory/local storage use SlateDB's default 100 ms flush interval. Remote storage
defaults to 200 ms; this RustFS demo explicitly sets `wal_flush_interval_us=100`
to preserve its **100 microsecond** flush interval. Actual writes take additional
time. `src/incremental/cdc.rs` keeps its 200 ms polling interval. With a
250 ms freshness target, polling phase and object-store latency are material parts
of the result.

### Validation

The live-query integration tests cover insertions, updates, retractions, duplicate
rows, initially empty views, irrelevant writes, reconnect/priming, the existing
AuctionMark procedures, and fixture restoration. Tracker tests cover delivery
before acknowledgement, missing and incorrect deliveries, deltas covering several
transactions, changes that cancel within a batch, and unchanged transactions. A two-stage driver test checks measurement and restoration together.
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
