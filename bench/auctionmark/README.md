# AuctionMark

This directory holds a Triplox port of the [BenchBase](https://github.com/cmu-db/benchbase) AuctionMark benchmark. AuctionMark is an OLTP workload that models an online auction site. The benchmark runs on [benchy](https://github.com/FiV0/benchy).

A run has two stages:

1. The load stage installs the schema and generates users, items, bids and watches. Each load step is a timed setup stage.
2. The OLTP stage runs a weighted mix of procedures on a pool of worker threads. A background job runs `check-winning-bid` and `post-auction` every 10 seconds.

All workers share one connection, because `TriploxNode` is thread-safe.

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
