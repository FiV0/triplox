# Incremental query initialization cost

Measurements from September 26, 2026 show that repeated database scans dominate
AuctionMark subscription registration. The benchmark's explicit wait for the
first result contributes almost nothing: the server has already computed that
result before `subscribe` returns.

## Setup

Each diagnostic run loaded a fresh fixture with seed 42, then opened subscriptions
sequentially and validated their initial results. All subscriptions stayed open
until registration completed. The registration-only probes performed no workload
writes.

- Java 26.0.2 and the JVM client JAR used in the September 21 capacity runs.
- Server and client: 1.75 CPU cores, 7,424 MiB memory, no swap.
- MinIO: 0.25 CPU cores and 768 MiB memory.
- Pooled DBSP: two foreground workers, one merger, 4,096 MiB cache.
- Instrumented source: `c9db34cf4`, with its automatic RSS budget.
- Original capacity-test binary: SHA-256
  `be0f39d5b49811d16e29f65133bfbba6599163f5f17d2291f084fefeb868d14e`,
  using the launcher from `3c6500326` and its original configuration.

Temporary timers separated client calls and server registration stages. The
instrumentation was removed after measurement and the normal release binary
rebuilt.

## Client measurements

All times below are totals across the indicated subscriptions.

| Server | Subscriptions | Users / items | Overall registration | Inside `subscribe` | Explicit first-result waits |
|---|---:|---|---:|---:|---:|
| Original capacity-test binary | 1,000 | 200 / 1,000 | 34.583 s | 34.333 s | 15.509 ms |
| Instrumented current branch | 1,000 | 200 / 1,000 | 30.758 s | 30.580 s | 18.230 ms |
| Instrumented current branch, larger fixture | 100 | 2,000 / 10,000 | 32.610 s | 32.471 s | 5.391 ms |

All three probes validated every initial result and reported no consumer errors.
On the instrumented 1,000-query run, reference-model computation took another
119 ms total. The remaining client time includes result validation, consumer
startup, and bookkeeping.

## Server measurements

These stages occur inside the client's `subscribe` call; their times should not
be added to the client totals above.

| Stage | 1,000 subscriptions, small fixture | 100 subscriptions, larger fixture |
|---|---:|---:|
| Scan and prepare initial triples | **24.721 s** | **28.325 s** |
| Construct circuits | 0.862 s | 0.105 s |
| Execute initial batches | 3.968 s | 3.864 s |
| Wait for registration lock | 0.301 ms | 12.635 ms |
| EAV entries visited per subscription | 36,291 | 359,828 |

For the small fixture, scanning accounts for approximately 80% of overall
registration time, initial execution for 13%, and circuit construction for 3%.
The explicit first-result waits account for about 0.06%.

[`scan_current_triples`](../../src/incremental/cdc.rs) traverses the entire EAV
range for every query. It decodes entries, filters by the query's attributes,
reconstructs current triples from stored versions, and sorts the selected triples.
Query constants do not narrow this scan. The scan timer includes decoding and
hash-table work, so it does not measure disk I/O alone.

The small-fixture run visited **36,291,000 EAV entries** across 1,000 registrations
and produced 4,759,600 input triples across their independent initial batches.
The initial query results contained only 2,199 rows in total. EAV entry counts
include stored versions; they are not counts of distinct current facts.

Increasing the fixture by roughly tenfold raised average scan time from
**24.72 ms to 283.25 ms per query**. The larger-fixture diagnostic opened only
100 subscriptions; it was not a 10,000-subscription capacity attempt.

## Why waiting for the first result is cheap

Registration follows this sequence:

1. [`open-subscriptions!`](src/auctionmark/sync.clj) calls `tc/subscribe`.
2. The [HTTP handler](../../src/server.rs) awaits server registration before
   sending the subscription's opening frame.
3. [`register_query`](../../src/incremental.rs) acquires the registration lock,
   plans the query, scans its initial input, and awaits circuit initialization.
4. Initialization constructs the circuit, applies the initial triples, and queues
   any nonempty initial result before returning.
5. The client receives the opening frame and returns from `subscribe`. The
   benchmark computes its expected result and calls `tc/take!` to consume the
   initial delta, which is normally already available.

The `10000` argument to `tc/take!` is a maximum timeout, not a fixed delay. Removing
that call would not remove scanning or initial execution: both have already
completed before `subscribe` returns. Initial-result computation has a measurable
cost; the additional client wait does not.

Parallel client requests would still encounter the server's registration lock,
which covers scanning and initialization. The service also processes registration
commands serially. Lock contention was negligible in these sequential runs; the
cost is the work performed while registration is serialized. The initial result
does not wait for the 200 ms CDC polling interval.

## Optimization direction and limits

The first optimization to investigate is reducing or reusing initial scans:
attribute-index scans could avoid unrelated attributes, and registrations at a
common basis could potentially share snapshot preparation. Any constant-based
narrowing must preserve join correctness for later updates and the registration
handoff to CDC. These optimizations were not implemented in this investigation.

Each configuration was measured once. Instrumentation adds overhead, and the
current source differs from the original binary. These measurements attribute
registration cost; they do not establish a performance change between revisions
or sustainable ingestion capacity.

Local diagnostic artifacts are under
[`target/auctionmark-sync/registration-20260926/`](../../target/auctionmark-sync/registration-20260926/):
`summary.json`, `FINDINGS.md`, per-run server logs and client timings,
`instrumentation.patch`, and the probe scripts. This directory is ignored by Git.
Use `report.json.registration.json` for registration measurements; the probe's
ordinary `report.json` contains compatibility placeholders for throughput and RSS.

`FINDINGS.md` also records two excluded diagnostic attempts: a run that used the
ordinary workload driver, and an old-binary launch rejected for an incompatible
configuration field. The measurements above use the completed registration-only
probes. Benchmark processes and disposable MinIO containers were cleaned up.
