# Incremental Query Hardening Report

Date: 2026-08-25

Benchmark tree: `22b4809c6`

## Executive summary

Triplox incremental queries remained correct with 1,000 healthy subscriptions,
but the service is not operationally safe at that scale yet.

The main findings are:

- CDC processing scales almost exactly linearly with the number of
  subscriptions, including for transactions that cannot affect any query.
- One slow subscriber can stop the shared subscription service and CDC loop for
  every subscriber.
- 1,000 simple subscriptions use about 1.2 GiB RSS, 3,000 server threads, and
  5,000 file descriptors.
- One Rust `ClientNode` stalls at 200 long-lived subscriptions because it keeps
  one HTTP/2 connection saturated. The Java and Clojure clients automatically
  open more connections.
- Registration, runtime failures, and disconnect cleanup have service-wide
  latency or failure implications.

This is consistent with the website describing incremental queries as
experimental and calling for heavy-workload testing. See the
[incremental query overview](https://triplox.xyz/incremental-queries/overview/).

## Test setup

The tests used a locked optimized release build from the benchmark tree with
DBSP/Feldera 0.337. The machine had 8 logical CPUs and 30.9 GiB RAM.

The server used the in-memory transaction log and storage. This excludes
persistent-storage latency and therefore makes the results optimistic. The
server's file descriptor limit was raised to 65,536 for the 1,000-subscription
tests. The normal shell limit was 1,024.

The primary query was intentionally trivial and identical across subscribers:

```clojure
[:find ?v :where [?e :load/hot ?v]]
```

This isolates subscription orchestration from query complexity and exposes the
current lack of identical-query sharing. More complex and heterogeneous queries
would add work on top of the overhead measured here.

Fire-and-forget transaction submission was used where appropriate so that log
acceptance time could be compared with incremental CDC catch-up time. Every
healthy-consumer test checked the final transaction ID and exact delta count.

## Results

### All three clients at 1,000 subscriptions

Each client received the correct transaction from all 1,000 subscriptions.

| Client API | Connection objects | Registration | One update delivered to all |
|---|---:|---:|---:|
| Rust | 6 `ClientNode`s | 2.387 s | 328 ms |
| Java | 1 `TriploxNode` | 2.423 s | 236 ms |
| Clojure | 1 connection | 2.256 s | 199 ms |

Java and Clojure used about 1,015 client-side threads because the JVM
implementation creates one reader thread per subscription. OkHttp automatically
opened six TCP connections.

The Rust client required six explicit `ClientNode`s. With one `ClientNode`,
registration reached exactly 200 subscriptions and subscription 201 remained
pending for more than 12 seconds. Hyper defaults to 200 concurrent HTTP/2
streams, and Triplox does not override that default. Java and Clojure work around
the limit because OkHttp opens additional connections; reqwest did not do so in
this test.

Filling five Rust connections with exactly 200 subscriptions each also starves
unary transaction requests made through those clients. A connection strategy
that only adds clients until all are full does not reserve control-plane
capacity.

### Matching transaction fan-out

With 1,000 Rust subscriptions and 100 matching transactions:

- Transaction submission took 27.8 ms.
- The expected 100,000 deltas were received.
- No subscription reported the wrong final transaction.
- Full catch-up took 5.409 seconds.
- p50 lag was 2.856 seconds.
- p95 lag was 5.166 seconds.
- Maximum lag was 5.407 seconds.

Correctness held, but each transaction was serially stepped through 1,000 DBSP
circuits.

### Irrelevant ingestion

The test ingested 200 transactions of 50 rows each, for 10,000 rows on an
attribute that none of the subscriptions referenced, followed by one matching
sentinel transaction.

| Subscriptions | Ingestion time | Sentinel lag |
|---:|---:|---:|
| 100 | 42.5 ms | 2.138 s |
| 1,000 | 46.3 ms | 22.195 s |

The 10.4x latency increase for a 10x subscription increase is nearly linear.
Ingestion stayed fast while CDC debt accumulated: the log accepted the full
1,000-subscription workload in 46 ms, but incremental processing remained behind
for more than 22 seconds.

This demonstrates that CDC can fall substantially behind even when every
transaction is irrelevant to every registered query.

### Slow subscriber isolation

The slow-consumer test created one unread subscription and one healthy reader,
then submitted 600 updates with 8 KiB values.

- Submission took 59 ms.
- The healthy reader stopped at update 469.
- It made no progress during a ten-second observation window.
- Dropping only the unread subscription let the healthy reader reach all 600
  updates in 43 ms.

One slow HTTP consumer can therefore stall the shared service and CDC loop
indefinitely. This is global head-of-line blocking, not only aggregate CPU lag.

### Registration and priming

After inserting 10,000 rows on an irrelevant attribute, registering 1,000
subscriptions took 10.913 seconds, or 10.9 ms per subscription.

Registration scans the full EAV range and filters attributes afterward. It also
holds the same gate CDC needs from snapshot selection through scan and circuit
registration. A burst of registrations against a populated database can pause
live CDC processing for roughly the duration of the registration convoy.

### Resource footprint

At 1,000 trivial subscriptions:

| Resource | Idle server | 1,000 subscriptions | Approximate per subscription |
|---|---:|---:|---:|
| RSS | 16.7 MiB | 1.19 GiB | 1.17 MiB |
| Virtual memory | 499 MiB | 11.3 GiB | 11.1 MiB |
| Threads | 10 | 3,010 | 3 |
| File descriptors | 10 | 5,010 | 5 |

The normal descriptor limit of 1,024 cannot support this architecture. Around
200 subscriptions already approach exhaustion.

Each query gets a separate one-worker, file-backed DBSP runtime in
[`src/incremental/circuit.rs`](src/incremental/circuit.rs). The three server
threads per subscription are approximately one worker, one merger, and one RSS
monitor.

## Profile attribution

A symbolized Samply profile was captured from the current release build while
running the 1,000-subscription irrelevant-ingestion workload. The profile was
stored locally as:

```text
/tmp/triplox-incremental-cold-1000-eca30e8d.json.gz
```

The filename contains the transient local commit name used while the dependency
upgrade was being finalized. Its tree was verified byte-for-byte equivalent to
the benchmark tree named at the top of this report. The artifact is not checked
into the repository.

The profiled run took 24.776 seconds to catch up, compared with 22.195 seconds
without sampling. The unprofiled result is used as the primary latency number.

At 99 Hz, the profile captured 3,069 on-CPU samples:

- 47.6% were on the single `triplox-incremental-query` service thread.
- 47.3% were on the 1,000 DBSP worker threads.
- 4.2% were on Tokio server and CDC workers.

After excluding circuit registration, inclusive service-thread attribution was:

- `QueryCircuit::apply`: 46.6%
- DBSP transaction coordination: 24.1%
- Cloning transaction input: at least 20.5%

The percentages are inclusive and overlap, but they identify per-query input
cloning and circuit stepping as the dominant work. WAL retrieval and HTTP
decoding were not the main bottlenecks in this workload.

## Current architecture and failure boundaries

The relevant flow is:

```text
SlateDB WAL / CDC
       |
       v
single CDC task
       |
 registration gate
       |
       v
single incremental-service OS thread
       |
       +-- clone batch -> query 1 DBSP transaction -> bounded output
       +-- clone batch -> query 2 DBSP transaction -> bounded output
       ...
       +-- clone batch -> query N DBSP transaction -> bounded output
```

The service thread and command loop are in
[`src/incremental.rs`](src/incremental.rs). `apply_triples` iterates the query
`HashMap`, clones the whole input batch for each query, steps its circuit, and
possibly sends a delta before moving to the next query.

Each query creates its own DBSP runtime using `CircuitConfig::with_workers(1)` in
[`src/incremental/circuit.rs`](src/incremental/circuit.rs).

The CDC task in [`src/incremental/cdc.rs`](src/incremental/cdc.rs) processes one
WAL transaction at a time, fetches the schema, decodes the complete transaction,
takes the registration gate, and awaits the shared service.

The service-to-HTTP channel has capacity 128. The HTTP response body reads and
writes one delta at a time in [`src/server.rs`](src/server.rs), intentionally
propagating HTTP/2 flow control back to that channel. The Rust and JVM clients
also have bounded queues of 128 in
[`triplox-client/src/subscription.rs`](triplox-client/src/subscription.rs) and
[`triplox-jvm/src/main/java/xyz/triplox/client/Subscription.java`](triplox-jvm/src/main/java/xyz/triplox/client/Subscription.java).

This bounded design prevents unlimited output buffering, but it currently
propagates one consumer's backpressure into the shared evaluation and CDC path.

### Registration failure

A circuit build or priming failure is isolated to the attempted registration.
The query is not installed, existing subscriptions continue, and registration
returns an error. A newly created `query-N` storage directory can remain after a
failed build.

### Live update failure

A later `QueryCircuit::apply` failure is service-wide. `apply_triples` returns
early, the shared CDC task exits through `?`, and all subscriptions stop
receiving future deltas. Existing HTTP streams can remain open without a
terminal error frame. There is no automatic CDC restart or per-query isolation.

A direct panic on the `triplox-incremental-query` thread is more severe: the
command receiver disappears, later operations report `Incremental query service
stopped`, and all registered DBSP handles are dropped.

### Cursor and lifecycle behavior

The shared CDC stream is currently constructed from `CdcCursor::default()`.
Per-query cursor fields are updated as bookkeeping but do not drive shared CDC
restart or durable recovery.

The HTTP body retains the delta receiver, not an unregister guard containing the
query handle. Closed subscriptions are discovered when the service next
processes register, unregister, or apply work. If CDC has stopped and no later
command arrives, cleanup need not run promptly.

Subscriptions can currently start only at the latest indexed basis. Clients
cannot reconnect from a known transaction key after lag, network failure, or
server restart.

## Hardening priorities

### 1. Isolate delivery backpressure

This is the highest-priority safety issue. CDC and query evaluation must never
await client I/O.

Recommended contract:

- Publish evaluated deltas into independent per-subscription mailboxes without
  blocking the shared service.
- When a mailbox is full, disconnect or mark only that subscriber as lagged.
- Send a terminal `lagged` error containing the last delivered transaction and
  the required resubscription point.
- Allow healthy subscribers and the shared CDC cursor to continue.
- Make mailbox capacity and overflow policy configurable.
- Measure queue utilization and slow-subscriber termination.

Evaluation and network egress should be separate scheduling domains. A bounded
mailbox is still appropriate, but its overflow must be handled locally.

### 2. Share identical queries and route transactions by relevance

All 1,000 benchmark subscriptions were identical, but Triplox created 1,000
circuits.

The first optimization should be to canonicalize `(query, arguments, schema
version)`, keep one circuit per unique definition, and fan its deltas out to
multiple subscriber sinks.

For distinct queries:

- Index registered queries by referenced attributes and query constants.
- Skip circuit stepping when a transaction cannot affect a query.
- Share base arrangements or DBSP runtimes where possible.
- Pass transaction input using shared ownership instead of cloning nested
  vectors for every query.
- Shard unique-query execution over a bounded worker pool instead of creating
  three OS threads per subscription.
- Consider batching scheduler work while preserving the current per-transaction
  delta and ordering contract.

The irrelevant-ingestion result suggests relevance routing alone could remove
most of the measured 22-second backlog.

### 3. Remove registration from the CDC critical section

Registration currently holds the shared gate across snapshot acquisition, full
scan, circuit construction, and installation.

Registration should instead:

1. Capture a basis and CDC cursor.
2. Scan and build outside the CDC gate.
3. Atomically install the query.
4. Replay the bounded gap from the captured cursor.

Additional work:

- Use AVE/AEV scans and query constants rather than scanning all EAV data.
- Share one priming scan among equivalent simultaneous registrations.
- Add registration concurrency and resource admission limits.
- Add registration timeouts and cancellation.

### 4. Add per-query failure isolation and CDC supervision

A single live circuit failure must not terminate the shared service.

Required behavior:

- Quarantine and remove only the failing query.
- Send a terminal error to that query's subscribers.
- Continue applying transactions to other circuits.
- Supervise and restart the CDC task.
- Expose CDC task failure immediately in readiness and health checks.
- Persist or reconstruct a safe shared CDC cursor.
- Test both returned errors and worker panics during live application.

### 5. Add subscription admission control

Triplox needs explicit capacity boundaries rather than relying on thread, file
descriptor, virtual-memory, or HTTP/2 exhaustion.

Add:

- A global maximum subscription count.
- Per-client and per-tenant limits.
- A separate maximum unique-circuit count.
- Resource preflight checks for file descriptors and storage.
- Explicit overload errors with retry guidance.
- Configuration and documentation for expected capacity.

### 6. Fix HTTP/2 capacity and client behavior

For the Rust client:

- Do not let subscriptions consume every stream on the only HTTP/2 connection.
- Open additional connections or reserve a separate connection for unary
  transactions and queries.
- Put a bounded timeout on subscription registration.
- Surface an explicit capacity error instead of waiting indefinitely at stream
  201.

For the JVM clients:

- Replace one reader thread per subscription with shared asynchronous
  socket/frame dispatch.
- Preserve OkHttp's useful automatic multi-connection behavior.

For the server:

- Configure the HTTP/2 concurrent-stream limit explicitly.
- Align stream limits with subscription admission limits.
- Reserve enough connection capacity for control-plane operations.

### 7. Add eager cleanup and resumability

Needed lifecycle changes include:

- An HTTP stream guard that explicitly unregisters on disconnect.
- Storage cleanup after failed circuit construction.
- Idle and heartbeat timeouts.
- Resource-return tests for repeated subscribe/disconnect churn.
- Client resubscription from a transaction key.
- A defined response when the requested cursor is older than retained CDC data.
- Explicit durable cursor semantics for service restart.

### 8. Reduce fixed CDC overhead

The CDC poll interval is hard-coded to 200 ms, establishing an idle latency
floor. It should be configurable or replaced with an event-driven/adaptive
strategy where supported.

Schema retrieval also happens for every CDC transaction. Cache the schema by
metadata version or update it only when a schema transaction is observed.

These changes are lower priority than removing query-linear evaluation and
global backpressure.

## Required observability

At minimum, export:

- Latest WAL transaction and sequence.
- Latest CDC-decoded transaction.
- Latest transaction applied across all active queries.
- Per-query cursor or oldest-subscriber lag.
- CDC debt in transactions and wall-clock time.
- Apply time per transaction and per unique circuit.
- Registration and priming duration.
- Active subscribers versus active unique circuits.
- Output mailbox utilization and slow-subscriber disconnect count.
- Incremental service command latency and queue depth.
- DBSP threads, file descriptors, RSS, and storage by query or tenant.
- CDC task alive state, restart count, and last failure reason.

Without these metrics, a stopped CDC task or blocked subscriber can present as a
healthy open HTTP stream that simply stops producing data.

## Recommended release gates

Before treating incremental queries as production-ready, automated tests should
prove that:

- One unread subscriber cannot affect healthy subscribers' latency.
- 1,000 identical subscriptions share one evaluation circuit.
- Irrelevant transactions do not cause work proportional to all subscriptions.
- Concurrent registration does not pause established subscriptions beyond a
  defined service-level objective.
- A single circuit panic or error terminates only that subscription.
- CDC restarts and resumes from the correct transaction.
- Repeated connection churn returns threads, file descriptors, memory, and
  temporary storage to baseline.
- Rust, Java, and Clojure clients can transact while all subscription streams are
  occupied.
- A long-running heterogeneous workload has bounded CDC debt under the declared
  supported ingestion and subscription rates.
- Final incremental results match standard-query results at the same basis after
  every stress and recovery scenario.

The soak suite should cover:

- A mix of duplicate and unique queries.
- Single-pattern, join, negation, union, expression, and aggregate workloads.
- Relevant and irrelevant high-rate ingestion.
- Concurrent registration and unregistration.
- Slow, disconnected, and reconnecting consumers.
- Worker panic and returned-error injection.
- Server restart and retained-WAL boundaries.
- Local durable and remote object-store configurations.

## Validation performed

The investigation completed the following checks:

- Locked release server and Rust client build.
- 1,000-subscription Rust, Java, and Clojure public API tests.
- 100,000-delta matching fan-out test.
- 10,000-row irrelevant-ingestion comparison at 100 and 1,000 subscriptions.
- Slow-consumer service-wide blocking and recovery reproduction.
- Priming over 10,000 existing irrelevant rows.
- HTTP/2 stream-capacity checks for all client implementations.
- Server RSS, virtual memory, thread, and file descriptor measurement.
- Symbolized Samply profile of the current release workload.
- The six existing release subscription integration tests, all passing:
  priming, transaction deltas, standard-query equivalence, subscription drop,
  shutdown, and the existing small slow-consumer case.

The audit did not alter production source code. Temporary load-test harnesses
were removed after use.
