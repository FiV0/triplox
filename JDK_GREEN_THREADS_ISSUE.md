# Java 21 virtual-thread carrier exhaustion in the subscription client

Documented: 2026-09-17. Experiments: 2026-09-16.

The AuctionMark benchmark's Java 21 client exhausted its default carrier-thread
pool after 256 subscriptions completed initialization. Its virtual subscription
readers were waiting in OkHttp 4.12.0's HTTP/2 implementation, which calls
`Object.wait()`. On the tested JVM, that operation blocks the carrier OS thread as
well as the virtual thread. The scheduler compensates by adding carriers, until
it reaches its configured maximum.

A diagnostic retry increased that maximum to 1,100. All 1,000 subscriptions then
initialized, but the client used 1,038 OS threads at its sampled peak. Merely
switching these readers to `Thread.ofVirtual()` therefore did not achieve the
intended reduction in native threads on this Java 21 dependency stack.

The retry subsequently failed a delivery-backlog guard during warmup. That is a
separate observation: this investigation explains the registration stall and
client thread growth, but does not establish the cause of the later backlog.

## Terminology and expected behavior

This document uses the JDK term **virtual thread** for the mechanism referred to
as “green threads” in the filename.

| Term | Meaning |
|---|---|
| Virtual thread | A Java thread scheduled by the JVM rather than directly by the operating system. |
| Platform thread | A Java thread backed by an OS thread. |
| Carrier | A platform thread currently executing a virtual thread. |
| Mount/unmount | Assign a virtual thread to a carrier, or suspend it and release that carrier. |
| Scheduler parallelism | The scheduler's target execution parallelism, distinct from its maximum pool size. |

Ordinary virtual-thread blocking can release a carrier so that another virtual
thread runs on it. A large population of mostly idle virtual threads can therefore
share a small population of OS threads. This helps with concurrency; it does not
make CPU work faster or create additional CPU capacity. See Oracle's
[virtual-thread guide](https://docs.oracle.com/en/java/javase/24/core/virtual-threads.html).

Java 21 has exceptions. `Object.wait()` is a blocking operation that captures the
carrier; the scheduler can compensate by growing its pool. JEP 444 distinguishes
this from blocking while **pinned**, for which the Java 21 scheduler does not
compensate by expanding parallelism. Both can occupy OS threads, but the
compensation behavior differs. The mechanism relevant to this incident is the
non-unmounting `Object.wait()` path. See
[JEP 444, scheduling virtual threads](https://openjdk.org/jeps/444#Scheduling-virtual-threads).

`Object.wait()` releases the object's monitor while waiting. Releasing that lock
and releasing the carrier are different operations. Another thread can deliver
bytes and notify the waiter even though the waiting virtual thread still occupies
an OS thread. This does not imply that every reader is contending for one global
lock.

## Triplox's client path

The benchmark opts into virtual readers through the Clojure connection options:

```clojure
(tc/connect host port
  {:subscription-thread-factory (.factory (Thread/ofVirtual))})
```

The connection passes that factory to each `Subscription`. Each subscription has
a reader that decodes MessagePack frames into a bounded queue. The benchmark
starts another virtual thread to consume that queue, apply signed result deltas,
and record the transaction's delivery time. The library's default reader factory
still creates platform threads; virtual readers are explicitly selected here.

Relevant repository sources:

- [Clojure connection API](triplox-jvm/src/main/clojure/xyz/triplox/api.clj): forwards the optional reader factory.
- [TriploxNode](triplox-jvm/src/main/java/xyz/triplox/client/TriploxNode.java): retains the factory and passes it when opening subscriptions.
- [Subscription](triplox-jvm/src/main/java/xyz/triplox/client/Subscription.java): reader creation, `readLoop`, the bounded queue, and stream cancellation.
- [Benchmark driver](bench/auctionmark/src/auctionmark/sync.clj): virtual consumers, priming checks, delivery accounting, and backlog guard.
- [JVM build configuration](triplox-jvm/build.gradle.kts): OkHttp 4.12.0 and the Java 21 build toolchain.

The captured wait path was:

```text
Subscription.readLoop
  MessageUnpacker.hasNext / ensureBuffer
    InputStreamBufferInput.next
      Okio response-body input stream
        Http2Stream.FramingSource.read
          Http2Stream.waitForIo
            Object.wait
              Object.wait0
```

When the per-stream buffer is empty and the stream is still open, OkHttp's
`FramingSource.read` waits for more data. Its `waitForIo` helper uses the object's
wait operation. This matches both the recorded stack and the tagged
[OkHttp 4.12.0 Http2Stream source](https://github.com/square/okhttp/blob/parent-4.12.0/okhttp/src/main/kotlin/okhttp3/internal/http2/Http2Stream.kt).
The source line numbers in a Kotlin stack trace can differ from the source file's
physical line numbers because of compiled/inlined code.

These HTTP responses are long lived. After consuming an initial snapshot, a
reader commonly spends most of its time waiting for the next delta. That idle
wait is enough to occupy a carrier in this configuration; high update traffic is
not required to trigger the problem.

The consumer's queue wait is a separate blocking point. The observed one-thread-
per-subscription growth was associated with the HTTP readers. Creating a second
virtual consumer did not imply a second dedicated OS thread per subscription.

## Why initialization stopped at 256

Inspection of the installed Java 21 `VirtualThread` bytecode confirmed that the
default maximum carrier pool size is `max(parallelism, 256)`. The benchmark ran
with `-XX:ActiveProcessorCount=2`, and did not explicitly override scheduler
parallelism, so its effective default maximum was 256.

The evidence supports this sequence:

1. A subscription reader receives its initial result and returns to waiting for
   another frame.
2. That reader blocks in OkHttp's `Object.wait()` path, occupying a carrier.
3. As more subscriptions are opened, scheduler compensation adds carriers.
4. The carrier pool reaches its maximum. Additional virtual-thread work can no
   longer reliably obtain a carrier while the existing readers remain idle.
5. A subsequent subscription fails the driver's 10-second priming wait.

The first trial recorded exactly 256 completed per-query initializations. It did
not enter the workload's timed write phase. The retry changed the carrier limit
and successfully passed this registration point, providing experimental support
for the diagnosis.

The benchmark's top-level `subscriptions: 1000` field is the requested count.
For the failed trial, `per-query-initialization-ms.count: 256` is the evidence of
completed initialization. Circuit-construction log counts can include attempts
whose client-side initialization did not finish; they should not be used as the
number of usable subscriptions.

The 256 threshold is not an HTTP/2 stream-limit measurement, a SlateDB limit, or
an established Triplox query-capacity limit. It is consistent with the inspected
JVM scheduler configuration and the observed client wait path. Shared HTTP/2
connections do not by themselves make these per-stream blocking readers cheap.

## Experiment configuration and results

Both runs used the same release server and logical workload:

| Setting | Value |
|---|---|
| JVM | OpenJDK 21.0.12, build 21.0.12+8 |
| HTTP client | OkHttp 4.12.0 |
| Target subscriptions | 1,000 distinct queries, five query families |
| Fixture | 200 users, 1,000 items, seed 42 |
| Target write rate | Five committed transactions per second |
| Planned timing | 10 seconds warmup, 30 seconds measurement |
| Priming timeout | 10 seconds for a nonempty initial result |
| Delivery-backlog guard | Five seconds |
| DBSP runtime | Two shared foreground workers, one merger worker |
| Shared DBSP cache capacity | 4,096 MiB |
| Storage | MinIO with a local file transaction log |
| CDC polling | 200 ms |
| Remote SlateDB flush configuration | 100 microseconds |
| Server/client application scope | 1.75 CPU cores, 7,424 MiB memory limit |
| MinIO container | 0.25 CPU cores, 768 MiB memory limit |
| Combined budget | Two CPU cores and 8 GiB; swapping disabled |

The retry changed two thread-related limits:

| Limit | Default trial | Diagnostic retry |
|---|---:|---:|
| `jdk.virtualThreadScheduler.maxPoolSize` | Default: 256 here | 1,100 |
| Application scope `TasksMax` | 1,024 | 1,400 |

`TasksMax` limits Linux tasks, including threads, within the scope. Raising it
provided room for the larger carrier pool and other process threads. Neither
change increased the CPU quota or memory budget. The overrides were experimental;
they were not made permanent in the benchmark launcher.

| Observation | Default trial | Diagnostic retry |
|---|---:|---:|
| Completed subscription initializations | 256 | 1,000 |
| Total initialization time | Incomplete | 36.310 seconds |
| Per-query initialization p50 | 35.4 ms | 35.5 ms |
| Per-query initialization p95 | 48.0 ms | 47.6 ms |
| Peak client OS threads | 285 | 1,038 |
| Peak server OS threads | 12 | 13 |
| Peak combined sampled RSS | 902.5 MiB | 2,622.6 MiB |
| Terminal failure | Priming timed out | Expected delivery exceeded backlog age limit |
| Valid measured-window latency samples | None | None |

In the retry, peak client RSS was 483.5 MiB and peak server RSS was 1,927.1 MiB.
The combined peak was approximately 2.56 GiB. The configured 4 GiB DBSP cache is
its capacity, not the amount of memory necessarily resident during a run.

The captured retry thread dump contained **1,000 subscription-reader stacks**
waiting in `Object.wait` through `Http2Stream.waitForIo`. The total native-thread
count also includes GC, JIT, HTTP infrastructure, and other JVM threads. Counts of
all entries in a virtual-thread dump should not be equated with OS thread counts;
the reported OS counts came from Linux process sampling.

Increasing the carrier ceiling enabled registration by allowing more OS threads
to wait. It did not demonstrate the intended virtual-thread scaling benefit.

## What the later backlog failure does and does not show

The retry entered the write phase but stopped during warmup because at least one
expected delivery was more than five seconds overdue. At abort, 78 expected
deliveries remained pending.

Those 78 entries mean the driver had not matched their expected client-applied
results before stopping. They do not prove permanent data loss. The stage aborted
before completing its normal drain and final verification path.

No transactions or deliveries reached the measurement window. Consequently,
`transactions-per-second: 0` and null latency percentiles in the report are not
valid measurements of steady-state throughput or freshness. In particular, they
do not mean that no warmup transactions were submitted or committed.

The client also emitted an `EAGAIN` native-thread creation warning for
`Logging-Cleaner` during shutdown. The precise limiting resource and any causal
relationship to the earlier delivery backlog were not established.

The retry therefore establishes successful initialization with expanded client
thread limits, followed by failure of the configured backlog guard. It does not
separate circuit processing time, CDC polling/object-store delays, client
scheduling, or CPU-quota stalls as causes of that backlog. Average CPU or RSS
alone cannot make that distinction. No measured p95 should be quoted for either
1,000-subscription attempt.

## Why a newer JDK is the next experiment

JEP 491 changed the JVM's monitor implementation, including the ability to
unmount virtual threads waiting in `Object.wait()` and its timed variants. The
[OpenJDK implementation discussion](https://mail.openjdk.org/pipermail/nio-dev/2024-November/018122.html)
explicitly lists this work. Oracle's
[JDK 24 release notes](https://www.oracle.com/java/technologies/javase/24-relnote-issues.html)
record the delivered feature; the proposal is
[JEP 491: Synchronize Virtual Threads without Pinning](https://openjdk.org/jeps/491).

Run the same client on JDK 25, retaining the server binary, OkHttp version,
workload, CPU/memory limits, and default carrier ceiling. This isolates the JVM
change. The expected result is that idle subscription readers release carriers
and initialization passes 256 without native-thread growth proportional to the
subscription count. This is a hypothesis to validate: no JDK 25 result was
collected in these experiments, and success would not by itself explain or fix
the later delivery backlog.

The Java 21 Gradle toolchain declaration and the JVM actually launching the
Clojure benchmark are separate choices. Record the running client's Java
version; changing a build setting alone does not establish which JVM performed
the measurement.

If Java 21 must remain the runtime, alternatives worth evaluating are an HTTP
client implementation with virtual-thread-compatible waits, or a streaming
consumption design that avoids an OS-blocking wait per subscription. Any such
change needs tests for HTTP/2 multiplexing, backpressure, frame boundaries,
terminal errors, cancellation, and reader cleanup. Merely increasing the carrier
ceiling remains a resource-expensive workaround.

## Reproduction and diagnostics

Run from the `incremental-auctionmark` checkout. The launcher requires Linux,
a systemd user session, Docker, the locally published JVM artifact, and a release
server. Follow the [AuctionMark build instructions](bench/auctionmark/README.md).
Use a fresh output directory for each run.

The default trial was equivalent to:

```bash
python3 -B bench/auctionmark/run-sync.py \
  --server-binary target/auctionmark-validation/triplox \
  --output-dir target/auctionmark-sync/capacity-1000 \
  --timeout 600 -- \
  --mode capacity --stages 1000 --users 200 --items 1000 \
  --rate 5 --warmup 10 --duration 30
```

The `target/auctionmark-validation/triplox` executable was an isolated copy of the
verified release binary. A new checkout can pass its own freshly built release
binary instead. The launcher supplies `-J-Xmx768m` and
`-J-XX:ActiveProcessorCount=2` to Clojure.

The diagnostic retry used this client property:

```text
-Djdk.virtualThreadScheduler.maxPoolSize=1100
```

The saved [retry driver](target/auctionmark-sync/retry-1000.py) imported the
launcher's `benchmark` function and set that property through `JAVA_TOOL_OPTIONS`.
It was invoked in a scope with:

```bash
systemd-run --user --scope --quiet \
  -p CPUQuota=175% -p MemoryMax=7424M -p MemorySwapMax=0 \
  -p TasksMax=1400 \
  nice -n 10 python3 -B target/auctionmark-sync/retry-1000.py
```

The driver uses fixed output paths; choose a new directory before repeating it.
Putting the unchanged launcher's `main` inside a larger outer scope is not an
equivalent override: its own inner scope still sets `TasksMax=1024`.

For an active client, collect a virtual-thread dump with the matching JDK's
`jcmd` tool:

```bash
jcmd CLIENT_PID Thread.dump_to_file -format=json /absolute/path/threads.json
ps -p CLIENT_PID,SERVER_PID -o pid,etime,pcpu,rss,nlwp
```

Replace the PID and path placeholders. Capture during initialization or mark the
diagnostic time in the run record, so its overhead is not mistaken for workload
latency. Search reader stacks for `Subscription.readLoop`,
`Http2Stream.waitForIo`, and `Object.wait` together.

The following read-only command exposes scheduler property handling and defaults
in the selected Java installation:

```bash
javap -p -c java.lang.VirtualThread
```

Inspect `createDefaultScheduler` and its generated helper for
`jdk.virtualThreadScheduler.maxPoolSize`. This incident's installed JVM used
`max(parallelism, 256)` when the property was absent. Do not assume that every
JVM version or vendor has identical internal code.

## Validation criteria for the follow-up

A useful JDK comparison should establish both client scalability and correctness:

1. Verify the actual client JVM version and retain the same server binary and
   client dependencies. Remove the experimental `maxPoolSize=1100` override.
2. Start below the observed threshold, then cross 256 and attempt 1,000 within
   the existing CPU/memory budget. Record successfully primed subscriptions.
3. Compare OS thread counts while readers are idle. Confirm that the slope is
   no longer approximately one OS thread per subscription; do not prescribe an
   exact count for unrelated JVM infrastructure.
4. Capture reader stacks and scheduler evidence at the same phase in each run.
5. Run writes and track expected deliveries, rather than timing only updates
   that happen to arrive. Retain the backlog guard and report an early abort
   separately from a completed measurement window.
6. Compare final results against the reference model and ordinary database
   queries, then verify cancellation and complete process/container cleanup.
7. If the backlog persists after client thread scaling improves, instrument the
   commit-to-CDC-to-circuit-to-client stages before assigning a server-capacity
   limit or changing the polling interval.

## Evidence and reference links

The experiment's implementation revision was
`9f8c0d67ef0c19ea4ead1e04ea1807cdd8af4131`. The Feldera runtime dependency was pinned
to `ded0d390b64bdfe82a9afac9084404ad3809547e`. Both attempts used server SHA-256
`be0f39d5b49811d16e29f65133bfbba6599163f5f17d2291f084fefeb868d14e`.

Local evidence:

- [Default-run JSON report](target/auctionmark-sync/capacity-1000/report.json).
- [Default-run environment](target/auctionmark-sync/capacity-1000/environment.json).
- [Retry JSON report](target/auctionmark-sync/capacity-1000-expanded-carriers/report.json).
- [Retry environment](target/auctionmark-sync/capacity-1000-expanded-carriers/environment.json).
- [Explicit retry overrides](target/auctionmark-sync/capacity-1000-expanded-carriers/run-overrides.json).
- [Captured reader/thread stacks](target/auctionmark-sync/capacity-1000-expanded-carriers/initialization-threads.json).
- [Trial summary](target/auctionmark-sync/capacity-1000-summary.md).

These files are retained under the worktree's ignored `target/` directory. They
are local artifacts, not files guaranteed to exist in another checkout. The key
configuration, results, and interpretation are preserved in this document so it
remains useful without those artifacts. The retry overrides are in a separate
JSON file; `environment.json` alone does not record the changed thread limits.

Primary external references:

- [JEP 444: Virtual Threads](https://openjdk.org/jeps/444): the Java 21 scheduling model, blocking exceptions, and compensation versus pinning.
- [JEP 491: Synchronize Virtual Threads without Pinning](https://openjdk.org/jeps/491): the later monitor changes.
- [OpenJDK JEP 491 implementation discussion](https://mail.openjdk.org/pipermail/nio-dev/2024-November/018122.html): explicit coverage of `Object.wait()` and timed waits.
- [Oracle JDK 24 release notes](https://www.oracle.com/java/technologies/javase/24-relnote-issues.html): delivered release of JEP 491.
- [Oracle virtual-thread guide](https://docs.oracle.com/en/java/javase/24/core/virtual-threads.html): thread terminology and diagnostic commands.
- [OkHttp 4.12.0 Http2Stream source](https://github.com/square/okhttp/blob/parent-4.12.0/okhttp/src/main/kotlin/okhttp3/internal/http2/Http2Stream.kt): the wait path seen in the captured stacks.
