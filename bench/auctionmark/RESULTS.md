# Initial bounded smoke result

Measured on 2026-09-16 with an Intel Core Ultra 7 258V, Linux 6.18.44, and
OpenJDK 21.0.12. This is one local smoke run, not a capacity maximum.

The release server used the pooled runtime (two foreground workers, one merger
worker, 4096 MiB shared cache), MinIO, and a local file log. The launcher capped the
server/client scope at 1.75 CPU cores and 7424 MiB, and MinIO at 0.25 cores and
768 MiB. CDC polling remained 200 ms. The fixture was 20 users/100 items, seed 42,
with five transactions/second, 10 seconds warmup and 30 seconds measured per stage.

| Subscriptions | Committed tx/s | Measured deliveries | Applied latency p50/p95/p99 (ms) | Combined peak RSS (MiB) | Outcome |
|---|---:|---:|---|---:|---|
| 5 | 4.97 | 8 | 57 / 244 / 244 | 581 | Passed smoke thresholds |
| 20 | 4.97 | 53 | 170 / 431 / 484 | 638 | Stopped: p95 exceeded 250 ms |

Both stages had zero missing/unmatched deliveries and no correctness errors.
Final client results matched the reference model and ordinary database queries.
The runner did not attempt 50 or 100 subscriptions after the failed stage.
Only eight changes reached the five-subscription stage's views, all in the bids
and seller-listings families. That stage's passing p95 is a weak statistical
result; it does not establish freshness for every query family.

| Subscriptions | Server peak OS threads | Client peak OS threads | Server/client/MinIO average CPU cores | Server/client peak RSS (MiB) |
|---|---:|---:|---|---|
| 5 | 12 | 35 | 0.106 / 0.091 / 0.107 | 51 / 189 |
| 20 | 15 | 50 | 0.204 / 0.080 / 0.161 | 84 / 190 |

The shared runtime avoided one DBSP worker group per subscription. JVM OS thread
counts still grew despite virtual readers, so client thread scaling remains a
limitation to investigate. These samples do not identify its cause. Low average
CPU and RSS do not rule out short CPU-quota stalls or object-store/polling delays;
this experiment does not attribute the freshness failure to a particular stage.
The 4 GiB cache was a configured capacity, not resident usage.

Transaction acknowledgement p95 was 9.1 ms at five subscriptions and 12.9 ms at
20; write scheduling delay p95 was about 1.1 ms in both. Peak pending deliveries
were four and seven, respectively, and drained completely. Initialization took
42 ms and 255 ms. MinIO file-descriptor counts were unavailable to the host user
and are null in JSON rather than estimated.

The full local report and logs were retained under
`target/auctionmark-sync/implementation-smoke/` in the shared build directory.
The report identifies implementation revision
`5e89d2f3e14bb9c4a321983d6d3007653c9d6873`, Feldera revision
`ded0d390b64bdfe82a9afac9084404ad3809547e`, and server binary SHA-256
`be0f39d5b49811d16e29f65133bfbba6599163f5f17d2291f084fefeb868d14e`.
The checkout was marked dirty because the launcher and documentation were awaiting
the final workflow commit; Rust and Clojure implementation files were committed.
The subsequent rebase onto main's `c262f38dc` build-cache change left those sources
unchanged; the equivalent implementation revision is now `f0ad2c15c`.
A separate forced one-second timeout check confirmed process/container cleanup.
