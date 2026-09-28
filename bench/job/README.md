# JOB benchmark

Run the 113 Join Order Benchmark queries ported from Datalevin against
Datalevin, Datomic Pro Peer, Triplox standard queries, and Triplox incremental
queries. The source, schema, numeric ID offsets, and expected answers are
vendored; no sibling Datalevin checkout is needed. See [NOTICE.md](NOTICE.md).

Triplox uses the placeholder support added in
[PR #493](https://github.com/FiV0/triplox/pull/493) for missing-attribute checks.
Build the server and client from this checkout before running Triplox queries.

## Prerequisites

Linux, Python 3.11+, Java 21, Clojure CLI, and Docker with Compose v2.
Building Triplox uses the repository Dockerfile and Gradle wrapper. Internet
access is needed for the initial dependency/image downloads. Use a machine
with room for the CSVs, archive, Docker build cache, and a separate persisted
database for every engine and repetition. The full dataset is not downloaded
implicitly.

Run commands from this directory. Paths supplied to the launcher resolve from
your shell's working directory.

```bash
cd bench/job
./job prepare --fixture --data-dir data/fixture --engine baselines
./job test
./job run --engine baselines --data-dir data/fixture \
  --workload maintenance --batch-size 1 --cycles 1
```

This checks both baselines, including the full cache warmup and answer
comparison. The small fixture exercises only a few joins; most query answers
are empty. Its timings are smoke-test output, not representative JOB results.

Build this checkout and check all four modes:

```bash
./job prepare --build-triplox --engine all
./job run --engine all --data-dir data/fixture \
  --workload maintenance --batch-size 1 --cycles 1
```

The fixture includes matches for queries 11a and 11b, whose missing-attribute
checks translate to `(not [?mc :movie-companies/note _])`.

For a focused correctness regression, start a dev server from the
repository root (`cargo run -- config/triplox-dev.toml`), publish this checkout's
client, and run from this directory:

```bash
../../triplox-jvm/gradlew -p ../../triplox-jvm publishToMavenLocal -x test \
  -PtriploxVersion=0.1.0-job-benchmark -PsignAllPublications=false
clojure -M:datalevin:triplox:integration-test
```

Run `./job test` first to compile the CSV reader. Set `TRIPLOX_HOST` and
`TRIPLOX_PORT` to override `127.0.0.1:5490`. Use the dev configuration, which
gives each connection its own in-memory database.
It registers all 113 views before batched ingestion and compares every query's
standard and incremental answers with the original Datalevin query at nine
checkpoints, including the empty database. It checks view freshness even when
answers do not change, and asserts query 11b's known answers while adding,
changing, and retracting its optional note, retracting/restoring a join, and
updating/restoring a year. Most other queries are empty on this small fixture;
this validates execution and these transitions, not full IMDb answer coverage.
The correctness oracle disables Datalevin's result cache: version 1.1.0 can
return stale `missing?` results after an attribute-only change. Timed benchmark
runs retain the normal cache settings described below.

For the original IMDb snapshot:

```bash
./job prepare --download-data --data-dir data/imdb --engine all
./job run --engine all --data-dir data/imdb
./job run --engine all --data-dir data/imdb --workload maintenance
```

You can supply an existing directory containing all 21 original CSV files
instead of downloading. Original-data runs check the initial answers against
the vendored Datalevin expectations. Generated fixtures carry `fixture.json`,
which disables that check; cross-engine comparisons still run. A modified
IMDb dataset needs its own expectations and should not be presented as the
original JOB dataset.

Select `--engine datalevin`, `datomic`, `triplox-standard`,
`triplox-incremental`, `baselines`, or `all`. `all` runs the four modes
sequentially with fresh state. Use `--queries 1a,11a,32a` for a subset; the
manifest records the selection. Full-suite results require all 113 queries to
finish successfully. Each invocation is one repetition; repeat the command
for independent runs, retaining each run directory.

## What is measured

| Mode | Initial score | Maintenance score per batch |
|---|---|---|
| Datalevin | Complete query pass after warmup | Transaction + complete query pass |
| Datomic Pro Peer | Complete query pass after warmup | Transaction + complete query pass |
| Triplox standard | Complete query pass on loaded data | Transaction + complete query pass |
| Triplox incremental | Ingestion + final view catchup | Transaction + final view catchup |

Datalevin and Datomic load the entire dataset, then execute one complete pass
of the selected queries without measuring it. Only after that pass completes
does measurement start. Warmup and measurement use the same JVM, connection,
database value, query order, and query thread, with normal caches enabled.
There is no cache flush or process restart between them. Warmup failure fails
the run. Triplox standard has no explicit warmup; reports label this.

Every incremental query is registered before ingestion begins. Each client
view continuously drains its subscription while batched transactions load the
data. The last committed transaction supplies the target transaction key.
Catchup ends when **every view has applied** through that key, including
transactions that leave its result unchanged. Registration and schema setup
are reported outside the score. Ingestion includes CSV parsing, transaction
construction, sampling, and transaction acknowledgments. Result serialization
and verification happen after timed work.

This is deliberately an apples-to-pears comparison: warmed query execution on
an already loaded database versus loading and maintaining all selected
results together. Report both phases and engine cache/storage settings;
these scores do not establish general query speedups or equal durability.
JOB emphasizes multiway joins and optimization; the small aggregate outputs
can also conceal substantial intermediate maintenance work.

`--workload maintenance` first performs the same initial workload, then replays
a seeded trace. Each cycle retracts sampled `movie_companies` row attributes,
restores them, increments sampled title production years, and restores those
years. Identity attributes remain so references stay valid. A bounded seeded
reservoir samples the source rows; every engine constructs the same trace.
After each transaction, standard modes recompute the entire selected suite;
incremental mode waits for all views. There are no concurrent writes during
checkpoint snapshots. Saved trace hashes must match before comparison.

Default controls:

- `--batch-size 1000`: maximum source rows per ingestion/mutation transaction.
- `--max-bytes 262144`: maximum serialized EDN transaction bytes; large batches
  split deterministically. This is not a wire-byte or datom-count limit.
- `--seed 42 --cycles 10`: maintenance sampling and cycles. Small datasets can
  yield fewer populated cycles; the report records actual batch counts.
- `--heap 4g --transactor-heap 2g`: runner JVM and separate transactor heaps.
- `--timeout-ms 600000`: transaction, query, registration, and view-catchup
  deadlines. Query interruption is best effort; the process exits on failure.
- `--run-timeout-seconds 86400`: outer limit for each JVM process.
- `--port 15490 --datomic-port 14334`: local service ports; Datomic also needs
  the next port for H2. Occupied ports fail instead of reusing another service.

The portable loader adds `:job/id` with unique identity and creates stubs for
forward references. It keeps the upstream ID offsets but lets each engine
allocate its own entity IDs. All modes receive the same transaction data.
`load.json` counts source rows and attempted datoms, including repeated
identity assertions and stubs; it does not claim those are distinct writes.

Datalevin runs the original query forms. Datomic and Triplox get portable
variable names without dots. Datomic gets pure Clojure predicate helpers for
Datalevin's nested predicates, string comparisons, `like`, and `in`.
Triplox gets native scalar expressions
and anchored regular expressions for `like`. Missing attributes use negation
with `_` placeholders. These adaptations preserve intent but can affect query
planning and cost. Each engine's actual query forms are saved in `queries.edn`.

## Services and storage

Datalevin 1.1.0 runs embedded with its database under `state/`. Datomic Pro
1.0.7705 uses the Peer library and its own local transactor process, with
`protocol=dev` and an explicit disk `data-dir`. The launcher downloads the
matching Pro distribution, records its archive checksum, and writes each
transactor's properties. This is persisted H2-backed dev storage, not an
in-memory Datomic database. See the [Pro releases](https://docs.datomic.com/releases-pro.html)
and [storage documentation](https://docs.datomic.com/operation/storage.html).

Triplox uses [compose.yml](compose.yml): a pinned RustFS image, bucket setup,
and the image built from this checkout. Object storage, local log, cache, and
incremental state use dedicated named volumes for each run. Only the Triplox
client port is published, bound to localhost. The credentials in
[config/triplox.toml](config/triplox.toml) are local benchmark defaults.
`JOB_TRIPLOX_IMAGE` overrides the server image for infrastructure diagnostics;
container inspection records the actual image IDs. Rebuild the default image
when the checkout changes.

A query-free infrastructure check is also available:

```bash
./job run --engine datomic --data-dir data/fixture --ingest-only
./job run --engine triplox-standard --data-dir data/fixture --ingest-only
```

For manual service inspection, `./job up --engine datomic` or
`./job up --engine triplox-standard` prints the owned state directory and
leaves the service running. Normal `run` invocations stop their services on
completion or failure, retain databases/volumes, and preserve logs. Cleanup is
limited to the specific owned state directory:

```bash
./job down --state state/<run-id>-datomic --remove
./job down --state state/<run-id>-triplox-incremental --remove
```

`--remove` deletes that run's database state and Compose volumes. Omit it to
stop services while keeping storage. Run artifacts are retained either way.

## Saved results

Every invocation creates `runs/<UTC timestamp>-<random suffix>/`. Keep the
whole directory as an immutable experiment artifact; regenerate derived
reports with `./job report --runs runs/<run-id>`.

- `manifest.json`: configuration, engine versions, Git revision, dirty status,
  diff hash, benchmark source hashes, JVM and machine details, run status.
- `dataset.json`: byte sizes and SHA-256 for every source CSV.
- `<engine>/config.json`, `selection.json`, `queries.edn`: exact workload inputs.
- `<engine>/measurements.jsonl`: raw phase/query/checkpoint observations.
  Successful warmup has no measured durations.
- `<engine>/answers/<checkpoint>/<query>.edn`: normalized, sorted answer sets;
  integer types are normalized. Measurement events also contain answer hashes.
- `<engine>/trace.edn`, `trace.json`: replayable maintenance transactions,
  batch count, and trace checksum.
- `<engine>/load.json`, `resources.jsonl`, `disk.json`: ingestion counts,
  sampled process RSS/container statistics, and persisted state size.
  Container runs also include `containers.json` and `container-disk.json`.
  Resource sampling includes setup and warmup, not just measured query phases.
- `<engine>/status.json`, `runner.log`, `transactor.log` or `compose.log`:
  success/failure and diagnostic evidence.
- `summary.csv`, `queries.csv`, `summary.md`, `comparison.json`: regenerated
  summaries, per-query timings, p50/p95 maintenance latency, source-row
  throughput, and answer mismatches/missing checkpoints.

The reporter fails on incomplete runs, missing warmup, incompatible workload
configurations, differing traces, or differing/missing answers. A single
engine run can check the original expected answers but cannot establish
cross-engine parity. Ingestion-only reports explicitly omit query verification.
Do not combine a failed/partial run into a full-suite performance score.

Keep generated data, state, downloads, and runs out of Git. For published
results, archive the run directory along with the corresponding source commit
(and patch when dirty), and link the archive from your writeup. Store large
artifacts in object storage rather than committing them. This preserves raw
measurements and answers while allowing reports to evolve.
