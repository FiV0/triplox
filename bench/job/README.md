# JOB benchmark

Run all commands from this directory. Paths that you give to `./job.py` start
from the working directory of your shell. See [NOTICE.md](NOTICE.md) for the
sources of the vendored files.

## Separate filesystems for Triplox runs

Triplox runs must keep the RustFS data and the SlateDB disk cache on separate
loop-mounted ext4 filesystems. The Triplox file log stays on the root
filesystem. If they share one filesystem, each fsync of the file log can wait
for gigabytes of RustFS and cache writes.

Create and mount the images once after each boot:

```bash
sudo ./disks setup    # 70G rustfs.img + 20G slatedb-cache.img under disks.d/
./disks status
```

To change the sizes on the first setup, set `JOB_RUSTFS_DISK_SIZE` and
`JOB_SLATEDB_CACHE_DISK_SIZE`. `./job.py` does not start a Triplox run if the
mounts are missing. It writes the mounts to `<engine>/disks.json`.

To unmount the images, run `sudo ./disks teardown`. To also delete them, add
`--delete`. The filesystems are on the same physical drive, so a device cache
flush affects all of them. For full isolation, use separate drives.

## Run on the fixture

The fixture is a small generated dataset. Most of its query answers are empty,
and its timings are not JOB results.

To run the Datalevin baseline:

```bash
./job.py prepare --fixture --data-dir data/fixture --engine baselines
./job.py test
./job.py run --engine baselines --data-dir data/fixture
```

To build the Triplox server from this checkout and run all three modes:

```bash
./job.py prepare --build-triplox --engine all
./job.py run --engine all --data-dir data/fixture --batch-size 1
```

## Run the integration test

The integration test compares the final results of all three modes on the
witness data in [resources/job/witness.edn](resources/job/witness.edn). The
witness data gives each of the 113 queries at least one answer.

1. Run `./job.py test` to compile the Java sources.
2. From the repository root, start a dev server with
   `cargo run -- config/triplox-dev.toml`.
3. From this directory, run `clojure -M:integration-test`.

The test connects to `127.0.0.1:5490`. To use a different address, set
`TRIPLOX_HOST` and `TRIPLOX_PORT`. You must use the dev configuration, because
it gives each connection its own in-memory database.

If you change a query, run `clojure -M:witness` to write the witness file
again. A unit test fails when the file is out of date.

## Run on the IMDb data

```bash
./job.py prepare --download-data --data-dir data/imdb --engine all
./job.py run --engine all --data-dir data/imdb
```

Instead of the download, you can give a directory that contains all 21
original CSV files.

## Run options

Use `--engine` to select what runs:

- `datalevin`, `triplox-standard`, or `triplox-incremental` runs one mode.
- `baselines` runs Datalevin.
- `all` runs the three modes one after the other, each with fresh state.

Use `--queries 1a,11a,32a` to run a subset of the queries. Each invocation is
one repetition. For independent runs, run the command again and keep each run
directory.

The other options and their defaults are:

- `--batch-size 1000`: The maximum number of source rows in one Triplox
  transaction. Datalevin ignores it.
- `--heap 4g`: The heap of the runner JVM.
- `--timeout-ms 600000`: The deadline for each transaction, query, and
  registration.
- `--catchup-timeout-ms 600000`: The deadline for all incremental views to
  apply the last loaded transaction.
- `--run-timeout-seconds 86400`: The limit for each JVM process.
- `--port 15490`: The local Triplox port. If the port is in use, the run fails.

To override the Triplox server image, set `JOB_TRIPLOX_IMAGE`. If you change
the checkout, build the default image again.

## Services

To test the infrastructure without queries:

```bash
./job.py run --engine datalevin --data-dir data/fixture --ingest-only
./job.py run --engine triplox-standard --data-dir data/fixture --ingest-only
```

To start the services and keep them running for inspection, run
`./job.py up --engine triplox-standard`. The command prints the state
directory of the run.

A normal `run` stops its services at the end, and it keeps the databases,
volumes, and logs. To stop the services of a run:

```bash
./job.py down --state state/<run-id>-triplox-incremental --remove
```

`--remove` deletes the database state and the Compose volumes of that run. It
also deletes the directories of the run on the separate filesystems. Without
`--remove`, the command stops the services and keeps the storage.

## Results

Each invocation writes its results to `runs/<UTC timestamp>-<random suffix>/`.
To make the reports again, run `./job.py report --runs runs/<run-id>`.

## Development

- `clojure -T:build javac` compiles the Java sources into `classes/`.
- `clojure -M:dev` starts an nREPL server on a free port and writes the port
  to `.nrepl-port`. Add `--port <n>` to select the port. Before you use
  `job.source` or the client, load `build.clj` and evaluate `(javac nil)` from
  its comment block.
- `clojure -T:build uber` builds `target/job.jar`. `./job.py prepare` and
  `./job.py run` also build it.
- `clojure -T:build clean` deletes `target/`.
