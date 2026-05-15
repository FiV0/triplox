# Graph Ingestion Benchmark

Tiny Clojure benchmark for loading a generated graph into Triplox. It installs
a minimal graph schema, ingests vertices and edges, and prints ingestion timing.
It intentionally does not run benchmark queries.

## Run a local Triplox node

Use local persisted storage for now: local transaction log plus local SlateDB
files.

From the repository root:

```bash
cargo run --release -- config/triplox-local.toml
```

This listens on `127.0.0.1:5490` and stores data under `./data/`.

To start the node from the repo Docker image instead:

```bash
./docker/scripts/build-image.sh
docker run -p 5490:5490 \
  -e TRIPLOX_STORAGE=local \
  -v triplox-data:/var/lib/triplox \
  triplox:latest
```

To use a published image:

```bash
docker run -p 5490:5490 \
  -e TRIPLOX_STORAGE=local \
  -v triplox-data:/var/lib/triplox \
  ghcr.io/fiv0/triplox:0.1.0-alpha.2
```

The general Docker setup lives under `docker/`; use
`docker/docker-compose.yml` only when you need the remote S3-compatible storage
stack.

## Run the benchmark

If you are benchmarking a locally checked-out server, publish the matching JVM
client first:

```bash
cd triplox-jvm
./gradlew publishToMavenLocal -x test
```

Then run the benchmark:

```bash
cd bench/graph
clojure -M:run --vertices 100 --batch-size 1000
```

### Run with `clj`

The same main entry point can be invoked with `clj`:

```bash
cd bench/graph
clj -M -m graph.main --vertices 100 --batch-size 1000
```

The `:run` alias is equivalent:

```bash
clj -M:run --vertices 100 --batch-size 1000
```

Configuration can also come from environment variables:

```bash
TRIPLOX_HOST=localhost TRIPLOX_PORT=5490 VERTICES=100 BATCH_SIZE=1000 clojure -M:run
```
