# Triplox Examples

## Prerequisites

Start the Triplox server first (from the project root):

```bash
cargo run
```

This uses the default `config/triplox.toml` config (in-memory storage, listening on `127.0.0.1:5490`).

## Running against Docker

Alternatively, start the server via Docker using the pre-built image:

```bash
docker run -p 5490:5490 ghcr.io/fiv0/triplox:main
```

Or build locally (from the project root):

```bash
./docker/scripts/build-image.sh
docker run -p 5490:5490 triplox:latest
```

Then run the examples as shown below. The Docker image binds to `0.0.0.0:5490`, so examples connecting to `127.0.0.1:5490` work out of the box.

## Running

### Rust

From the `examples/rust/` directory:

```bash
cargo run --bin simple-example
cargo run --bin streaming-example
```

### Clojure

See [examples/clojure](clojure/README.md) for REPL sessions and a live stock notebook with sales, refills, and incremental query changes.

### Java

From the `examples/java/` directory:

```bash
../../triplox-jvm/gradlew run                              # SimpleExample
../../triplox-jvm/gradlew run -PmainClass=StreamingExample # StreamingExample
```

### simple-example

The simple example, defines schema, add data for the new schema and queries the data back.

### streaming-example

The streaming example, defines schema, subscribes to changes for the new attribute, submits 3 transactions with data for the new attribute, awaits at most 3 deltas from the subscription and finally closes the subscription.
