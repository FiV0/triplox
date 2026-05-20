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
```

### Clojure

See `examples/clojure/src/simple_example.clj` for a simple REPL session of how to interact
with the server.

### Java

From the `examples/java/` directory:

```bash
../../triplox-jvm/gradlew run
```

### simple-example

Connects to the running server, defines `:name` and `:age` schema attributes, inserts two documents (alice and bob), queries them back, and prints the results.
