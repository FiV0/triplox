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

See `examples/clojure/src/simple_example.clj` and `streaming_example.clj` for REPL sessions
showing how to interact with the server.

### Java

From the `examples/java/` directory:

```bash
../../triplox-jvm/gradlew run                              # SimpleExample
../../triplox-jvm/gradlew run -PmainClass=StreamingExample # StreamingExample
```

### simple-example

Connects to the running server, defines `:name` and `:age` schema attributes, inserts two documents (alice and bob), queries them back, and prints the results.

### streaming-example

Defines a `:name` attribute, subscribes to `[:find ?name :where [?e :name ?name]]`, transacts a few names, and prints the result delta (`[values weight]`) the subscription emits for each transaction. Run against the default in-memory server (one shared node), so the subscription and the transactions share state.
