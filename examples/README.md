# Triplox Examples

## Prerequisites

Start the Triplox server first (from the project root):

```bash
cargo run
```

This uses the default `triplox.toml` config (in-memory storage, listening on `127.0.0.1:5490`).

## Running

In a separate terminal, from the `examples/` directory:

```bash
cargo run --bin simple-example
```

### simple-example

Connects to the running server, defines `:name` and `:age` schema attributes, inserts two documents (alice and bob), queries them back, and prints the results.
