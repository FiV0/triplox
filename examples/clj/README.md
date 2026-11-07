# Live stock notebook

This [Clerk](https://github.com/nextjournal/clerk) notebook shows how a low-stock subscription updates as sales and refills change stock.

## Run

You need Java 21 or newer and the Clojure CLI.
From the repository root, start an in-memory server:

```bash
cargo run
```

In another terminal, start the notebook:

```bash
cd examples/clj
clojure -M:run
```

Open [localhost:7777](http://localhost:7777).
To use another server, set `TRIPLOX_HOST` and `TRIPLOX_PORT`.
