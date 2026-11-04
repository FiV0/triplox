# Live stock notebook

This Clerk notebook shows the original low-stock query as sales and refills change its result.
The example has three products, two warehouses, and six stock records.
Each product has a reorder threshold.

## Run

Install Java 21 or newer and the Clojure CLI.
From the repository root, start the default in-memory server:

```bash
cargo run
```

In another terminal, start the notebook:

```bash
cd examples/clj
clojure -M:run
```

Open [localhost:7777](http://localhost:7777).
Sales and refills start automatically, with one transaction every 1.5 seconds.
Select Pause, then Next transaction, to follow one transaction at a time.
Select Resume to continue the loop.
Select Stop to close the subscription and connection.
Press Ctrl-C to exit Clerk and release the same resources.

The server uses `config/triplox.toml` and listens on port 5490.
Set `TRIPLOX_HOST` and `TRIPLOX_PORT` to use another server.
Startup creates the schema and resets the example's six stock records.
Use an in-memory server for this example.

## What the notebook shows

The low-stock table starts with the subscription's initial result.
The client then adds and removes rows using the received weights.
It applies each batch in one update and keeps the latest eight batches visible.
It never reruns the low-stock query to refresh this table.
Clerk sends changes in the client's view to the browser.

A sale transaction creates a `:sale/id` record with a stock reference and quantity.
The same transaction reduces `:stock/available` by that quantity.
A refill transaction records the added quantity and increases stock.
The example uses one writer and reads current stock before each transaction.
The all-stock table shows quantities from these committed transactions.

For Berlin coffee, the first cycle changes stock from `12` to `10`, `7`, `5`, and back to `12`.
At `10`, the row stays outside the query result because the comparison is strictly less than `10`.
At `7`, the subscription adds the row.
At `5`, it removes the old row and adds the new row.
The refill removes the row from the result.
The loop then moves to another stock record and repeats.

Hamburg tea starts below its threshold to show a nonempty initial result.
The change feed shows actual received batches, which can combine several transactions.
Batch numbers count deliveries to the client, separately from transaction IDs.

## Test

Keep the example server running and stop the notebook before the test.
From `examples/clj`, run:

```bash
clojure -M:test
```

The test compares the subscribed view with the standard query after sales and a refill.
It also reads stored sale records and checks their quantities.

The notebook uses [Clerk](https://github.com/nextjournal/clerk) and the published Triplox Clojure client.
