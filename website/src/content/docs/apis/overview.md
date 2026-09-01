---
title: APIs Overview
description: The core methods every Triplox client API exposes.
---

Every Triplox client API, regardless of language, currently exposes the same small set of methods. `connect` returns a node object, and on that node you get two ways to submit transactions (`submit_tx` and `execute_tx`). From that node object you can also
take out a consistent db value (sometimes called db snapshot in other systems) with `db` or `db_as_of`. You can query
data on db values with `q`/`query`. [Incremental query](/incremental-queries/overview/) subscriptions give you a way to listen to changes to the Triplox system with the full power of Datalog.

## connect

Establishes a connection to a running Triplox server and returns a client node. The node is the entry point for everything else: submitting transactions and opening DB snapshots. A node holds network resources, so it should be closed when no longer needed.

## submit

`submit_tx` appends a transaction to the log and returns as soon as the server has accepted it, yielding a `TxKey`. It does **not** wait for the transaction to be indexed, so the change is not yet visible to queries when the call returns. Use it when the caller is not interested when a transaction has been indexed.

## execute

`execute_tx` submits a transaction and waits until the indexer has applied it, returning a `TransactionResult` that reports whether the transaction committed or aborted along with the `TxKey`. This is the method you want when you need to know the outcome of the transaction before continuing, or when the next step relies on the new data being visible.

## db value

Open an immutable database value at the latest transaction known to the node with `db`. All reads happening against this db value see a consistent point-in-time view that is unaffected by concurrent writes.

## query

Runs a Datalog query against a db value and returns the matching facts.
See the [query language overview](/query-language/overview/) for the syntax of EDN Datalog.

## subscribe

The `subscribe` API takes a node and a query (in the same syntax as standard queries) and returns a subscription handler (also referred to as a subscription). As changes to the incremental queries are computed on the server, they get pushed to the client. The subscription handler can be queried for new deltas to the incremental query. It is the responsibility of the client to consume newly computed changes on the subscription as
otherwise data will accumulate on the client. A subscription requires resources on the server and hence needs closing
when no longer needed. Currently every `subscribe` triggers a fresh incremental query on the server, there is currently no
way to have one incremental query on the server that pushes its changes to multiple clients.

## API method lifecycle and client state

TODO: Describe HTTP/2 connection and multiplexing

See the language-specific pages ([Clojure](/apis/clojure/), [Rust](/apis/rust/), [Java](/apis/java/)) for the exact signatures and idioms of each language. If you want to build a new client, the wire format and endpoints are documented in the [protocol doc](https://github.com/FiV0/triplox/blob/main/design/PROTOCOL.md).
