---
title: Life of a transaction
description: The lifecycle of a Triplox transaction.
---

Below is a diagram of a Triplox setup with the single writer node + a single
reader node. In the following we are going to describe how a Triplox transaction
flows through this system and the data eventually becomes queryable.

```
                   ┌────────────────────────────────────────┐
                   │           Object Storage (S3)          │
                   │                                        │
                   │  ┌─────────────┐      ┌─────────────┐  │
                   │  │   SlateDB   │      │   SlateDB   │  │
                   │  │  (Writer)   │      │  (Reader 1) │  │
                   │  └──────┬──────┘      └──────┬──────┘  │
                   │         │                    │         │
                   └─────────┼────────────────────┼─────────┘
                             │                    │
     Queries/Indices         ▲ read/write         ▼ read
                             │                    │
        ┌────────────────────┴────────┐  ┌────────┴────────┐
        │         Writer Node         │  │  Reader Node 1  │
        │                             │  │                 │
        │      ┌──────────────┐       │  │                 │
  ┌─────┼────▶│   Indexer    │       │  │                 │
  │     │      └──────────────┘       │  │                 │
  │     │                             │  │                 │
  │     └─────────────┬───────────────┘  └─────────────────┘
  │                   │
  │  Transactions     │ write
  │                   ▼
  │     ┌──────────────────────────────────────────────────┐
  │     │                                                  │
  │     │                      Log                         │
  │     │                                                  │
  │     │    ┌─────┬─────┬─────┬─────┬─────┬─────┐         │
  └─────┼────┤ tx0 │ tx1 │ tx2 │ tx3 │ tx4 │ ... │         │
   read │    └─────┴─────┴─────┴─────┴─────┴─────┘         │
        │                                                  │
        └──────────────────────────────────────────────────┘
```

A transaction is sent to the primary node for indexing. The node appends the transaction data
verbatim (serializing the transaction to some canonical format) to a log. This could be a Kafka log
or a WAL writing directly to an S3 express bucket. You might ask why there is an extra log and
if the transactions could not be buffered on the primary server. As Triplox
is a client/server system, we want to acknowledge transactions without a
lot of latency even when there is a lot of back pressure from indexing.

A transaction is appended to the log and a `TxKey` is returned. The
`TxKey` holds a `tx_id`, an offset into the log (file or Kafka)
identifying the transaction and a `system_time`, the wall-clock instant the transaction
was appended. At this point the transaction is durable but lives on the log as plain,
unindexed data. The indexer subscribes to the log and, for each record, materializes the
transaction data into the [covering indexes](/data-model/#indexes). This means each user
datom gets indexed into the EAV, AVE and AEV indexes. Datoms containing a unique attribute
also get indexed into the VAE index.

A transaction key `TxKey` serves two purposes. It allows us to uniquely identify a transaction on the log.
Secondly, it serves as a basis for database values as we will see below. For this reason "transaction key" and "basis"
are sometimes used synonymously, but `TxKey` is mostly used in the log context and basis in a database value
context.

When transaction data gets indexed into SlateDB in the tx pipeline, we also mint a transaction entity.
The entity id of this transaction entity is what gets referenced as `tx` in every other datom of the transaction
and also the transaction entity itself. It is this `tx_eid` that allows us to later filter the indexes
up to a particular point (basis). There is a one-to-one relationship between the `tx_id` (read transaction id)
and the `tx_eid` (transaction entity id). The `tx_eid` is the `tx_id` allocated in the `TX_PARTITION`
(see [partitions](/transactions/partitions)). So the relationship can be expressed in the following
formula `tx_eid = (TX_PARTITION << 42) | tx_id`. This is what allows us to cleanly map `TxKey` to a database value.

The transaction entity carries the following attributes:
- `:db/txId` — the `tx_id` from the `TxKey` of this transaction.
- `:db/txInstant` — the `system_time` of the `TxKey`
- `:db/txResult` — whether the transaction was committed or aborted
- `:db/txError` — an optional transaction error

In the future we might add, similar to Datomic, a special tempid identifier so one can attach extra attributes
to this transaction entity for transaction traceability. Every transaction written to the log gets processed
by the indexer and always gets a transaction entity id. The user-supplied transaction data is only written
if no tx pipeline constraint was violated (see [Transaction model](/transactions/transaction-model)).

The relationship between `tx_id` and `tx_eid` allows us to immediately run queries given a `TxKey` as it directly
maps `TxKey` to a basis for filtering the indexes.

The transaction entity ids are the only entity ids that are set through an external mechanism (the monotonically
increasing `tx_id`s of the log). All other entity ids are allocated in the indexer.

:::note
There is a difference between transactions as they are processed by Triplox and Datomic.
A transaction violating some constraint (for example, a uniqueness constraint) has no trace in Datomic. In Triplox
every transaction gets written to the log and processed by the indexer, it's the indexer that decides if
a transaction is valid or not.
:::

### Concepts

- **TxKey** — identifies a unique transaction on the log (`tx_id` +
  `system_time`). Returned by the submit-tx API.
- **tx_id** (transaction id) — an offset into the transaction log identifying the start of the
  transaction record; also serves as the transaction's identity on the log.
- **transaction entity** — the entity in the `TX_PARTITION` partition that carries
  information about the result and state of a transaction *after* it has
  been indexed.
- **tx_eid** (transaction entity id) — the entity id of that transaction
  entity. This is what every indexed datom is tagged with in the `Tx` position, and what a
  query execution uses to filter indexes to a given snapshot. The relationship
  between `tx_id` and `tx_eid` is given by the formula `tx_eid = (TX_PARTITION << 42) | tx_id`.
