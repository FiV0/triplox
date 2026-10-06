# Indexer lock simplification

Findings and plan for #520. Generated with Claude Code (Claude Opus 5.5).

## Problem

`Node` shares the indexer as `Arc<tokio::sync::RwLock<Indexer>>`. The subscription task takes the
write lock to index:

- `src/log.rs:129` (live): one transaction per write lock.
- `src/log.rs:63` (catch-up): the whole batch under one write lock. That is up to 100 transactions
  at startup and up to `u16::MAX` after a broadcast lag (#519).

Several read paths take the read lock only to copy a small piece of indexer state. They queue
behind indexing even though they don't need anything the indexer is changing. tokio's `RwLock` is
fair, so once a writer is queued, new readers wait behind it.

## Read-lock call sites

Line numbers are from `b017f8781`.

| # | Location | Reads | Who waits |
|---|---|---|---|
| 1 | `src/node.rs:346-357`, `db()` | clones `schema.ident_map` | `POST /db/open` (`src/server.rs:144`) |
| 2 | `src/node.rs:263`, `db_as_of_with_timeout` | `tx_waiter()` (broadcast subscription + `latest_indexed_tx`) | every `POST /db/query` (`src/server.rs:182`), `/db/open` with a tx key (`:154`) |
| 3 | `src/node.rs:268-275`, same function | clones `schema.ident_map` | same requests, second wait on the lock |
| 4 | `src/node.rs:50-54`, `SchemaProvider::schema()`, called from `src/incremental/cdc.rs:96` | clones the whole `Schema` | the CDC loop, once per WAL transaction, so every incremental subscription |
| 5 | `src/incremental.rs:214`, `register_query` | `latest_tx_key()` + whole `Schema` | `POST /db/subscribe` (`src/server.rs:320`) |
| 6 | `src/node.rs:325`, `execute_tx` | `tx_waiter()` | `POST /tx/execute`, which waits for the current batch before it can append to the log |

Not contended: `src/node.rs:88` (startup) runs before `subscribe()` starts the indexer task.
Test-only: `src/node.rs:425`, `:1292`, `:4020`, `:4048` and the indexer tests.

### Aggravating factors

- **The `db_as_of` timeout misses both lock waits.** `DB_AS_OF_INDEXING_TIMEOUT` (30 s) wraps only
  `await_indexed`. The `read()` before it (row 2) and after it (row 3) are unbounded, so a
  `/db/query` can block for longer than 30 s without returning `TxIndexingTimeout`.
- **Registration stalls CDC for every subscription.** `register_query` takes `registration_gate`
  and then waits for the indexer lock while holding it. The CDC loop needs the gate to apply each
  transaction, so one `/db/subscribe` that waits on the indexer stops delta delivery to every
  existing subscription. This comes on top of the CDC loop's own wait in row 4.

## What readers need

- the schema, or just its `ident_map`
- the latest indexed `TxKey`
- a subscription to the tx-completion broadcast, plus the baseline `TxKey` at subscription time

## Guarantees the lock provides today

Removing the lock naively breaks the first three.

1. **CDC decoding.** `write_with_options` doesn't wait for durability, so the WAL can be flushed
   between the SlateDB write and `apply_schema_update`. CDC can then read transaction T before
   the in-memory schema covers it. Today `schema()` blocks until the indexer releases the lock. A
   lock-free read could see the old schema and fail with `Unknown attribute entity_id`, which
   ends the CDC loop.
2. **Registration basis.** `register_query` needs a basis at or after every transaction CDC has
   already applied. Otherwise the initial scan misses T and CDC never resends it. The lock
   guarantees this because the SlateDB write and the `latest_indexed_tx` update happen under the
   same write lock. See the `NOTE(coverage)` comment in `src/incremental.rs`.
3. **`TxWaiter` baseline.** Subscribing to the broadcast and reading `latest_indexed_tx` must not
   miss a transaction in between. Without the lock, this holds if the indexer publishes state
   before broadcasting and the waiter subscribes before reading the baseline.
4. **`db()` consistency (not guaranteed today).** `db()` drops the lock before
   `DB::from_latest_sdb`, so the basis can be a transaction whose schema change isn't in the
   `ident_map` yet.

## Approaches

| Option | Reads | Wait for "indexed >= T" | Notes |
|---|---|---|---|
| `tokio::sync::watch` | short std `RwLock` read, never held across `.await` | yes, `wait_for()` | already in tokio |
| `ArcSwap` | fully lock-free | no; needs a separate `Notify` or polling | `arc-swap` is only an indirect dependency |
| std `RwLock<Arc<_>>` | like `watch` | no | `watch` without the notification |
| smaller tokio lock held only around the swap | readers still queue behind a waiting writer | no | shortens the stalls, doesn't remove them |

Recommendation: `watch`. One primitive covers both reading the latest state and waiting until a
transaction is published, and CDC needs the second.

## Plan

- **Publish one value**, `IndexedState { tx_key, schema: Arc<Schema> }`, through a `watch`
  channel. Registration needs the tx key and schema to match, which answers #520's second open
  question. Build a new `Arc<Schema>` only when a transaction changes the schema. Publish after
  the SlateDB write and before the completion broadcast, on both the commit and abort paths.
- **`IndexerHandle`** (watch receiver, broadcast sender clone, SlateDB `Arc`) lives in `Node` and
  gives out `TxWaiter`s without the lock.
- **`db()`** takes both the `ident_map` and the basis `tx_key` from the same published value, using
  `DB::new` instead of `DB::from_latest_sdb`. This fixes guarantee 4, which answers #520's first
  open question, and drops a SlateDB scan per call. The basis becomes the latest *published*
  transaction instead of the latest *written* one. Read-your-writes after `execute_tx` still holds,
  because publishing comes before the broadcast.
- **`db_as_of`, `execute_tx` and startup catch-up** get their waiter from the handle.
- **`register_query`** reads the published state.
- **CDC** reads the tx id from the `tx_eid` in a transaction's EAV key, which needs no schema. It
  waits until the published state reaches that tx id, then decodes with that state's schema. This
  keeps guarantees 1 and 2. `SchemaProvider` goes away; its only implementation is
  `RwLock<Indexer>`.
- **Keep the lock** as the subscription task's writer lock. `Node` keeps its `Arc` only for tests.
- **Test:** take the write lock and index a schema transaction through the guard. While still
  holding the lock, assert that `db()` (including the new attribute in its `ident_map`),
  `db_as_of`, query registration and the CDC delta all complete.

## Status

Done in `src/indexer.rs`:

- `IndexedState`, and a `watch::Sender<IndexedState>` that replaces `latest_indexed_tx`.
- `publish_indexed` runs before the completion broadcast in `transact_tx_inner` and
  `write_aborted_tx`.
- `IndexerHandle` with `indexed_state()`, `await_indexed_state(tx_id)` and `tx_waiter()`.
- `Indexer::tx_waiter()` is test-only; `Indexer::latest_tx_key()` is removed.

The crate doesn't build yet. `src/node.rs` and `src/incremental.rs` still call
`Indexer::tx_waiter()` and `latest_tx_key()` under the lock.

Remaining:

- `src/node.rs`: hold an `IndexerHandle`; rewrite `db()`, `db_as_of_with_timeout`, `execute_tx`
  and the startup waiter; remove `SchemaProvider`.
- `src/incremental.rs`: `register_query` and `start_cdc_once` take the handle; update the
  `NOTE(coverage)` comment to the new invariant.
- `src/incremental/cdc.rs` and `src/slate/cdc.rs`: extract the tx id, wait for it to be published,
  then decode.
- The lock test described above.

## Open points

- If a SlateDB write lands but reports an error, and the follow-up aborted-tx write also fails,
  that tx id is never published. CDC then waits for the next published transaction before
  applying it.
- Follow-up: once these readers are off the lock, only the subscription task and tests use it, so
  it could be removed. `DB::from_latest_sdb` would also be unused.
