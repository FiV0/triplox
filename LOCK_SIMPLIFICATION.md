# Indexer lock simplification

Findings and implementation notes for #520. Generated with Claude Code (Claude Opus 5.5).

## Problem

`Node` shared the indexer as `Arc<tokio::sync::RwLock<Indexer>>`. The subscription task takes the
write lock once per transaction, in both the live path and catch-up (catch-up used to hold it for a
whole batch; #521 fixed that). Several read paths took the read lock only to copy a small piece of
indexer state, so they queued behind every indexed transaction. tokio's `RwLock` is fair, so once a
writer is queued, new readers wait behind it.

## Read-lock call sites (before)

Line numbers are from `5536b266` (`main` before this change).

| # | Location | Reads | Who waits |
|---|---|---|---|
| 1 | `src/node.rs:76`, startup | `tx_waiter()` | nobody; runs before `subscribe()` starts the task |
| 2 | `src/node.rs:251`, `db_as_of_with_timeout` | `tx_waiter()` | every `POST /db/query`, `/db/open` with a tx key |
| 3 | `src/node.rs:260`, same function | `schema.ident_map` clone | same requests, second wait on the lock |
| 4 | `src/node.rs:313`, `execute_tx` | `tx_waiter()` | `POST /tx/execute` |
| 5 | `src/node.rs:337`, `db()` | `latest_tx_key()` + `ident_map` clone | `POST /db/open` |
| 6 | `src/incremental/cdc.rs:89`, CDC loop | whole `Schema` clone | every incremental subscription, once per WAL tx |
| 7 | `src/incremental.rs:214`, `register_query` | `latest_tx_key()` + whole `Schema` | `POST /db/subscribe`, while holding `registration_gate` |

Test-only: `src/node.rs:419`, `:1286`, `:4014`, `:4042`, `src/indexer.rs:1122`, `:1466`.

Two aggravating factors:

- The 30 s `DB_AS_OF_INDEXING_TIMEOUT` wrapped only `await_indexed`; the lock waits in rows 2 and 3
  were unbounded.
- `register_query` waited on the indexer lock while holding `registration_gate`, and the CDC loop
  needs the gate to apply each transaction. One slow `/db/subscribe` stalled every subscription.

## Guarantees the lock provided

1. **CDC decoding.** `write_with_options` doesn't await durability, so the WAL can be flushed
   between the SlateDB write and `apply_schema_update`. CDC must not decode transaction T with a
   schema that doesn't cover T yet, or it fails with `Unknown attribute entity_id` and the loop ends.
2. **Registration basis.** `register_query` needs a basis at or after every transaction CDC has
   already applied, or the initial scan misses T and CDC never resends it.
3. **`TxWaiter` baseline.** Subscribing to the completion broadcast and reading the baseline must
   not miss a transaction in between.
4. **`db()` consistency.** The basis `tx_key` and `ident_map` must come from the same point, so the
   `ident_map` covers every attribute visible at the basis. `5536b266` fixed this under the lock.

## Approach

The indexer publishes `IndexedState { tx_key, schema: Arc<Schema> }` through a
`tokio::sync::watch` channel. Readers hold an `IndexerHandle` (watch receiver, completion broadcast
sender, SlateDB `Arc`) and never touch the lock.

`watch` over `ArcSwap`: both give cheap reads of the latest value, but `db_as_of`, startup and CDC
also need to wait until tx T is published. `watch::Receiver::wait_for` checks the current value and
then waits, and errors when the indexer is dropped. `ArcSwap` has no wait, so each waiter would pair
it with the broadcast and repeat the subscribe-then-load ordering and lag handling `TxWaiter` already
needs. A std `RwLock<Arc<_>>` is `watch` without the notification.

How each guarantee holds now:

1. CDC reads the tx id from the first EAV key's `tx_eid` (no schema needed), waits until the
   published state reaches it, and decodes with that state's schema.
2. CDC applies T only once published ≥ T, and applies it under `registration_gate`.
   `register_query` reads the published state under the same gate, so its basis covers every
   applied tx.
3. The indexer publishes before broadcasting; `IndexerHandle::tx_waiter` subscribes before reading
   the baseline.
4. `db()` takes `tx_key` and `ident_map` from one published value.

## Implementation

- `src/indexer.rs`
  - `IndexedState` in a `watch::Sender`, replacing `latest_indexed_tx`.
  - `publish_indexed` runs before the completion broadcast on every path: commit, abort, and the
    two `Failed` paths (deserialization failure, failed aborted-tx write). Published ≥ T means the
    indexer is done with T, matching what the broadcast-based `await_indexed` used to mean.
    Without this, `db_as_of(T)`, startup and CDC would hang on a failed T until the next tx.
  - A new `Arc<Schema>` is built only when a tx changes the schema, outside `send_modify`.
  - `IndexerHandle` with `indexed_state()`, `await_indexed_state(tx_id)` and `tx_waiter()`.
  - `TxWaiter::await_indexed` is removed; `TxWaiter` only serves `execute_tx`, which needs the
    outcome. `Indexer::tx_waiter()` is test-only; `Indexer::latest_tx_key()` is removed.
- `src/node.rs`
  - `Node` holds an `IndexerHandle`. Its `Arc<RwLock<Indexer>>` field is `#[cfg(test)]`; in
    production only the subscription task holds the lock.
  - `db()` reads one published state.
  - `db_as_of_with_timeout` waits once via `await_indexed_state` inside the timeout and takes the
    `ident_map` from the returned state. That state can be later than T; its `ident_map` still
    covers T (`db_as_of` used the current `ident_map` before too).
  - Startup waits for catch-up with `await_indexed_state`; it no longer needs a waiter created
    before `subscribe()`.
  - `execute_tx` gets its waiter from the handle.
- `src/incremental.rs`: `register_query` and `start_cdc_once` take the handle; `NOTE(coverage)`
  describes the new invariant.
- `src/slate/cdc.rs`: `tx_id_from_cdc_transaction`.
- `src/incremental/cdc.rs`: the CDC loop takes the handle and waits per tx as above.

## Tests

- `node::tests::test_readers_complete_while_indexer_write_lock_is_held`: takes the write lock,
  indexes a schema tx through the guard and, still holding it, checks that `db()` (with the new
  attribute in its `ident_map`), `db_as_of`, query registration and the CDC delta all complete.
- `indexer::tests::test_deserialization_failure_notifies_failed` also asserts the failed tx is
  published.

## Open points

- If a SlateDB write lands but reports an error and the follow-up aborted-tx write also fails, the
  tx is published as processed with the old schema. If that tx changed the schema, CDC fails to
  decode it. This is the existing #118 error-escalation gap; before this change CDC could hit the
  same thing.
- `Node` keeps no `Arc` to the indexer outside tests, so if the subscription task ends (log
  closed), the watch sender is dropped and `await_indexed_state` returns an error instead of
  hanging until the `db_as_of` timeout. `execute_tx` still hangs in that case: `IndexerHandle`
  holds a broadcast sender clone so `TxWaiter`s can subscribe, which keeps the broadcast open.
  `Node::close` shuts down the CDC loop before cancelling the subscription, so normal shutdown
  doesn't hit this.

## Follow-up

Only the subscription task and tests use the lock now. `subscribe()` could take ownership of the
`Indexer` and drop the `RwLock`; the remaining test users (`src/node.rs` partition-map tests,
`src/indexer.rs` waiter tests, the `MockSubscriber` tests in the log modules) would need adjusting.
