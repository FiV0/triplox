# Rust / Tokio Anti-Pattern Audit — triplox

Audit of the real source tree (`src/`, `edn/`, `triplox-client/` — ~39k LOC),
excluding `#[cfg(test)]` code and the `.claude/worktrees` copies. Findings are
ordered by severity. The highest-impact claims (EDN-parser reachability, the
`spawn_blocking` bridge, the panic strategy) were verified directly against the
code.

## Headline verdict on the async architecture

The scariest-looking pattern — sync iterators calling `handle.block_on(...)`
(`slate_iterator.rs`, `temporal_filter_iterator.rs`) to drive async SlateDB
reads — **is correct, not a live bug.** Every production caller wraps the
synchronous join in `spawn_blocking`:

- `node.rs:126` — `query_with_args` → `spawn_blocking(|| execute_query(...))`
- `schema.rs:857` — `load_schema_from_indices` does the same
- `incremental.rs:287` — `send_delta`'s `runtime.block_on` runs on a dedicated
  OS thread (`thread::Builder::spawn`), not a runtime worker

`Handle::block_on` from a `spawn_blocking` pool thread is permitted (it panics
only from a *runtime worker* thread). So this is the sanctioned sync-over-async
bridge.

**But the safety is an unenforced invariant.** Nothing on `SlateIterator` /
`GenericPrefixExtender` / `execute_query` stops a future caller from invoking it
directly from an `async` task on the multi-threaded runtime — the moment that
happens, every `block_on` becomes a
`"Cannot block the current thread from within a runtime"` panic.

Recommendations:
1. Document the contract loudly on `execute_query`.
2. Add a `debug_assert!(Handle::try_current().is_err() || in_blocking_context)`
   guard, or migrate the join to async long-term.

There is also a real throughput cost — one blocking thread is pinned per
in-flight query, and `count()` / `propose()` re-open a fresh SlateDB scan on
every call (see the performance section).

---

## 1. Async / Tokio anti-patterns

### HIGH — Blocking `std::fs` / `fsync` inside `async fn` on runtime workers

These block a Tokio worker thread for the duration of disk I/O — the canonical
Tokio anti-pattern.

- **`file_log.rs:73-97`** — `read_txs_after` is `async` but does fully
  synchronous `OpenOptions::open` + `seek` + a loop of
  `bincode::deserialize_from(&mut file)` (blocking `read` syscalls). Invoked from
  the spawned subscriber catch-up loop, so it stalls the runtime. `kafka_log.rs:411`
  already does this right via `spawn_blocking` — mirror that.
- **`file_log.rs:119-121`** — `append_tx` / `ensure_bootstrap_record` hold the
  `tokio::sync::Mutex` guard while calling synchronous `flush()` + `sync_data()`
  (an `fsync`, often multi-ms). Blocking **and** lock-holding on a worker.
- **`slate/mod.rs:125`**, **`node.rs:260/267/282/303`** — synchronous
  `std::fs::create_dir_all(...)` inside `async fn` node/slate constructors.
  Startup-only, so MED, but should be `tokio::fs` or `spawn_blocking`.

### MED — Dropped `JoinHandle` hides panics

- **`log.rs:104-155`** — `subscribe` spawns the subscriber task and returns only
  a `CancellationToken`; the `JoinHandle` is dropped, so a panic in the task is
  silently swallowed and shutdown can't be awaited. Retain the handle (or use a
  `TaskTracker`). By contrast, `subscription.rs:99/117` and `incremental/cdc.rs:69`
  handle their handles correctly (abort-on-drop / awaited).

### MED — No client-side timeouts

- **`triplox-client/src/client.rs:60`** — `reqwest::Client::builder()` sets no
  `.timeout()` / `.connect_timeout()`, and no `send().await` / `bytes().await` is
  wrapped in `tokio::time::timeout`. A stalled server hangs the client
  indefinitely (reqwest uses HTTP/2 here, so one stuck stream matters).

### LOW — Unbounded command channel

- **`incremental.rs:100/118`** — the service `commands` channel is an unbounded
  `std_mpsc`; no backpressure if the service thread stalls. Low risk since each
  caller awaits a oneshot reply, but worth bounding.

### Already correct (good)

- The subscription stream uses a **bounded** `mpsc` with `biased`
  cancellation-safe `select!` (`server.rs:364`).
- The `cdc.rs` poll loop is cancellation-safe.
- Only **1 `unsafe` block** in the whole codebase.

---

## 2. Panics reachable from untrusted / external data (most serious)

### HIGH — EDN parser `.unwrap()` panics on attacker-controlled query text

**Verified reachable from the network:** `server.rs:187` runs
`query_request.query.into_query()` on the client-supplied query string →
`edn::parse::parse_query`. The grammar matches unbounded digit runs, then
unwraps the conversion:

- **`parse.rs:62`** `raw_integer`: `i.parse::<i64>().unwrap()` — panics on
  `99999999999999999999999999`
- **`parse.rs:56/58`** `raw_octalinteger` / `raw_hexinteger`:
  `from_str_radix(...).unwrap()` — overflow panic on long octal/hex
- **`parse.rs:60`** `raw_basedinteger`: two `.unwrap()`s (radix + digits)
- **`parse.rs:108/111`** & **`116/119`** `inst_micros` / `inst_millis`:
  `parse::<i64>().unwrap()` + `DateTime::from_timestamp(...).unwrap()`
  (out-of-range)
- **`parse.rs:127`** `uuid_string`:
  `Uuid::parse_str(u).expect("this is a valid UUID string")`

A malformed query aborts the connection task (default `unwind`, no `CatchPanic`
layer) instead of returning a clean `ParseError` 400, dropping other requests
multiplexed on that HTTP/2 connection. **The fix is already demonstrated in the
same file** — `inst_string` (line 100) uses the fallible
`{? ... .map_err(...) }` form. Convert all the integer/inst/uuid rules to match.

### MED — Codec decoders: guarded-`unwrap` fragility on untrusted bytes

- **`codec.rs`** (lines ~125/139/156/180/207/320, and `cursor[0]` reads
  throughout) — `cursor[..N].try_into().unwrap()` decoding on-disk/index/network
  bytes. Currently *safe* (each has a preceding length check), but correctness
  depends on every guard staying in sync with every slice. Convert to
  `cursor.get(..N).ok_or(DecodeError)?` / `.split_first_chunk::<N>()` /
  `cursor.first()` so bounds-safety is local and a guard regression can't crash
  on corrupt input.

### MED/HIGH — Schema load panics on corrupt persisted data (startup)

- **`schema.rs:888-936`** — `load_schema_from_indices` returns `Schema` (not
  `Result`), so it uses `.expect("Schema-load query failed")` and a wall of
  `panic!("Expected Long for entity_id, got ...")` + `row[0..3]` indexing on data
  read from the indices. Corrupt/unexpected index contents crash the process at
  startup. Make it `Result<Schema>` and `?` through.

### MED — Unchecked length math on keys read from storage

These underflow-panic (debug) / wrap-then-slice-panic (release) on short/corrupt
keys, on the read hot path:

- **`util.rs:72, 122-160, 204-219`** — `total_length - codec::ENTITY_LENGTH -
  codec::TX_EID_OP_SUFFIX` style subtractions in the key extractors.
- **`temporal_filter_iterator.rs:104`** `assert!(key.len() >= ...)` and the
  `&key[.. len - SUFFIX]` slices (lines 41/112).
- **`tx.rs:314`** `assert!(key.len() >= TX_EID_OP_SUFFIX)`.

Note `indexer.rs:709` (`strip_temporal_key`) already does this correctly by
returning `Err("Key too short")` — be consistent and return errors.

### MED — Panic on valid-but-unsupported client input

- **`tx.rs:235`** — `expand_tx_ops` does
  `panic!("Delete/Erase not yet implemented")` on `TxOp::Delete` / `Erase`. A
  client submitting a Delete op crashes the tx task. Use `bail!`.

---

## 3. Panics on internal invariants (contained, but should be `Result`)

- **`generic_prefix_extender.rs:120/131/166/177/186/196/204`** (HIGH within the
  query engine) — `propose` / `count` / `intersect` / iterator-creation do
  `.unwrap_or_else(|e| panic!(...))` (marked `// TODO: proper error handling`).
  Because the `PrefixExtender` trait is infallible, **routine SlateDB I/O errors
  during a normal query become panics.** Make the trait fallible (`-> Result`).
  Same class: `generic_predicate_prefix_extender.rs:59/66` and
  `generic_fn_prefix_extender.rs:45` `.expect("failed to decode...")` on
  intermediate query values.
- **`server.rs:508`** (HIGH) — `DevServer::listen_on`:
  `Arc::try_unwrap(node).unwrap_or_else(|_| panic!("refcount should be 1"))`. If
  any spawned task or live subscription still holds a node `Arc` at close, this
  panics the connection task. Use a shared-`Arc` shutdown path. (Dev-only, so
  lower real-world blast radius.)
- **`node.rs:96`** (HIGH) — `pub fn entity(&self, _eid) { todo!() }` — a public
  method that unconditionally panics. Remove or gate it.
- **`bootstrap.rs:107-143`** — the fresh-DB `init_db` branch uses ~7 `.unwrap()`s
  (plus `assert!` / `assert_eq!`) even though the fn already returns `Result`; a
  first-init write error panics instead of propagating. Replace with `?`.
- **`circuit.rs:66/86/92/117`** — `.expect` + unchecked `row[*position]` indexing
  inside DBSP worker closures during query build; a planner/circuit width
  mismatch panics on a worker thread.
- **`query.rs:799/833/843/854`** — `tuple[idx]` indexing in projection/
  aggregation; a short join tuple panics. Use `.get(idx).ok_or(...)`.
- **`inc_query.rs`** — several `unreachable!()` guarded only by validation order;
  prefer `bail!` for refactor-safety.
- **`main.rs:32`** — `signal(SIGTERM).expect(...)` inside a detached spawn; a
  failure silently loses shutdown signaling. **`server.rs:116`** —
  `encode_error_body(...).expect("infallible")` while building an error response
  from arbitrary runtime strings.

---

## 4. General non-idiomatic Rust / performance

- **`generic_and_prefix_extender.rs:77`** (perf, MED) — `sort_by_key(|c|
  c.count(prefix))` recomputes `count()` O(n log n) times, and **each `count()`
  re-opens a SlateDB scan via `block_on`**. Use `sort_by_cached_key` /
  precompute counts once. Compounds the per-query blocking-thread cost.
- **`index.rs:13-20`** — `Bytes → Vec → Bytes` round-trip just to add/strip one
  prefix byte; `Bytes::slice(1..)` is zero-copy.
- **`indexer.rs:468`** — `HashSet → Vec` collect loses deterministic datom
  ordering; harmless only if nothing downstream depends on order (worth a
  comment).
- **`edn/types.rs:654-680`** — `FromMicros` / `FromMillis` trait impls panic via
  `from_timestamp(...).unwrap()`; `protocol::micros_to_datetime` is the correct
  non-panicking version — dedupe onto it.
- Minor: `Box<dyn Error>` in `transaction.rs:15` (crate otherwise uses
  `anyhow`); eager `unwrap_or(vec![])` vs `unwrap_or_default()`
  (`edn/query.rs:1075`); needless query `.clone()` in `IntoQuery` (`ops.rs:53`,
  already TODO'd).

---

## Suggested fix priority

1. **EDN parser overflow panics** (`parse.rs`) — convert integer/inst/uuid rules
   to fallible `{?}`; small, self-contained, closes a network-reachable DoS.
   *(Quick win.)*
2. **Blocking I/O in async** — wrap `file_log.rs` reads/flush/fsync in
   `spawn_blocking`; switch `create_dir_all` to `tokio::fs`.
3. **Make `PrefixExtender` fallible** — stops query-time SlateDB errors from
   panicking; also fixes `schema.rs` load and `circuit.rs` to return `Result`.
4. **Codec & key-length math** — `get(..N)?` / `checked_sub` so corrupt bytes
   can't panic.
5. **Document + guard the `block_on` / `spawn_blocking` invariant**; retain the
   `log.rs` subscriber `JoinHandle`; add client timeouts.
