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
- **`bootstrap.rs:107-143`** — the fresh-DB `init_db` branch uses ~7 `.unwrap()`s
  (plus `assert!` / `assert_eq!`) even though the fn already returns `Result`; a
  first-init write error panics instead of propagating. Replace with `?`.
- **`main.rs:32`** — `signal(SIGTERM).expect(...)` inside a detached spawn; a
  failure silently loses shutdown signaling.
- **`server.rs:116`** —
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

### 5.1 Async / concurrency (new)

- **HIGH — One slow subscriber stalls the entire node (head-of-line
  blocking).** The incremental-query subsystem runs on a *single* dedicated OS
  thread behind a *single* command channel. When the CDC loop applies a tx it
  holds the async `registration_gate` across the whole apply
  (`incremental/cdc.rs:103-104`), and the per-query fan-out
  (`incremental.rs:409-428`) calls `send_delta`, which `block_on`s a **bounded
  128-slot** per-subscriber channel (`incremental.rs:287`). If one HTTP
  subscriber stops reading, its channel fills → `send_delta` blocks the one
  service thread indefinitely → because `registration_gate` is held across the
  stalled `apply_triples().await`, this simultaneously (a) freezes delta
  delivery to *every other* query, (b) blocks *all* new `register_query` calls
  (they wait on the same gate, `incremental.rs:142`), and (c) halts CDC/WAL
  progress node-wide. Per-connection backpressure becomes a global stall; only
  shutdown-cancel recovers. (The `block_on` at 287 is mechanically fine on a
  dedicated thread — it's the *global* consequence that's the bug.)
- **MED — Blocking `thread::join()` inside `Drop` on a possible runtime worker.**
  `kafka_log.rs:170-178` `LiveConsumer::drop` calls `handle.join()`; the
  consumer only checks its stop flag between `poll(Timeout::After(100ms))` calls
  (`kafka_log.rs:207`). `LiveConsumer` is owned by `KafkaLog` (`_live_consumer`,
  `:247`), so dropping a node from async shutdown code can block a Tokio worker
  for up to ~100 ms.
- **MED — Wrong lock primitive: async guard never crosses an `.await`.**
  `file_log.rs` uses `tokio::sync::Mutex<FileLogState>` and `memory_log.rs` uses
  `tokio::sync::RwLock<MemoryLogState>`, but every critical section is fully
  synchronous (`stream_position`/`serialize_into`/`flush`/`sync_data`, resp.
  `clock.now()` + `Vec::push`) — no `.await` under any guard. These should be
  `std::sync::Mutex`/`RwLock`. In `file_log` the async mutex also *obscures* that
  it holds a lock while doing a blocking `fsync` (the §1 blocking-fsync finding).
- **LOW — Dropped `JoinHandle` on the signal-handler spawn** (`main.rs:26-47`) —
  same class as the `log.rs` §1 finding; a panic in the SIGINT/SIGTERM handler
  is silently swallowed and shutdown is never signalled. (Process-lifetime task,
  so low blast radius.)

### 5.2 Error handling (new)

- **HIGH — Incremental-query CDC loop dies silently on the first error.**
  `incremental/cdc.rs:94-104`: every step uses `?` (`next_transaction`,
  `datoms_from_cdc_transaction`, `tx_key_from_datoms`, `datoms_to_tuples`,
  `apply_triples`). On any `Err` the task returns and exits. `spawn_cdc_loop`
  (`cdc.rs:69`) is a bare `tokio::spawn`; the `JoinHandle<Result<()>>` is
  inspected **only** at shutdown (`await_cdc_task`, `incremental.rs:250`), never
  during operation, and the error path logs *nothing* (only the Ok path logs
  `info!`). Per-query delta senders stay alive, so subscribers' receivers never
  see a close and client subscriptions (`server.rs:382` `deltas.recv()`) hang
  forever with no error frame. A single transient object-store/WAL read error
  thus silently kills the whole incremental-query subsystem. Contrast
  `log.rs:79-89` `catch_up_transactions`, which retries reads with exponential
  backoff — mirror that.
- **MED — `FileLog` treats mid-file corruption as clean EOF.**
  `file_log.rs:90-95` (and `83-86`): `bincode::deserialize_from(...) Err(_) =>
  break // EOF or corrupted data`. bincode returns `Err` for both clean EOF and
  genuine corruption and the code can't distinguish them, so a torn record
  mid-file is read as end-of-log. Since `catch_up_transactions` treats a
  short/empty batch as "caught up" (`log.rs:60/72`), every record after the
  corruption is silently dropped and the subscriber stops — no error logged.
- **MED — `anyhow` errors flattened to `String` across an internal thread
  boundary.** `incremental.rs:73` `type ServiceResult<T> = Result<T, String>`,
  with `map_err(|err| format!("{:#}", err))` at `:370/:373/:418`, re-wrapped as
  `anyhow!("{}", err)` at callers (`:213/:224/:245/:259`). `QueryCircuit` errors
  are already `Send + Sync` `anyhow::Error` and channel-safe; stringifying loses
  type/backtrace at an internal (non-wire) boundary.
- **LOW — `broadcast` send failure logged as `warn!` on the normal
  no-subscribers path.** `memory_log.rs:84-90` and `file_log.rs:125-127`:
  `broadcast::Sender::send` only errors when `receiver_count == 0` (a normal
  condition), yet warns on *every* append when nobody is subscribed
  (`memory_log` even carries a TODO questioning this).
- **LOW — `Drop` swallows query-storage removal errors.**
  `incremental.rs:495-498` `let _ = self.remove_all_queries();` discards a
  `Result` that deletes on-disk DBSP storage dirs — filesystem errors silently
  leak storage with no log line.

### 5.4 Performance / hot path (new)

- **MED — `GenericPredicatePrefixExtender::intersect` builds a `HashMap` per
  candidate extension** (`iterator/generic_predicate_prefix_extender.rs:63-78`).
  Prefix bindings are pre-decoded once (good), but a fresh
  `HashMap<Variable, &DataType>` — with a `Variable` (Arc) clone per entry — is
  constructed *inside* the filter closure for every extension. Build the base map
  once and update only the extension-var entry per row.
- **MED — `CountDistinctAccumulator` bincode-serializes every value**
  (`aggregate.rs:47-55`): a per-row `Vec<u8>` into `HashSet<Vec<u8>>`. `DataType`
  already derives `Hash`/`Eq`, so `HashSet<DataType>` (`value.clone()`) drops the
  per-row allocation and the serialize step for scalar variants.
- **LOW (churn worth noting):** `apply_extensions` clones the whole `prefix`
  `Vec` per output tuple (`algo/generic_join.rs:20-29`; the final extension could
  consume it); `make_extractor_fn` boxes a fresh `Box<dyn Fn>` per
  `count`/`propose`/`intersect` (`iterator/generic_prefix_extender.rs:139-154`);
  unique-constraint validation double-clones the datom value on the write path
  (`indexer.rs:517-524`).

---

## Discovered during fixes (not yet addressed)

- **LOW — `DataType` `Eq`/`Hash` disagree on NaN.**
  `triplox-client/src/ops.rs:61-63`: `impl Eq for DataType {}` sits on top of a
  derived `PartialEq` (so `Double(NaN) != Double(NaN)`), while the manual
  `Hash` uses `to_bits()` (so both NaNs hash equal). This violates the
  `Eq`/`Hash` contract for NaN values: hash-based containers
  (`HashSet<DataType>`, e.g. in `CountDistinctAccumulator`) will treat each
  NaN as distinct despite identical hashes. Decide on bitwise equality
  (compare via `to_bits()` in `PartialEq`) or exclude NaN at ingestion.

---

## Checklist

- [x] 1. `file_log.rs`: `read_txs_after` blocking file I/O in async fn → `spawn_blocking` (§1 HIGH)
- [x] 2. `file_log.rs`: blocking `flush`/`sync_data` while holding lock in `append_tx`/`ensure_bootstrap_record` → `spawn_blocking` + `std::sync::Mutex` (§1 HIGH; also covers the `file_log` half of §5.1 wrong-lock-primitive)
- [x] 3. `slate/mod.rs`, `node.rs`: sync `create_dir_all` in async fns → `tokio::fs` (§1 MED)
- [x] 4. `log.rs`: `subscribe` drops the subscriber task `JoinHandle` → retain it (§1 MED)
- [x] 5. `triplox-client/src/client.rs`: no client-side timeouts on reqwest client (§1 MED)
- [x] 6. `edn/src/parse.rs`: integer/inst/uuid rules `.unwrap()` on attacker-controlled query text → fallible `{? ...}` rules (§2 HIGH)
- [x] 7. `codec.rs`: guarded-`unwrap` decoding on untrusted bytes → locally bounds-safe `get(..)`/`split_first_chunk` + `DecodeError` (§2 MED)
- [x] 8. `util.rs`, `temporal_filter_iterator.rs`, `tx.rs:314`: unchecked key-length math → return errors like `indexer.rs` `strip_temporal_key` (§2 MED)
- [x] 9. `tx.rs:235`: `panic!` on `TxOp::Delete`/`Erase` → `bail!` (§2 MED)
- [x] 10. `generic_prefix_extender.rs` & friends: make `PrefixExtender` trait fallible; remove `panic!`/`expect` on SlateDB I/O and decode errors (§3 HIGH)
- [x] 11. `server.rs:508`: `DevServer::listen_on` `Arc::try_unwrap(...).unwrap_or_else(panic!)` → shared-`Arc` shutdown (§3 HIGH)
- [x] 12. `bootstrap.rs:107-143`: `init_db` unwraps/asserts in `Result` fn → `?` (§3)
- [x] 13. `main.rs:26-47`: signal-handler spawn — `expect` on `signal(SIGTERM)` + dropped `JoinHandle` (§3 + §5.1 LOW)
- [x] 14. `server.rs:116`: `encode_error_body(...).expect("infallible")` → fallback response (§3)
- [x] 15. `generic_and_prefix_extender.rs:77`: `sort_by_key` recomputes `count()` → precompute/`sort_by_cached_key` (§4 MED; fixed as part of item 10 — counts are now computed once per child)
- [x] 16. `index.rs:13-20`: `Bytes → Vec → Bytes` round-trip → zero-copy `slice(1..)` (§4; also fixed `remove_index_type` keeping the type byte instead of stripping it)
- [x] 17. `indexer.rs:468`: `HashSet → Vec` ordering comment (§4)
- [x] 18. `edn/types.rs:654-680`: `FromMicros`/`FromMillis` panic → dedupe onto `protocol::micros_to_datetime` (§4; the traits were unused everywhere, so they were removed outright)
- [x] 19. Minor idiom fixes: `Box<dyn Error>` in `transaction.rs:15`, `unwrap_or(vec![])` in `edn/query.rs:1075`, needless query `.clone()` in `ops.rs:53` (§4; the `&ParsedQuery` clone is inherent to the by-value `IntoQuery` contract and stays TODO'd — changing the trait to `Cow` is not worth the API churn)
- [x] 20. Incremental subsystem head-of-line blocking: slow subscriber stalls CDC/registration node-wide (§5.1 HIGH; slow subscribers are now disconnected via `try_send` instead of blocking the service thread)
- [x] 21. `kafka_log.rs:170-178`: blocking `thread::join()` in `Drop` (§5.1 MED)
- [x] 22. `memory_log.rs`: `tokio::sync::RwLock` never held across `.await` → `std::sync::RwLock` (§5.1 MED; `file_log` half done in item 2)
- [x] 23. `incremental/cdc.rs:94-104`: CDC loop dies silently on first error → log + retry like `log.rs` catch-up (§5.2 HIGH)
- [x] 24. `file_log.rs:83-95`: mid-file corruption treated as clean EOF → distinguish EOF from corruption (§5.2 MED)
- [x] 25. `incremental.rs:73`: `ServiceResult<T> = Result<T, String>` → keep `anyhow::Error` across channel (§5.2 MED)
- [x] 26. `memory_log.rs:84-90`, `file_log.rs:125-127`: `warn!` on normal no-subscribers broadcast send (§5.2 LOW)
- [x] 27. `incremental.rs:495-498`: `Drop` swallows `remove_all_queries` errors → log them (§5.2 LOW)
- [x] 28. `generic_predicate_prefix_extender.rs:63-78`: per-extension `HashMap` rebuild → build base map once (§5.4 MED)
- [x] 29. `aggregate.rs:47-55`: `CountDistinctAccumulator` bincode round-trip → `HashSet<DataType>` (§5.4 MED)
- [x] 30. Churn: `apply_extensions` prefix clone, `make_extractor_fn` re-boxing, `indexer.rs:517-524` double clone (§5.4 LOW)
