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
- **Zero `unsafe`** blocks/impls/fns in the codebase (production *or* tests). The
  only occurrence of the token is a stale doc comment
  (`edn/src/namespaceable_name.rs:191`) inherited from Mentat — even cleaner than
  a prior draft's "only 1 unsafe block."

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

## 5. Second-pass findings (new)

A follow-up sweep (locks/concurrency, untrusted-input robustness, error
handling, hot-path perf) surfaced the items below. Each was verified against the
source; the parser stack-overflow was reproduced empirically.

### 5.0 HIGHEST — EDN parser has no recursion depth limit → whole-process stack overflow (remote DoS)

The single most severe finding in this audit, and a **quick fix**.
`edn/src/parse.rs` is a recursive-descent peg grammar with no depth guard:
`value()` (180-187) → `vector()`/`list()`/`map()`/`set()` (161-176) →
`value()*` recurses on the native stack per nesting level. The where-clause
grammar is likewise self-recursive (`where_clause` 437 → `or_clause` 391 →
`or_where_clause` 387 → `or_and_clause` 384 → `where_clause`). Reached from
**unauthenticated** `POST /db/query` and `/db/subscribe` (`server.rs` handlers)
via `String::into_query` → `parse_query`, in a data-pattern value position
(`pattern_value_place` 343 → `value()`).

**Reproduced:** a body of **~1–2 KB** (≈1000 nested `[` inside a pattern value,
`[:find ?x :where [?e :a [[[[…]]]]]]`) reliably overflows a 2 MiB stack —
Tokio's default worker/blocking-thread stack — and aborts the process with
`SIGABRT` ("thread has overflowed its stack"). The overflow threshold sits
between 500 and 1000 nesting levels; 50k (≈100 KB) is nowhere near the 64 MB
body limit (`server.rs:425`, ~30,000× too generous to mitigate).

This is **strictly worse than the parser `.unwrap()` panics in §2**: those
unwind and abort only the connection *task*, but a stack overflow trips the
guard page and calls `abort()`, killing the **entire node process** (every
multiplexed HTTP/2 stream). A `CatchPanic` layer cannot save it. Fix: thread a
depth counter through the recursive rules (or a cheap pre-parse
bracket-depth/paren-depth check) and reject beyond a sane limit as a clean
`ParseError`.

Related (LOW): the storage codec recurses without a limit too —
`codec.rs` `decode_datatype` (379) → `decode_datatype_payload` (388) →
`decode_composite` (406-418) / `decode_map` (420-440), plus the `skip_*` /
`encode_*` variants. For client data the nesting is capped at write time by
rmpv's msgpack depth limit (~1024), so this is a stack-overflow risk only on
corrupt/adversarial on-disk bytes — a distinct site from the guarded-`unwrap`
fragility in §2.

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

### 5.3 Untrusted-input / resource limits (new)

- **MED — No cap on concurrent incremental-query registrations (subscription
  DoS).** `incremental.rs:312` (`queries` map) / `:376` (insert) / `:409`
  (per-tx fan-out); entry via `server.rs:319` → `node.rs` `register_query`. Each
  `POST /db/subscribe` registers a query held for the lifetime of the HTTP/2
  stream, with no cap; cleanup only reclaims streams the client already dropped.
  An attacker holding many open subscription streams grows the map unboundedly
  **and** multiplies per-transaction indexing work by N (memory + CPU
  amplification). Compounds the 5.1 head-of-line-blocking issue.

*(Verified clean: the 64 MB body limit is present (`server.rs:425`); msgpack
decode is depth-limited (rmpv 1024) and never pre-allocates from claimed lengths;
no truncating `as` casts on the tx/query attack surface; the `regex` predicate
path is linear-time (no ReDoS); response parsing in `triplox-client` checks
status before decoding and guards all slice reads.)*

### 5.4 Performance / hot path (new)

- **MED — `SingleLevelExtender::intersect` rebuilds a `HashSet` over
  already-sorted values every call** (`algo/generic_join.rs:57-64`). `values` is
  sorted in `new()`, yet `intersect` allocates + fills a fresh `HashSet` per
  call — once per prefix for every non-proposing extender (e.g. a `BindColl`
  `:in` binding). Use `binary_search` on the sorted slice instead.
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

## Suggested fix priority

1. **EDN parser recursion depth limit** (§5.0, `parse.rs`) — add a depth
   counter/guard to the recursive rules. A ~2 KB request aborts the *whole
   process*; this is the highest-severity, network-reachable item and is a quick,
   self-contained fix. *(Quick win.)*
2. **EDN parser overflow panics** (`parse.rs`) — convert integer/inst/uuid rules
   to fallible `{?}`; small, self-contained, closes a network-reachable DoS.
   *(Quick win.)*
3. **Silent failures in the incremental-query path** (§5.1/§5.2) —
   (a) don't hold `registration_gate` across `apply_triples`, and bound/parallelize
   delta delivery so one slow subscriber can't stall the node; (b) make the CDC
   loop log + retry (mirror `catch_up_transactions`) instead of dying silently
   and hanging every subscriber.
4. **Blocking I/O in async** — wrap `file_log.rs` reads/flush/fsync in
   `spawn_blocking`; switch `create_dir_all` to `tokio::fs`; switch the
   `file_log`/`memory_log` locks to `std::sync` (§5.1).
5. **Make `PrefixExtender` fallible** — stops query-time SlateDB errors from
   panicking; also fixes `schema.rs` load and `circuit.rs` to return `Result`.
6. **Codec & key-length math** — `get(..N)?` / `checked_sub` so corrupt bytes
   can't panic.
7. **Document + guard the `block_on` / `spawn_blocking` invariant**; retain the
   `log.rs` subscriber `JoinHandle`; add client timeouts; cap concurrent
   subscriptions (§5.3).
