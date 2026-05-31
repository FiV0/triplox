# Spec: Incremental Streaming Client API

Status: **DRAFT — Phase 1 (Specify)**, awaiting review before Plan.
Protocol target: **0.2** (adds live subscriptions; see [PROTOCOL.md](PROTOCOL.md) §6).
Relates to: [INCREMENTAL_QUERIES.md](INCREMENTAL_QUERIES.md) (server-side engine, merged in #311).

---

## 1. Objective

PR #311 landed the **server-side** writer-node incremental query engine
(`IncrementalQueryService`): a query can be registered, primed from a snapshot,
and fed WAL deltas to produce a stream of result changes. None of this is
reachable by a client — the wire protocol lists live subscriptions as deferred
([PROTOCOL.md](PROTOCOL.md) §6) and the Java client ships `subscribe`/`unsubscribe`
stubs that throw `UnsupportedOperationException`.

This spec defines the **client-facing incremental streaming API**: the wire
protocol addition that exposes subscriptions, and the idiomatic surface for the
three clients (Rust, Java, Clojure).

A client registers a query once with `subscribe` and receives a **`Subscription`**:
a stateful, closeable handle that yields **deltas** — result-set changes between
consecutive db values — until it is closed. Closing releases the server-side
circuit and its file-backed DBSP storage.

**Success looks like:** a user on any of the three clients can write the
canonical loop —

```
sub = subscribe(node, query)      # registers at latest indexed basis
loop:
    delta = take(sub)             # blocks for the next transaction's changes
    apply(delta.rows)             # [(row, weight)] z-set changes
close(sub)                        # tears down server circuit
```

— and observe exactly the deltas the server-side engine already produces in
`src/incremental.rs`, with no result loss under backpressure and deterministic
cleanup when the stream is closed or dropped.

### Non-goals (v0.2)

- **Historical replay.** Subscriptions start at the latest indexed basis only.
  No starting `Db`/basis argument (the engine cannot replay from an arbitrary
  past basis yet — see INCREMENTAL_QUERIES.md "Current Scope").
- **Reader/secondary-node subscriptions.** Writer-node only, as in #311.
- **Heartbeats / liveness reaping, prepared statements, auth, TLS.** Tracked
  separately; cleanup in v0.2 is driven purely by stream close/drop.
- **Coalesced multi-transaction deltas.** One delta == one WAL transaction's
  changes, matching the engine's current granularity.

---

## 2. Design Decisions

These were settled with the maintainer before writing the spec.

| # | Decision | Choice |
|---|----------|--------|
| D1 | **Transport** | A single long-lived **streaming HTTP/2 response**: `POST /db/subscribe` returns one body carrying an `open` frame (basis + columns) followed by `delta` frames. Close = cancel the HTTP/2 stream → server drops the receiver → automatic teardown. Backpressure rides HTTP/2 flow control. Chosen over a poll/subscription-id model (needs a server registry + heartbeat reaping that does not exist yet) and over SSE (text envelope around binary msgpack). |
| D2 | **Verb / object** | The verb is **`subscribe`** and the returned object is a **`Subscription`** in *all three* clients. The pull method is **`take`** (`take!` in Clojure). No callback or core.async flavor in v0.2. |
| D3 | **Rust shape** | `Subscription` implements `futures::Stream<Item = Result<Delta>>` (so `.next().await` + combinators); `drop` unsubscribes. |
| D4 | **Java shape** | `Subscription implements AutoCloseable`, blocking `take()` + `poll(timeout, unit)` (BlockingQueue-style); `close()` unsubscribes. |
| D5 | **Clojure shape** | `(subscribe node query)` → `Subscription` (a `Closeable`, usable with `with-open`); `(take! sub)` blocks for the next delta, `(take! sub timeout-ms)` for the bounded variant. |

### Assumptions carried into the spec

1. Subscriptions are pinned at the **latest indexed basis** at registration time;
   that basis is reported back in the `open` frame. No initial snapshot rows are
   emitted — only transactions strictly after the basis (matches
   INCREMENTAL_QUERIES.md "Registration Basis").
2. **One delta per transaction.** `take` yields exactly one transaction's worth
   of changes.
3. The **column schema** is sent once, in the `open` frame, then deltas carry
   positional rows. As today, `ColumnDescription.type` is `255` (Unknown) until
   the engine does type analysis.
4. Delta rows are **z-set pairs** `(values, weight)`: `weight > 0` added,
   `weight < 0` retracted. Clients expose the raw signed weight and must not
   assume `±1`.

---

## 3. Protocol Addition

This promotes "Live subscriptions" out of [PROTOCOL.md](PROTOCOL.md) §6 (Future
Extensions) and into the protocol. PROTOCOL.md is updated in lockstep when this
spec is implemented (see Tasks, Phase 3).

### 3.1 Endpoint

| Endpoint        | Method | Request Body | Success Body                                  |
|-----------------|--------|--------------|-----------------------------------------------|
| `/db/subscribe` | `POST` | Subscribe    | **Frame stream** (`Content-Type: application/vnd.triplox+msgpack-stream`) |

Request `Content-Type` stays `application/vnd.triplox+msgpack`. Only the
**response** is a frame stream.

### 3.2 Subscribe Request

```
{"db": <DbBasis>|nil, "query": <str>, "args": [<QueryArg>, ...]}
```

- `db` is **reserved** for future historical replay and **MUST be `nil`/omitted**
  in v0.2. A non-nil `db` is rejected with HTTP 400 / `InvalidQuery`.
- `query` is Datalog EDN text, same as `/db/query`.
- `args` provides `:in` bindings (only Scalar implemented today, as in `/db/query`).

### 3.3 Frame Stream Format

The response body is a sequence of **length-delimited frames**:

```
frame := <u32 length, big-endian> <msgpack map of exactly `length` bytes>
```

The length prefix gives every client an unambiguous frame boundary without a
streaming msgpack parser, and bounds each frame against the existing 64 MB
`DEFAULT_MAX_MESSAGE_SIZE` guard (a prefix exceeding it → client aborts with
`MessageTooLarge`). Each frame map carries a `kind` discriminator, consistent
with the existing tagged-union convention (§2.3 of PROTOCOL.md).

#### `open` frame — always first, exactly once

```
{"kind": "open",
 "basis":   {"tx_id": <int>, "system_time": <Timestamp>, "tx_eid": <int>},
 "columns": [<ColumnDescription>, ...]}
```

`basis` is the registration basis; deltas describe transactions **strictly
after** it. `columns` mirrors `QueryResponse.columns`.

#### `delta` frame — zero or more, one per affecting transaction

```
{"kind": "delta",
 "basis":   {"tx_id": <int>, "system_time": <Timestamp>, "tx_eid": <int>}|nil,
 "wal_seq": <int>,
 "rows":    [[[<DataType>, ...], <int weight>], ...]}
```

Each entry of `rows` is a 2-element array `[values, weight]`: `values` has one
`DataType` per column (positional, per the `open` frame), `weight` is a signed
int. This is the wire form of the server's
`IncrementalQueryDelta { basis, wal_seq, rows: Vec<(Vec<DataType>, isize)> }`.
`basis` is `nil` when the engine could not derive a `TxBasis` for that WAL entry.

#### `error` frame — terminal, optional

```
{"kind": "error", "severity": "E"|"F", "code": <int>,
 "message": <str>, "detail": <str>|nil, "hint": <str>|nil}
```

Errors that occur **after** the 200 response headers (circuit failure, server
shutdown) cannot change the HTTP status, so they arrive as an `error` frame; the
server then closes the stream. Clients surface it as the stream's terminal error.

> A future `{"kind": "heartbeat"}` keep-alive frame is reserved but out of scope
> for v0.2. Clients MUST ignore unknown `kind` values for forward compatibility.

### 3.4 Status Codes & Pre-stream Errors

Errors detected **before** streaming begins are ordinary non-200 responses with
an [ErrorResponse](PROTOCOL.md#410-errorresponse--non-200-body) body, exactly
like the unary endpoints:

| Status | When |
|--------|------|
| 200 | Subscription registered; frame stream follows. |
| 400 | Decode failure, parse error, non-nil `db`, or unsupported query (`IncrementalUnsupported`). |
| 409 | (reserved) basis not indexed, once historical replay exists. |
| 500 | Internal engine error during registration. |

**New error code:** `2004 IncrementalUnsupported` (range 2xxx, query errors) —
the query is valid for one-shot execution but not yet supported by the
incremental engine (INCREMENTAL_QUERIES.md notes the incremental path lags the
one-shot path and applies an extra restriction check).

### 3.5 Lifecycle & Cleanup

1. On request, the server calls `Node::register_incremental_query`, obtains the
   `IncrementalQuerySubscription`, writes the `open` frame from
   `subscription.basis`, then drains `subscription.deltas`, writing one `delta`
   frame per received `IncrementalQueryDelta`.
2. **Client close** (Rust `drop`, Java `close()`, Clojure `with-open`/`.close`)
   cancels the HTTP/2 stream (RST_STREAM). The server's response future is
   dropped → the `IncrementalQuerySubscription` (and its `mpsc::Receiver`) is
   dropped → the existing teardown runs (unregister, delete per-query DBSP
   storage), exactly as the engine already handles a dropped receiver.
3. **Backpressure:** if the client stops reading, HTTP/2 flow control stalls the
   server's body writes, which stops draining the bounded receiver
   (`SUBSCRIPTION_CAPACITY`), applying backpressure into the circuit — the
   server-side behavior INCREMENTAL_QUERIES.md already describes.
4. **Server shutdown:** emit an `error` frame (`ServerShuttingDown`, 4004) then
   close, or close the stream directly.

---

## 4. Client APIs

`subscribe` lives on the **node** in every client (there is no basis to pin in
v0.2). Deltas/columns share the existing `DataType`/`ColumnDescription` mappings.

### 4.1 Rust (`triplox-client`)

New module `triplox-client/src/subscription.rs`.

```rust
impl ClientNode {
    /// Register an incremental query at the latest indexed basis.
    /// Opens the HTTP/2 stream and reads the `open` frame before returning.
    pub async fn subscribe(
        &self,
        query: impl IntoQuery,
        args: &[QueryArg],
    ) -> Result<Subscription>;
}

/// Live result stream. Dropping it unsubscribes (cancels the HTTP/2 stream).
pub struct Subscription { /* basis, columns, framed byte stream */ }

impl Subscription {
    pub fn basis(&self) -> TxBasis;
    pub fn columns(&self) -> &[ColumnDescription];
}

/// `futures::Stream<Item = Result<Delta>>` — use `.next().await` + combinators.
impl futures::Stream for Subscription { type Item = Result<Delta>; /* ... */ }

pub struct Delta {
    pub basis: Option<TxBasis>,
    pub wal_seq: u64,
    pub rows: Vec<(Vec<DataType>, i64)>, // (values, weight)
}
```

Implementation notes:
- `reqwest::Response::bytes_stream()` → `tokio_util::io::StreamReader` (AsyncRead)
  → `FramedRead<_, LengthDelimitedCodec>` yields one frame's bytes per poll;
  decode each with the shared msgpack frame decoder.
- An `error` frame maps to `Some(Err(..))` then stream end; a clean server close
  maps to `None`.
- Keep `Delta` minimal — no `added()/retracted()` convenience wrappers (those
  belong in user code; cf. the project's "no trivial wrappers" guidance).

### 4.2 Java (`triplox-jvm`)

Replaces the throwing `subscribe(Db, String)` / `unsubscribe()` stubs on
`TriploxNode`.

```java
public Subscription subscribe(String edn) throws IOException;
public Subscription subscribe(String edn, List<QueryArg> args) throws IOException;

public final class Subscription implements AutoCloseable {
    public TxBasis basis();
    public List<ColumnDesc> columns();
    public Delta take() throws InterruptedException;                 // blocks; null at end
    public Delta poll(long timeout, TimeUnit unit) throws InterruptedException; // null on timeout
    @Override public void close();                                   // unsubscribe
}

public record Delta(TxBasis basis, long walSeq, List<Row> rows) {}
public record Row(List<Object> values, long weight) {}
```

Implementation notes:
- `HttpClient.send(req, BodyHandlers.ofInputStream())` → read the 4-byte length +
  frame bytes, decode with `org.msgpack:msgpack-core`'s `MessageUnpacker`.
- `subscribe(...)` reads the `open` frame synchronously, so `basis()`/`columns()`
  are populated on return; a daemon reader thread then pushes decoded `delta`
  frames into a bounded `BlockingQueue<Delta>` that backs `take()`/`poll()`.
- `close()` closes the `InputStream` (cancels the HTTP/2 stream) and interrupts
  the reader; a terminal `error` frame surfaces as a `TriploxException` from the
  next `take()`/`poll()`.

### 4.3 Clojure (`xyz.triplox.api`)

Thin wrapper over the Java `Subscription` (already `AutoCloseable` → `with-open`).

```clojure
(defn subscribe
  "Register an incremental query at the latest indexed basis.
   Returns a Subscription (Closeable)."
  (^xyz.triplox.client.Subscription [conn query] ...)
  (^xyz.triplox.client.Subscription [conn query & args] ...))

(defn take!
  "Block for the next delta. Returns a vector of [row-values weight] pairs
   (values converted via types/wire->clj), or nil when the stream is closed.
   The 2-arity bounds the wait; returns ::timeout on expiry."
  ([sub] ...)
  ([sub timeout-ms] ...))

(defn basis [sub] {:tx-id ..., :system-time ..., :tx-eid ...})
```

Usage mirrors the hooray2 design the maintainer prototyped, with `subscribe` as
the verb:

```clojure
(with-open [sub (api/subscribe node '{:find [name] :where [[e :name name]]})]
  (api/transact node [{:db/id :ivan :name "Ivan"}])
  (api/take! sub))          ;; => [[["Ivan"] 1]]
```

---

## 5. Tech Stack

- **Server:** Rust, `axum` + `hyper` HTTP/2 (`src/server.rs`); streaming via
  `axum::body::Body::from_stream`. Engine in `src/incremental.rs` (unchanged).
- **Wire codec:** shared `triplox_client::msgpack_codec` (used by both server and
  Rust client) gains Subscribe-request decode and frame encode/decode.
- **Rust client:** `triplox-client`, `reqwest` 0.12 (`http2`); **add** the
  `stream` feature plus `tokio-util` (`io`, `codec`) and `futures` — these are
  new client dependencies (Ask First).
- **JVM client:** Java 17+, `java.net.http.HttpClient`, `org.msgpack:msgpack-core:0.9.8`;
  Clojure wrapper in `xyz.triplox.api`.

---

## 6. Commands

```bash
# Rust (run from repo root / this worktree)
cargo build
cargo test -p triplox            # server + engine
cargo test -p triplox-client     # client streaming
cargo test --workspace           # all members
cargo clippy --workspace --all-targets --all-features
cargo fmt --check

# JVM client (see triplox-jvm/README.md for integration tests)
cd triplox-jvm && ./gradlew test
```

---

## 7. Project Structure

```
design/STREAMING_CLIENT.md      → this spec
design/PROTOCOL.md              → +/db/subscribe, frame defs; bump to 0.2 (edit in lockstep)
src/server.rs                   → subscribe handler + route (streaming Body)
src/incremental.rs              → unchanged (already exposes register/subscription/delta)
triplox-client/src/msgpack_codec.rs → Subscribe-request decode, frame encode/decode
triplox-client/src/subscription.rs  → Subscription, Delta, Stream impl (new)
triplox-client/src/client.rs    → ClientNode::subscribe
triplox-jvm/.../client/Subscription.java, Delta.java, Row.java (new)
triplox-jvm/.../client/TriploxNode.java → real subscribe(), remove stubs
triplox-jvm/.../client/WireCodec.java   → encodeSubscribeBody, frame decoders
triplox-jvm/.../clojure/xyz/triplox/api.clj → subscribe, take!, basis
examples/{rust,java,clojure}/   → a streaming example per client
```

---

## 8. Code Style

Match the surrounding crate idioms. Server handlers stay shaped like the existing
unary handlers in `src/server.rs` — decode, register, map errors to status codes,
then return the streaming body:

```rust
async fn subscribe<L: TxLog + 'static>(
    State(server): State<Arc<Server<L>>>,
    body: Bytes,
) -> Response {
    let req = match decode_subscribe_request(&body) {
        Ok(req) => req,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, ErrorCode::ParseError, e),
    };
    // register_incremental_query -> open frame -> stream delta frames
}
```

One-line comments where possible (AGENTS.md). No trivial field-wrapper methods.

---

## 9. Testing Strategy

| Level | Where | Covers |
|-------|-------|--------|
| Wire codec | `triplox-client` unit tests | Frame round-trips (open/delta/error), length framing, unknown-`kind` skip, oversize-frame rejection. |
| Server handler | `src/server.rs` / `tests/` | 200 stream happy path; pre-stream 400 (bad query, non-nil `db`, `IncrementalUnsupported`); mid-stream `error` frame; client-disconnect → engine teardown (assert per-query storage removed). |
| Rust client e2e | `triplox-client` integration | `subscribe` → transact → `take`/`Stream::next` yields expected `(rows, weight)`; `drop` unsubscribes; backpressure (slow consumer) loses no deltas. |
| JVM client e2e | `triplox-jvm` (gradle) | `take()`/`poll(timeout)` semantics, `close()` teardown, terminal `error` → `TriploxException`. |
| Clojure | `xyz.triplox` integration | `with-open` + `take!`, timeout arity, `[row weight]` shape, `wire->clj` conversion. |
| Parity | server | A subscription's accumulated deltas reconstruct the same result set as a one-shot `/db/query` at the matching basis (mirror the existing `assert_incremental_matches_db` style in `src/node.rs`). |

Tests must pass across `triplox`, `triplox-client`, and the JVM client; clippy
clean; `cargo fmt --check` clean (AGENTS.md).

---

## 10. Boundaries

- **Always:** run the full workspace test suite + clippy + `cargo fmt --check`
  before declaring work done; keep `design/PROTOCOL.md` in sync with any wire
  change in the same PR; preserve decoder tolerance to key order and unknown
  `kind` frames.
- **Ask first:** adding the `reqwest` `stream` feature and `tokio-util`/`futures`
  deps to `triplox-client`; the new `2004 IncrementalUnsupported` error code and
  the `application/vnd.triplox+msgpack-stream` content type; any change to the
  frame format or `delta` row shape; touching `src/incremental.rs`.
- **Never:** expose historical-replay/basis-pinned subscriptions in v0.2 (engine
  can't honor them); silently drop deltas to keep up (backpressure, don't lose);
  leak a registered query when a client disconnects; commit secrets or edit
  generated/vendor files.

---

## 11. Success Criteria

1. `POST /db/subscribe` returns a 200 frame stream: one `open` frame (basis +
   columns) then a `delta` frame per affecting transaction, matching the bytes
   the shared codec round-trips in unit tests.
2. All three clients implement `subscribe` → `Subscription` with the agreed
   surface (D2–D5): Rust `Stream`, Java `take()`/`poll()`, Clojure `take!`.
3. Closing/dropping a subscription tears down the server circuit and deletes its
   per-query DBSP storage (asserted by a server test).
4. A slow consumer applies backpressure and loses **no** deltas.
5. The reconstructed result set from a subscription's deltas equals a one-shot
   query at the same basis (parity test).
6. PROTOCOL.md documents `/db/subscribe`, the frame format, and `2004`; "Live
   subscriptions" is removed from §6 Future Extensions. Workspace tests + clippy
   + fmt are clean; an example exists for each client.

---

## 12. Open Questions

1. **Content type for the stream** — `application/vnd.triplox+msgpack-stream`, or
   reuse `application/vnd.triplox+msgpack` and let framing be implicit? (Spec
   assumes a distinct type.)
2. **`take!` timeout sentinel in Clojure** — `::timeout` keyword vs `nil`. Using
   `nil` collides with "stream closed". Spec assumes a distinct `::timeout`.
3. **Empty-delta frames** — the engine sends only non-empty deltas today. Do we
   ever want explicit "still alive, no change" frames, or is that the future
   heartbeat? (Spec defers to heartbeat.)
4. **Multiple subscriptions per connection** — fine over HTTP/2 multiplexing, but
   do we cap concurrent subscriptions per connection/server? (Out of scope;
   relates to the future liveness/heartbeat work.)
5. **Weight semantics surfaced to users** — expose raw signed weight (current
   plan) vs pre-split added/retracted collections? (Spec keeps raw weight.)

---

## Next Phases (gated — do not start without sign-off)

- **Phase 2 Plan:** order of work (shared codec frames → server handler →
  Rust client → JVM client → Clojure → examples → PROTOCOL.md), parallelizable
  vs sequential, verification checkpoints.
- **Phase 3 Tasks:** discrete ≤5-file tasks with acceptance + verify steps.
- **Phase 4 Implement:** TDD per task.
