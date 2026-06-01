# Implementation Plan: Incremental Streaming Client API

Plan for [STREAMING_CLIENT.md](STREAMING_CLIENT.md) (Phase 2/3 of the gated
spec-driven workflow). Read-only planning artifact — **no code until the plan is
signed off** (Checkpoint reviews are explicit below).

## Overview

Expose the writer-node incremental query engine (merged in #311) to clients via a
new `POST /db/subscribe` endpoint that streams self-delimiting msgpack frames over
one long-lived HTTP/2 response. Implement the `subscribe → Subscription → take`
surface in the Rust, Java, and Clojure clients, plus a protocol-doc sync and one
example per client. The server engine (`src/incremental.rs`) is **unchanged**;
all work is wire + client + a thin server handler.

## Architecture Decisions

Transport and per-client shapes are settled in the spec (D1–D5). Plan-level
decisions:

- **Contract first, then parallelize.** The wire frame/request codec (Task 1) is
  the shared contract; the server handler and the Rust client are written against
  it independently, then joined in an e2e task. (Skill: "features that share an
  API contract — define the contract first.")
- **Risk-first vertical slice.** Prove the whole transport (server streaming body
  + client framed decode + disconnect-teardown + backpressure) on the **Rust↔server**
  path first (Checkpoint B), because Rust and server share the codec and fail
  cheapest. Only then invest in the JVM and Clojure slices.
- **No new server dependency.** `futures 0.3` is already a workspace dep, so the
  streaming `Body` is built with `futures::stream::unfold` over the subscription
  receiver — write-one/recv-one, so HTTP/2 flow control backpressures the engine
  (no unbounded buffer). `async-stream` would be more readable but is a new dep
  (avoid; Ask-First only if we decide readability wins).
- **Client deps are the only Ask-First gate** (Task 2): `reqwest` `stream`
  feature, `tokio-util` (`io`,`codec`), `futures` added to `triplox-client`.
- **Shared fixtures.** Rust and Java codec tests decode the **same** frame byte
  fixtures so the two independent codecs can't drift from the contract.

---

## Task List

### Phase 1 — Foundation: the wire contract

#### Task 1: Wire contract — frame & request codec + `2004` error code
**Description:** In the shared `triplox-client` wire layer, add the subscribe
request type and the three frame kinds with msgpack encode/decode, reusing the
existing `DataType`, `ColumnDescription`, `TxBasis`, and `ErrorResponseBody`
codecs. Add `ErrorCode::IncrementalUnsupported = 2004`.

**Acceptance criteria:**
- [ ] `SubscribeRequest { db: Option<TxBasis>, query, args }` decodes from the §3.2 map (decoder parses `db` as optional; the "must be nil" rule is enforced in the handler, Task 3).
- [ ] A `kind`-tagged frame enum (`Open{basis,columns}`, `Delta{basis:Option,wal_seq,rows:Vec<(Vec<DataType>,i64)>}`, `Error(ErrorResponseBody)`, `Unknown`) round-trips encode→decode; unknown `kind` → `Unknown`; decode is key-order tolerant.
- [ ] `ErrorCode::IncrementalUnsupported` (2004) added to `from_u16`/`as_u16`.

**Verification:**
- [ ] `cargo test -p triplox-client` (new codec unit tests: per-kind round-trip, unknown-kind, 2004 mapping)
- [ ] `cargo build --workspace`

**Dependencies:** None.
**Files:** `triplox-client/src/msgpack_codec.rs`, `triplox-client/src/protocol.rs`, (maybe) `triplox-client/src/lib.rs` re-exports.
**Scope:** M.

#### Task 2: Add client streaming dependencies  ⚠️ Ask-First
**Description:** Add to `triplox-client/Cargo.toml`: `reqwest` `stream` feature,
`tokio-util` with `["io","codec"]`, `futures`. Versions are already pinned at the
workspace root.

**Acceptance criteria:**
- [ ] `triplox-client` builds with `reqwest::Response::bytes_stream`, `tokio_util::io::StreamReader`, `tokio_util::codec::FramedRead`, and `futures::Stream` all resolvable.

**Verification:**
- [ ] `cargo build -p triplox-client`

**Dependencies:** None (blocks Task 4). **Requires maintainer approval before merge** (Boundaries: Ask-First).
**Files:** `triplox-client/Cargo.toml`, workspace `Cargo.toml` (only if a feature needs exposing).
**Scope:** XS.

#### ✅ Checkpoint A (after 1–2)
- [ ] Contract round-trips green in unit tests
- [ ] `cargo build --workspace` clean
- [ ] Dependency additions approved

---

### Phase 2 — Server + Rust e2e (risk-first vertical slice)

#### Task 3: Server `/db/subscribe` handler + streaming body
**Description:** Add the route and handler to `src/server.rs`, shaped like the
existing unary handlers. Decode the request; pre-stream-reject non-nil `db` (400
`InvalidQuery`), parse errors (400 `ParseError`), and unsupported queries (400
`IncrementalUnsupported`). On success call `Node::register_incremental_query`,
write the `open` frame, then build a `Body::from_stream` (`futures::stream::unfold`)
that owns the subscription receiver and emits one `delta` frame per `recv()`
(write-one/recv-one); emit an `error` frame on engine failure / shutdown. The
receiver living inside the body ensures client disconnect drops it → engine
teardown.

**Acceptance criteria:**
- [ ] `POST /db/subscribe` returns 200, `Content-Type: application/vnd.triplox+msgpack`; first body frame is `open` (registration basis + columns).
- [ ] Pre-stream 400 for non-nil `db`, parse error, and unsupported query (`2004`); 500 on internal registration error.
- [ ] Client disconnect drops the receiver → query unregistered and per-query DBSP storage directory deleted.

**Verification:**
- [ ] `cargo test -p triplox` server tests: happy-path stream (open + ≥1 delta), the three pre-stream 400s, disconnect→teardown (assert storage dir gone), mid-stream `error` frame
- [ ] `cargo clippy --workspace --all-targets --all-features`

**Dependencies:** Task 1.
**Files:** `src/server.rs` (handler + route + tests), maybe a small body-builder helper.
**Scope:** M — **highest risk** (novel streaming + teardown wiring).

#### Task 4: Rust client — framed decoder + `Subscription` (Stream) + `Delta` + `subscribe`
**Description:** New `triplox-client/src/subscription.rs`. Implement
`MsgpackFrameDecoder` (`tokio_util::codec::Decoder`) yielding one frame per
complete msgpack value (`Ok(None)` on unexpected-EOF, `Err` on corruption, oversize
guard). `ClientNode::subscribe(query, args)` POSTs, reads the `open` frame
(basis kept; columns internal), and returns `Subscription: futures::Stream<Item =
Result<Delta>>` with `basis()`. Wire `bytes_stream → StreamReader → FramedRead`.

**Acceptance criteria:**
- [ ] `node.subscribe(q, &[]).await?` returns after the `open` frame; `sub.basis()` exposes the registration basis; columns are not exposed.
- [ ] `Stream::next().await` yields a `Delta` per server delta; `error` frame → `Some(Err)` then `None`; clean close → `None`.
- [ ] Decoder reassembles frames split at arbitrary byte boundaries; truncation-at-EOF → error; oversize → `MessageTooLarge`.

**Verification:**
- [ ] `cargo test -p triplox-client` (decoder unit tests with chunked / truncated / corrupt input; `subscribe` parsing of a synthetic in-memory stream)
- [ ] `cargo clippy` clean

**Dependencies:** Tasks 1, 2.
**Files:** `triplox-client/src/subscription.rs`, `triplox-client/src/client.rs`, `triplox-client/src/lib.rs`.
**Scope:** M.

#### Task 5: Rust ↔ server e2e — teardown, backpressure, parity
**Description:** Integration tests that spin a real server + client.

**Acceptance criteria:**
- [ ] subscribe → transact → `Stream::next` yields the expected `(rows, weight)`.
- [ ] `drop(sub)` unsubscribes → server query gone + storage deleted.
- [ ] Slow consumer applies backpressure and loses **no** deltas (assert full sequence received; server memory stays bounded).
- [ ] A subscription's accumulated deltas reconstruct the same result set as a one-shot `/db/query` at the matching basis (parity, mirroring `assert_incremental_matches_db` in `src/node.rs`).

**Verification:**
- [ ] `cargo test -p triplox-client --test subscription` (integration)
- [ ] `cargo test --workspace`

**Dependencies:** Tasks 3, 4.
**Files:** `triplox-client/tests/subscription.rs` (+ optional test helper).
**Scope:** M.

#### ✅ Checkpoint B (after 3–5) — FAIL-FAST MILESTONE — review with human
- [ ] Rust↔server streaming works end-to-end
- [ ] Disconnect→teardown, backpressure-no-loss, and one-shot parity all green
- [ ] `cargo test --workspace` + clippy + `cargo fmt --check` clean
- [ ] **The transport design is validated — get sign-off before the JVM/Clojure slices.**

---

### Phase 3 — JVM client slice

#### Task 6: JVM wire codec — subscribe-body encode + frame decoders
**Description:** Add `WireCodec.encodeSubscribeBody(db, edn, args)` and frame
decoders using `org.msgpack:msgpack-core`, reusing `DataTypeCodec`, `ColumnDesc`,
`TxBasis`. Decode tests consume the **same fixtures** as the Rust codec tests.

**Acceptance criteria:**
- [ ] `encodeSubscribeBody` produces the §3.2 wire map.
- [ ] Each frame kind decodes from the shared fixture bytes; unknown `kind` skipped.

**Verification:**
- [ ] `cd triplox-jvm && ./gradlew test` (`WireCodecTest`)

**Dependencies:** Task 1 (contract + fixtures). Independent of Rust client code.
**Files:** `triplox-jvm/.../client/WireCodec.java`, `.../WireCodecTest.java`.
**Scope:** S–M.

#### Task 7: JVM `Subscription`/`Delta`/`Row` + `TriploxNode.subscribe()` + integration
**Description:** New `Subscription` (`AutoCloseable`; `take()`/`poll(timeout)`/
`basis()`; daemon reader thread reads frames from the blocking `InputStream` via
`MessageUnpacker.unpackValue()` into a bounded `BlockingQueue`), `Delta`, `Row`.
Replace the throwing `subscribe/unsubscribe` stubs on `TriploxNode` with real
`subscribe(edn[, args])` that reads the `open` frame synchronously. `close()`
closes the stream + interrupts the reader; terminal `error` frame → `TriploxException`
from the next `take()`/`poll()`. Integration tests against a live server.

**Acceptance criteria:**
- [ ] try-with-resources subscribe → transact → `take()` yields rows/weight; `poll(timeout)` returns null on timeout.
- [ ] `close()` tears down the server-side query (assert removed).
- [ ] A mid-stream `error` frame surfaces as `TriploxException`.

**Verification:**
- [ ] `./gradlew test` (JVM integration; harness per `triplox-jvm/README.md`)

**Dependencies:** Tasks 3 (server), 6 (codec).
**Files:** `Subscription.java`, `Delta.java`, `Row.java`, `TriploxNode.java`, integration test (5 files).
**Scope:** M.

#### ✅ Checkpoint C (after 6–7)
- [ ] JVM client streams against the server; integration green; existing unary tests still pass

---

### Phase 4 — Clojure, docs, examples

#### Task 8: Clojure `xyz.triplox.api` — `subscribe` / `take!` / `basis`
**Description:** Thin wrappers over the Java `Subscription`. `take!` returns
`[[row weight] …]` (values via `types/wire->clj`), `nil` on close, and `::timeout`
on the bounded arity's expiry (via `poll`). `basis` returns a map.

**Acceptance criteria:**
- [ ] `(with-open [sub (api/subscribe node q)] … (api/take! sub))` matches the §4.3 example shape `[[["Ivan"] 1]]`.
- [ ] `(api/take! sub timeout-ms)` returns `::timeout` on expiry; `nil` on close.

**Verification:**
- [ ] `./gradlew test` (Clojure integration ns)

**Dependencies:** Task 7.
**Files:** `triplox-jvm/.../clojure/xyz/triplox/api.clj`, test ns.
**Scope:** S.

#### Task 9: PROTOCOL.md sync
**Description:** Add `/db/subscribe` to the endpoint table; document the frame
stream (open/delta/error) and self-delimiting framing; add `2004
IncrementalUnsupported`; remove "Live subscriptions" from §6 Future Extensions;
bump version 0.1 → 0.2.

**Acceptance criteria:**
- [ ] PROTOCOL.md documents the implemented wire contract; no stale "not yet implemented" for subscriptions; links resolve.

**Verification:**
- [ ] Manual review against the implemented frames (diff vs STREAMING_CLIENT.md §3)

**Dependencies:** Contract stable (after Task 3; ideally after Checkpoint B). Parallelizable with Phase 3.
**Files:** `design/PROTOCOL.md`.
**Scope:** S.

#### Task 10: Streaming examples (rust / java / clojure)
**Description:** One subscribe example per client mirroring §4 usage (subscribe,
loop over a few deltas, close).

**Acceptance criteria:**
- [ ] Each example builds and, against a local `cargo run` server, prints deltas for a transacted change.

**Verification:**
- [ ] Build each example; manual run against a local server

**Dependencies:** Task 4 (rust), 7 (java), 8 (clojure).
**Files:** `examples/rust/...`, `examples/java/...`, `examples/clojure/...`.
**Scope:** S.

#### ✅ Checkpoint D (Complete)
- [ ] `cargo test --workspace` + clippy + `cargo fmt --check` clean
- [ ] `./gradlew test` green (unary + streaming)
- [ ] Examples run; PROTOCOL.md synced
- [ ] Spec §11 success criteria all met → ready for PR/review

---

## Parallelization

- **Sequential spine:** Task 1 → (2) → 3/4 → 5 → Checkpoint B.
- **After Task 1 (contract defined):** Task 3 (server), Task 4 (Rust client, needs 2), and Task 6 (JVM codec) can proceed **in parallel** — they share only the codec contract.
- **After Checkpoint B:** Task 7 (JVM), Task 9 (PROTOCOL.md) in parallel; Task 8 (Clojure) after 7; Task 10 per client as each lands.
- **Must stay sequential:** Task 1 before all; Task 2 before Task 4; Task 3 before Tasks 5 & 7; Task 7 before Task 8.
- **Safe anytime after contract:** Task 9 (docs).

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| R1 Server streaming body + disconnect→teardown: does dropping the axum response future promptly drop the subscription receiver? | High | Hold the receiver **inside** the body stream (Task 3); Task 5 disconnect test asserts storage removal. Surfaced at Checkpoint B. |
| R2 Backpressure: avoid unbounded buffering; the single shared service thread can stall across queries under a slow client. | Med | write-one/recv-one (Task 3); Task 5 slow-consumer test; shared-thread stall documented as a known, out-of-scope engine limitation (INCREMENTAL_QUERIES.md). |
| R3 New client deps (reqwest `stream`, tokio-util io/codec, futures) | Low–Med | Isolated in Task 2 behind an Ask-First approval gate. |
| R4 OkHttp streaming read over **h2c** with `Protocol.H2_PRIOR_KNOWLEDGE` | Med | Task 7 integration tests cover subscribe + transact on the dev server over one multiplexed HTTP/2 connection. |
| R5 `msgpack-core` blocking `unpackValue()` + interrupt-on-`close()` semantics | Low | Task 7 close/interrupt test. |
| R6 Mapping query-validation errors to `2004` vs parse/internal | Low | Task 3 maps `plan_query`/validation failures; align with `src/query_validation.rs` + `src/inc_query.rs`. |
| R7 Rust decoder distinguishing truncation (need-more) from corruption with `rmpv` | Med | Task 4 unit tests with chunked + truncated + corrupt inputs. |

## Open Questions (implementation-level; resolve while building, non-blocking)

- Does `register_incremental_query` / `plan_query` already reject unsupported
  queries cleanly, or does the handler need to pre-run the one-shot validator?
  Resolve by reading `src/inc_query.rs` + `src/query_validation.rs` in Task 3.
- If OkHttp does **not** stream incrementally over h2c (R4), the fallback is to
  consume the body as an Okio source and re-frame from buffered chunks — same self-delimiting decode,
  different plumbing. Decide in Task 7 only if R4 materializes.

---

*All design-level open questions are resolved in the spec. This plan awaits
sign-off before implementation (Phase 4).*
