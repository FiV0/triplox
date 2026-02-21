# Java/Clojure Client Design

## Context

The Triplox database has a Rust server with a binary wire protocol over TCP. We need a JVM client so Clojure (and eventually Java) applications can connect. The low-level TCP + binary codec lives in Java; the user-facing API is idiomatic Clojure modeled after Datomic. The project structure mirrors `../hooray2` (Gradle + Clojurephant).

## Architecture

```
triplox-jvm/
  src/main/java/io/triplox/client/   ← wire codec + TCP connection (Java)
  src/main/clojure/triplox/          ← public Clojure API
  src/test/java/                     ← codec unit tests
  src/test/clojure/                  ← Clojure API tests
  dev/dev.clj                        ← REPL playground
```

## Implementation Order

### Phase 1: Project Scaffold

Create `triplox-jvm/` with:

- **`settings.gradle.kts`** — root project name + foojay resolver (same as hooray2)
- **`build.gradle.kts`** — Java 17 + Clojurephant `0.8.0-beta.7`, deps: Clojure 1.12.3, tools.logging, logback, JUnit 5, cider-nrepl, jovial. No Kotlin.
- **`.dir-locals.el`** — CIDER → `gradle clojureRepl`
- **`.gitignore`** — `.gradle`, `build`, `.idea`, `.nrepl-port`
- **`gradlew`, `gradlew.bat`, `gradle/wrapper/`** — copy from hooray2 (Gradle 8.10)
- Empty source dirs so `./gradlew build` works immediately

### Phase 2: Java Constants & Type Definitions

**`MessageTypes.java`** — Protocol constants: type bytes (`O`, `L`, `Q`, `E`, `S`, `U`, `X`, `R`, `H`, `J`, `T`, `D`, `C`, `B`, `Z`, `Y`, `G`, `N`, `K`, `W`), DataType tags (1-14, 255), TxOp tags (0-4), status bytes, severity bytes, protocol version (0, 1).

**`TxOp.java`** — Sealed interface with records: `Put(TreeMap<String,Object>)`, `Add(long entity, String attribute, Object value)`, `Retract(...)`, `Delete(long)`, `Erase(long)`.

**`BackendMessage.java`** — Sealed interface with records for all 13 backend message types: `AuthenticationOk`, `DbOpened`, `DbClosed`, `RowDescription`, `DataRow`, `CommandComplete`, `DataBatchComplete`, `ReadyForQuery`, `TxKey`, `TxResult`, `UnsubscribeComplete`, `Heartbeat`, `ErrorResponse`.

**`ColumnDesc.java`** — Record: `name(String)`, `dataType(byte)`.

**`TriploxException.java`** — Exception wrapping `ErrorResponse` fields + `isFatal()`.

### Phase 3: Java Codec — DataType

**`DataTypeCodec.java`** — Encode/decode `DataType` values using 1-byte tag + payload.

Type mappings (wire → Java):
| Wire Tag | Java Type |
|----------|-----------|
| 1 BigInt (i128) | `BigInteger` (validate ≤128 bits) |
| 2 Boolean | `Boolean` |
| 3 Bytes | `byte[]` |
| 4 Double | `Double` |
| 5 Float | `Float` |
| 6 Instant (i64 micros) | `java.time.Instant` |
| 7 Long | `Long` |
| 8 Ref | *Reserved — not yet supported by server; codec throws on encounter* |
| 9 String | `String` |
| 10 Tuple | `List<Object>` |
| 11 Uuid | `java.util.UUID` |
| 12 Vector | `List<Object>` |
| 13 Map | `TreeMap<String, Object>` |
| 14 Keyword | `clojure.lang.Keyword` |

Uses `ByteBuffer` with `BIG_ENDIAN`. Strings: u32 length prefix + UTF-8.

### Phase 4: Java Codec — TxOp

**`TxOpCodec.java`** — Encode `List<TxOp>` for `Execute` messages. 1-byte variant tag + payload per op. Triple = `i64 entity + String attribute + DataType value`. Document = `Map<String, DataType>`.

### Phase 5: Java Codec — Wire Framing

**`WireCodec.java`** — Core codec class. Two concerns:

1. **Write frontend messages** to `OutputStream`:
   - `writeStartup(out, params)` — special: no type byte, just `[length(u32)][major(u16)][minor(u16)][params(Map)]`
   - `writeOpenDb(out, basisTxId)` — `'O' + [len][Option<i64>]`
   - `writeCloseDb(out, dbId)` — `'L' + [len][u32]`
   - `writeQuery(out, queryString, dbId)` — `'Q' + [len][String][u32]` *(confirmed from Rust: db_id not basis_tx_id)*
   - `writeExecute(out, ops, awaitIndexing)` — `'E' + [len][Vec<TxOp>][bool]`
   - `writeSubscribe(out, queryString, dbId)` — `'S' + [len][String][u32]` *(same as Query)*
   - `writeUnsubscribe(out)` / `writeTerminate(out)` — type + `[len=4]`

2. **Read backend messages** from `InputStream`:
   - Read 1 byte (type), 4 bytes (length), (length-4) bytes (payload)
   - Dispatch on type byte → decode into `BackendMessage` record

### Phase 6: Java Connection

**`TriploxNode.java`** — Manages `Socket` with `TCP_NODELAY=true`, `BufferedInputStream`/`BufferedOutputStream`.

```java
public class TriploxNode implements AutoCloseable {
    static TriploxNode connect(String host, int port);
    static TriploxNode connect(String host, int port, Map<String,String> params);

    DbHandle openDb();                          // → DbOpened
    DbHandle openDb(long basisTxId);
    void closeDb(DbHandle db);                  // → DbClosed

    QueryResult query(DbHandle db, String edn); // → RowDesc + DataRows + CommandComplete
    TxKeyResult submitTx(List<TxOp> ops);       // Execute(await=false) → TxKey
    TxResultValue executeTx(List<TxOp> ops);    // Execute(await=true) → TxResult

    void subscribe(DbHandle db, String edn);  // stub — throws UnsupportedOperationException
    void unsubscribe();                        // stub — throws UnsupportedOperationException

    void close(); // Terminate
}
```

Connection flow: `connect()` sends `Startup`, reads `AuthenticationOk` + `ReadyForQuery('I')`.

`QueryResult` is a record containing `List<ColumnDesc> columns` and `List<List<Object>> rows`.

**Thread safety:** `TriploxNode` is **not** thread-safe. The wire protocol is serial (one operation at a time), so callers needing concurrency should use separate connections.

**Subscription:** `subscribe`/`unsubscribe` are stubs. The server does not yet support incremental queries; calling these methods throws `UnsupportedOperationException`.

### Phase 7: Clojure Layer

**`triplox.types`** — `wire->clj` / `clj->wire` conversions. Most types pass through (Long, String, Boolean, etc). Maps: `TreeMap<String,Object>` → Clojure map with keyword keys. Keywords: `clojure.lang.Keyword` both sides.

**`triplox.tx`** — Convert Datomic-style tx-data to `List<TxOp>`:
- `{:db/id 1 :person/name "alice"}` → `TxOp.Put` (keyword keys → string keys via `(subs (str kw) 1)`)
- `[:db/add e a v]` → `TxOp.Add`
- `[:db/retract e a v]` → `TxOp.Retract`
- `[:db/delete eid]` → `TxOp.Delete`
- `[:db/erase eid]` → `TxOp.Erase`

**`triplox.client`** — Public API:
```clojure
(connect host port)           ;; → conn (map wrapping TriploxNode + lock)
(close conn)

(open-db conn)                ;; → db {:conn _ :db-id _ :tx-id _}
(open-db conn {:basis-tx-id 42})
(close-db conn db)

(q db '{:find [?e ?name] :where [[?e :person/name ?name]]})  ;; → vec of vecs

(transact conn [{:db/id 1 :name "alice"}])     ;; → {:tx-id _ :system-time _ :committed? _ :seq-num _}
(submit-tx conn [{:db/id 1 :name "alice"}])    ;; → {:tx-id _ :system-time _}

(subscribe conn db query)              ;; stub — throws, not yet supported by server
```

`TriploxNode` is not thread-safe; the Clojure layer does not add synchronization. Callers needing concurrency should use separate connections.

### Phase 8: Dev/REPL

**`dev/dev.clj`** — Namespace with `(comment ...)` blocks demonstrating connect, transact, query, close.

### Phase 9: Tests

**Java unit tests** (run via JUnit 5):
- `DataTypeCodecTest` — round-trip every DataType variant
- `TxOpCodecTest` — round-trip every TxOp variant
- `WireCodecTest` — message framing, primitive encoding, full message round-trips

**Clojure unit tests** (run via jovial):
- `triplox.types-test` — wire↔clj conversions
- `triplox.tx-test` — Datomic tx-data → TxOp conversion

**Integration tests** (tagged, excluded from default run):
- `TriploxNodeTest.java` — requires running Rust server
- `triplox.client-test` — full Clojure API flow

## Key Files Referenced

- `design/WIRE_PROTOCOL.md` — protocol spec
- `src/protocol.rs` — reference codec implementation (Query/Subscribe use `db_id:u32`)
- `src/client.rs` — reference client (connect flow, Arc<Mutex> wrapping)
- `src/ops.rs` — DataType/TxOp/Document/Triple definitions
- `src/node.rs` — Node/DB trait design
- `../hooray2/build.gradle.kts` — Gradle template to replicate

## Verification

1. `cd triplox-jvm && ./gradlew build` — compiles Java + Clojure, runs unit tests
2. `./gradlew clojureRepl` — starts nREPL with CIDER middleware
3. Java codec round-trip tests pass for all DataType and TxOp variants
4. Integration: start Rust server (`cargo run`), connect from Clojure REPL, transact + query
