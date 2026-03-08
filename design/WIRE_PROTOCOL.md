# Triplox Wire Protocol Specification

Version 0.1

## Overview

The Triplox Wire Protocol is a stateful, binary protocol over TCP for communication between clients and a Triplox database server. It supports querying via Datalog, submitting transactions, and subscribing to live query result streams. The design is inspired by the PostgreSQL wire protocol (pgwire).

A connection is serial: one operation at a time, no pipelining. A subscription blocks the connection until cancelled.

---

## 1. Framing Format

Every message on the wire uses the following envelope:

```
+------+--------+-------------------+
| type | length |     payload       |
| 1 B  | 4 B   | (length - 4) B    |
+------+--------+-------------------+
```

- **type**: 1-byte message type identifier.
- **length**: 4-byte big-endian unsigned integer. Includes itself (4 bytes) but does NOT include the type byte. Total bytes on the wire = 1 + length. Payload size = length - 4.
- **payload**: Message-specific data, serialized per [Section 11 (Wire Encoding)](#11-wire-encoding).

**Exception**: The initial Startup message from the client has no type byte. It consists only of `[length][payload]`. The server identifies it by context -- the first message on any new connection is always a Startup.

**Maximum message size**: Servers should enforce a configurable limit (default 64 MB). Messages exceeding this limit are rejected with an ErrorResponse.

---

## 2. Version Negotiation

The Startup message carries a protocol version as two 16-bit unsigned integers: major and minor. The initial version is **0.1** (major=0, minor=1).

The server checks the version:
- Compatible: responds with AuthenticationOk.
- Incompatible: responds with ErrorResponse (code 1000) and closes the connection.

---

## 3. Message Catalog

### Frontend Messages (Client to Server)

| Type Byte  | Name        | Description                     |
|------------|-------------|---------------------------------|
| *(none)*   | Startup     | Connection handshake            |
| `O` (0x4F) | OpenDb      | Open a DB snapshot              |
| `L` (0x4C) | CloseDb     | Release a DB snapshot           |
| `Q` (0x51) | Query       | Execute a Datalog query         |
| `E` (0x45) | Execute     | Submit a transaction            |
| `S` (0x53) | Subscribe   | Start a live query subscription |
| `U` (0x55) | Unsubscribe | Cancel the active subscription  |
| `F` (0x46) | BasisForTx  | Look up basis for a transaction |
| `X` (0x58) | Terminate   | Close the connection            |

### Backend Messages (Server to Client)

| Type Byte  | Name                | Description                              |
|------------|---------------------|------------------------------------------|
| `R` (0x52) | AuthenticationOk    | Handshake accepted                       |
| `H` (0x48) | DbOpened            | DB snapshot handle returned              |
| `J` (0x4A) | DbClosed            | DB snapshot released                     |
| `T` (0x54) | RowDescription      | Result schema (column names and types)   |
| `D` (0x44) | DataRow             | One row of result data                   |
| `B` (0x42) | DataBatchComplete   | Subscription batch boundary              |
| `Z` (0x5A) | ReadyForQuery       | Server is ready for the next request     |
| `Y` (0x59) | TxKey               | Transaction submitted (fire-and-forget)  |
| `G` (0x47) | TxResult            | Transaction outcome (awaited indexing)   |
| `A` (0x41) | BasisResult         | Basis for a transaction                  |
| `N` (0x4E) | UnsubscribeComplete | Subscription cancelled                   |
| `K` (0x4B) | Heartbeat           | Subscription keepalive                   |
| `W` (0x57) | ErrorResponse       | Error                                    |

---

## 4. Message Definitions

### 4.1 Startup (Frontend, no type byte)

Sent as the first message after TCP connection is established.

| Field          | Type                  | Description                            |
|----------------|-----------------------|----------------------------------------|
| version_major  | u16                   | Protocol major version (0)             |
| version_minor  | u16                   | Protocol minor version (1)             |
| params         | Map\<String, String\> | Reserved. May contain `client_name`.   |

### 4.2 AuthenticationOk (Backend, `R`)

Sent after successful version negotiation.

| Field          | Type   | Description                          |
|----------------|--------|--------------------------------------|
| server_version | String | Server identifier, e.g. "triplox 0.1.0" |

### 4.3 OpenDb (Frontend, `O`)

Open a DB snapshot. The server pins a point-in-time database view and returns a handle the client uses for subsequent queries. Either all three basis fields are present (pinned snapshot) or all three are absent (latest indexed).

| Field             | Type            | Description                                          |
|-------------------|-----------------|------------------------------------------------------|
| basis_tx_id       | Option\<i64\>   | If set, snapshot at this tx; otherwise latest indexed |
| basis_system_time | Option\<i64\>   | Microseconds since epoch of the tx                   |
| basis_seq_num     | Option\<u64\>   | Sequence number for the snapshot                     |

### 4.4 DbOpened (Backend, `H`)

Confirms a DB snapshot has been opened.

| Field  | Type | Description                                |
|--------|------|--------------------------------------------|
| db_id  | u32  | Server-assigned handle for this DB         |
| tx_id  | i64  | Actual tx_id the snapshot is pinned to     |

Followed by ReadyForQuery with status `I`.

### 4.5 CloseDb (Frontend, `L`)

Release a previously opened DB snapshot.

| Field | Type | Description       |
|-------|------|-------------------|
| db_id | u32  | Handle to release |

### 4.6 DbClosed (Backend, `J`)

Confirms a DB snapshot has been released.

| Field | Type | Description              |
|-------|------|--------------------------|
| db_id | u32  | Handle that was released |

Followed by ReadyForQuery with status `I`.

### 4.7 ReadyForQuery (Backend, `Z`)

Sent when the server is ready for the next client request. Acts as a sync/flush point.

| Field  | Type | Description                                    |
|--------|------|------------------------------------------------|
| status | u8   | `I` (0x49) = idle, `S` (0x53) = subscribed    |

Sent after: AuthenticationOk, DbOpened, DbClosed, DataRow (end of query results), TxKey, TxResult, BasisResult, UnsubscribeComplete, non-fatal ErrorResponse.

### 4.8 Query (Frontend, `Q`)

Execute a one-shot Datalog query against an open DB snapshot.

| Field        | Type   | Description                          |
|--------------|--------|--------------------------------------|
| query_string | String | Datalog query in EDN text            |
| db_id        | u32    | DB snapshot handle to query against  |

The client must open a DB with OpenDb before issuing a Query.

Example query string:
```edn
{:find [?name ?age] :where [[?e :person/name ?name] [?e :person/age ?age]]}
```

### 4.9 RowDescription (Backend, `T`)

Describes the schema of the result set that follows.

| Field   | Type                      | Description       |
|---------|---------------------------|-------------------|
| columns | Vec\<ColumnDescription\>  | One per column    |

Each ColumnDescription:

| Field     | Type        | Description                           |
|-----------|-------------|---------------------------------------|
| name      | String      | Variable name, e.g. "?name"          |
| data_type | DataTypeTag | Type discriminant (see section 10)    |

When `data_type == 127` (Union), two additional fields follow:

| Field        | Type               | Description                                        |
|--------------|--------------------|----------------------------------------------------|
| member_count | u8                 | Number of member types (>= 2)                      |
| member_tags  | [u8; member_count] | Sorted ascending, each a valid concrete tag (1–126) |

**Constraints:**
- `member_count` >= 2 (a single-type union should use the concrete tag directly)
- Each member tag must be a concrete type tag (1–126; tags 0, 127, and 255 are not valid members)
- Tags must be sorted ascending with no duplicates
- Servers SHOULD prefer concrete tags when all values in a column share one type

**Semantics:**
- Every DataRow value in a Union column MUST have a tag matching one of the declared members
- Clients MAY optimize deserialization using the member list (e.g. pre-allocating typed arrays)
- A Union listing all defined concrete types is valid and distinct from Unknown (closed set vs. indeterminate)

> **Note**: The `data_type` field in RowDescription is currently always set to `TAG_UNKNOWN` (255) because the query engine does not yet perform type analysis. The Union type is specified here for future use once the engine can infer output types.

Sent before the first DataRow of a query result or subscription. In subscription mode, the RowDescription includes an extra `tpx_diff` column.

### 4.10 DataRow (Backend, `D`)

One row of result data.

| Field  | Type             | Description                    |
|--------|------------------|--------------------------------|
| values | Vec\<DataType\>  | One value per column           |

The wire format is the same for queries and subscriptions. In subscription mode, rows have an additional `tpx_diff` column (described in RowDescription) with value `+1` for added rows and `-1` for retracted rows. Updates to existing data are represented as a retraction (`-1`) of the old row followed by an addition (`+1`) of the new row, both within the same transaction batch.

### 4.11 Execute (Frontend, `E`)

Submit a transaction.

| Field          | Type          | Description                                        |
|----------------|---------------|----------------------------------------------------|
| ops            | Vec\<TxOp\>   | Transaction operations                             |
| await_indexing | bool          | If true, server waits for indexer before responding |

Uses the `TxOp` enum (Put, Add, Retract, Delete, Erase). See [Section 11.8](#118-txop-encoding) for wire encoding.

### 4.12 TxKey (Backend, `Y`)

Returned for fire-and-forget transactions (Execute with `await_indexing = false`). Maps directly to the `TxKey` type.

| Field       | Type    | Description                                             |
|-------------|---------|---------------------------------------------------------|
| tx_id       | i64     | Transaction ID assigned by the server                   |
| system_time | Instant | Timestamp of the transaction (microseconds since epoch) |

### 4.13 TxResult (Backend, `G`)

Returned for awaited transactions (Execute with `await_indexing = true`). Maps directly to the `TransactionResult` type.

| Field         | Type            | Description                                          |
|---------------|-----------------|------------------------------------------------------|
| status        | u8              | 0 = committed, 1 = aborted                          |
| tx_id         | i64             | Transaction ID assigned by the server                |
| system_time   | Instant         | Timestamp of the transaction (microseconds since epoch) |
| seq_num       | u64             | Database sequence number (meaningful when status = 0) |
| error_message | Option\<String\>| Present if status = aborted                          |

> **TODO**: TxKey and TxResult will likely be collapsed into a single message once we have a good and unique format for a Basis.

### 4.14 BasisForTx (Frontend, `F`)

Look up the `Basis` (tx_key + seq_num) for a previously committed transaction. The server waits until the transaction has been indexed before responding.

| Field       | Type | Description                                             |
|-------------|------|---------------------------------------------------------|
| tx_id       | i64  | Transaction ID to look up                               |
| system_time | i64  | Timestamp of the transaction (microseconds since epoch) |

Server responds with `BasisResult` followed by `ReadyForQuery`, or `ErrorResponse` + `ReadyForQuery` if the transaction is unknown.

### 4.15 BasisResult (Backend, `A`)

Returns the full basis for a transaction.

| Field       | Type | Description                                             |
|-------------|------|---------------------------------------------------------|
| tx_id       | i64  | Transaction ID                                          |
| system_time | i64  | Timestamp of the transaction (microseconds since epoch) |
| seq_num     | u64  | Database sequence number                                |

### 4.16 Subscribe (Frontend, `S`)

Start a live push subscription. Blocks the connection until cancelled.

| Field        | Type   | Description                                                    |
|--------------|--------|----------------------------------------------------------------|
| query_string | String | Datalog query in EDN text                                      |
| db_id        | u32    | DB snapshot; subscription streams changes after this point     |

The client must open a DB with OpenDb before subscribing. The subscription uses the DB's tx_id as the starting point: initial results are computed from that snapshot, and subsequent pushes contain changes from later transactions.

On receiving Subscribe, the server:
1. Validates and compiles the query.
2. Sends RowDescription with the result schema.
3. Sends initial results as DataRow messages (with `tpx_diff = +1`), followed by a DataBatchComplete.
4. Enters subscription mode. As new transactions arrive and affect the query results, sends additional DataRow messages followed by DataBatchComplete for each transaction.

While subscribed, the only valid client messages are Unsubscribe and Terminate. The server concurrently reads client messages and writes subscription data using asynchronous I/O.

### 4.17 DataBatchComplete (Backend, `B`)

Marks the end of a batch of DataRow messages produced by a single transaction within a subscription stream.

| Field | Type | Description                                   |
|-------|------|-----------------------------------------------|
| tx_id | i64  | Transaction that produced this batch of rows  |

The server flushes its write buffer after sending DataBatchComplete. Clients use this to group rows by originating transaction.

### 4.18 Unsubscribe (Frontend, `U`)

Cancel the active subscription.

Payload: empty (length = 4).

### 4.19 UnsubscribeComplete (Backend, `N`)

Confirms the subscription has been torn down.

Payload: empty (length = 4).

Followed by ReadyForQuery with status `I`.

### 4.20 ErrorResponse (Backend, `W`)

| Field    | Type            | Description                                         |
|----------|-----------------|-----------------------------------------------------|
| severity | u8              | `E` (0x45) = ERROR, `F` (0x46) = FATAL             |
| code     | u16             | Error code (see [section 8](#8-error-codes))        |
| message  | String          | Human-readable error message                        |
| detail   | Option\<String\>| Optional additional detail                          |
| hint     | Option\<String\>| Optional suggestion for how to fix the problem      |

**FATAL** errors (e.g. protocol version mismatch, subscription write timeout): the server sends ErrorResponse then closes the connection. The `severity` byte is `F` (0x46).

**Non-fatal** errors (e.g. bad query syntax): the server sends ErrorResponse followed by ReadyForQuery, allowing the client to continue. The `severity` byte is `E` (0x45).

### 4.21 Terminate (Frontend, `X`)

Graceful connection close.

Payload: empty (length = 4).

The server closes the TCP connection after receiving this. No response is sent.

### 4.22 Heartbeat (Backend, `K`)

Sent by the server during an active subscription when no data has been sent within the keepalive interval (see [Section 12](#12-connection-keepalive-and-timeouts)). The client MUST silently ignore this message (no response required).

Payload: empty (length = 4).

---

## 5. Protocol Flows

### 5.1 Connection Handshake

```
Client                                Server
  |                                     |
  |--- Startup(v1.0, params) --------->|
  |                                     |  check version
  |<------------ AuthenticationOk -----|
  |<------------ ReadyForQuery('I') ---|
  |                                     |
```

### 5.2 Open DB

```
Client                                Server
  |                                     |
  |--- OpenDb(basis_tx_id?) --------->|
  |                                     |  obtain snapshot
  |<------------ DbOpened(db_id, tx) -|
  |<------------ ReadyForQuery('I') --|
  |                                     |
```

### 5.3 Query

```
Client                                Server
  |                                     |
  |--- Query(edn, db_id) ------------>|
  |                                     |  parse, compile, execute
  |<------------ RowDescription ------|
  |<------------ DataRow -------------|  \
  |<------------ DataRow -------------|   } 0..N rows
  |<------------ DataRow -------------|  /
  |<------------ ReadyForQuery('I') --|
  |                                     |
```

### 5.4 Query Error

```
Client                                Server
  |                                     |
  |--- Query(bad_edn, db_id) -------->|
  |                                     |  parse fails
  |<------------ ErrorResponse -------|
  |<------------ ReadyForQuery('I') --|
  |                                     |
```

### 5.5 Transaction

```
Client                                Server
  |                                     |
  |--- Execute([Put, Add, ...]) ----->|
  |                                     |  submit to log
  |<------------ TxResult ------------|
  |<------------ ReadyForQuery('I') --|
  |                                     |
```

### 5.6 Subscription

```
Client                                Server
  |                                     |
  |--- OpenDb(basis_tx_id?) --------->|
  |<------------ DbOpened(db_id, tx) -|
  |<------------ ReadyForQuery('I') --|
  |                                     |
  |--- Subscribe(edn, db_id) -------->|
  |                                     |  compile query, run initial
  |<------------ RowDescription ------|
  |<------------ DataRow(+1, ...) ----|  } initial results (all +1)
  |<------------ DataRow(+1, ...) ----|
  |<------------ DataBatchComplete ---|
  |                                     |
  |        ... DB changes (tx 42) ...
  |                                     |
  |<------------ DataRow(+1, ...) ----|  } additions from tx 42
  |<------------ DataRow(-1, ...) ----|  } retractions from tx 42
  |<------------ DataBatchComplete ---|
  |                                     |
  |        ... heartbeat (no changes) ...
  |                                     |
  |<------------ Heartbeat ------------|  } keepalive
  |                                     |
  |--- Unsubscribe ------------------>|
  |<------------ UnsubscribeComplete -|
  |<------------ ReadyForQuery('I') --|
  |                                     |
```

### 5.7 Close DB

```
Client                                Server
  |                                     |
  |--- CloseDb(db_id) --------------->|
  |<------------ DbClosed(db_id) -----|
  |<------------ ReadyForQuery('I') --|
  |                                     |
```

### 5.8 Termination

```
Client                                Server
  |                                     |
  |--- Terminate -------------------->|
  |                                     |  close TCP
  |           [connection closed]       |
```

---

## 6. Buffering and Batching

### Query Results

There is no protocol-level batching for query results. Each DataRow is an individual protocol message. Implementations should use buffered writes (e.g. BufWriter) and flush at sync points (after ReadyForQuery).

### Subscription Results

When a transaction causes query results to change, the server sends all affected DataRow messages followed by a DataBatchComplete message carrying the originating tx_id. The server flushes its write buffer after each DataBatchComplete. This provides transaction-level grouping without a batching envelope.

### Subscription Backpressure

Subscription uses **in-band** Unsubscribe. The server concurrently reads client messages (Unsubscribe, Terminate) and writes DataRow/DataBatchComplete using asynchronous I/O (e.g. `tokio::select!`).

**Write timeout**: If the server cannot write to a subscribed client within a configurable timeout (default 30 seconds), it SHOULD terminate the subscription by sending `ErrorResponse(severity='F', code=4001, message="subscription write timeout")` and closing the connection. This prevents unbounded server-side buffering for slow clients.

---

## 7. Connection State Machine

```
                ┌──────────┐
                │ Startup  │
                └────┬─────┘
                     │ Startup message received, version ok
                     ▼
              ┌──────────────┐
         ┌───>│    Idle      │<───────────────────────┐
         │    └─┬──┬──┬──┬─┬─┘                        │
         │      │  │  │  │ │                           │
         │ Open/│  │  │  │ │Subscribe                  │
         │ Close│  │  │  │ │                           │
         │  Db  │  │  │  │ ▼                           │
         │      │  │  │  │┌────────────┐               │
         │      │  │  │  ││ Subscribed │─Unsubscribe──>│
         │  Query  │  │  │└────────────┘               │
         │      │  │  │  │                             │
         │      ▼  │  │  │                             │
         │  ┌──────┐  │  │                             │
         │  │Query │  │  │                             │
         │  │  In  │  │  │                             │
         │  │Progr.│  │  │                             │
         │  └──┬───┘  │  │                             │
         │     │   Execute│                            │
         │     │      │   │                            │
         │     │      ▼   │                            │
         │     │ ┌───────────┐                         │
         │     │ │ Executing │                         │
         │     │ └─────┬─────┘                         │
         │     │       │                               │
         └─────┴───────┘                               │
                                                       │
         Terminate from any state ──> Closed            │
         ErrorResponse (non-fatal) ────────────────────┘
         ErrorResponse (fatal) ──> Closed
```

### Valid Messages Per State

| State           | Valid Frontend Messages                            | Server Sends                                                |
|-----------------|----------------------------------------------------|-------------------------------------------------------------|
| Startup         | Startup                                            | AuthenticationOk + ReadyForQuery, or ErrorResponse + close  |
| Idle            | OpenDb, CloseDb, Query, Execute, Subscribe, Terminate | --                                                       |
| QueryInProgress | *(client waits)*                                   | RowDescription, DataRow, ReadyForQuery                      |
| Executing       | *(client waits)*                                   | TxResult, ReadyForQuery                                     |
| Subscribed      | Unsubscribe, Terminate                             | RowDescription, DataRow, DataBatchComplete, Heartbeat, ErrorResponse |
| Closed          | *(none)*                                           | *(none)*                                                    |

---

## 8. Error Codes

| Range | Category              | Codes                                                              |
|-------|-----------------------|--------------------------------------------------------------------|
| 1xxx  | Connection errors     | 1000 ProtocolVersionMismatch, 1001 InvalidStartup                  |
| 2xxx  | Query errors          | 2000 ParseError, 2001 QueryError, 2002 InvalidQuery, 2003 EmptyQuery |
| 3xxx  | Transaction errors    | 3000 TxError, 3001 TxAborted                                      |
| 4xxx  | Subscription errors   | 4000 SubscriptionError, 4001 SubscriptionTimeout                   |
| 5xxx  | Internal/protocol     | 5000 InternalError, 5001 MessageTooLarge, 5002 InvalidMessageType, 5003 QueryCancelled, 5004 ServerShuttingDown |
| 6xxx  | DB handle errors      | 6000 InvalidDbHandle, 6001 TooManyOpenDbs                          |

### Severity

Error severity is encoded in the `severity` field of ErrorResponse:

| Byte | Name  | Meaning                                                |
|------|-------|--------------------------------------------------------|
| `E` (0x45) | ERROR | Non-fatal. ReadyForQuery follows. Connection continues. |
| `F` (0x46) | FATAL | Fatal. Connection will be closed after this message.    |

---

## 9. DB Handle Management

The server maintains a set of open DB snapshots per connection. Each snapshot is identified by a `db_id` (u32) assigned by the server.

- **Lifetime**: A DB handle is valid from `DbOpened` until `DbClosed` or connection close.
- **Cleanup**: When a connection closes (via Terminate or TCP drop), all open DB handles for that connection are released.
- **Limits**: Servers should enforce a configurable maximum number of open DB handles per connection (default 16). Exceeding this limit returns an `ErrorResponse` with code 6001 (TooManyOpenDbs).
- **Invalid handles**: Using an invalid or closed `db_id` in a Query or Subscribe message returns an `ErrorResponse` with code 6000 (InvalidDbHandle).

---

## 10. Data Type Tags

Used in RowDescription to describe column types and as the discriminant byte in the wire encoding of `DataType` values.

| Tag | Name    |
|-----|---------|
| 1   | BigInt  |
| 2   | Boolean |
| 3   | Bytes   |
| 4   | Double  |
| 5   | Float   |
| 6   | Instant |
| 7   | Long    |
| 8   | Ref     |
| 9   | String  |
| 10  | Tuple   |
| 11  | Uuid    |
| 12  | Vector  |
| 13  | Map     |
| 14  | Keyword |
| 127 | Union   |
| 255 | Unknown |

Tags 1–14 (and future concrete types up to 126) are used in two contexts: as the discriminant byte in DataRow value encoding, and as a column type in ColumnDescription.

Tag 127 (Union) and tag 255 (Unknown) are **ColumnDescription-only** — they never appear as DataRow value discriminants. `Union` means the column contains values from a known, finite set of concrete types (members listed inline in the ColumnDescription). `Unknown` means the type is truly indeterminate.

---

## 11. Wire Encoding

All message payloads are encoded using the binary format described in this section. The encoding is language-neutral and designed to be straightforward to implement in any language (Rust, Java, Clojure, etc.).

All multi-byte integers use **big-endian** (network) byte order.

### 11.1 Primitive Types

| Type   | Encoding                                         |
|--------|--------------------------------------------------|
| `bool` | 1 byte: `0x00` = false, `0x01` = true            |
| `u8`   | 1 byte, unsigned                                  |
| `i8`   | 1 byte, signed two's complement                   |
| `u16`  | 2 bytes, big-endian, unsigned                      |
| `u32`  | 4 bytes, big-endian, unsigned                      |
| `i64`  | 8 bytes, big-endian, signed two's complement       |
| `u64`  | 8 bytes, big-endian, unsigned                      |
| `i128` | 16 bytes, big-endian, signed two's complement      |
| `f32`  | 4 bytes, IEEE 754 binary32, big-endian             |
| `f64`  | 8 bytes, IEEE 754 binary64, big-endian             |

### 11.2 Strings

```
+----------+------------------+
| len: u32 | UTF-8 bytes      |
+----------+------------------+
```

`len` is the byte count of the UTF-8 encoded string (not the character count).

### 11.3 Byte Arrays

```
+----------+------------------+
| len: u32 | raw bytes        |
+----------+------------------+
```

### 11.4 Optional Values (`Option<T>`)

```
+----------+-----------+
| tag: u8  | value: T  |    (if tag = 0x01)
+----------+-----------+
```

- `0x00` = None (no following bytes)
- `0x01` = Some, followed by the encoded value of type T

### 11.5 Sequences (`Vec<T>`)

```
+-----------+-------------------+
| count: u32| elements: T × N   |
+-----------+-------------------+
```

`count` elements encoded sequentially.

### 11.6 Maps (`Map<K, V>`)

```
+-----------+----------------------------+
| count: u32| entries: (K, V) × N         |
+-----------+----------------------------+
```

`count` key-value pairs encoded sequentially. Keys are sorted lexicographically (for `Map<String, V>`).

### 11.7 DataType Encoding

A `DataType` value is encoded as a 1-byte type tag (from [Section 10](#10-data-type-tags)) followed by the variant payload:

| Tag | Name    | Payload                                          |
|-----|---------|--------------------------------------------------|
| 1   | BigInt  | `i128` (16 bytes)                                |
| 2   | Boolean | `bool` (1 byte)                                  |
| 3   | Bytes   | `Bytes` (u32 length + raw bytes)                 |
| 4   | Double  | `f64` (8 bytes)                                  |
| 5   | Float   | `f32` (4 bytes)                                  |
| 6   | Instant | `i64` (microseconds since Unix epoch)            |
| 7   | Long    | `i64` (8 bytes)                                  |
| 8   | Ref     | `i64` (8 bytes, entity reference)                |
| 9   | String  | `String` (u32 length + UTF-8 bytes)              |
| 10  | Tuple   | `Vec<DataType>` (u32 count + elements)           |
| 11  | Uuid    | 16 bytes, raw RFC 4122 layout                    |
| 12  | Vector  | `Vec<DataType>` (u32 count + elements)           |
| 13  | Map     | `Map<String, DataType>` (u32 count + entries)    |
| 14  | Keyword | `String` (u32 length + UTF-8 bytes)              |

### 11.8 TxOp Encoding

A `TxOp` value is encoded as a 1-byte variant tag followed by the variant payload:

| Tag | Name     | Payload                                    |
|-----|----------|--------------------------------------------|
| 0   | Put      | `Document` (encoded as `Map<String, DataType>`) |
| 1   | Add      | `Triple` (see below)                       |
| 2   | Retract  | `Triple` (see below)                       |
| 3   | Delete   | `EntityId` (`i64`)                         |
| 4   | Erase    | `EntityId` (`i64`)                         |

**Triple encoding**:

```
+-------------------+---------------------+------------------+
| entity: i64       | attribute: String    | value: DataType  |
+-------------------+---------------------+------------------+
```

### 11.9 Message Payloads

Each message payload is the concatenation of its fields in declaration order, encoded using the types above.

**Startup** (no type byte):
```
version_major : u16
version_minor : u16
params        : Map<String, String>
```

**AuthenticationOk** (`R`):
```
server_version : String
```

**ReadyForQuery** (`Z`):
```
status : u8
```

**Query** (`Q`):
```
query_string : String
db_id        : u32
```

**RowDescription** (`T`):
```
columns : Vec<ColumnDescription>
```
where each ColumnDescription is:
```
name         : String
data_type    : u8               (DataTypeTag from Section 10)
// If data_type == 127 (Union):
member_count : u8               (number of member types, >= 2)
member_tags  : [u8; member_count]  (concrete tags, sorted ascending)
```

**Wire examples:**

- Union column `?val` with Long|String: `data_type=0x7F, member_count=0x02, member_tags=[0x07, 0x09]`
- Concrete column `?e` as Long: `data_type=0x07` (no additional fields)

**DataRow** (`D`):
```
values : Vec<DataType>
```

**Execute** (`E`):
```
ops            : Vec<TxOp>
await_indexing : bool
```

**TxKey** (`Y`):
```
tx_id       : i64
system_time : i64        (microseconds since Unix epoch)
```

**TxResult** (`G`):
```
status        : u8         (0 = committed, 1 = aborted)
tx_id         : i64
system_time   : i64        (microseconds since Unix epoch)
seq_num       : u64
error_message : Option<String>
```

**BasisForTx** (`F`):
```
tx_id       : i64
system_time : i64        (microseconds since Unix epoch)
```

**BasisResult** (`A`):
```
tx_id       : i64
system_time : i64        (microseconds since Unix epoch)
seq_num     : u64
```

**Subscribe** (`S`):
```
query_string : String
db_id        : u32
```

**DataBatchComplete** (`B`):
```
tx_id : i64
```

**Heartbeat** (`K`):
```
(empty)
```

**Unsubscribe** (`U`):
```
(empty)
```

**UnsubscribeComplete** (`N`):
```
(empty)
```

**ErrorResponse** (`W`):
```
severity : u8            ('E' or 'F')
code     : u16
message  : String
detail   : Option<String>
hint     : Option<String>
```

**Terminate** (`X`):
```
(empty)
```

---

## 12. Connection Keepalive and Timeouts

### TCP_NODELAY

Implementations MUST set `TCP_NODELAY` on both client and server sockets. The protocol uses buffered writes with explicit flushes at sync points, so Nagle's algorithm provides no benefit and only adds latency (up to 200ms per round-trip).

### TCP Keepalive

Implementations SHOULD configure TCP keepalive on both client and server sockets (`SO_KEEPALIVE` with a recommended interval of 60 seconds). This detects dead peers and prevents silent connection drops due to NAT timeouts or network failures.

### Subscription Heartbeat

During an active subscription, if no real data has been sent within the keepalive interval, the server SHOULD send a `Heartbeat` message. The client MUST silently ignore it (no response required). This serves two purposes:
1. Confirms the subscription is still alive.
2. Prevents intermediate network equipment (NAT gateways, firewalls, load balancers) from evicting their connection-tracking entries due to inactivity.

### Idle Connection Timeout

Servers MAY enforce an idle connection timeout for connections in the Idle state (configurable, suggested default 5 minutes). After the timeout, the server sends `ErrorResponse(severity='F', code=5004)` and closes the connection.

---

## 13. Future Extensions

The following features are deliberately deferred to a later protocol version. They are documented here to inform client and server implementations of planned evolution:

- **SSL/TLS negotiation**: Pre-Startup SSLRequest exchange (pgwire pattern). Client sends magic number, server responds with `S`/`N`, TLS handshake follows if accepted.
- **Query cancellation**: BackendKeyData message during startup (connection ID + secret key). CancelRequest on a separate TCP connection to cancel long-running queries without closing the main connection.
- **NoticeResponse**: Non-fatal informational messages (warnings, deprecation notices). Same structure as ErrorResponse but does not affect protocol flow.
- **Authentication**: Methods beyond version check (password, SCRAM, certificate). Would reuse the `R` type byte with sub-type codes.
- **Prepared statements / extended query protocol**: Parse, Bind, Describe, Execute, Sync flow for parameterized queries.
