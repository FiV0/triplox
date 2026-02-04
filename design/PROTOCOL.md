# Triplox Wire Protocol Specification

Version 1.0

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
- **payload**: Message-specific data, serialized with bincode.

**Exception**: The initial Startup message from the client has no type byte. It consists only of `[length][payload]`. The server identifies it by context -- the first message on any new connection is always a Startup.

**Maximum message size**: Servers should enforce a configurable limit (default 64 MB). Messages exceeding this limit are rejected with an ErrorResponse.

---

## 2. Version Negotiation

The Startup message carries a protocol version as two 16-bit unsigned integers: major and minor. The initial version is **1.0** (major=1, minor=0).

The server checks the version:
- Compatible: responds with AuthenticationOk.
- Incompatible: responds with ErrorResponse (code 1000) and closes the connection.

---

## 3. Message Catalog

### Frontend Messages (Client to Server)

| Type Byte  | Name        | Description                     |
|------------|-------------|---------------------------------|
| *(none)*   | Startup     | Connection handshake            |
| `Q` (0x51) | Query       | Execute a Datalog query         |
| `E` (0x45) | Execute     | Submit a transaction            |
| `S` (0x53) | Subscribe   | Start a live query subscription |
| `U` (0x55) | Unsubscribe | Cancel the active subscription  |
| `X` (0x58) | Terminate   | Close the connection            |

### Backend Messages (Server to Client)

| Type Byte  | Name                | Description                              |
|------------|---------------------|------------------------------------------|
| `R` (0x52) | AuthenticationOk    | Handshake accepted                       |
| `T` (0x54) | RowDescription      | Result schema (column names and types)   |
| `D` (0x44) | DataRow             | One row of result data                   |
| `C` (0x43) | CommandComplete     | Query finished                           |
| `B` (0x42) | DataBatchComplete   | Subscription batch boundary              |
| `Z` (0x5A) | ReadyForQuery       | Server is ready for the next request     |
| `G` (0x47) | TxResult            | Transaction outcome                      |
| `N` (0x4E) | UnsubscribeComplete | Subscription cancelled                   |
| `W` (0x57) | ErrorResponse       | Error                                    |

---

## 4. Message Definitions

### 4.1 Startup (Frontend, no type byte)

Sent as the first message after TCP connection is established.

| Field          | Type                  | Description                            |
|----------------|-----------------------|----------------------------------------|
| version_major  | u16                   | Protocol major version (1)             |
| version_minor  | u16                   | Protocol minor version (0)             |
| params         | Map\<String, String\> | Reserved. May contain `client_name`.   |

### 4.2 AuthenticationOk (Backend, `R`)

Sent after successful version negotiation.

| Field          | Type   | Description                          |
|----------------|--------|--------------------------------------|
| server_version | String | Server identifier, e.g. "triplox 0.1.0" |

### 4.3 ReadyForQuery (Backend, `Z`)

Sent when the server is ready for the next client request. Acts as a sync/flush point.

| Field  | Type | Description                                    |
|--------|------|------------------------------------------------|
| status | u8   | `I` (0x49) = idle, `S` (0x53) = subscribed    |

Sent after: AuthenticationOk, CommandComplete, TxResult, UnsubscribeComplete, non-fatal ErrorResponse.

### 4.4 Query (Frontend, `Q`)

Execute a one-shot Datalog query.

| Field        | Type         | Description                                      |
|--------------|--------------|--------------------------------------------------|
| query_string | String       | Datalog query in EDN text                        |
| basis_tx_id  | Option\<i64\> | If set, query at this transaction point in time |

Example query string:
```edn
[:find ?name ?age :where [?e :person/name ?name] [?e :person/age ?age]]
```

### 4.5 RowDescription (Backend, `T`)

Describes the schema of the result set that follows.

| Field   | Type                      | Description       |
|---------|---------------------------|-------------------|
| columns | Vec\<ColumnDescription\>  | One per column    |

Each ColumnDescription:

| Field     | Type        | Description                           |
|-----------|-------------|---------------------------------------|
| name      | String      | Variable name, e.g. "?name"          |
| data_type | DataTypeTag | Type discriminant (see section 9)     |

Sent before the first DataRow of a query result or subscription.

### 4.6 DataRow (Backend, `D`)

One row of result data.

| Field  | Type             | Description                            |
|--------|------------------|----------------------------------------|
| values | Vec\<DataType\>  | One value per column, bincode-encoded  |

Uses the existing `DataType` enum directly (Nil, BigInt, Boolean, Bytes, Double, Float, Instant, Long, Ref, String, Tuple, Uuid, Vector, Map).

### 4.7 CommandComplete (Backend, `C`)

Signals that a query has finished producing results.

| Field     | Type   | Description                            |
|-----------|--------|----------------------------------------|
| tag       | String | Command tag, e.g. "SELECT"             |
| row_count | u64    | Number of DataRow messages sent        |

### 4.8 Execute (Frontend, `E`)

Submit a transaction.

| Field          | Type          | Description                                        |
|----------------|---------------|----------------------------------------------------|
| ops            | Vec\<TxOp\>   | Transaction operations (bincode-serialized)        |
| await_indexing | bool          | If true, server waits for indexer before responding |

Uses the existing `TxOp` enum directly (Put, Add, Retract, Delete, Erase).

### 4.9 TxResult (Backend, `G`)

Transaction outcome.

| Field         | Type           | Description                              |
|---------------|----------------|------------------------------------------|
| status        | u8             | 0 = committed, 1 = aborted              |
| tx_id         | i64            | Transaction ID assigned by the server    |
| system_time   | DateTime\<Utc\>| Timestamp of the transaction             |
| error_message | Option\<String\>| Present if status = aborted            |

### 4.10 Subscribe (Frontend, `S`)

Start a live push subscription. Blocks the connection until cancelled.

| Field        | Type          | Description                                        |
|--------------|---------------|----------------------------------------------------|
| query_string | String        | Datalog query in EDN text                          |
| after_tx_id  | Option\<i64\> | If set, only stream changes after this transaction |

On receiving Subscribe, the server:
1. Validates and compiles the query.
2. Sends RowDescription with the result schema.
3. Sends initial results as DataRow messages, followed by a DataBatchComplete.
4. Enters subscription mode. As new transactions arrive and affect the query results, sends additional DataRow messages followed by DataBatchComplete for each transaction.

While subscribed, the only valid client messages are Unsubscribe and Terminate.

### 4.11 DataBatchComplete (Backend, `B`)

Marks the end of a batch of DataRow messages produced by a single transaction within a subscription stream.

| Field | Type | Description                                   |
|-------|------|-----------------------------------------------|
| tx_id | i64  | Transaction that produced this batch of rows  |

The server flushes its write buffer after sending DataBatchComplete. Clients use this to group rows by originating transaction.

### 4.12 Unsubscribe (Frontend, `U`)

Cancel the active subscription.

Payload: empty (length = 4).

### 4.13 UnsubscribeComplete (Backend, `N`)

Confirms the subscription has been torn down.

Payload: empty (length = 4).

Followed by ReadyForQuery with status `I`.

### 4.14 ErrorResponse (Backend, `W`)

| Field   | Type            | Description                         |
|---------|-----------------|-------------------------------------|
| code    | u16             | Error code (see section 8)          |
| message | String          | Human-readable error message        |
| detail  | Option\<String\>| Optional additional detail          |

**Fatal errors** (e.g. protocol version mismatch): server sends ErrorResponse then closes the connection.

**Non-fatal errors** (e.g. bad query syntax): server sends ErrorResponse followed by ReadyForQuery, allowing the client to continue.

### 4.15 Terminate (Frontend, `X`)

Graceful connection close.

Payload: empty (length = 4).

The server closes the TCP connection after receiving this. No response is sent.

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

### 5.2 Query

```
Client                                Server
  |                                     |
  |--- Query(edn, basis) ------------>|
  |                                     |  parse, compile, execute
  |<------------ RowDescription ------|
  |<------------ DataRow -------------|  \
  |<------------ DataRow -------------|   } 0..N rows
  |<------------ DataRow -------------|  /
  |<------------ CommandComplete -----|
  |<------------ ReadyForQuery('I') --|
  |                                     |
```

### 5.3 Query Error

```
Client                                Server
  |                                     |
  |--- Query(bad_edn) --------------->|
  |                                     |  parse fails
  |<------------ ErrorResponse -------|
  |<------------ ReadyForQuery('I') --|
  |                                     |
```

### 5.4 Transaction

```
Client                                Server
  |                                     |
  |--- Execute([Put, Add, ...]) ----->|
  |                                     |  submit to log
  |<------------ TxResult ------------|
  |<------------ ReadyForQuery('I') --|
  |                                     |
```

### 5.5 Subscription

```
Client                                Server
  |                                     |
  |--- Subscribe(edn, after_tx) ----->|
  |                                     |  compile query, run initial
  |<------------ RowDescription ------|
  |<------------ DataRow -------------|  } initial results
  |<------------ DataRow -------------|
  |<------------ DataBatchComplete ---|
  |                                     |
  |        ... DB changes (tx 42) ...
  |                                     |
  |<------------ DataRow -------------|  } pushed results from tx 42
  |<------------ DataBatchComplete ---|
  |                                     |
  |        ... DB changes (tx 43) ...
  |                                     |
  |<------------ DataRow -------------|  } pushed results from tx 43
  |<------------ DataRow -------------|
  |<------------ DataBatchComplete ---|
  |                                     |
  |--- Unsubscribe ------------------>|
  |<------------ UnsubscribeComplete -|
  |<------------ ReadyForQuery('I') --|
  |                                     |
```

### 5.6 Termination

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

There is no protocol-level batching for query results. Each DataRow is an individual protocol message. Implementations should use buffered writes (e.g. BufWriter) and flush at sync points: after CommandComplete and ReadyForQuery. This follows the pgwire approach.

### Subscription Results

When a transaction causes query results to change, the server sends all affected DataRow messages followed by a DataBatchComplete message carrying the originating tx_id. The server flushes its write buffer after each DataBatchComplete. This provides transaction-level grouping without a batching envelope.

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
         │    └──┬───┬───┬───┘                        │
         │       │   │   │                            │
         │  Query│   │   │Subscribe                   │
         │       │   │   │                            │
         │       ▼   │   ▼                            │
         │  ┌──────┐ │ ┌────────────┐                 │
         │  │Query │ │ │ Subscribed │──Unsubscribe───>│
         │  │  In  │ │ └────────────┘                 │
         │  │Progr.│ │                                │
         │  └──┬───┘ │Execute                         │
         │     │     │                                │
         │     │     ▼                                │
         │     │ ┌───────────┐                        │
         │     │ │ Executing │                        │
         │     │ └─────┬─────┘                        │
         │     │       │                              │
         └─────┴───────┘                              │
                                                      │
         Terminate from any state ──> Closed           │
         ErrorResponse (non-fatal) ───────────────────┘
         ErrorResponse (fatal) ──> Closed
```

### Valid Messages Per State

| State           | Valid Frontend Messages          | Server Sends                                                |
|-----------------|----------------------------------|-------------------------------------------------------------|
| Startup         | Startup                          | AuthenticationOk + ReadyForQuery, or ErrorResponse + close  |
| Idle            | Query, Execute, Subscribe, Terminate | --                                                      |
| QueryInProgress | *(client waits)*                 | RowDescription, DataRow, CommandComplete, ReadyForQuery     |
| Executing       | *(client waits)*                 | TxResult, ReadyForQuery                                     |
| Subscribed      | Unsubscribe, Terminate           | RowDescription, DataRow, DataBatchComplete, ErrorResponse   |
| Closed          | *(none)*                         | *(none)*                                                    |

---

## 8. Error Codes

| Range | Category              | Codes                                          |
|-------|-----------------------|------------------------------------------------|
| 1xxx  | Connection errors     | 1000 ProtocolVersionMismatch, 1001 InvalidStartup |
| 2xxx  | Query errors          | 2000 ParseError, 2001 QueryError, 2002 InvalidQuery |
| 3xxx  | Transaction errors    | 3000 TxError, 3001 TxAborted                   |
| 4xxx  | Subscription errors   | 4000 SubscriptionError                         |
| 5xxx  | Internal/protocol     | 5000 InternalError, 5001 MessageTooLarge, 5002 InvalidMessageType |

---

## 9. Data Type Tags

Used in RowDescription to describe column types. Maps from the existing `DataType` enum.

| Tag | Name    |
|-----|---------|
| 0   | Nil     |
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
| 255 | Unknown |

`Unknown` (255) is used when the column type is not uniform or cannot be determined in advance.
