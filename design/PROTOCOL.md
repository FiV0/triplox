# Triplox Protocol Specification

Version 0.1

## Overview

Triplox speaks **HTTP/2** with **MessagePack**-encoded request and response bodies.
Each operation maps to a REST-style endpoint; each body is a single self-contained
msgpack map. Triplox-specific value types (UUID, Instant, Keyword, BigInt) use
[MessagePack extension types](https://github.com/msgpack/msgpack/blob/master/spec.md#extension-types)
for unambiguous round-trip encoding.

The Content-Type for every request and response body is:

```
application/vnd.triplox+msgpack
```

Subscriptions (live query streaming) are **not yet implemented** in this version
and are reserved for a future revision.

---

## 1. Endpoints

| Endpoint                  | Method   | Request Body              | Success Body               |
|---------------------------|----------|---------------------------|-----------------------------|
| `/db/open`                | `POST`   | OpenDb                    | DbOpened                    |
| `/db/{db_id}`             | `DELETE` | *(empty)*                 | DbClosed                    |
| `/db/{db_id}/query`       | `POST`   | Query                     | QueryResponse               |
| `/tx/submit`              | `POST`   | Execute                   | TxKey                       |
| `/tx/execute`             | `POST`   | Execute                   | TxResult                    |

### 1.1 HTTP Status Codes

HTTP status distinguishes transport/protocol errors from application-level
outcomes. A transaction that *commits with an aborted status* (e.g. constraint
violation) still returns **200**; the caller inspects the body to learn the
outcome.

| Status | Meaning             | When returned |
|--------|---------------------|---------------|
| **200** | Success             | Request processed successfully. For `/tx/execute`, check the `status` field of the TxResult body: `0` = committed, `1` = aborted. |
| **400** | Bad Request         | Malformed request body (decode failure), Datalog parse error, or invalid field combinations. |
| **404** | Not Found           | Invalid or expired `db_id` handle. |
| **500** | Internal Server Error | Unexpected engine error: query execution failure, transaction infrastructure error, DB cache exhaustion. |

All non-200 responses carry an [ErrorResponse](#410-errorresponse--non-200-body) body.

### 1.2 DB Handle Lifecycle

DB handles are scoped to an HTTP/2 connection. Each connection is assigned a
unique `conn_id` on accept, and all handles opened on that connection are
tracked against it. Two cleanup mechanisms ensure handles are released:

1. **Connection-drop** (fast path): When an HTTP/2 connection closes, all
   handles belonging to that connection are released immediately.
2. **TTL reaper** (safety net): A background task periodically evicts handles
   that have not been used for 24 hours.

Servers should enforce a configurable maximum number of open DB handles per
connection (default 16). Exceeding the limit returns
`ErrorResponse(code=5001 TooManyOpenDbs)`.

Using an invalid or closed `db_id` returns
`ErrorResponse(code=5000 InvalidDbHandle)` with HTTP status 404.

---

## 2. Wire Encoding

Bodies are encoded as a single MessagePack value (always a map at the top level).
Every map field shown below is **mandatory** unless explicitly typed
`<value>|nil`.

### 2.1 DataType → MessagePack

| DataType   | Encoding                                       |
|------------|------------------------------------------------|
| Boolean    | msgpack `bool`                                 |
| Long (i64) | msgpack `int`                                  |
| Float      | msgpack `float 32`                             |
| Double     | msgpack `float 64`                             |
| String     | msgpack `str` (UTF-8)                          |
| Bytes      | msgpack `bin`                                  |
| Vector     | msgpack `array` of DataType                    |
| Map        | msgpack `map` with `str` keys, DataType values |
| BigInt     | ext 1 (see §2.2)                               |
| Uuid       | ext 2 (see §2.2)                               |
| Keyword    | ext 3 (see §2.2)                               |
| Instant    | ext -1 (msgpack Timestamp; see §2.2)           |

The encoder iterates the source map as given — **no key sorting is imposed**.
Tests asserting byte-identical output should construct deterministic-order
maps themselves (`BTreeMap` in Rust, `TreeMap` in JVM tests).

### 2.2 Extension Type Table

| Ext code | Type    | Payload                                                                                  |
|---------:|---------|------------------------------------------------------------------------------------------|
| -1       | Instant | Standard MessagePack [Timestamp](https://github.com/msgpack/msgpack/blob/master/spec.md#timestamp-extension-type) (4 / 8 / 12 bytes; nanosecond precision). |
| 1        | BigInt  | 16 bytes, big-endian i128 (two's-complement, sign-extended).                             |
| 2        | Uuid    | 16 bytes, RFC 4122 raw layout.                                                           |
| 3        | Keyword | UTF-8 bytes of `"ns/name"` (or just `"name"` for un-namespaced). No leading colon.       |

### 2.3 Tagged Unions

`EntityRef`, `TxOp`, and `QueryArg` are encoded as msgpack maps with a `kind`
discriminator field.

#### EntityRef

```
{"kind": "id",     "id":    <int>}
{"kind": "temp",   "temp":  <str>}
{"kind": "ident",  "ident": <str (keyword wire form)>}
{"kind": "lookup", "attr":  <str (keyword wire form)>, "value": <DataType>}
```

#### TxOp

```
{"kind": "put",     "doc":    <map<str (keyword wire form), DataType>>}
{"kind": "add",     "entity": <EntityRef>, "attr": <str>, "value": <DataType>}
{"kind": "retract", "entity": <EntityRef>, "attr": <str>, "value": <DataType>}
{"kind": "delete",  "entity": <EntityRef>}
{"kind": "erase",   "entity": <EntityRef>}
```

`Put.doc` keys are stringified keywords without the leading `:` — same wire
form as the ext-3 Keyword payload.

#### QueryArg

```
{"kind": "scalar",     "value":  <DataType>}
{"kind": "collection", "values": [<DataType>, ...]}
{"kind": "tuple",      "values": [<DataType>, ...]}
{"kind": "relation",   "rows":   [[<DataType>, ...], ...]}
```

| QueryArg   | EDN `:in` syntax | Description                           |
|------------|------------------|---------------------------------------|
| Scalar     | `?x`             | Single value for one variable         |
| Collection | `[?x ...]`       | Multiple values for one variable      |
| Tuple      | `[?x ?y]`        | One row of multiple variables         |
| Relation   | `[[?x ?y]]`      | Multiple rows of multiple variables   |

> **Note**: Only Scalar bindings are currently implemented in the query engine.

---

## 3. Data Type Tags

Used in [ColumnDescription](#41-columndescription) to describe column types:

| Tag | Name    |
|-----|---------|
| 1   | BigInt  |
| 2   | Boolean |
| 3   | Bytes   |
| 4   | Double  |
| 5   | Float   |
| 6   | Instant |
| 7   | Long    |
| 8   | Ref *(reserved)* |
| 9   | String  |
| 10  | Uuid    |
| 11  | Vector  |
| 12  | Map     |
| 13  | Keyword |
| 127 | Union   |
| 255 | Unknown |

Tags 1–13 are concrete data types. Tag 127 (Union) means the column contains
values from a known, finite set of concrete types (members listed inline).
Tag 255 (Unknown) means the column type is indeterminate.

> **Note**: The `type` field in ColumnDescription is currently always set to
> `255` (Unknown) because the query engine does not yet perform type analysis.

---

## 4. Message Definitions

### 4.1 ColumnDescription

```
{"name": <str>, "type": <int>}                                 # concrete or unknown
{"name": <str>, "type": 127, "members": [<int>, <int>, ...]}   # union
```

`members` lists the concrete member tags (1–126), sorted ascending, with no
duplicates. A single-type column should use the concrete tag directly rather
than a single-member union.

### 4.2 OpenDb Request — `POST /db/open`

```
{"tx_id": <int>|nil, "system_time": <Timestamp>|nil}
```

Either both fields are present (pinned snapshot) or both are `nil` (latest
indexed). Mixing `nil` with a value is rejected with HTTP 400.

### 4.3 DbOpened Response

```
{"db_id": <int>, "tx_id": <int>}
```

`db_id` is a server-assigned u32 handle. `tx_id` is the actual tx the snapshot
is pinned to (may differ from the requested `tx_id`).

### 4.4 DbClosed Response — `DELETE /db/{db_id}`

```
{"db_id": <int>}
```

Echoes the released handle.

### 4.5 Query Request — `POST /db/{db_id}/query`

```
{"query": <str>, "args": [<QueryArg>, ...]}
```

`query` is a Datalog query in EDN text. `args` provides values for variables
declared in the query's `:in` clause; the number and order must match. For
queries without `:in`, pass an empty array.

### 4.6 QueryResponse

```
{"columns": [<ColumnDescription>, ...], "rows": [[<DataType>, ...], ...]}
```

Each row is an array of values, one per column, in the order declared by
`columns`.

### 4.7 Execute Request — `POST /tx/{submit,execute}`

```
{"ops": [<TxOp>, ...]}
```

Both endpoints take the same body. They differ in semantics:

- `/tx/submit` is fire-and-forget; the response is a [TxKey](#48-txkey-response).
- `/tx/execute` waits for indexing; the response is a [TxResult](#49-txresult-response).

### 4.8 TxKey Response

```
{"tx_id": <int>, "system_time": <Timestamp>}
```

Returned for `/tx/submit`. The server has accepted the transaction and will
durably log it; `system_time` is its assigned timestamp.

### 4.9 TxResult Response

```
{
  "status":         <int (0=committed, 1=aborted)>,
  "tx_id":          <int>,
  "system_time":    <Timestamp>,
  "error_message":  <str>|nil
}
```

Returned for `/tx/execute`. `error_message` is present only when
`status = 1` (aborted, e.g. constraint violation). Note: an aborted
transaction is **not** an HTTP-level error — the response is HTTP 200 and
the caller inspects `status`.

### 4.10 ErrorResponse — non-200 body

```
{
  "severity":  "E"|"F",
  "code":      <int>,
  "message":   <str>,
  "detail":    <str>|nil,
  "hint":      <str>|nil
}
```

`severity` is `"E"` for non-fatal errors and `"F"` for fatal ones. `code` is
a numeric error code (see §5).

---

## 5. Error Codes

| Range | Category              | Codes                                                              |
|-------|-----------------------|--------------------------------------------------------------------|
| 1xxx  | Connection errors     | 1000 ProtocolVersionMismatch, 1001 InvalidStartup                  |
| 2xxx  | Query errors          | 2000 ParseError, 2001 QueryError, 2002 InvalidQuery, 2003 EmptyQuery |
| 3xxx  | Transaction errors    | 3000 TxError, 3001 TxAborted                                       |
| 4xxx  | Internal/protocol     | 4000 InternalError, 4001 MessageTooLarge, 4002 InvalidMessageType, 4003 QueryCancelled, 4004 ServerShuttingDown |
| 5xxx  | DB handle errors      | 5000 InvalidDbHandle, 5001 TooManyOpenDbs                          |

---

## 6. Future Extensions

The following features are deliberately deferred to a later protocol version:

- **Live subscriptions**: Streaming query result deltas via HTTP/2 server-push
  or SSE-over-HTTP/2.
- **TLS**: Currently HTTP/2 cleartext. TLS termination will be handled at the
  HTTP/2 layer.
- **Authentication**: Currently no authentication. To be layered onto HTTP
  headers in a future revision.
- **Query cancellation**: Cooperative cancellation via HTTP/2 stream reset.
- **Prepared statements / extended query protocol**: Parse, Bind, Describe,
  Execute, Sync flow for parameterized queries.
- **NoticeResponse**: Non-fatal informational messages (warnings, deprecation
  notices), distinct from ErrorResponse.
