# Triplox Protocol Specification

Version 0.1

## Overview

Triplox uses HTTP/2. Request and response bodies use MessagePack encoding.
Each operation has its own REST-style endpoint. Each body is a single, self-contained
MessagePack map. Value types that are specific to Triplox (UUID, Instant, Keyword, BigInt) use
[MessagePack extension types](https://github.com/msgpack/msgpack/blob/master/spec.md#extension-types).
These types make the round trip unambiguous. A round trip is an encode followed by a decode of the same value.

The Content-Type for every request and response body is:

```
application/vnd.triplox+msgpack
```

Live query subscriptions stream result deltas over a single long-lived HTTP/2
response (`POST /db/subscribe`). A subscription is a query that stays open and reports changes.
A delta is a set of changed result rows. The response body is a frame stream, not
a single value. A frame is one MessagePack map in the stream. See [§4.11](#411-subscribe-request--post-dbsubscribe) and
[§4.12](#412-subscription-frame-stream).

---

## 1. Endpoints

| Endpoint                  | Method   | Request Body              | Success Body               |
|---------------------------|----------|---------------------------|-----------------------------|
| `/db/open`                | `POST`   | OpenDb                    | DbOpened                    |
| `/db/query`               | `POST`   | Query                     | QueryResponse               |
| `/db/subscribe`           | `POST`   | Subscribe                 | Frame stream (§4.12)        |
| `/tx/submit`              | `POST`   | Execute                   | TxKey                       |
| `/tx/execute`             | `POST`   | Execute                   | TxResult                    |

### 1.1 HTTP Status Codes

The HTTP status separates transport and protocol errors from application outcomes.
If a transaction commits with an aborted status (for example, a constraint
violation), the response still has status 200. The caller reads the body to learn the
outcome.

| Status | Meaning             | When returned |
|--------|---------------------|---------------|
| 200 | Success             | The server processed the request. For `/tx/execute`, read the `status` field of the TxResult body: `0` = committed, `1` = aborted. |
| 400 | Bad Request         | The request body is malformed (decode failure), the Datalog query has a parse error, or a combination of fields is not valid. |
| 409 | Conflict            | The requested DB basis (the log position that a read uses) is not indexed yet. |
| 500 | Internal Server Error | An unexpected engine error: a query execution failure or an error in the transaction infrastructure. |

Each non-200 response carries an [ErrorResponse](#410-errorresponse--non-200-body) body.

### 1.2 DB Values

A DB value is an immutable read basis. The transaction log key
(`tx_id` + `system_time`) pins a DB value. The transaction entity ID bounds temporal reads
on a DB value. This ID is derived from `tx_id` (`tx_eid = TX_PARTITION_MASK | tx_id`), so it
never travels on the wire. The server does not allocate or keep resources for each DB value.
Each query request carries the DB value. When the query executes, the server creates a temporary
read view from that basis. It is not necessary to close a DB value.

---

## 2. Wire Encoding

Each body is a single MessagePack value. At the top level, this value is always a map.
Each map field in the sections that follow is mandatory, unless its explicit type is
`<value>|nil`.

### 2.1 DataType → MessagePack

| DataType   | Encoding                                       |
|------------|------------------------------------------------|
| Boolean    | MessagePack `bool`                             |
| Long (i64) | MessagePack `int`                              |
| Float      | MessagePack `float 32`                         |
| Double     | MessagePack `float 64`                         |
| String     | MessagePack `str` (UTF-8)                      |
| Bytes      | MessagePack `bin`                              |
| Vector     | MessagePack `array` of DataType                |
| Map        | MessagePack `map` with `str` keys and DataType values |
| BigInt     | ext 1 (see §2.2)                               |
| Uuid       | ext 2 (see §2.2)                               |
| Keyword    | ext 3 (see §2.2)                               |
| Instant    | ext -1 (MessagePack Timestamp, see §2.2)       |

The encoder writes the keys of the source map in the order that it gets them.
It does not sort the keys. If a test asserts byte-identical output, build the maps in the
test with a deterministic order. Examples are `BTreeMap` in Rust and `TreeMap` in JVM tests.

Decoders MUST accept any key order for every map in this document.
A tagged union is a map whose `kind` field names its variant.
The `kind` field is the discriminator, and the key order rule also applies to it.
The field order of the encoder is only for test determinism. It is not part of the protocol contract.

### 2.2 Extension Type Table

| Ext code | Type    | Payload                                                                                  |
|---------:|---------|------------------------------------------------------------------------------------------|
| -1       | Instant | Standard MessagePack [Timestamp](https://github.com/msgpack/msgpack/blob/master/spec.md#timestamp-extension-type) (4 / 8 / 12 bytes, nanosecond precision). |
| 1        | BigInt  | 16 bytes, big-endian i128 (two's-complement, sign-extended).                             |
| 2        | Uuid    | 16 bytes, RFC 4122 raw layout.                                                           |
| 3        | Keyword | UTF-8 bytes of `"ns/name"`, or `"name"` for a keyword without a namespace. There is no leading colon. |

### 2.3 Tagged Unions

`EntityRef`, `TxOp`, and `QueryArg` are MessagePack maps with a `kind`
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
{"kind": "retractEntity", "entity": <EntityRef>}
{"kind": "erase",   "entity": <EntityRef>}
```

The keys of `Put.doc` are keywords as strings, without the leading `:`. This is the same wire
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
| Scalar     | `?x`             | A single value for one variable       |
| Collection | `[?x ...]`       | Multiple values for one variable      |
| Tuple      | `[?x ?y]`        | One row of multiple variables         |
| Relation   | `[[?x ?y]]`      | Multiple rows of multiple variables   |

> Note: The query engine currently implements only Scalar bindings.

### 2.4 TxKey

A `TxKey` identifies a transaction and the immutable DB read basis that it pins:

```
{"tx_id": <int>, "system_time": <Timestamp>}
```

`tx_id` is the sequence position that the log assigns. `system_time` is the wall-clock
timestamp of the transaction. The transaction entity ID, which bounds temporal reads, is derived from
`tx_id` (see [§1.2](#12-db-values)). It does not travel on the wire.

---

## 3. Data Type Tags

[ColumnDescription](#41-columndescription) uses these tags to describe column types:

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

Tags 1–13 are concrete data types. Tag 127 (Union) means that the column contains
values from a known, finite set of concrete types. The column description lists these members inline.
Tag 255 (Unknown) means that the column type is indeterminate.

> Note: The `type` field in ColumnDescription is currently always `255` (Unknown),
> because the query engine does not do type analysis yet.

---

## 4. Message Definitions

### 4.1 ColumnDescription

```
{"name": <str>, "type": <int>}                                 # concrete or unknown
{"name": <str>, "type": 127, "members": [<int>, <int>, ...]}   # union
```

`members` lists the concrete member tags (1–126) in ascending order, with no
duplicates. A column with a single type uses the concrete tag directly, not a
union with a single member.

### 4.2 OpenDb Request — `POST /db/open`

```
{"tx_id": <int>|nil, "system_time": <Timestamp>|nil}
```

Either both fields are present (a pinned transaction basis), or both are
`nil` (the latest indexed basis). If one field is `nil` and the other has a value,
the server rejects the request with HTTP 400.

### 4.3 OpenDb Response

The response body is a [`TxKey`](#24-txkey): the immutable DB read basis.

### 4.4 Query Request — `POST /db/query`

```
{"tx_key": <TxKey>,
 "query": <str>,
 "args": [<QueryArg>, ...]}
```

`tx_key` is a [`TxKey`](#24-txkey) DB read basis. The `OpenDb` response or a previous transaction result returns this value.
`query` is a Datalog query in EDN text. `args` gives the values for the variables
that the `:in` clause of the query declares. The number and the order of the values must match.
If the query has no `:in` clause, pass an empty array.

### 4.6 QueryResponse

```
{"columns": [<ColumnDescription>, ...], "rows": [[<DataType>, ...], ...]}
```

Each row is an array of values, with one value for each column. The values are in the order
that `columns` declares.

### 4.7 Execute Request — `POST /tx/{submit,execute}`

```
{"ops": [<TxOp>, ...]}
```

Both endpoints take the same body, but their semantics are different.
`/tx/submit` is fire-and-forget (it does not wait for the result). Its response is a [TxKey](#48-txkey-response).
`/tx/execute` waits for indexing. Its response is a [TxResult](#49-txresult-response).

### 4.8 TxKey Response

The response body is a [`TxKey`](#24-txkey). The server returns it for `/tx/submit`.
It means that the server accepted the transaction and will log it durably.
`system_time` is the timestamp that is assigned to the transaction.

### 4.9 TxResult Response

```
{
  "status":         <int (0=committed, 1=aborted)>,
  "tx_id":          <int>,
  "system_time":    <Timestamp>,
  "error_message":  <str>|nil
}
```

The server returns this body for `/tx/execute`. `error_message` is present only if
`status = 1` (aborted, for example because of a constraint violation). Note: An aborted
transaction is not an HTTP-level error. The response is HTTP 200, and
the caller reads `status`.

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

`severity` is `"E"` for an error that affects only a single request or subscription.
After an `"E"` error, the server stays healthy and continues to serve other connections.
`severity` is `"F"` for an error that the server did not expect, or that the server possibly
cannot recover from. After `"F"`, clients cannot expect the connection to work. `code` is a numeric error code
(see §5). In the future, it can be useful to add another severity for streaming queries
that is between `"E"` and `"F"`. This severity is for the case where the IQ (incremental query)
stream cannot recover, but the server stays healthy.

### 4.11 Subscribe Request — `POST /db/subscribe`

```
{"tx_key": <TxKey>|nil, "query": <str>, "args": [<QueryArg>, ...]}
```

The body has the same `query` and `args` shape as a [Query Request](#44-query-request--post-dbquery),
but `tx_key` is optional. In this version, `tx_key` MUST be `nil` or omitted.
The server rejects a non-nil value with HTTP 400 / `InvalidQuery`. The field is reserved for future historical
replay. Also in this version, `args` MUST be empty. This field is reserved for future incremental `:in` bindings.
At some point, it can make sense to unify Query Request and Subscribe Request.
If the incremental engine does not support a query or its args yet, the server rejects the request
with HTTP 500 / `QueryError` (code 2001). This is the same as a standard query failure.

If the request succeeds, the response is HTTP 200 with `Content-Type: application/vnd.triplox+msgpack`.
The body is a frame stream (§4.12), not a single value.
The subscription starts at the latest indexed basis. If the result at that basis is not empty,
the server sends it as the first delta. Deltas for later transactions follow it.
To unsubscribe, close the HTTP/2 stream. This also releases the server-side resources.

### 4.12 Subscription Frame Stream

The `/db/subscribe` response body is a sequence of bare, self-delimiting
MessagePack frames. That is, it is a concatenation of MessagePack maps with no length prefix.
MessagePack is self-describing, so a decoder reads one frame at a time. Each frame is the
same kind of bare map as a unary body (the one-map body of other endpoints). The stream only repeats it.
HTTP/2 DATA chunks do not align to frame boundaries. Thus, a client accumulates bytes and
decodes one value at a time. Undecoded bytes that remain at the end of the stream are a protocol error.

Each frame is a map with a `kind` discriminator. Decoders MUST reject unknown
`kind` values as protocol errors.

#### `open` frame — always first, exactly once

```
{"kind": "open",
 "tx_key":  <TxKey>,
 "columns": [<ColumnDescription>, ...]}
```

`tx_key` is the registration basis. The registration basis is the basis where the subscription starts.
The priming result is the query result at the registration basis.
A delta with the priming result can use the same key. Later deltas describe transactions
strictly after it. `columns` matches [QueryResponse](#46-queryresponse).

#### `delta` frame — optional priming result, then one per affecting transaction

```
{"kind": "delta",
 "tx_key":  <TxKey>,
 "rows":    [[[<DataType>, ...], <int weight>], ...]}
```

If a non-empty priming result exists, it is the first delta and uses the registration
`tx_key`. Each later delta uses the key of the transaction that produced the change.
Each entry of `rows` is a 2-element array `[values, weight]`. `values` has one
`DataType` for each column, by position, in the column order of the `open` frame.
`weight` is a raw signed multiplicity. A multiplicity is the number of copies of a row.
A weight `> 0` means added, and a weight `< 0` means retracted. A weight is not limited to `±1`.
The server emits a `delta` frame only for a transaction that produces a non-empty change,
or for a non-empty priming result. Thus, `rows` is never empty.

#### `error` frame — terminal

```
{"kind": "error", "severity": "E"|"F", "code": <int>,
 "message": <str>, "detail": <str>|nil, "hint": <str>|nil}
```

If an error occurs after the 200 response headers (an engine failure or a server
shutdown), it arrives as an `error` frame. Then the server closes the stream.
If the server detects an error before streaming starts, the response is an ordinary non-200
response with an [ErrorResponse](#410-errorresponse--non-200-body) body.

---

## 5. Error Codes

| Range | Category              | Codes                                                              |
|-------|-----------------------|--------------------------------------------------------------------|
| 1xxx  | Connection errors     | 1000 ProtocolVersionMismatch, 1001 InvalidStartup                  |
| 2xxx  | Query errors          | 2000 ParseError, 2001 QueryError, 2002 InvalidQuery, 2003 EmptyQuery|
| 3xxx  | Transaction errors    | 3000 TxError, 3001 TxAborted, 3002 TxNotIndexed                    |
| 4xxx  | Internal and protocol errors | 4000 InternalError, 4001 MessageTooLarge, 4002 InvalidMessageType, 4003 QueryCancelled, 4004 ServerShuttingDown |

---

## 6. Future Extensions

The features that follow are deferred on purpose to a later protocol version:

- TLS: The protocol currently uses HTTP/2 cleartext. The HTTP/2 layer will handle
  TLS termination.
- Authentication: Currently, there is no authentication. A future revision will layer
  it onto HTTP headers.
- Query cancellation: Cancellation is cooperative and uses an HTTP/2 stream reset.
- Prepared statements / extended query protocol: Parameterized queries use a Parse, Bind,
  Describe, Execute, Sync flow.
- NoticeResponse: Informational messages that are not fatal (warnings, deprecation
  notices). They are different from ErrorResponse.
