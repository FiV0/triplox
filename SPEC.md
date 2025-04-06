# Triplox Specification (v0.1 MVP)

## Overview

**Triplox** is a temporal, immutable triple-store database written in **Rust**, inspired by **Datomic**, and backed by the **SlateDB** distributed key-value store. It supports rich querying, transactions, and an optional **incremental view maintenance (IVM)** engine powered by **DBSP**. Clients connect over **TCP+SSL** and communicate using a custom **binary-framed protocol**.

---

## Architecture

### Core Components
- **Storage Engine**: Triplox uses **SlateDB** for durable, distributed key-value storage.
- **Triplox Core Engine**:
  - Applies and indexes transactions.
  - Maintains four indices: `EAVT`, `AEVT`, `AVET`, `VAET`.
  - Encodes triples as binary-prefixed keys.
- **Transaction Log**:
  - Supports multiple backends: in-memory, local file, Kafka.
  - Assigns timestamps and serializes transactions.
  - Drives replay and recovery logic.
- **Query Engine**:
  - Interprets EDN (Clojure) or ADT (Rust) queries.
  - Supports Datalog-style queries.
- **IVM Engine**: Built on **DBSP**.
- **Client Interfaces**:
  - Initial support for **Rust** and **Clojure** clients.

---

## Data Model

### Triples
Each fact in Triplox is a triple:
```
(entity_id: i64, attribute: String, value: DataType, timestamp: i64, add: bool)
```

### Value Types
Defined in the `DataType` enum. Includes:
- Primitives: `Long`, `Boolean`, `Float`, `Double`, `String`, `Uuid`, `Bytes`, `Instant`, `BigInt`
- Collections: `Vector`, `Map`
- Ref: `Ref(i64)`
- Nil

### Indexing
Triples are indexed into:
- `EAVT`, `AEVT`, `AVET`, `VAET` — stored as **prefixed binary keys** in SlateDB
- Key format: `[prefix-byte][entity][attribute][value][timestamp][add]`
- Value: empty or may contain metadata (TBD)

---

## Transactions

### Format
Each transaction is a sequence of operations:
```
enum TxOp {
    Put(Document),         // Entity map
    Add(Triple),
    Retract(Triple),
    Delete(EntityId),
    Erase(EntityId),
}
```

### Submission Flow
- Submitted to **log** (in-memory, file, Kafka)
- Log assigns **timestamp** and ensures **serial ordering**
- Triplox **replays log** to apply transactions into indices

### Entity IDs
- Clients may submit:
  - **Custom IDs**: `i64` (UUIDs later)
  - **Tempids**: Resolved during indexing
- Server returns a **tempid → real ID map** in `TxAck`

### Status Tracking
- Clients may query a **tx status table**
- Optional blocking API waits for tx to be **indexed**, returns result and tx-id

---

## Querying

### Datalog Queries
- **Clojure client** submits raw EDN queries
- **Rust client** uses an AST-like ADT
- MVP supports:
  - Basic Datalog queries
  - `triplox/q` → returns full result set
- Planned (not in MVP):
  - Rules
  - Pull queries
  - Nested expressions
  - `as-of` queries (time-travel)

---

## Incremental View Maintenance (IVM)

### Subscription API
- `triplox/q-subscribe` (naming TBD)
- Client sends normal query with IVM intent
- Server responds:
  1. Initial **full result**
  2. **Streaming diffs** as new data arrives

### Engine
- Built on **DBSP**
- Keeps query state alive until **client cancels**

---

## Protocol

### Transport
- Persistent **TCP + SSL** connection
- Custom **binary-framed** messages:
  ```
  [type (1 byte)][length (4 bytes)][payload (length bytes)]
  ```

### Message Types
| Type              | Description                             |
|-------------------|-----------------------------------------|
| `Startup`         | Handshake / inception                   |
| `SubmitTx`        | Submit transaction batch                |
| `TxAck`           | Transaction applied + tempid mapping    |
| `Query`           | Submit a datalog query                  |
| `QueryResultChunk`| Chunked query results                   |
| `IVMSubscribe`    | Subscribe to IVM query                  |
| `IVMUpdate`       | Streaming diffs for a view              |
| `IVMUnsubscribe`  | Cancel a subscription                   |
| `Heartbeat`       | Keepalive                               |
| `Error`           | Structured + human-readable error       |

### Error Message
```
struct Error {
  code: ErrorCode,
  message: String
}
```
- Code is structured (e.g., `ERR_INVALID_QUERY`, `ERR_TX_FAILED`)
- Message is human-readable

---

## Persistence & Recovery

### Transaction Replay
- On restart, Triplox:
  - Scans SlateDB for **last indexed tx**
  - **Replays** transactions from the log starting from there

### ACID Guarantees
- **Durability & isolation** handled by **SlateDB**
- Triplox ensures **serializability via log**
- Indexing is deterministic and **replayable**

---

## Access Control
- **None in MVP**
- Authentication or role-based access may be introduced later

---

## Testing Plan

### Unit Tests
- Encoding/decoding of triples, documents, transactions
- Index writing and lookup by all index types
- Query evaluation engine with small static datasets

### Integration Tests
- Tx submission + indexing flow
- Log replay after simulated crash
- Query results vs. expected values
- IVM subscription + live update tests

### Protocol Tests
- Binary framing correctness
- Error responses to malformed queries / txs
- Simulated network disconnects and reconnects

### Load/Stress Testing (Later)
- Large volume of txs
- High IVM subscription churn
- Log replay performance

---

## Future Considerations

| Feature                | Phase |
|------------------------|-------|
| Schema definitions     | v0.2  |
| As-of queries          | v0.2  |
| Attribute/entity introspection | v0.2  |
| Sharded index structure (current/historic) | v0.3 |
| TTL / data compaction  | Later |
| Access control         | Later |
| Re-indexing support    | Later |
