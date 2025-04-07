# Triplox v0.1 MVP - Todo Checklist

This checklist outlines the detailed, step-by-step tasks required to build the Triplox MVP. Each task is broken down to ensure incremental progress, comprehensive testing, and proper integration.

---

## 1. Project Setup & Data Model

- [ ] **Initialize Project**
  - [ ] Create a new Rust Cargo project (e.g., via `cargo new triplox`).
  - [ ] Set up a modular structure with directories/modules for:
    - `data`
    - `indexing`
    - `storage`
    - `tx_log`
    - `core_engine`
    - `query_engine`
    - `ivm_engine`
    - `server`
    - `client`
    - `tests` (or integrate tests in module files)

- [ ] **Define Core Data Types (Module: `data`)**
  - [ ] Create an enum `DataType` with variants:
    - Primitives: `Long`, `Boolean`, `Float`, `Double`, `String`, `Uuid`, `Bytes`, `Instant`, `BigInt`
    - Collections: `Vector`, `Map`
    - Reference: `Ref(i64)`
    - `Nil`
  - [ ] Create a struct `Triple` with fields:
    - `entity_id: i64`
    - `attribute: String`
    - `value: DataType`
    - `timestamp: i64`
    - `add: bool`
  - [ ] **Unit Tests**
    - [ ] Verify creation of a `Triple`.
    - [ ] Test serialization/deserialization (if applicable) for both `DataType` and `Triple`.

---

## 2. Indexing Functions (Module: `indexing`)

- [ ] **Implement Encoding Functions**
  - [ ] Develop functions to encode a `Triple` into binary keys for the four index types:
    - EAVT, AEVT, AVET, VAET
  - [ ] Decide on a binary key format (prefix byte + entity + attribute + value + timestamp + add flag).

- [ ] **Implement Decoding Functions**
  - [ ] Create functions to convert binary keys back into a `Triple`.

- [ ] **Unit Tests**
  - [ ] Validate that encoding followed by decoding returns the original `Triple` for each index type.
  - [ ] Cover edge cases in encoding/decoding.

---

## 3. Storage Engine Integration (Module: `storage`)

- [ ] **Define Storage Interface**
  - [ ] Create a trait `StorageEngine` with functions:
    - `fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError>;`
    - `fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError>;`

- [ ] **Implement a Mock/Stub Storage**
  - [ ] Provide an in-memory implementation simulating SlateDB for early testing.

- [ ] **Unit Tests**
  - [ ] Test writing data and retrieving it.
  - [ ] Validate error handling for missing keys or invalid data.

---

## 4. Transaction Log (Module: `tx_log`)

- [ ] **Define Transaction Types**
  - [ ] Create an enum `TxOp` with variants:
    - `Put(Document)` – for entity maps.
    - `Add(Triple)`
    - `Retract(Triple)`
    - `Delete(i64)` – deleting by entity ID.
    - `Erase(i64)` – permanently erasing an entity.
  - [ ] Define a transaction structure (batch of `TxOp`s) with:
    - Timestamp
    - Ordering information

- [ ] **Implement In-Memory Transaction Log**
  - [ ] Enable appending transactions.
  - [ ] Assign monotonically increasing timestamps.
  - [ ] Provide a replay mechanism to iterate over transactions in order.

- [ ] **Unit Tests**
  - [ ] Confirm that transactions are stored in the correct order.
  - [ ] Validate the replay functionality against expected operations.

---

## 5. Triplox Core Engine (Module: `core_engine`)

- [ ] **Transaction Processing**
  - [ ] Implement a function to apply a single `TxOp`, updating the indices (using `indexing` functions).
  - [ ] Implement a batch processing function to apply transactions from the log.

- [ ] **Recovery via Log Replay**
  - [ ] Create mechanisms for replaying the transaction log on startup.

- [ ] **Integration Tests**
  - [ ] Simulate submission of transaction batches.
  - [ ] Test log replay and validate that indices reflect the correct state.

---

## 6. Query Engine (Module: `query_engine`)

- [ ] **Basic Query Parsing**
  - [ ] Develop a simple parser or AST builder for Datalog-style queries (EDN for Clojure or ADT for Rust).

- [ ] **Query Evaluation**
  - [ ] Implement a query evaluator that fetches data from the indices.
  - [ ] Ensure proper handling of expected result sets.

- [ ] **Unit Tests**
  - [ ] Test simple queries with preloaded triples.
  - [ ] Validate error handling for malformed queries.

---

## 7. Incremental View Maintenance (IVM) Engine (Module: `ivm_engine`)

- [ ] **Design IVM Subscription API**
  - [ ] Define functions for subscribing to IVM queries.
  - [ ] Ensure the API returns an initial full result and streams diffs on new data.
  - [ ] Include an unsubscribe function.

- [ ] **Implement Basic/Stub IVM Engine**
  - [ ] Integrate a stub or basic implementation using DBSP (or a mock).

- [ ] **Tests**
  - [ ] Validate subscription returns correct initial results.
  - [ ] Simulate streaming updates (diffs) on transaction changes.
  - [ ] Confirm proper unsubscription.

---

## 8. TCP+SSL Protocol & Server (Module: `server`)

- [ ] **Network Setup**
  - [ ] Establish a TCP server that listens on a configurable port.
  - [ ] Integrate SSL for secure connections.

- [ ] **Implement Binary-Framed Protocol**
  - [ ] Define message framing: 1 byte type, 4 bytes length, and payload.
  - [ ] Define message types:
    - Startup
    - SubmitTx
    - TxAck
    - Query
    - QueryResultChunk
    - IVMSubscribe
    - IVMUpdate
    - IVMUnsubscribe
    - Heartbeat
    - Error

- [ ] **Message Handlers**
  - [ ] Implement handler functions for each message type.
  - [ ] Ensure error responses for malformed or unrecognized messages.

- [ ] **Testing**
  - [ ] Write tests to validate correct framing and parsing.
  - [ ] Simulate network disconnects and test error handling.
  - [ ] Verify integration with the core, query, and IVM engines.

---

## 9. Client Interfaces (Module: `client`)

- [ ] **Rust Client Implementation**
  - [ ] Develop a client to establish a secure TCP+SSL connection to the server.
  - [ ] Implement functions to:
    - Submit transaction batches (`SubmitTx`)
    - Issue queries (`Query`) and handle chunked responses
    - Subscribe to IVM updates and process streaming diffs

- [ ] **Integration Tests / Example Usage**
  - [ ] Test client connection to the server.
  - [ ] Verify submission of a transaction.
  - [ ] Execute a query and handle the response.
  - [ ] Simulate IVM subscription and update reception.

---

## 10. Wiring Everything Together (Main Entry Point)

- [ ] **Main Integration (main.rs)**
  - [ ] Initialize the storage engine.
  - [ ] Set up the transaction log.
  - [ ] Launch the core engine to process transactions.
  - [ ] Start the query engine.
  - [ ] Activate the IVM engine (if applicable).
  - [ ] Start the TCP+SSL server and begin listening for client connections.

- [ ] **End-to-End Integration Test**
  - [ ] Create an end-to-end test or sample run script that:
    - Submits a batch of transactions.
    - Replays the log on startup.
    - Executes a query and prints results.
    - Demonstrates IVM subscription with live updates.
    - Validates that all components interact correctly.
  - [ ] Ensure graceful startup error handling and proper shutdown procedures.

---

## 11. Testing & Documentation

- [ ] **Unit & Integration Testing**
  - [ ] Run all unit tests for each module.
  - [ ] Execute integration tests to cover full system scenarios.

- [ ] **Documentation**
  - [ ] Document each module and overall architecture in a README.md.
  - [ ] Include inline comments and usage instructions.
  - [ ] Outline future considerations (e.g., schema definitions, as-of queries, access control).

---

## 12. Future Considerations (Post-MVP Planning)

- [ ] Schema definitions (v0.2)
- [ ] As-of queries (v0.2)
- [ ] Attribute/entity introspection (v0.2)
- [ ] Sharded index structure (v0.3)
- [ ] TTL / data compaction (Later)
- [ ] Access control (Later)
- [ ] Re-indexing support (Later)

---

This checklist ensures that each component of Triplox is built incrementally and integrated with strong testing and clear documentation. Use it to track progress and ensure that no part is left unintegrated.
