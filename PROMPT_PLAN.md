Below is a comprehensive blueprint for building Triplox (v0.1 MVP), followed by a series of iterative, test-driven prompt sections. Each section builds on the previous ones, gradually wiring together a robust system while enforcing small, safe increments and continuous testing.

---

## Blueprint Overview

**1. Project Initialization & Data Model**
• **Step 1.1:** Create a new Rust project (Cargo workspace) with a clear module structure.
• **Step 1.2:** Define the core data types – the `DataType` enum and the `Triple` struct representing facts (entity, attribute, value, timestamp, add flag).
• **Step 1.3:** Write unit tests to validate serialization and basic operations for these types.

**2. Indexing Functions**
• **Step 2.1:** Implement functions to encode a `Triple` into binary-prefixed keys (for indices: EAVT, AEVT, AVET, VAET).
• **Step 2.2:** Create decoding functions to convert binary keys back into triples.
• **Step 2.3:** Write unit tests covering all encoding/decoding edge cases.

**3. Storage Engine Integration (SlateDB)**
• **Step 3.1:** Define a generic storage trait (e.g., `StorageEngine`) for read/write operations.
• **Step 3.2:** Implement a module that interfaces with SlateDB (or a stub/mock version for early testing).
• **Step 3.3:** Develop tests to ensure the storage interface behaves correctly.

**4. Transaction Log**
• **Step 4.1:** Define transaction types (`TxOp` enum including Put, Add, Retract, Delete, Erase) and the transaction structure.
• **Step 4.2:** Implement an in-memory transaction log for ordering, timestamping, and serializing transactions.
• **Step 4.3:** Write tests to verify transaction ordering and replay functionality.

**5. Triplox Core Engine**
• **Step 5.1:** Create functions that apply a single transaction operation, updating the indices accordingly.
• **Step 5.2:** Build the engine that processes batches of transactions by reading from the log and applying them.
• **Step 5.3:** Write integration tests to simulate transaction replay and validate index updates.

**6. Query Engine**
• **Step 6.1:** Implement a basic parser (or ADT builder) for simple Datalog/EDN queries.
• **Step 6.2:** Develop the evaluator that retrieves data from the indices to answer queries.
• **Step 6.3:** Write unit tests to ensure the query engine returns expected results.

**7. Incremental View Maintenance (IVM) Engine**
• **Step 7.1:** Design the subscription API to support IVM (initial full result plus streaming diffs).
• **Step 7.2:** Integrate a stub or basic implementation using DBSP for managing live query state.
• **Step 7.3:** Write tests for subscribing, receiving diffs, and unsubscribing.

**8. TCP+SSL Protocol & Server**
• **Step 8.1:** Set up a TCP+SSL server skeleton that accepts connections.
• **Step 8.2:** Implement the custom binary-framed protocol (framing: type, length, payload).
• **Step 8.3:** Write tests to validate framing correctness, message parsing, and error responses.

**9. Client Interfaces**
• **Step 9.1:** Develop a basic Rust client library to interact with the server over TCP+SSL.
• **Step 9.2:** (Plan for later) Outline a Clojure client interface.
• **Step 9.3:** Write integration tests that simulate client-server communication.

**10. Wiring Everything Together**
• **Step 10.1:** Create the main entry point that instantiates the storage engine, transaction log, core engine, query engine, IVM engine, and TCP server.
• **Step 10.2:** Wire all components together with proper initialization and graceful shutdown.
• **Step 10.3:** Write end-to-end integration tests that simulate a full lifecycle (transaction submission, query processing, IVM updates).

---

## Iterative, Test-Driven Prompts for a Code-Generation LLM

Below are the detailed prompt sections. Each prompt is marked as a separate markdown code block tagged as `text`. These prompts should be provided one after the other, ensuring that each step builds on the previous implementation and concludes with wiring components together.

---

### **Prompt Section 1: Project Setup and Data Model**

```text
[Prompt Section 1]
You are to initialize the Triplox project as a new Rust Cargo project. Create a clear module structure that includes at least the following modules:
- `data`: for the core data types.
- `tests`: for unit tests.

In the `data` module, define:
1. An enum `DataType` that includes variants for:
   - Primitive types: Long, Boolean, Float, Double, String, Uuid, Bytes, Instant, BigInt.
   - Collection types: Vector, Map.
   - A reference type: Ref(i64).
   - A Nil variant.
2. A struct `Triple` with the fields:
   - `entity_id: i64`
   - `attribute: String`
   - `value: DataType`
   - `timestamp: i64`
   - `add: bool`

Write unit tests (using Rust’s built-in test framework) to verify:
- Proper creation of a `Triple`.
- Basic serialization and deserialization (if applicable) of `DataType` and `Triple`.

Your output should include all code for the new Cargo project setup, the `data` module, and the associated unit tests. End your prompt by ensuring that the test module is wired to run with `cargo test`.
```

---

### **Prompt Section 2: Indexing Functions**

```text
[Prompt Section 2]
Building on the previously defined data model, implement functions in a new module called `indexing` that can encode a `Triple` into a binary key and decode a binary key back into a `Triple`. The encoding should support the four index types: EAVT, AEVT, AVET, and VAET. Consider a key format that begins with a prefix byte indicating the index type, followed by the entity, attribute, value, timestamp, and add flag in a binary representation.

Ensure that:
1. There are clear functions for encoding and decoding.
2. Each function is accompanied by unit tests that validate the correctness of the encoding/decoding process for all four index types.

Include instructions for how the tests should run to confirm that the keys are reversible. The prompt should end with wiring these functions into the overall module structure.
```

---

### **Prompt Section 3: Storage Engine Integration (SlateDB)**

```text
[Prompt Section 3]
Next, implement a storage abstraction layer in a module named `storage`. Define a Rust trait, `StorageEngine`, with functions such as:
- `fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError>;`
- `fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError>;`

Then, provide an initial implementation (a stub or a mock) that simulates interaction with SlateDB. This mock should allow you to store and retrieve binary data in-memory, enabling early testing.

Include unit tests to:
1. Verify that data can be written and then retrieved correctly.
2. Test error handling for missing keys or invalid data.

Ensure the module and its tests are fully integrated into the project structure.
```

---

### **Prompt Section 4: Transaction Log**

```text
[Prompt Section 4]
Now, create a module named `tx_log` for handling transactions. In this module:
1. Define the enum `TxOp` with variants:
   - `Put(Document)` – for entity maps.
   - `Add(Triple)`
   - `Retract(Triple)`
   - `Delete(i64)` – for deleting an entity by ID.
   - `Erase(i64)` – for permanently erasing an entity.
2. Create a structure to represent a transaction (which could be a batch of `TxOp`s), and include a timestamp and ordering information.

Implement an in-memory transaction log that:
- Allows appending transactions.
- Assigns monotonically increasing timestamps.
- Supports replaying transactions (i.e., iterating over logged transactions in order).

Write unit tests to:
1. Verify that transactions are stored in the correct order.
2. Validate the replay functionality by ensuring that replayed transactions produce the expected operations.

Conclude this section by integrating the transaction log module with the previous modules.
```

---

### **Prompt Section 5: Triplox Core Engine**

```text
[Prompt Section 5]
Develop the core engine in a module named `core_engine`. This component is responsible for:
1. Reading transactions from the `tx_log`.
2. Applying each transaction’s operations to update the indices using the functions from the `indexing` module.
3. Interacting with the `storage` layer to persist index updates.

Your implementation should include:
- A function to apply a single `TxOp` to update the in-memory index.
- A function to process a batch of transactions and update indices accordingly.
- Mechanisms for replaying the transaction log upon startup for recovery.

Write integration tests that simulate:
1. Submitting a batch of transactions.
2. Replaying the log.
3. Verifying that the indices reflect the correct state.

End this prompt by wiring the core engine with the transaction log and indexing modules.
```

---

### **Prompt Section 6: Query Engine**

```text
[Prompt Section 6]
Create a module called `query_engine` to implement a basic query interpreter. For the MVP:
1. Design a simple parser (or AST builder) that can interpret basic Datalog-style queries (EDN for Clojure or an ADT for Rust).
2. Implement a query evaluator that accesses the indices built by the core engine and returns result sets.

Include unit tests to:
1. Verify that simple queries return expected results given a preloaded set of triples.
2. Test edge cases and error handling for malformed queries.

Ensure that the query engine module is wired into the core engine so that queries can run against the current state.
```

---

### **Prompt Section 7: Incremental View Maintenance (IVM) Engine**

```text
[Prompt Section 7]
Introduce the IVM engine by creating a module named `ivm_engine`. This component should:
1. Define an API for IVM subscriptions that accepts a query and returns an initial full result along with streaming diffs.
2. Include a basic implementation (or stub) that simulates IVM using DBSP. For the MVP, you may mock the DBSP integration, ensuring the API structure is in place.

Write tests to:
1. Validate that subscribing to a query returns the correct initial result.
2. Simulate live updates (streaming diffs) when new transactions are applied.
3. Confirm that unsubscribing terminates the update stream.

Make sure this module is integrated with the core engine and query engine.
```

---

### **Prompt Section 8: TCP+SSL Protocol & Server**

```text
[Prompt Section 8]
Develop the network interface by creating a module named `server`. This module must:
1. Set up a TCP server that listens on a configurable port and accepts SSL connections.
2. Implement the custom binary-framed protocol. Each message should follow the structure:
   - 1 byte for the message type.
   - 4 bytes for the payload length.
   - The payload itself (binary data).

Define the message types:
- Startup, SubmitTx, TxAck, Query, QueryResultChunk, IVMSubscribe, IVMUpdate, IVMUnsubscribe, Heartbeat, Error.

Implement handlers for each message type, ensuring that:
- Incoming messages are parsed correctly.
- Appropriate error messages are returned for malformed or unrecognized messages.

Write tests to:
1. Validate that messages are correctly framed and parsed.
2. Simulate network disconnects and verify proper error handling.

Conclude this section by wiring the server to call into the core engine, query engine, and IVM engine.
```

---

### **Prompt Section 9: Client Interfaces**

```text
[Prompt Section 9]
Create a module named `client` for the Rust client interface. This client should:
1. Establish a secure TCP+SSL connection to the Triplox server.
2. Implement functions to:
   - Send transaction batches (`SubmitTx`).
   - Query the database (`Query`) and handle chunked results.
   - Subscribe to IVM updates and handle incoming diffs.

Write integration tests (or example usage code) that:
1. Connect to the server.
2. Submit a transaction.
3. Perform a query.
4. Subscribe to an IVM query and simulate receiving updates.

Wire this client module with the protocol specifications, ensuring it uses the same binary framing.
```

---

### **Prompt Section 10: Wiring Everything Together (Main Entry Point & Integration Tests)**

```text
[Prompt Section 10]
Finally, create the main entry point for Triplox. In your `main.rs` file, wire together all the components:
1. Initialize the storage engine.
2. Set up the transaction log.
3. Start the core engine to process transactions.
4. Launch the query engine.
5. (Optionally) Activate the IVM engine.
6. Start the TCP+SSL server and listen for client connections.

Include an end-to-end integration test or a sample run script that:
- Submits a batch of transactions.
- Replays the log on startup.
- Runs a query and prints the result.
- Demonstrates an IVM subscription with live updates.
- Verifies that all components interact correctly.

Ensure that your main entry point gracefully handles startup errors and shutdown signals.

This prompt should fully integrate all previously developed modules and include comments/documentation explaining the wiring and interaction between components.
```

---

By following these prompt sections in order, you will gradually build the Triplox MVP with robust unit and integration tests, ensuring that each component is validated before moving on to the next. Each prompt is designed to produce small, testable pieces of code that ultimately wire together into a fully integrated system.
