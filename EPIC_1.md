# Epic 1: Transaction Processing & Indexing - Implementation Plan

**Epic ID:** triplox-xup
**Scope:** Transaction write path (log → indexer → indices)
**Out of Scope:** Query execution (deferred to Epic 2)

## Overview

Implement the complete transaction processing pipeline to get transactions flowing through the system and verify indices are correctly updated. This establishes the **write path** for Triplox.

**Current State:**
- ✅ Indexer core logic complete (transact_tx writes to all 5 indices)
- ✅ Subscriber pattern fully defined and tested
- ✅ Log implementations working (MemoryLog, FileLog)
- ✅ Indexer has broadcast-based synchronization (await_tx method exists!)
- ❌ Indexer doesn't implement Subscriber trait
- ❌ Node API methods are all todo!() placeholders
- ❌ No integration between components

**Goal:** Execute transactions via both execute_tx() and submit_tx(); verify all 5 indices updated correctly.

---

## Work Breakdown (4 Tasks)

### Task 1: Indexer Subscriber Integration

**Goal:** Make Indexer implement Subscriber trait to receive transactions from the log.

**Changes Required:**

**File:** `src/indexer.rs`

**Good news:** The Indexer already has a broadcast-based synchronization mechanism:
- `latest_indexed_tx: Option<TxKey>` - tracks the latest indexed transaction
- `tx_completion_sender: broadcast::Sender<TxKey>` - broadcasts when transactions complete
- `await_tx(tx_key)` method - returns a future that waits for a specific transaction to be indexed

**No changes needed to Indexer struct!** The synchronization is already better than the polling approach originally planned.

**Implement Subscriber trait:**
```rust
impl Subscriber for Indexer {
    async fn accept(&mut self, record: Record) {
        let tx_ops: Vec<TxOp> = bincode::deserialize(&record.record)
            .expect("Failed to deserialize TxOps from log record");
        self.transact_tx(record.tx_key, tx_ops).await
            .expect("Indexer failed to process transaction");
    }
}
```

**Design Decisions:**
- accept() is async, so transact_tx can be awaited directly (no block_in_place needed)
- Log errors without panicking (system continues running)
- No new fields needed - existing broadcast channel handles synchronization
- Keep attribute_to_id as regular HashMap (no Arc<RwLock> needed since Indexer is already behind RwLock)

**Validation:**
- Compile successfully
- Existing indexer tests still pass

---

### Task 2: Node::execute_tx() Implementation

**Goal:** Synchronous transaction execution that waits for indexing completion.

**Changes Required:**

**File:** `src/node.rs`

1. **Change Node struct to use Arc<RwLock> for sharing:**
   ```rust
   pub struct Node {
       log: Arc<RwLock<dyn TxLog>>,      // Changed from Box
       indexer: Arc<RwLock<Indexer>>,    // Changed from Box
       slatedb: Arc<slatedb::Db>,
   }
   ```

2. **Update Node::memory_node() to wire subscription:**
   ```rust
   pub async fn memory_node() -> Self {
       let slatedb = Arc::new(in_memory_slate().await);
       let indexer = Arc::new(RwLock::new(Indexer::new(slatedb.clone())));
       let log = Arc::new(RwLock::new(MemoryLog::new(Box::new(clock::SystemClock))));

       // Wire up subscription (log → indexer)
       subscribe(log.clone(), None, indexer.clone());

       Node { log, indexer, slatedb }
   }
   ```

3. **Implement execute_tx:**
   ```rust
   async fn execute_tx(&self, ops: Vec<TxOp>) -> TransactionResult {
       // 1. Serialize TxOps to Vec<u8>
       let serialized = match bincode::serialize(&ops) {
           Ok(data) => data,
           Err(e) => return TransactionResult::TxAborted(
               TxKey { tx_id: -1, system_time: Instant::now() },
               Box::new(e)
           ),
       };

       // 2. Append to log
       let tx_key = {
           let mut log = self.log.write().unwrap();
           log.append_tx(serialized).await
       };

       // 3. Wait for indexer to complete using await_tx
       // The lock is dropped before awaiting, so indexer can process
       let wait_future = self.indexer.read().unwrap().await_tx(tx_key);
       match wait_future.await {
           Ok(_) => TransactionResult::TxCommited(tx_key),
           Err(e) => TransactionResult::TxAborted(tx_key, e.into()),
       }
   }
   ```

**Design Decisions:**
- Use Arc<RwLock> to share log and indexer between Node and subscriber thread
- Serialize TxOps using bincode before appending to log
- Use existing `await_tx()` method which uses broadcast channel (already implemented!)
- The await_tx future doesn't borrow self, so RwLock can be dropped before awaiting

**Validation:**
- execute_tx returns after indexing completes
- Indices contain correct data after execute_tx returns

---

### Task 3: Node::submit_tx() Implementation

**Goal:** Async fire-and-forget transaction submission returning TxKey immediately.

**Changes Required:**

**File:** `src/node.rs`

1. **Implement submit_tx:**
   ```rust
   async fn submit_tx(&self, ops: Vec<TxOp>) -> TxKey {
       // 1. Serialize TxOps
       let serialized = bincode::serialize(&ops)
           .expect("Serialization should not fail");

       // 2. Append to log (don't wait for indexing)
       let mut log = self.log.write().unwrap();
       log.append_tx(serialized).await
   }
   ```

**Design Decisions:**
- Much simpler than execute_tx - no waiting required
- Panic on serialization failure (indicates programming bug, not runtime error)
- Return immediately after log write (indexer processes asynchronously)

**Validation:**
- submit_tx returns quickly (doesn't wait for indexing)
- Data is eventually indexed (verify after delay)

---

### Task 4: Integration Tests

**Goal:** End-to-end tests proving transactions flow from Node → Log → Indexer → Indices.

**Changes Required:**

**File:** `tests/transaction_test.rs` (NEW)

**Test Cases:**

1. **test_execute_tx_basic** - Basic execute_tx flow with Put operation
   - Create Node, transact data, verify TxCommited result
   - Query SlateDB indices directly to verify data was indexed

2. **test_submit_tx_basic** - submit_tx basic flow
   - Call submit_tx and get TxKey
   - Wait briefly, verify data eventually indexed

3. **test_multiple_transactions** - Sequential transactions
   - Execute 10 transactions in sequence
   - Verify all entities indexed correctly

4. **test_add_retract** - Add/Retract operations
   - Add a triple, verify ADD in indices
   - Retract same triple, verify RETRACT in indices

5. **test_all_indices_populated** - All 5 indices verification
   - Transact data
   - Scan all 5 indices (EAV, AVE, AEV, AE, AV)
   - Verify each contains correct keys

6. **test_transaction_ordering** - Ordering guarantees
   - Verify execute_tx waits for indexing completion
   - Test transaction ordering guarantees

**Helper Functions Needed:**
- `query_eav_index()` - Read data from EAV index
- `query_ave_index()` - Read data from AVE index
- `query_aev_index()` - Read data from AEV index
- `query_ae_index()` - Read data from AE index
- `query_av_index()` - Read data from AV index
- `read_attribute_map()` - Get attribute name → ID mapping

**Validation:**
- All tests pass with `cargo test`
- Integration tests cover happy path and error cases

---

## Critical Files to Modify

1. **`src/indexer.rs`**
   - Add Subscriber trait implementation
   - Use existing broadcast channel mechanism (no new fields needed)
   - Add deserialization in accept()

2. **`src/node.rs`**
   - Change to Arc<RwLock> for log and indexer
   - Implement execute_tx with serialization + await_tx
   - Implement submit_tx (fire-and-forget)
   - Wire subscription in memory_node()

3. **`tests/transaction_test.rs`** (NEW FILE)
   - 6+ integration test cases
   - Helper functions for querying indices
   - End-to-end flow validation

**Reference Files (read-only):**
- `src/log.rs` - Subscriber trait and subscribe() function
- `src/memory_log.rs` - Example of subscription pattern in tests
- `src/ops.rs` - TxOp definitions and serialization

---

## Architecture Decisions

### 1. Synchronization: execute_tx Waiting

**Decision:** Use existing `await_tx()` method with broadcast channel.

**Why:** Already implemented! Uses broadcast::Sender<TxKey> to notify waiters when transactions complete.

**Implementation:**
- Indexer broadcasts TxKey after successful transact_tx (lines 180-186 in indexer.rs)
- execute_tx calls `indexer.await_tx(tx_key)` which subscribes to channel
- Fast path: returns immediately if transaction already indexed
- Slow path: waits on broadcast channel for completion notification
- The await_tx future doesn't borrow self, so RwLock can be dropped before awaiting

**Benefits:** More efficient than polling, scales better with multiple waiters

### 2. Error Handling in Subscriber.accept()

**Decision:** Log errors with error!() macro and continue processing.

**Why:** Don't crash subscriber thread. System continues running.

**Error scenarios:**
- Deserialization failure: log + skip
- Indexing failure: log + skip
- Invalid attributes: caught by indexer assertions

**Future:** Add metrics, dead-letter queue for retry

### 3. Shared State: attribute_to_id HashMap

**Decision:** Keep as regular HashMap<String, u64>

**Why:** Indexer itself is already wrapped in Arc<RwLock<Indexer>>, so the HashMap is already protected

**Impact:** No changes needed to get_and_create_attribute_id - it already works with &mut self

### 4. Serialization/Deserialization Location

**Decision:**
- Serialize in Node (execute_tx/submit_tx) before log.append_tx
- Deserialize in Indexer.accept() after receiving Record

**Why:** Clear separation of concerns. Log is agnostic to transaction contents.

---

## Success Criteria (Detailed)

### Functional Requirements:
- ✅ execute_tx successfully transacts data and waits for indexing completion
- ✅ submit_tx returns immediately without waiting for indexing
- ✅ All 5 indices (EAV, AVE, AEV, AE, AV) populated correctly
- ✅ Put, Add, and Retract operations work end-to-end
- ✅ Transaction ordering preserved (tx N+1 waits for tx N)
- ✅ Errors logged appropriately (deserialization, indexing failures)

### Testing Requirements:
- ✅ All existing tests pass (cargo test)
- ✅ 6+ new integration tests pass
- ✅ Manual verification: transact 100 entities, verify all indices

### Code Quality:
- ✅ No compiler warnings
- ✅ Proper error handling with Result types
- ✅ Logging at appropriate levels (trace/info/error)
- ✅ Code comments on complex logic

---

## Out of Scope (Epic 2)

The following are explicitly deferred to Epic 2 (Query Execution Pipeline):

- ❌ DB::query() implementation
- ❌ QueryResult types and serialization
- ❌ Simple pattern execution (query_exec.rs)
- ❌ Pattern-to-iterator bridge (PrefixExtender)
- ❌ Query validation logic

**Rationale:** Epic 1 = write path, Epic 2 = read path. Clean separation.

---

## Implementation Sequence

1. **Task 1: Indexer Subscriber** (~1 day)
   - Implement Subscriber trait
   - Use existing broadcast mechanism

2. **Task 2: Node::execute_tx()** (~1 day)
   - Refactor to Arc<RwLock>
   - Implement serialization + await_tx
   - Wire subscription in memory_node()

3. **Task 3: Node::submit_tx()** (~0.5 day)
   - Implement fire-and-forget logic
   - Simpler than execute_tx

4. **Task 4: Integration Tests** (~1-2 days)
   - Create test file and helpers
   - Write 6+ test cases
   - Debug and iterate

**Total Estimate:** 3-4 days

---

## Verification Plan

After implementation, run these verification steps:

1. **Automated Tests:**
   ```bash
   cargo test
   cargo test --test transaction_test
   ```

2. **Manual Index Verification:**
   ```rust
   // Create node, transact data
   let node = Node::memory_node().await;
   node.execute_tx(vec![TxOp::Put(...)]).await;

   // Scan SlateDB raw keys
   let iter = node.slatedb.scan_from(vec![codec::EAV]);
   // Verify keys exist for all 5 indices
   ```

3. **Stress Test:**
   ```rust
   // Transact 100 entities
   for i in 0..100 {
       node.execute_tx(vec![TxOp::Put(...)]).await;
   }
   // Verify all indexed correctly
   ```

---

## Next Steps After Epic 1

Once Epic 1 is complete:

1. **Update Epic 2 (Query Execution Pipeline):**
   - Implement DB::query()
   - Create query_exec.rs
   - Pattern-to-iterator bridge
   - Single-pattern queries

2. **Continue with Epic 3 (Multi-Pattern Joins):**
   - Use Generic Join algorithm
   - Join variable analysis
   - Result projection
