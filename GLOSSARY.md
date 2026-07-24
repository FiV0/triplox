# Triplox Glossary

This glossary explains the high-level concepts in Triplox to help you understand the system architecture.

---

## Datalog Query Concepts

Triplox uses a Datalog-inspired query language to retrieve data from the database. Queries are declarative, meaning you describe what data you want rather than how to fetch it.

The query language uses **variables** (prefixed with `?`) to represent unknown values you want to find, and **patterns** that match against entity-attribute-value triples in the database. Queries consist of a **find specification** (which variables to return) and **where clauses** that constrain the data.

Where clauses can express complex logic through triple patterns, negation (Not), conjunction (And), disjunction (Or), and specialized joins (NotJoin, OrJoin). The query compiler analyzes these patterns to determine the most efficient execution plan, selecting appropriate indices and join strategies to retrieve the requested data.

---

## Index Types

To enable efficient queries from different perspectives, Triplox maintains five different index views of the same data: **EAV** (Entity-Attribute-Value), **AVE** (Attribute-Value-Entity), **AEV** (Attribute-Entity-Value), **AE** (Attribute-Entity), and **AV** (Attribute-Value).

Each index orders the data differently, optimizing for specific query patterns. For example, EAV is ideal when you know the entity and want to look up its attributes, while AVE is perfect when searching for entities by a specific attribute-value combination.

This multi-index strategy represents a classic database trade-off: write amplification (storing data in five different ways) in exchange for read performance (always having an optimized index for your query pattern). The query compiler automatically selects the best index based on which parts of your query pattern are known versus unknown.

---

## Indexer

The indexer is the write-side component responsible for transforming transaction data into queryable structures. It subscribes to the log to receive transactions asynchronously and materializes them across all five index views simultaneously.

The indexer takes incoming transaction operations, determines how they affect each index view, and writes the appropriate key-value pairs to storage. This process ensures that no matter what kind of query you run later, there's always an optimized index available to serve it efficiently.

The indexer enables the system's query performance—without it, querying would require scanning through the entire transaction log to find relevant data. By using the subscriber pattern to consume transactions asynchronously, the indexer can operate independently from the write path, allowing for more flexible scaling and deployment strategies.

---

## Log

The log is an append-only record of all transactions that have modified the database. It implements an event sourcing pattern where every change is captured as an immutable event with a unique transaction ID and timestamp.

The log serves multiple purposes: it provides a complete audit trail of all database modifications, enables time-travel queries to see historical states, and allows new consumers to catch up by replaying the transaction history. It also supports live subscriptions, where consumers can stream new transactions as they arrive.

Different implementations of the log exist for different deployment scenarios—in-memory for testing and development, file-based for single-machine persistence, and Kafka-based for distributed systems. Regardless of implementation, the log guarantees that transactions are totally ordered and can be replayed to reconstruct any database state.

---

## Node

The node is the central component that applications interact with, acting as the "database instance" that provides a unified interface for both submitting transactions and running queries.

When you submit a transaction to a node, it appends the transaction to the log where it receives a unique ID and timestamp. Subscribers then asynchronously consume the transaction from the log to update indices and perform other processing. When you query a node, it uses the query compiler to analyze your Datalog query and execute it against the indexed data.

The node design allows for different deployment topologies—you can have multiple nodes reading from the same shared log for read scaling, or a single node combining all functionality for simplicity. It encapsulates the complexity of the underlying components and presents a clean database-like API.

---

## Query Compiler

The query compiler is the read-side component that transforms declarative Datalog queries into efficient execution plans. It analyzes query patterns to understand which parts are known (constants) and which parts are unknown (variables), then selects the optimal index view to use.

Beyond index selection, the query compiler establishes a variable order for patterns and executes the resulting extenders with a multi-way Generic Join over the indexed data.

The query compiler's job is to make queries fast without requiring users to understand the underlying index structure. You write declarative queries expressing what you want, and the compiler figures out the best way to get it.

---

## Storage Layer

The storage layer provides persistent key-value storage for the indexed data. Triplox uses SlateDB, an embedded database that handles the low-level details of writing data to disk and reading it back efficiently.

The storage layer supports both in-memory and persistent backends, allowing you to choose between performance and durability based on your needs. In-memory storage is useful for testing or scenarios where data can be regenerated from the log, while persistent storage ensures data survives across restarts.

All indexed data created by the indexer is ultimately stored in this layer. When the query compiler executes a query, it reads from the storage layer using the chosen index view. The storage layer serves as the foundation that makes the entire query system possible.

---

## Subscriber

A subscriber is a component that consumes transactions from the log using an event-driven pattern. Subscribers enable the asynchronous, decoupled architecture where multiple independent consumers can process the same transaction stream without interfering with each other.

When a component subscribes to the log, it goes through two phases: first, it catches up on historical transactions starting from any point in the log's history (or from the beginning). Then it transitions to receiving live transactions as they're appended to the log. This two-phase approach allows new subscribers to join at any time and rebuild their state by replaying the transaction history.

The subscriber pattern is the mechanism that makes the log the single source of truth. Multiple subscribers can independently consume transactions—the indexer subscribing to update query indices, analytics systems subscribing to track metrics, auditing systems subscribing to monitor changes. If a subscriber falls behind, it automatically catches up by reading historical batches. This pattern enables flexible deployment topologies where components can be scaled, restarted, or added without affecting other parts of the system.

---

## Transaction

A transaction is a unit of change submitted to the database. It represents a batch of operations that modify data—adding new facts, updating existing information, or removing data. Each transaction is atomic, meaning all operations within it succeed or fail together.

When you submit a transaction to a node, it begins a journey through the system: first, it's appended to the log where it receives a unique transaction ID and timestamp. Next, subscribers (such as the indexer) consume the transaction from the log to update their respective views—the indexer updates all relevant index views, while other subscribers might perform analytics, auditing, or other processing. Finally, the changes are persisted to the storage layer. This event-driven pipeline ensures that every modification is recorded, asynchronously processed by interested consumers, and made queryable.

Transactions enable ordered, consistent changes to the database state. The transaction ID creates a total ordering of all modifications, allowing the system to reconstruct any historical state and reason about the sequence of changes over time.
