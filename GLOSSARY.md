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

The indexer is the write-side component responsible for transforming transaction data into queryable structures. It receives transactions from the LogProcessor and materializes them across all five index views simultaneously.

The indexer takes incoming transaction operations, determines how they affect each index view, and writes the appropriate key-value pairs to storage. This process ensures that no matter what kind of query you run later, there's always an optimized index available to serve it efficiently.

The indexer enables the system's query performance—without it, querying would require scanning through the entire transaction log to find relevant data. By receiving transactions asynchronously from the LogProcessor, the indexer can operate independently from the write path, allowing for more flexible scaling and deployment strategies.

---

## Log

The log is an append-only record of all transactions that have modified the database. It implements an event sourcing pattern where every change is captured as an immutable event with a unique transaction ID and timestamp.

The log serves multiple purposes: it provides a complete audit trail of all database modifications, enables time-travel queries to see historical states, and allows new consumers to catch up by replaying the transaction history. It also supports live subscriptions, where consumers can stream new transactions as they arrive.

Different implementations of the log exist for different deployment scenarios—in-memory for testing and development, file-based for single-machine persistence, and Kafka-based for distributed systems. Regardless of implementation, the log guarantees that transactions are totally ordered and can be replayed to reconstruct any database state.

---

## LogProcessor

The LogProcessor is an event-driven component that acts as the bridge between the transaction log and the indexer. It subscribes to the log and asynchronously processes each transaction as it arrives, decoupling the write path from the indexing path.

This decoupling enables a more flexible architecture. The Node can focus solely on accepting transactions and appending them to the log, while the LogProcessor independently consumes those transactions and coordinates indexing. This separation allows for different deployment topologies—you could have multiple LogProcessors consuming the same log for parallel processing, or scale the write and read paths independently.

By making the log the single source of truth with independent consumers, the LogProcessor enables an event-driven architecture where different components can react to transactions without tight coupling. This improves system modularity and allows components to evolve independently.

---

## Node

The node is the central component that applications interact with, acting as the "database instance" that provides a unified interface for both submitting transactions and running queries.

When you submit a transaction to a node, it appends the transaction to the log where it receives a unique ID and timestamp. The LogProcessor then asynchronously picks up the transaction and coordinates the indexing process. When you query a node, it uses the query compiler to analyze your Datalog query and execute it against the indexed data.

The node design allows for different deployment topologies—you can have multiple nodes reading from the same shared log for read scaling, or a single node combining all functionality for simplicity. It encapsulates the complexity of the underlying components and presents a clean database-like API.

---

## Query Compiler

The query compiler is the read-side component that transforms declarative Datalog queries into efficient execution plans. It analyzes query patterns to understand which parts are known (constants) and which parts are unknown (variables), then selects the optimal index view to use.

Beyond index selection, the query compiler also determines the best join order when queries involve multiple patterns. It considers which variables are shared between patterns and chooses strategies to minimize the amount of data that needs to be processed. The compiler uses sophisticated multi-way join algorithms like leapfrog trie-join to efficiently navigate the indexed data.

The query compiler's job is to make queries fast without requiring users to understand the underlying index structure. You write declarative queries expressing what you want, and the compiler figures out the best way to get it.

---

## Storage Layer

The storage layer provides persistent key-value storage for the indexed data. Triplox uses SlateDB, an embedded database that handles the low-level details of writing data to disk and reading it back efficiently.

The storage layer supports both in-memory and persistent backends, allowing you to choose between performance and durability based on your needs. In-memory storage is useful for testing or scenarios where data can be regenerated from the log, while persistent storage ensures data survives across restarts.

All indexed data created by the indexer is ultimately stored in this layer. When the query compiler executes a query, it reads from the storage layer using the chosen index view. The storage layer serves as the foundation that makes the entire query system possible.

---

## Transaction

A transaction is a unit of change submitted to the database. It represents a batch of operations that modify data—adding new facts, updating existing information, or removing data. Each transaction is atomic, meaning all operations within it succeed or fail together.

When you submit a transaction to a node, it begins a journey through the system: first, it's appended to the log where it receives a unique transaction ID and timestamp. Next, the LogProcessor consumes the transaction from the log and sends it to the indexer, which updates all relevant index views. Finally, the changes are persisted to the storage layer. This event-driven pipeline ensures that every modification is recorded, asynchronously indexed, and made queryable.

Transactions enable ordered, consistent changes to the database state. The transaction ID creates a total ordering of all modifications, allowing the system to reconstruct any historical state and reason about the sequence of changes over time.
