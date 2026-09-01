---
title: Transaction Model
description: Triplox's transaction model.
---

This page tries to give a high-level overview of how Triplox's transaction model works.
We try to follow [Datomic's transaction model](https://docs.datomic.com/transactions/model.html),
so it is encouraged to also read that page (just be aware that we don't support
all features Datomic supports). This page is likely a bit more ad hoc and concerns
the transaction pipeline of Triplox so you have some understanding of how data gets validated
and why transactions get rejected or not indexed.

Every transaction is a set of facts (also called Datoms) that get indexed all at once into Triplox.
When certain constraints on these facts are not satisfied, transactions get rejected.
The order of these facts and how they appear in the
[transaction data](/transactions/transaction-data) is not important. Every transaction gets
validated with the state of Triplox just before this transaction is applied. You can think of this as
the database $DB_t$ at point $t$ moving to a new database $DB_{t+1}$ by applying the transaction $t+1$. This is also
why transactions need a [total order](https://en.wikipedia.org/wiki/Total_order) before they go through the
transaction pipeline. As the [schema](/transactions/schema/) is mainly responsible for transaction
validation and is first class data itself, it needs to be fixed and available before the next transaction is
processed. The Datomic folks have done a much more thorough job explaining why you might want such semantics
than I ever could, so I encourage you to read the above linked document. The important part is that
 a Triplox transaction either gets fully indexed as a set of facts or not indexed at all.

The rest of the page is concerned with transaction validation and why a transaction might
not get indexed. It illustrates the transaction semantics via all the validation that
happens in the pipeline before a transaction gets written to SlateDB indexes.

In Triplox every submitted transaction gets processed by the indexer. The indexer is the
single writer (see [Life of a transaction](/transactions/life-of-a-transaction/)) and the
only component that mutates the [covering indexes](/data-model/#indexes). Every transaction
(be it valid or not) is on the log and gets processed by the indexer.

The following sections give you an overview of the different validation of constraints that happens
to a transaction before it gets indexed into SlateDB.

### High-level overview

In broad terms, the transaction pipeline turns the data submitted by a client into a
fully resolved set of facts that is safe to add to the database. It first expands the
different transaction forms into a common representation and works out what every
entity reference means. This is where lookup refs are resolved, tempids either connect
to existing entities or receive new ids, and invalid references are rejected.

Once every fact refers to a concrete entity, Triplox checks the transaction against the
schema and the current state of the database. It verifies types, cardinality and
uniqueness, and adds any retractions needed when a cardinality-one value is replaced.
These checks consider the transaction as a whole, so facts within the same transaction
cannot quietly contradict one another.

If all checks succeed, the complete set of changes is written to the indexes in one
atomic commit. Only then does Triplox update its schema and partition counters and
announce that the transaction was indexed. If any step fails, none of the submitted
facts become visible; instead, the
[transaction entity](/transactions/life-of-a-transaction/#concepts) records the failure.
For someone using Triplox, the important part is that a transaction's facts become
visible together, or none of them do.

### The details

The pipeline begins by expanding the submitted
[transaction data](/transactions/transaction-data) into Datoms, the common representation
used by the remaining steps. Assertions, retractions, map forms, idents and `:db/id`
sugar all end up in this form. The [schema](/transactions/schema) tells Triplox how to
interpret the attributes and values along the way. Unknown attributes and malformed
references cause the transaction to be rejected before any data is written.

Triplox then resolves references to entities that already exist. A
[lookup ref](/transactions/transaction-data/#lookup-refs) identifies an entity through a
unique identity attribute and its value. The indexer looks up that value in the current
database and replaces the reference with the corresponding entity id. Explicit entity
ids are checked as well, both when they identify the entity being changed and when they
appear as reference values. A reference to an entity that does not exist aborts the
transaction.

Next, the pipeline deals with tempids and upserts. Tempids make it possible to describe
new entities and refer to them several times without knowing their final ids. When a
tempid is used with a `:db.unique/identity` attribute, it may resolve to an existing
entity instead; this is an upsert. Tempids that remain unresolved receive new entity ids.
The transaction is rejected if the same tempid would resolve to different entities, or
if it appears only as a reference value without anything defining the referenced entity.

At this point every entity has a concrete id, so Triplox validates the Datoms against the
schema. Each value must have the type declared by its attribute. A
[cardinality-one](/transactions/schema/#dbcardinality) attribute may not receive several
different values in the same transaction, and asserting and retracting the same fact at
once is also considered a conflict.

After validation, the pipeline accounts for data that is already in the database. When
a transaction assigns a new value to a cardinality-one attribute, Triplox retrieves its
current value. Reasserting that value is a no-op; replacing it adds an implicit retraction
of the old value. Triplox then checks [uniqueness constraints](/transactions/schema/#dbunique)
across both the transaction and the existing data. Two entities cannot claim the same
unique value, although a value may move to a new entity when its old assertion is
retracted in the same transaction.

Schema changes pass through the same pipeline because the schema is itself stored as
data. They also receive some additional checks to ensure that a schema entity contains
the required attributes and represents a valid update. The prepared schema is not made
active yet; like the user data, it has to wait for a successful commit.

Finally, Triplox encodes the resulting Datoms into its covering indexes and writes them
to SlateDB as one atomic batch. After that write succeeds, it updates the schema and
partition counters and announces that the transaction was indexed. If an error occurs
at any earlier point, the user data is not committed. Triplox writes only the
[transaction entity](/transactions/life-of-a-transaction/#concepts), which records that
the transaction failed and why.

:::note
Transactions that violate some constraint usually don't have any trace in Datomic and
get rejected by the transactor. In Triplox these transactions always get a
transaction entity documenting that failure.
:::
