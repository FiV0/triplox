---
title: Transaction Data
description: Triplox transaction data.
---

Transaction data is how data of a transaction is represented in Triplox.
Triplox uses transaction-data forms inspired by [Datomic](https://docs.datomic.com/transactions/transaction-data-reference.html).
It purely concerns the transaction data constructs and not the semantics of a Triplox transaction.
We will explain every concept via [EDN](https://github.com/edn-format/edn) syntax used
by the [Clojure client](/apis/clojure/). The Java and Rust clients also support
the EDN syntax as strings, but they both also have explicit typed constructors for the transaction data.
Every transaction data construct gets in the end translated to a [Datom](/getting-started/concepts/#datom) or a set of Datoms.
The Datom is the most important concept to understand if you wish to understand transaction data.
Transaction data is a vector of assertions, retractions,
assertions in map form (asserting multiple attributes about an entity) or an entity retraction.

### Assertion / Retraction

The most basic form to assert a fact is via a standard assertion.
```clojure
[:db/add entity-id attribute value]
```
The following example asserts that the entity ID `123` (presumably a person) should have the name "Lovelace".
```clojure
[:db/add 123 :person/name "Lovelace"]
```
To remove data in Triplox, one retracts the EAV triple.
```clojure
[:db/retract entity-id attribute value]
```
We currently expect a value for a retraction. A retraction without a value will fail.

### Map assertion form

A map assertion lets you assert several facts about the same entity. Each key/value pair represents
an attribute and its value. A map without `:db/id` creates a new entity. When `:db/id` is present,
its value identifies the entity to update.

For example asserting facts about a person could be achieved via
```clojure
{:person/first-name "Ada"
 :person/last-name "Lovelace"
 :person/profession "Programmer"}
```

The key `:db/id` is syntactic sugar for identifying an entity via entity ID. A map of the form
```clojure
{:db/id 123
 :person/first-name "Ada"
 :person/last-name "Lovelace" }
```
is the same as the two transaction operations
```clojure
[:db/add 123 :person/first-name "Ada"]
[:db/add 123 :person/last-name "Lovelace"]
```
### Entity retraction

An entity retraction retracts every currently visible fact whose entity position contains the specified
entity ID. The entity therefore has no visible attributes in the new database value.

```clojure
[:db/retractEntity 123]
```
If the entity also has a `:db.unique/identity` attribute, the entity can also be retracted via a lookup ref. For example
a social security number.
```clojure
[:db/retractEntity [:person/ssn "123-45-6789"]]
```
:::note
Triplox currently does not do recursive or cascading retractions. References to the retracted entity will remain active.
It is currently the user's responsibility to assure consistency in this regard.
:::

### Tempids

Entity IDs for new entities are assigned when a transaction commits. Tempids (short for temporary IDs)
are transaction-local strings that let multiple operations refer to the same new entity before its permanent
ID is known. They can also be used as the value of reference-typed attributes. The following transaction
illustrates the creation of two courses (Math and Physics) which
Alice attends
```clojure
[:db/add "math-course"     :course/title "Mathematics"]
[:db/add "physics-course"  :course/title "Physics"]

[:db/add "alice-id" :student/name "Alice"]
[:db/add "alice-id" :student/course "math-course"]
[:db/add "alice-id" :student/course "physics-course"]
```
In the above, `"math-course"`, `"physics-course"`, and `"alice-id"` are tempids. The course tempids allow later operations
to refer to the newly created courses, while `"alice-id"` groups several assertions about Alice.

A tempid can resolve to an existing entity instead of creating a new one. This is called an upsert. It happens
when the transaction asserts a `:db.unique/identity` attribute whose value already belongs to an existing entity.
For example, if `:course/title` is a `:db.unique/identity` attribute and the math course already exists, the tempid
`"math-course"` resolves to that course's existing entity ID.

### Lookup refs

Lookup refs let you identify an existing entity by an attribute/value pair when you don't know its entity ID. The attribute
must be declared as `:db.unique/identity`, and the attribute/value pair must already exist in Triplox. For example, if
`:person/email` is a `:db.unique/identity` attribute, you can write:
```clojure
[[:db/add [:person/email "jdoe@example.com"] :person/nickname "JD"]]
```
Lookup refs can be used in an operation's entity position or as the value of a reference-typed attribute.

:::note
We currently don't support lookup refs in queries.
:::

### Idents

Idents let you reference entities by name. An ident is the value of the special attribute `:db/ident`, which is also used
in [schema](/transactions/schema/) definitions. In transaction data, idents can be used in the entity position of
`:db/add` and `:db/retract`, as the value of `:db/id` in a map assertion, and as the value of a reference-typed attribute.

For example, consider the following schema attribute:
```clojure
{:db/ident :team/name
 :db/valueType :db.type/string
 :db/cardinality :db.cardinality/one
 :db/unique :db.unique/identity}
```
Because `:db/valueType`, `:db/cardinality`, and `:db/unique` are reference-typed attributes, their values
`:db.type/string`, `:db.cardinality/one`, and `:db.unique/identity` are themselves idents.

:::note
Idents are not currently supported as targets of `:db/retractEntity`.
:::

### Entity erasure

Entity erasure is a way to completely erase any trace of an entity. It removes all versions of an entity from Triplox. Use it with care and seldom. It is for example useful to comply with GDPR Right to Erasure. This is the only operation that has an effect on queries on old DB values.

```clojure
[:db/erase 123]
```
:::note
Triplox currently does not support entity erasure. The client APIs accept these operations, but the indexer aborts the transaction and creates an aborted transaction entity. I want to spend some time on how erasure should behave and be dealt with in incremental queries. As there
will be no retraction appearing in the SlateDB WAL, some thought needs to be put into how this gets communicated to the incremental
query circuits.

As with entity retraction, Triplox does (currently) not deal with recursive or cascading erasure.
:::
