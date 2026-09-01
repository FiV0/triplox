---
title: "Triplox Log 1 - Introduction"
date: 2026-04-28
authors:
  - name: Finn Völkel
---

I am working on a Datalog database engine à la [Datomic](https://www.datomic.com/) on top of object storage. The system is called Triplox (a portemanteau of Triple and Blocks). In an attempt to become a better communicator I decided to start a little log to explain some concepts in Triplox.

The backbone of the engine is [SlateDB](https://github.com/slatedb/slatedb/), a key-value store on top of object-storage. Think of SlateDB as [RocksDB](https://github.com/facebook/rocksdb) on top of object-storage. Datomic is the main inspiration and the data model, transaction semantics and query API closely follow Datomic.

By making object storage the single source of truth, you get separation of storage and compute. SlateDB has a single writer and many readers architecture and that naturally translates to Triplox. In that sense it's similar to Datomic. Triplox sits in the traditional client/server camp where queries run on the server.

The ideas of Triplox are roughly the following (in no particular order):
- Object storage first. In it's final version Triplox should simply need a single S3 bucket for deployment. You will see further down that this is currently not really the case.
- The Datomic Data model and API as main inspiration. Datomic is awesome.
- A Client/Server architecture. I hope that this will open the door to ecosystems outside of the JVM (where Datomic has had it's main success).
- Incremental queries à la [DBSP](https://arxiv.org/abs/2203.16684). You should be able to dynamically subscribe and detach from incremental Datalog queries. This is different to [Feldera](https://github.com/feldera/feldera) which compiles a new binary for every query. So incremental queries should be a lot lighter in Triplox. This is the most experimental part of Triplox and will need quite a bit of engineering effort to get right, make fast, and fully support of all features of Datalog (recursive rules being the most tricky part). The idea is to hook into SlateDB's [CDC](https://en.wikipedia.org/wiki/Change_data_capture) and produce new deltas for every WAL entry that comes through.

SlateDB is built with OLTP access patterns in mind and this translates directly to Triplox. If you plan to do giant OLAP aggregations on top of Triplox you might be bettter served by a different system. This doesn't mean Triplox doesn't support aggregates, it will just never beat something like [DuckDB](https://github.com/duckdb/duckdb) on these types of queries.

The architecture of Triplox for a 3 node setup then would roughly look as follows (subject to change):

```txt
             ┌────────────────────────────────────────────────────────────┐
             │                    Object Storage (S3)                     │
             │  ┌───────────┐          ┌───────────┐       ┌───────────┐  │
             │  │  SlateDB  │          │  SlateDB  │       │  SlateDB  │  │
             │  │ (Writer)  │          │(Reader 1) │       │(Reader 2) │  │
             │  └─────┬─────┘          └─────┬─────┘       └─────┬─────┘  │
             └────────┼──────────────────────┼───────────────────┼────────┘
                      │                      │                   │
 Queries/Indices      ▲ read/write           ▼ read              ▼ read
                      │                      │                   │
      ┌───────────────┴───────────┐    ┌─────┴─────┐       ┌─────┴─────┐
      │        Writer Node        │    │ Reader 1  │       │ Reader 2  │
      │   ┌──────────┐            │    │           │       │           │
 ┌────┼──▶│ Indexer  │            │    │           │       │           │
 │    │   └──────────┘            │    │           │       │           │
 │    └───────────────┬───────────┘    └───────────┘       └───────────┘
 │                    │ write
 │ Transactions       ▼
 │  ┌─────────────────┴───────────────────────────────────────────────────┐
 │  │                     Log (Kafka, S2, WAL3, etc.)                     │
 │  │          ┌─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┐          │
 └──┼──────────┤ tx0 │ tx1 │ tx2 │ tx3 │ tx4 │ tx5 │ tx6 │ ... │          │
read│          └─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┘          │
    └─────────────────────────────────────────────────────────────────────┘
```
Tthis he main wrinkle here is currently the log. It adds extra complexity I would prefer to avoid. Using something like [AutoMQ](https://github.com/automq/automq), which is Kafka backed by S3, would create another service in the architecture.  Some kind of log component to which the writer node just appends data would be preferable.   I am not using SlateDB's [MVCC](https://en.wikipedia.org/wiki/Multiversion_concurrency_control) because Triplox needs to control the [total order](https://en.wikipedia.org/wiki/Total_order) of transactions before they hit the indexes, rather than having that order determined internally by SlateDB.
Maybe something like [wal3](https://www.trychroma.com/engineering/wal3) would fit the bill, but it is currently not available as standalone dependency. We have also discussed creating a standalone slatedb-wal, extracting the wal component of SlateDB into a standalone dependency and simply use it as a log. So far this seems to be the best option to me, but I am happy to hear other ideas.

The following sections are likely familiar to Datomic users and can in this case be skipped.
## Data model
Triplox is an Entity-Attribute-Value (EAV) triple store. The database is made up of a set of triples called Datoms. Each Datom declares that some entity (for example, a person)  has a certain attribute (like a name) of a particular value (like "Ada Lovelace"). [^1] An entity might have many attributes. A [schema](https://docs.datomic.com/schema/schema-reference.html) defines the valid types and cardinality of attributes. This schema is also stored as triples. The system is self-referential and the only way data is stored in Triplox (also at the meta level) is in form of triples. Examples speak a thousand words. Consider the following person entity.
```clojure
{:person/first-name "Ada"
 :person/last-name "Lovelace"
 :person/sex :female
 :person/profession "programmer"}
```
It will get expanded into 4 triples (aka Datoms)
```clojure
[123 :person/first-name "Ada"]
[123 :person/last-name "Lovelace"]
[123 :person/sex :female]
[123 :person/profession "programmer"]
```
The 123 is what is called an entity id. A unique ID identifying an entity. These triples are stored in indexes `EAV`,`AVE`, `AEV` and `VAE` (the order of the initials means the order in which the triple is stored in each index). So `[123 :person/first-name "Ada"]` is stored as such in the EAV index and stored as `[:person/first-name "Ada" 123]` in the AVE index and so forth.

The four indexes EAV, AVE, AEV and VAE are called the covering indexes. The reason the same data is stored 4 times are access patterns and joins.

- The EAV index lets you quickly find all attributes plus their values. What do we know about Ada Lovelace?
- With the AVE index you can lookup entities that match a certain AV pair. Who is a professional programmer? One can also efficiently find entities for range queries. Which people are between the age `30` and `40`?
- The AEV index gives you "columnar style" access to an attribute. When you have a pattern like `[?e :person/age ?v]` (`?e` and `?v` being free variables) it's often the case that `?v` doesn't get constrained further, but `?e` will be (because of other triple patterns) and so it's essential to get the entity id in sorted order for further joining. In the context of WCOJ it's also essential to have both AEV and AVE because `?e` and `?v` may come first in different join orders.
- The schema allows for value types of `:db/ref`  which is a reference to another entity. For example `[?alice :person/follows ?bob]`. The `:person/follows` attribute points to another entity. The VAE index is only populated for reference attributes. It allows you do to do certain graph traversal navigation in reverse order. "Who is following Bob?" for the example above.

I have glossed over some aspects of a Datom. In reality a Datom is actually a 5-tuple of `[entity-id attribute value txn added?]`. In many contexts we are still using the term triple to refer to a Datom as the EAV part is the important part for queries. The`txn` is the entity id of the transaction this particular triple was added to Triplox and `added?` identifies if the triple was added or retracted. Triplox like Datomic is an immutable system of record. Every addition and retraction is stored. You can always go back to a previous version of a database an run a query.

So when adding an entity like the above to Triplox
```clojure
{:person/first-name "Ada"
 :person/last-name "Lovelace"
 :person/sex :female
 :person/profession "programmer"}
```
you actually get the following expanded Datoms in Triplox
```clojure
[123 :person/first-name "Ada" 124 true]
[123 :person/last-name "Lovelace" 124 true]
[123 :person/sex :female 124 true]
[123 :person/profession "programmer" 124 true]
[124 :db/txId 124 124 true]
[124 :db/Instant #inst "2026" 124 true]
[124 :db/txResult :db.result/commited 124 true]
```
The first 4 Datoms are the same as above (plus the transaction and assertion parts), the latter 3 are transaction Datoms. So also the transaction history is represented as triples (I hope you slowly get it, it's triples everywhere 😉). In case Ada Lovelace had a different profession before she became a programmer like carpenter,  you would also get a retraction like
```clojure
[123 :person/profession "carpenter" 124 false]
```

The entity id allocation (123 and 124) is simplified. Triplox will also support partitions. Entity ids can be allocated in different partitions. Partitions are assigned through the higher bits of an entity id . This will give you index locality when joining data in the same partition. In the beginning we will only have 3 partitions: A `DB_PARTITION` holding entities related to the schema and other database related concepts, a `TX_PARTITION` for transaction entities and a `USER_PARTITION` holding most of the user data. In the future we will likely add an option to create user partitions and allow users to specify partition assignment (via a special attribute).

You might wonder why not store entities like rows as in most traditional DBMSs. As outlined above the covering indexes give you good options for many different access patterns. Another advantage is flexibility and granularity. In a traditional row based stores every column needs to get filled (or nulled) for every row. The entity-attribute model allows for very flexible entity types. An example are sparse types.
```clojure
;; A book
{:product/sku "B-001"
 :product/price 19.99
 :book/isbn "978-0-13..."
 :book/author "Shannon"}

;; An apparel — same "type", different attributes
{:product/sku          "A-Hoodie-042"
 :product/price        24.99
 :apparel/size         :size/m
 :apparel/color        :color/black
 :apparel/material     "100% organic cotton"}
```
A store might sells books, apparel and potentially all kinds of other stuff. In SQL you would solve this problem with [Single Table Inheritance](https://en.wikipedia.org/wiki/Single_Table_Inheritance), which often creates sparse table pathologies.

A second example are join tables from SQL. They model many-to-many relationships.
```sql
CREATE TABLE students (
  id   INTEGER PRIMARY KEY,
  name TEXT
);

CREATE TABLE courses (
  id    INTEGER PRIMARY KEY,
  title TEXT
);

CREATE TABLE enrollments (
  student_id INTEGER REFERENCES students(id),
  course_id  INTEGER REFERENCES courses(id),
  PRIMARY KEY (student_id, course_id)
);
```
In the Datomic data model the pattern kind of resolves. You simply have the cardinality many attribute `:student/course` of type reference which maps a student to courses. The VAE index (V being a reference pointing to a course) let's you navigate this relationship in reverse if you like to find all students for a particular course.
## Query language
The main query language for Triplox is a variant of [Datalog](https://en.wikipedia.org/wiki/Datalog). Datalog is a logic-based query language inspired by [Prolog](https://en.wikipedia.org/wiki/Prolog). A Datalog program consists of a set of facts. These facts are the Datoms that sit in our covering indexes. Everything else is derived from these facts (modulo incoming parameters).
### Triple pattern
You match a certain pattern against these facts. Consider the pattern `[?e :person/age 42]` . `?e` is a free variable meaning it "joins" against any triple in the indices for which the latter two hold true. It would find us the entities of people with age 42. Most of the time you want to know more about entities.  For this aspect of Datalog has the concept of unification.
Consider the query [^2]
```clojure
{:find  [?e ?x]
 :where [[?e :age 42]
         [?e :likes ?x]]}
```
The clauses in the `:where` specify the triples we are interested in. In this case people of age 42 and and what they like.  First we find people of age 42 and then the unification of `?e` happens. The `?e`  now gets unified with the second triple pattern where we are looking for things people like (if they like anything ;)) by unifying their likings with `?x`. I am simplifying how Triplox actually does variable joins under the hood (I have described the join algorithm here), but this a good conceptual start for understanding unification. The `find` part is purely about the projection of the join variables.  Unification is the most fundamental part of Datalog and everything else follows naturally.
### Or
By default everything in the where clause is a conjuction (an `and`) of the facts that satisfy the triples. If you want to express disjunctions you need an `or` clause.
```clojure
{:find [?e]
 :where [[?e :age 42]
         (or [?e :likes "ice cream"]]
             [?e :likes "donuts"])]}

```
In this case, the outer unification can happen against any of the inner `or `branches. The above query will find us people who are 42 years old and like donuts or ice cream. A person who likes both ice cream and donuts will only appear once in the output.
### And
In `or` clauses disjunction is the default. If you want to get back to conjunction you need to use `and` clause.
```clojure
{:find [?e]
 :where [[?e :age 42]
         (or [?e :likes "icecream"]
             (and [?e :profession "programmer"]
                  [?e :likes "donuts"]))]}
```
The above query finds us people who are 42 years old and who like icecream or are professional programmers who like donuts.
### Not
In case you want to exclude certain types of facts you need to use the `not` clause.
```clojure
{:find [?e]
 :where [[?e :age 42]
         (not [?e :likes "icecream"]]}
```
This will find us people of age 42 who don't like ice cream. Be aware that a `not` works like an anti-join than an actual negation of facts. For example you cannot write the query
```clojure
{:find [?e]
 :where [(not [?e :likes "icecream"]]}
```
to find people who don't like ice cream. This is a bit contrary to classical literature Datalog where this query would be accepted.
### Predicates and Functions
Predicates are used to filter matching tuples and functions are used to create new join variables.
```clojure
{:find [?e ?birth-year]
 :where [[?e :person/age ?age]
         [(> ?age 30)]
         [(- 2026 ?age) ?birth-year]]}
```
This finds us people older than 30 and their birth year. The second where clause is a predicate filter and the final clause creates the birth year variable.

This gives you a little introduction tour of EDN Datalog. I have not touched rules which are the most powerful aspect of Datalog.

There are lots of Triplox parts that I explicitly left out for now. This includes the history aspects of the data model and Rich's [database as a value](https://www.youtube.com/watch?v=D6nYfttnVco) concept, APIs like [pull](https://docs.datomic.com/query/query-pull.html), which allows you to do graph traversals and the [entity API](https://docs.datomic.com/reference/entities.html). I want to focus on transactions and queries in a first Triplox version. There is also lots to be written about incremental Datalog queries. So stay tuned for more updates about Triplox.

[^1]: Adapted from Jepsen's Datomic report. <https://jepsen.io/analyses/datomic-pro-1.0.7075>

[^2]: Adapted from the Datomic docs: <https://docs.datomic.com/query/query-executing.html#unification>
