---
title: Data Model
description: The Triplox data model.
---

Triplox is an Entity-Attribute-Value (EAV) triple store. The database is made up of a set of triples called Datoms. Each Datom declares that some entity (for example, a person)  has a certain attribute (like a name) of a particular value (like "Ada Lovelace"). [^1] An entity might have many attributes. A [schema](/transactions/schema/) defines the valid types and cardinality of attributes. This schema is also stored as triples. The system is self-referential and the only way data is stored in Triplox (also at the meta level) is in form of triples. Consider the following entity of a person.
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
The number 123 is what is called an entity id. A unique ID identifying an entity.

## Indexes

These triples are stored in indexes `EAV`,`AVE`, `AEV` and `VAE` (the order of the initials means the order in which the triple is stored in each index). So `[123 :person/first-name "Ada"]` is stored as such in the EAV index and stored as `[:person/first-name "Ada" 123]` in the AVE index and so forth.

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


## Schema flexibility

You might wonder why not store entities like rows as in most traditional DBMSs. As outlined above the covering indexes give you good options for many different access patterns. Another advantage is flexibility and granularity.

### Sparse types

In a traditional row based stores every column needs to get filled (or nulled) for every row. The entity-attribute model allows for very flexible entity types. An example are sparse types.
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


### Join tables

Join tables in SQL model many-to-many relationships.
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
In the EAV-data model the pattern kind of resolves. You simply have the cardinality many attribute `:student/course` of type reference which maps a student to courses. In the following we create the relationship between course and students using
[tempids](/transactions/transaction-data/#tempids):

```clojure
[[:db/add "math-tempid"     :course/title "Mathematics"]
 [:db/add "physics-tempid"  :course/title "Physics"]
 [:db/add "history-tempid"  :course/title "History"]

 [:db/add "alice-tempid" :student/name "Alice"]
 [:db/add "alice-tempid" :student/course "math-tempid"]
 [:db/add "alice-tempid" :student/course "physics-tempid"]

 [:db/add "bog-tempid" :student/name "Bob"]
 [:db/add "bob-tempid" :student/course "physics-tempid"]
 [:db/add "bob-tempid" :student/course "history-tempid"]

 ;; TODO support vector syntax for cardinality many references
 {:student/name   "Alice"
  :student/course ["math" "physics"]}
 {:student/name   "Bob"
  :student/course ["physics" "history"]}]
```
The VAE index (V being a reference pointing to a course) let's you navigate this relationship in reverse in case you like to find all students for a particular course.

### Additive schema evolution

### Multi-role entities

TODO

### Polymorphic references

TODO

### Data fusion

TODO

### Transaction annotation

TODO

Link to the appropriate section under transactions.


[^1]: Example adapted from Jepsen’s Datomic report. <https://jepsen.io/analyses/datomic-pro-1.0.7075>
