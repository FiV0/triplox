---
title: Concepts
description: Some core concepts of Datomic-like systems
---

As Triplox is quite different to traditional SQL DBMS systems. This page tries to serve as a pointer for
people that might not be familiar with concepts quite prevalent in entity-attribute-value flavoured Datalog stores.

## Datom

The atomic unit of data in Triplox. A fact of the form (entity, attribute, value, tx, op), where `op` marks it an assertion or retraction. The transaction is tracked separately as another entity, not stored in the datom itself. The datom is the full 5-tuple.
Triple often refers to the first 3 parts of the Datom and is used in the context of the indexes.

## DB value

Traditionally, DB has meant the database system as a whole. Triplox in this particular case.
Postgres in the context of the Postgres DBMS ecosystem. It has usually meant a place
(the postgres instance) your application talks to through some client.
Something that you update, mutate and usually query as of now. Datomic has pioneered the
concept of the database as a value which means an immutable snapshot view of facts
at a certain point in time. The term snapshot might make you think that the db value is perishable. Something
that exists for certain amount of time to for example guarantee consistency during MVCC, but we really mean
an immutable value that remains valid until the end of time. A query against a database value
will always work and always return the same set of facts (with [erasure](/transactions/transaction-data/#entity-erasure) being the only exception to this rule).

## Keyword

The Keyword type comes from the Clojure programming language and has made it's way into Triplox (see [here](https://clojure.org/guides/faq#why_keywords)
for why they exist in Clojure). For example `:db/ident` identifies the ident attribute.
They are made up of an optional prefix (i.e. "db") called the `namespace` and the `name` (i.e. "ident").
Namespaces are useful for distinguishing otherwise identical names (for example `:person/id` vs `:order/id`).
There is only one instance of every Keyword. They are interned (ideally)
and thereby use less memory as every instance exists only once in your program.
Another way to think about keywords is to consider strings. There are usually two types
of strings in your program. Dynamically constructed strings like `"Hello $USER, nice to see you this $TIME_OF_DAY."` and
strings that are fixed like your schema. `:first_name` and `:last_name` are good examples.
In the context of Triplox keywords are overwhelmingly used for attributes. Keywords in
attribute position are named identifiers for attribute entities.
Another use case for keywords are non-closed enums. Examples are the Triplox [types](/transactions/schema):
```clojure
:db.type/keyword
:db.type/string
:db.type/long
...
```
or some other closed set of your domain
```clojure
:color/red
:color/yellow
:color/green
```
This is also where the usage of keywords as values makes most sense.

In most cases you can think of them as keys in maps. Instead of
writing a map in python like
```python
{"name": "Alan", "city": "London"}
```
you write
```clojure
{:name "Alan" :city "London"}
```
