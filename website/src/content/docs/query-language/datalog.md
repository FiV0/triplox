---
title: Datalog
description: The Datalog query language.
---
The main query language of Triplox is a variant of [Datalog](https://en.wikipedia.org/wiki/Datalog). Datalog is a logic-based query language inspired by [Prolog](https://en.wikipedia.org/wiki/Prolog). A Datalog program consists of a set of facts. These facts are the Datoms that sit in our covering indexes. Everything else is derived from these facts except for some optional parameters to a query. The variant of Datalog Triplox uses is called EDN Datalog. An EDN Datalog program has the following top-level shape:
```clojure
'{:find [?name]
  :in [...] ;; optional
  :where [[?p :person/name ?name]
          [?p :person/residence "Buckingham Palace"]]
  :limit 100 ;; optional
  :order [[?name]]} ;; optional
```
### Variables, Constants and Unification

Variables are symbols always prefixed by a `?`. A variable describes something we are looking for. A
variable can appear in multiple places in the Datalog program and almost always describes the
same thing. Unification assures that these variables match the same thing. There are certain
scopes (`or-join`/`not-join`) for which a variable might unify to different things, if the same
variable is used inside and outside of the inner scope. It is discouraged to use the same
variable name in this manner. We currently only support variables in entity and value position.
In the future this might change.


Unification happens when a variable appears in multiple patterns of a Datalog program. Consider
the following query:
```clojure
'{:find [?name]
  :where [[?p :person/name ?name]
          [?p :person/residence "Buckingham Palace"]]}
```
Here `?p` appears in the first and second triple pattern. We are looking for the entity
id of a person. We sometimes just say that we are looking for a person as the entity and entity id
are used colloquially for the same thing; the person. The person in question should
have a name (which is also a variable) and have "Buckingham Palace" as residence. By using
`?p` in two patterns we guarantee that the person has a name and that they live at "Buckingham Palace". We say that `?p` unifies across the patterns.

In the above program the ident `:person/residence` and the string "Buckingham Palace" are constants.
Constants are used to constrain patterns to facts matching these constants. In the above
example we were only interested in people that had a name attribute and had their
residence at Buckingham Palace.

## The `:where` clauses

The patterns appearing in the `:where` restrict the Datalog query to the data we are interested in.

### Triple pattern
The most basic and fundamental pattern is a triple pattern that matches directly against
the facts of the database. Consider the pattern `[?e :person/age 42]`. `?e` is a variable,
meaning it "joins" against any triple in the indexes for which the attribute is `:person/age` and
the value is 42. In most cases you want to know more about entities.

Consider the query:
```clojure
{:find  [?e ?x]
 :where [[?e :age 42]
         [?e :likes ?x]]}
```
The clauses in the `:where` specify the triples we are interested in. In this case people of age 42 and what they like. First we find people of age 42 and then the unification of `?e` happens. The `?e` now gets unified with the second triple pattern where we are looking for things people like (if they like anything ;)) by unifying their preferences with `?x`. I am simplifying how Triplox actually does variable joins under the hood, but this is a good conceptual start for understanding unification. The `:find` part is purely about the projection of the join variables. Unification is the most fundamental part of Datalog and everything else follows naturally.

### Or
By default everything in the `:where` clause is a conjunction (an `and`) of the facts that satisfy the triples. If you want to express disjunctions you need an `or` clause.
```clojure
{:find [?e]
 :where [[?e :age 42]
         (or [?e :likes "ice cream"]
             [?e :likes "donuts"])]}

```
In this case, the outer unification can happen against any of the inner `or` branches. The above query will find us people who are 42 years old and like donuts or ice cream. A person who likes both ice cream and donuts will only appear once in the output.

### Or-join
`or-join` lets you explicitly list the variables shared between its branches and the surrounding query. Other variables are local to each branch.

The following query finds people aged 42 who like ice cream themselves or have a friend who does.

```clojure
{:find [?e]
 :where [[?e :age 42]
         (or-join [?e]
                  [?e :likes "ice cream"]
                  (and [?e :friend ?friend]
                       [?friend :likes "ice cream"]))]}
```
Here, only `?e` is shared with the surrounding query.

### And
In `or` and `or-join` clauses disjunction is the default. If you want to get back to conjunction you need to use an `and` clause.
```clojure
{:find [?e]
 :where [[?e :age 42]
         (or [?e :likes "ice cream"]
             (and [?e :profession "programmer"]
                  [?e :likes "donuts"]))]}
```
The above query finds us people who are 42 years old and who like ice cream or are professional programmers who like donuts.

### Not
In case you want to exclude certain types of facts you need to use the `not` clause.
```clojure
{:find [?e]
 :where [[?e :age 42]
         (not [?e :likes "ice cream"])]}
```
This will find us people of age 42 who don't like ice cream. Be aware that a `not` works like an anti-join than an actual negation of facts. For example you cannot write the query
```clojure
{:find [?e]
 :where [(not [?e :likes "ice cream"])]}
```
to find people who don't like ice cream. This is a bit contrary to classical literature Datalog where this query would be accepted.

### Not-join
`not-join` works like `not`, but lets you explicitly list the variables shared with the surrounding query. These variables must already be bound. Other variables are local to the `not-join`.

The following query finds people aged 42 who have no friends who like ice cream:

```clojure
{:find [?e]
 :where [[?e :age 42]
         (not-join [?e]
                   [?e :friend ?friend]
                   [?friend :likes "ice cream"])]}
```

Only `?e` must be bound by the surrounding query. `?friend` is local to the `not-join`. A person is excluded if any friend likes ice cream. People with no friends remain in the result set.


### Predicates
Predicates are used to filter matching tuples.
```clojure
{:find [?e ?age]
 :where [[?e :age ?age]
         [(> ?age 30)]]}
```
The above query finds people older than 30.

### Functions
Functions are used to create new join variables.
```clojure
{:find [?e ?birth-year]
 :where [[?e :person/age ?age]
         [(> ?age 30)]
         [(- 2026 ?age) ?birth-year]]}
```
This finds us people older than 30 and their birth year. The second `:where` clause is a predicate filter and the final clause creates the birth year variable.

## Ordering and limit

`:limit` takes a positive integer and caps the number of result tuples.

`:order` takes a vector of order clauses, each of which is itself a vector wrapping a
variable from the `:find` clause. Note the double brackets: `:order [[?name]]`, not
`:order [?name]`.

```clojure
'{:find [?name ?age]
  :where [[?p :person/name ?name]
          [?p :person/age ?age]]
  :order [[?name]]}
```

The direction defaults to ascending and can be given explicitly with `:asc` or `:desc`.
Several order clauses are applied left to right.

```clojure
'{:find [?name ?age]
  :where [[?p :person/name ?name]
          [?p :person/age ?age]]
  :order [[?age :desc] [?name :asc]]
  :limit 10}
```

:::note
Limit and order are not supported by incremental queries, as they only really
make sense in the context of a whole result set.
:::

## Aggregates

A query without aggregates simply projects the result set from the `:where` clause to
the variables appearing in the `:find` clause.

Results are grouped by every non-aggregate variable in `:find`. Aggregates are then calculated
separately for each group. If `:find` contains only aggregates, all matching tuples form a single group.

```clojure
'{:find [?residence (count ?person) (avg ?age)]
  :where [[?person :person/residence ?residence]
          [?person :person/age ?age]]}
```

The above query calculates the number of people and their average age living at a particular residence.

| Aggregate        | Argument types                          | Description                                  |
| ---------------- | --------------------------------------- | -------------------------------------------- |
| `count`          | any                                     | Counts all values in the group.              |
| `count-distinct` | any                                     | Counts the distinct values in the group.     |
| `sum`            | `long`, `bigint`, `float`, `double`     | Adds all values in the group.                |
| `avg`            | `long`, `bigint`, `float`, `double`     | Calculates the arithmetic mean of the group. |
| `min`            | numeric, `string`, `boolean`, `instant` | Returns the smallest value in the group.     |
| `max`            | numeric, `string`, `boolean`, `instant` | Returns the largest value in the group.      |


For `min` and `max`, all values in a group must be comparable. Numeric types are comparable; other types cannot be mixed.

## Rules

TODO
