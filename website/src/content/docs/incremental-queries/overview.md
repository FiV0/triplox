---
title: Incremental Queries
description: Incremental queries in Triplox.
---

**🚧 Incremental queries are the most experimental part of Triplox. There will likely be issues about the non-happy paths and
they need more testing under heavy workloads. They are under active development. 🚧**

An incremental query delivers a stream of changes between two subsequent database values. A standard [query](/query-language/overview/) delivers a result set at a given point in time $t$. Let us call the database at that time $DB_t$. A transaction creates a new database $DB_{t+1}$. An incremental query gives you the changes of the one-time query between $DB_t$ and $DB_{t+1}$ from a given $t$ onwards (this is sometimes written as $\Delta DB$).

Let's say you query for the name and the residence of people:

```clojure
'{:find [?name ?residence]
  :where [[?p :person/name ?name]
          [?p :person/residence ?residence]]}
```
A standard result for a given $t$ would be
```clojure
[["Ada Lovelace" "12 St. James's Square"]
 ["Alan Turing" "Bletchley Park"]]
```
Let's assume Ada Lovelace now moves to Buckingham Palace and we transact the following
```clojure
[[:db/add [:person/name "Ada Lovelace"] :person/residence "Buckingham Palace"]]
```
Then the new result set a time $t+1$ would be
```clojure
[["Ada Lovelace" "Buckingham Palace"]
 ["Alan Turing" "Bletchley Park"]]
```
The above assumes `:person/name` is a unique attribute and that a given person can only have a single residence (which are both questionable data modelling choices).
The incremental version of the above query would therefore return the following result for the change of address of Ada Lovelace:
```clojure
[[["Ada Lovelace" "12 St. James's Square"] -1]
 [["Ada Lovelace" "Buckingham Palace"] 1]]
```
A result tuple of an incremental query is made up of pair. The first part being the usual result tuple and the second one an integer (sometimes called `:db/diff`) which specifies the change in the corresponding standard query result set. For the above `["Ada Lovelace" "12 St. James's Square"]` left the result set and
`["Ada Lovelace" "Buckingham Palace"]` has been added to the result set.

The example demonstrates some basic variable unification in an incremental query.
In Triplox's final version incremental queries will support the same
feature set as [standard queries](/query-language/datalog). This means Triplox deals with
the incremental evaluation of `or`/`or-join`, `and`, `not`/`not-join`, predicates and function evaluation.
Supporting rules will come in a later step as it involves compiling recursive DBSP circuits
for calculating fixed points which is a lot more tricky then compiling a circuit without recursion.

### Incremental query API and setup

An incremental query is registered on a connection. Incremental queries (IQs) are defined with the same syntax as [standard queries](/query-language/datalog/). The reason IQs take a connection and not a db value, as standard queries do, is because they return change between db values (in other systems you might call these DB snapshots).
Every Client API has a concept of a `subscribe` method which registers the incremental query on the server.
`subscribe` returns a stateful object that either needs to get closed or explicitly unregistered depending on the API. Incremental queries require resources on the server and the closing mechanics assure that these resources are properly cleaned up on the server. `subscribe` takes the connection and the query as arguments.

When an incremental query gets registered it takes out a DB value at a given `TxKey`. For now, this is the `TxKey` the node has caught up to indexing, meaning you can currently only register incremental queries at roughly where the indexer is at. It builds, what is called in [DBSP](https://docs.rs/dbsp/latest/dbsp/) terminology, a circuit. This circuit gets bootstrapped by the data from the given `TxKey`, meaning the data that is currently present in the indexes. You can think of this bootstrapping as running the standard query through the circuit. This means the circuit initialization might take quite a while depending on how much data is already in the indexes that is relevant for the given incremental query. I want to give some intuition of why the circuit needs to get bootstrapped with the old data when we are only interested in future deltas. Consider a join of two abstract relations $A \bowtie B$. When something in $A$ changes (written as $\Delta A$) we still might need to join it against the old data, i.e. $\Delta A \bowtie B_{old}$, to know if actually to emit a tuple from the query.

### Views

A view in traditional DBMSs acts like a virtual table. The data is often computed when access is requested or updated periodically. Systems like [Materialize](https://github.com/materializeinc/materialize) update the views incrementally. Once you have incremental queries, it is "easy" to implement views on top. I prefer to rather give the more primitive option of an incremental query and let users decide how they want to maintain their views. If there is a high demand for views maintained on the server, we can reconsider.
You can find an example of how to implement views for Clojure in the [incremental query tutorial](https://github.com/FiV0/triplox-incremental-tutorial/blob/079a7298c4658acd8fc46917ec00797871ad3f73/src/tutorial.clj#L281-L321). The idea should be fairly easy translatable to other client languages.

### Incremental query evaluation

Once a circuit is primed, we can start listening to changes. For this we use SlateDB's [Change Data Capture](https://slatedb.io/docs/design/change-data-capture/) with which we are tailing WAL files (here we mean WAL files from SlateDB not our external log) as they appear on object storage. A WAL file might contain many transactions, so we construct the transactions in order from the WAL and apply them to all registered incremental queries. Applying every transaction is of course heavier than applying coarser changes. In the future we might support applying all changes from one WAL file in one go to the circuits. The upside of this approach is that incremental query evaluation is likely quicker, but you lose granularity in the output. If a fact was added and later retracted and this addition and retraction happens to reside in the same WAL file, you won't see any delta changes in a corresponding delta query in the output as the two changes cancelled each other out when constructing the changes for a WAL file, so the tradeoff will be speed vs granularity.

### Example

The following is the above example spelled out in full using the Clojure API.

```clojure
(with-open [conn (t/connect "localhost" 5490)]
  ;; schmema
  (t/transact conn [{:db/ident :person/name
                     :db/valueType :db.type/string
                     :db/cardinality :db.cardinality/one
                     :db/unique :db.unique/identity}
                    {:db/ident :person/residence
                     :db/valueType :db.type/string
                     :db/cardinality :db.cardinality/one
                     :db/unique :db.unique/value}])
  ;; initial data
  (t/transact conn [{:person/name "Ada Lovelace"
                     :person/residence "12 St. James's Square"}
                    {:person/name "Alan Turing"
                     :person/residence "Bletchley Park"}])

  (with-open [sub (t/subscribe conn '{:find [?name ?residence]
                                      :where [[?p :person/name ?name]
                                              [?p :person/residence ?residence]]})]
    ;; change
    (t/transact conn [[:db/add [:person/name "Ada Lovelace"] :person/residence "Buckingham Palace"]])

    (t/take! sub 1000)))
;; => [[["Ada Lovelace" "12 St. James's Square"] -1]
;;     [["Ada Lovelace" "Buckingham Palace"] 1]]
```


### Outlook

As all the historical data is available in the indexes, nothing prevents us to start an incremental query at an older transaction basis. The catch-up phase (the phase where we play through transactions that have already made it through the indexer) needs a different mechanism to play through the transactions compared to the CDC backed listening of SlateDB. Either we
