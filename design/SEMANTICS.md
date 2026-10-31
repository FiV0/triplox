# Semantics

Version: 0.1

---

This document tries to clarify some high-level semantics of Triplox. Semantics are the rules for what an operation means. The semantics in this document are mostly about transactions and queries.


## Transactions

We try to keep the transaction semantics of Triplox close to those of Datomic. But it is hard to be fully compatible with Datomic.
The architecture of the two systems is different. Some internals are also very specific, for example schema changes and schema evolution.
A schema defines the attributes and their types. Schema evolution is how a schema changes over time.

These questions are still open:
- If a schema update changes a type, is it necessary to move the data to the new type?
- Does Triplox make sure that the new type applies correctly everywhere?
- Which approach to schema evolution do we recommend?


## Queries

The main point to clarify for queries is bag semantics versus set semantics. A tuple is one row in a query result.
Bag semantics lets a result hold duplicate tuples. Set semantics removes duplicate tuples from a result.
The example that follows comes from XTDB:
```clj
  (xt/submit-tx node
                [[::xt/put {:xt/id :person-1
                            :person/department :it-derpartment
                            :person/salary 100.0}]
                 [::xt/put {:xt/id :person-2
                            :person/department :it-derpartment
                            :person/salary 100.0}]
                 [::xt/put {:xt/id :it-derpartment
                            :department/name "IT deparment"
                            :department/domain ["programming" "architecture desing"]}]])

  ;; Salary spending by department, domain not projected, but unified
  (xt/q  (xt/db node)
         '{:find [?dept (sum ?salary)]
           :where [[?e :person/department ?d]
                   [?e :person/salary     ?salary]
                   [?d :department/name   ?dept]
                   [?d :department/domain ?domain]]})
  ;; => #{["IT deparment" 400.0]}

  ;; domain not unifed
  (xt/q  (xt/db node)
         '{:find [?dept (sum ?salary)]
           :where [[?e :person/department ?d]
                   [?e :person/salary     ?salary]
                   [?d :department/name   ?dept]]})
  ;; => #{["IT deparment" 200.0]}
```
XTDB uses bag semantics. Thus, in the first query, the unification with `?domain` "leaks" into the aggregate.
Unification binds a variable to one value across clauses. An aggregate such as `sum` combines many values.
In Datomic, you write the query like this:
```clj
{:find [?dept (sum ?salary)]
 :with [?e]
 :where [[?e :person/department ?d]
         [?e :person/salary     ?salary]
         [?d :department/name   ?dept]
         [?d :department/domain ?domain]]}
```
Datomic always removes duplicate tuples that differ only in the domain. The number of domains for a department does not matter.
The salary counts only once. The last clause does not change the result, except that it keeps only departments that have a domain.
This brings us to the core of the problem. What about semi-joins that are existence filters?

A semi-join keeps tuples that match the other side. An existence filter only tests that a match exists. For example:
```clj
;; Schema:
;;   :person/skill           cardinality/many, ref or keyword
;;   :company/required-skill cardinality/many, ref or keyword

{:find [(sum ?salary)]
 :where [[?e :person/salary ?salary]
         [?e :person/skill ?skill]
         [?company :company/required-skill ?skill]]}
```
The query tries to compute the salaries of the people who have a skill that the company requires.
If a person has two or more skills that the company requires, the sum under bag semantics includes that salary more than once.
In effect, bag semantics forces every join to multiply the number of tuples in the result.
Can we prevent this?

### Incremental queries

An incremental query updates its result after data changes. For incremental queries, set semantics needs more distinct calls than bag semantics.
A distinct call removes duplicate tuples. Distinct calls are expensive.
