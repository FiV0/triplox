# Semantics

**Version**: 0.1

---

The document tries to clarify some high-level semantics of Triplox. The semantics mainly concern transactions and queries.


## Transactions

Although we try to stick quite close to Datomic's transaction semantics, the difference in architecture and also quite specific
internals about for example schema changes and evolution make it quite hard to be completely compatible with Datomic.

Things to clarify:
- Schema updates. Does the data need to be moved to the new type or not? Does it check that the new type is correctly applied throughout?
- What should the recommended approach for schema evolution be?


## Queries

The main thing to clarify is the bag vs set semantics discussion. Consider the following example from XTDB:
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
XTDB has bag semantics, so the unification with `?domain` in the first example kind of "leaks" into the aggregate.
In Datomic you would write the query as
```clj
{:find [?dept (sum ?salary)]
 :with [?e]
 :where [[?e :person/department ?d]
         [?e :person/salary     ?salary]
         [?d :department/name   ?dept]
         [?d :department/domain ?domain]]}
```
Datomic will always deduplicate tuples that only differ in the domain (It doesn't matter if you have one or multiple
domains under you department belt, the salary only gets counted once). The last clause doesn't matter except for
the check that the department has a domain. This then also brings us directly to the crux of the issue. What about semi-joins
that are an existence filter. For example
```clj
;; Schema:
;;   :person/skill           cardinality/many, ref or keyword
;;   :company/required-skill cardinality/many, ref or keyword

{:find [(sum ?salary)]
 :where [[?e :person/salary ?salary]
         [?e :person/skill ?skill]
         [?company :company/required-skill ?skill]]}
```
The query tries to compute the salaries of people that have a required company skill. If a person has multiple skills that
the company requires, the salary gets counted multiple times under bag semantics. Every join is kind of forced to become a
cardinality multiplying join. Can this be avoided?

### Incremental queries

Set semantics will require some more distinct calls (which are expensive) then bag semantics.
