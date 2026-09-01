---
title: Incremental Query Tutorial
description: Learn how Triplox updates incremental Datalog queries as data changes.
---

The tutorial assumes some familiarity with [Datalog queries](/query-language/datalog/) and does
not explain EDN Datalog syntax. If you prefer to follow along with the tutorial in
a REPL session, you can do so in the [triplox-incremental-tutorial](https://github.com/FiV0/triplox-incremental-tutorial/).

Rows in an incremental result are paired with a weight. A weight of `1` adds a
row to the result, while `-1` removes it. The order of rows in a result is not
guaranteed.

Consider the query
```clojure
'{:find [?name ?residence]
  :where [[?p :person/name ?name]
          [?p :person/residence ?residence]]}
```
and its result set.
```clojure
[[["Ada Lovelace" "12 St. James's Square"] -1]
 [["Ada Lovelace" "Buckingham Palace"] 1]]
```
This result means that Ada Lovelace moved from 12 St. James's Square to Buckingham Palace.

In this tutorial, we are going to consider a minimal issue tracker with the entity
types `user`, `team`, `issue` and `status`. The tutorial examples will contain
[transaction data](/transactions/transaction-data/), [queries](/query-language/datalog/)
and result sets. They are client-independent and represented using standard
[EDN](https://github.com/edn-format/edn) syntax.

## Schema

The issue tracker has users, teams, issues, and three issue statuses. Notice
that a user can be part of multiple teams.

```clojure
[;; users
 {:db/ident :user/handle
  :db/valueType :db.type/string
  :db/cardinality :db.cardinality/one
  :db/unique :db.unique/identity}
 {:db/ident :user/name
  :db/valueType :db.type/string
  :db/cardinality :db.cardinality/one}
 {:db/ident :user/team
  :db/valueType :db.type/ref
  :db/cardinality :db.cardinality/many}

 ;; teams
 {:db/ident :team/name
  :db/valueType :db.type/string
  :db/cardinality :db.cardinality/one
  :db/unique :db.unique/identity}

 ;; issues
 {:db/ident :issue/key
  :db/valueType :db.type/string
  :db/cardinality :db.cardinality/one
  :db/unique :db.unique/identity}
 {:db/ident :issue/title
  :db/valueType :db.type/string
  :db/cardinality :db.cardinality/one}
 {:db/ident :issue/status
  :db/valueType :db.type/ref
  :db/cardinality :db.cardinality/one}
 {:db/ident :issue/priority
  :db/valueType :db.type/long
  :db/cardinality :db.cardinality/one}
 {:db/ident :issue/assignee
  :db/valueType :db.type/ref
  :db/cardinality :db.cardinality/one}
 {:db/ident :issue/label
  :db/valueType :db.type/string
  :db/cardinality :db.cardinality/many}

 ;; status enum
 {:db/ident :status/open}
 {:db/ident :status/in-progress}
 {:db/ident :status/closed}]
```

## Joins and unification

Let us start by adding four users and two teams. One user belongs to both teams,
which have three and two members, respectively. Before adding the data, we subscribe
to an incremental query that returns each user together with each team they belong to.

The query:
```clojure
{:find [?user-name ?team-name]
 :where [[?user :user/name ?user-name]
         [?user :user/team ?team]
         [?team :team/name ?team-name]]}
```

The data:
```clojure
[{:db/id "team-frontend" :team/name "Frontend"}
 {:db/id "team-backend" :team/name "Backend"}

 {:user/handle "ada" :user/name "Ada Lovelace" :user/team "team-frontend"}
 {:user/handle "alan" :user/name "Alan Turing" :user/team "team-frontend"}
 {:db/id "grace-tmpid" :user/handle "grace" :user/name "Grace Hopper"}
 ;; TODO: We currently don't support vector syntax for cardinality/many attributes,
 ;; hence we need to add the refs one at a time.
 [:db/add "grace-tmpid" :user/team "team-frontend"]
 [:db/add "grace-tmpid" :user/team "team-backend"]
 {:user/handle "edsger" :user/name "Edsger Dijkstra" :user/team "team-backend"}]
```

The initialization delta contains the equivalent of the corresponding standard query result.

```clojure
[[["Ada Lovelace" "Frontend"] 1]
 [["Alan Turing" "Frontend"] 1]
 [["Edsger Dijkstra" "Backend"] 1]
 [["Grace Hopper" "Backend"] 1]
 [["Grace Hopper" "Frontend"] 1]]
```
Suppose we now remove the Frontend team because we have decided to build a database (no frontend required 😉).

```clojure
[[:db/retract [:team/name "Frontend"] :team/name "Frontend"]]
```
The next `take!` on the incremental query subscription will return
```clojure
[[["Ada Lovelace" "Frontend"] -1]
 [["Alan Turing" "Frontend"] -1]
 [["Grace Hopper" "Frontend"] -1]]
```
All user-team pairs involving the Frontend team were removed.


## Predicates and functions

Let us now create a query that uses [predicates](/query-language/datalog/#predicates) and
[functions](/query-language/datalog/#functions). It returns high-priority issues,
their assignees, and their corresponding SLA response-time targets in hours.
The logic assumes that a lower priority number indicates greater urgency.

```clojure
{:find [?title ?assignee-name ?response-time-hours]
 :where [[?issue :issue/title ?title]
         [?issue :issue/priority ?priority]
         [(<= ?priority 2)]
         [?issue :issue/assignee ?assignee]
         [?assignee :user/name ?assignee-name]
         [(* ?priority 24) ?response-time-hours]]}
```

Add six issues with priorities from one to five:

```clojure
[{:issue/key "TPX-1"
  :issue/title "Sync engine drops updates on reconnect"
  :issue/priority 1
  :issue/assignee [:user/handle "ada"]}
 {:issue/key "TPX-2"
  :issue/title "Add dark mode to the dashboard"
  :issue/priority 4
  :issue/assignee [:user/handle "alan"]}
 {:issue/key "TPX-3"
  :issue/title "Incremental joins allocate per delta"
  :issue/priority 2
  :issue/assignee [:user/handle "grace"]}
 {:issue/key "TPX-4"
  :issue/title "Document the tutorial schema"
  :issue/priority 5
  :issue/assignee [:user/handle "edsger"]}
 {:issue/key "TPX-5"
  :issue/title "WAL replay is quadratic"
  :issue/priority 1
  :issue/assignee [:user/handle "grace"]}
 {:issue/key "TPX-6"
  :issue/title "Flaky test in the bid pipeline"
  :issue/priority 3
  :issue/assignee [:user/handle "alan"]}]
```

Three issues satisfy the query and appear in its initial result set:

```clojure
[[["Incremental joins allocate per delta" "Grace Hopper" 48] 1]
 [["Sync engine drops updates on reconnect" "Ada Lovelace" 24] 1]
 [["WAL replay is quadratic" "Grace Hopper" 24] 1]]
```

We now prioritize the flaky test and deprioritize the incremental join allocation
issue:

```clojure
[[:db/add [:issue/key "TPX-6"] :issue/priority 2]
 [:db/add [:issue/key "TPX-3"] :issue/priority 4]]
```

One issue enters the result and one leaves it:

```clojure
[[["Flaky test in the bid pipeline" "Alan Turing" 48] 1]
 [["Incremental joins allocate per delta" "Grace Hopper" 48] -1]]
```

## Or

Let us also add a status to every issue.

```clojure
[[:db/add [:issue/key "TPX-1"] :issue/status :status/open]
 [:db/add [:issue/key "TPX-2"] :issue/status :status/open]
 [:db/add [:issue/key "TPX-3"] :issue/status :status/open]
 [:db/add [:issue/key "TPX-4"] :issue/status :status/closed]
 [:db/add [:issue/key "TPX-5"] :issue/status :status/in-progress]
 [:db/add [:issue/key "TPX-6"] :issue/status :status/in-progress]]
```

An issue is active when its status is either open or in progress. The `or`
clause lets either branch satisfy the query. Note that we currently
don't support ident keywords in ref value position, which explains the indirect
unification through `?status`.

```clojure
{:find [?title]
 :where [[?issue :issue/title ?title]
         [?issue :issue/status ?status]
         (or [?status :db/ident :status/open]
             [?status :db/ident :status/in-progress])]}
```

All issues except `TPX-4` are initially active:

```clojure
[[["Add dark mode to the dashboard"] 1]
 [["Flaky test in the bid pipeline"] 1]
 [["Incremental joins allocate per delta"] 1]
 [["Sync engine drops updates on reconnect"] 1]
 [["WAL replay is quadratic"] 1]]
```

Let's move one open issue to in progress and close another issue:

```clojure
[[:db/add [:issue/key "TPX-1"] :issue/status :status/in-progress]
 [:db/add [:issue/key "TPX-5"] :issue/status :status/closed]]
```

Changing from open to in progress keeps `TPX-1` in the active result, so it
produces no delta. Closing `TPX-5` removes it.

```clojure
[[["WAL replay is quadratic"] -1]]
```

## Not

The `not` clause can express the same active-set boundary by excluding closed
statuses:

```clojure
{:find [?title]
 :where [[?issue :issue/title ?title]
         [?issue :issue/status ?status]
         (not [?status :db/ident :status/closed])]}
```

At this point, four issues are not closed, which is what the initialization delta
will contain.

```clojure
[[["Add dark mode to the dashboard"] 1]
 [["Flaky test in the bid pipeline"] 1]
 [["Incremental joins allocate per delta"] 1]
 [["Sync engine drops updates on reconnect"] 1]]
```

Close the flaky test:

```clojure
[[:db/add [:issue/key "TPX-6"] :issue/status :status/closed]]
```
It leaves the result set:

```clojure
[[["Flaky test in the bid pipeline"] -1]]
```

## Aggregates, Not-join, Or-join and Rules

We currently don't support any of these, but plan to add them.
Rules will likely be the last addition.
