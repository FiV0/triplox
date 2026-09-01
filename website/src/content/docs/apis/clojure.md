---
title: Clojure API
description: The Triplox Clojure client.
---

The Triplox Clojure client is published on [Maven Central](https://central.sonatype.com/artifact/xyz.triplox/triplox) under `xyz.triplox/triplox`.
The full API reference is available at [cljdoc.org](https://cljdoc.org/d/xyz.triplox/triplox/0.1.0-alpha.5/api/xyz.triplox.api).

## Installation

Add the dependency to your `deps.edn`:

```clojure
xyz.triplox/triplox {:mvn/version "0.1.0-alpha.4"}
```

Or, with Leiningen, in your `project.clj`:

```clojure
[xyz.triplox/triplox "0.1.0-alpha.4"]
```

## Example

The following REPL session defines a small schema, inserts two entities, and runs a Datalog query against a db value.
You will need a running Triplox server. See the [quick start](/getting-started/quick-start/) for how to launch one.

```clojure
(require '[xyz.triplox.api :as tc])

(def host "localhost")
(def port 5490)

(def conn (tc/connect host port))

;; 1. Transact a schema
(tc/transact conn [{:db/ident :name
                    :db/valueType :db.type/string
                    :db/cardinality :db.cardinality/one}
                   {:db/ident :age
                    :db/valueType :db.type/long
                    :db/cardinality :db.cardinality/one}])
;; => {:tx-id 0,
;;     :system-time 1775733871873835,
;;     :committed? true,
;;     :error-message nil}

;; 2. Transact some data
(tc/transact conn [{:name "alice" :age 30}
                   {:name "bob" :age 25}])
;; => {:tx-id 1,
;;     :system-time 1775733942779513,
;;     :committed? true,
;;     :error-message nil}

;; 3. Open a DB value and query
(def db (tc/db conn))
(tc/q db '{:find [?e ?name ?age]
           :where [[?e :name ?name]
                   [?e :age ?age]]})
;; => [[8796093022209 "alice" 30] [8796093022208 "bob" 25]]
```
