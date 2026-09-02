---
title: Clojure API
description: The Triplox Clojure client.
---

The Triplox Clojure client is published on [Maven Central](https://central.sonatype.com/artifact/xyz.triplox/triplox) under `xyz.triplox/triplox`.
The full API reference is available at [cljdoc.org](https://cljdoc.org/d/xyz.triplox/triplox/0.1.0-alpha.8/api/xyz.triplox.api).

## Installation

Add the dependency to your `deps.edn`:

```clojure
xyz.triplox/triplox {:mvn/version "0.1.0-alpha.8"}
```

Or, with Leiningen, in your `project.clj`:

```clojure
[xyz.triplox/triplox "0.1.0-alpha.8"]
```

## Example

The following REPL session defines a small schema, inserts two entities, and runs a Datalog query against a db value.
You will need a running Triplox server. See the [quick start](/getting-started/quick-start/) for how to launch one.

```clojure
(require '[xyz.triplox.api :as tc])

(def conn (tc/connect "localhost" 5490))

;; 1. Transact a schema
(tc/transact conn [{:db/ident :name
                    :db/valueType :db.type/string
                    :db/cardinality :db.cardinality/one}
                   {:db/ident :age
                    :db/valueType :db.type/long
                    :db/cardinality :db.cardinality/one}])
;; => {:tx-id 1,
;;     :system-time
;;     #object[java.time.Instant 0xd2e9b4 "2026-09-02T12:30:20.557187Z"],
;;     :committed? true}


;; 2. Transact some data
(tc/transact conn [{:name "alice" :age 30}
                   {:name "bob" :age 25}])
;; => {:tx-id 2,
;;     :system-time
;;     #object[java.time.Instant 0x6b26b3c6 "2026-09-02T12:30:29.598486Z"],
;;     :committed? true}



;; 3. Open a DB value and query
(def db (tc/db conn))
(tc/q db '{:find [?e ?name ?age]
           :where [[?e :name ?name]
                   [?e :age ?age]]})
;; => [[8796093022209 "bob" 25] [8796093022208 "alice" 30]]
```
