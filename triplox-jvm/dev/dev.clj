(ns dev
  (:require [xyz.triplox.api :as t]))

(comment
  (def conn (t/connect "localhost" 5490))

  (t/transact conn [{:db/id 50 :db/ident :person/name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
                    {:db/id 51 :db/ident :person/age :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])

  ;; Transact some data
  (t/transact conn [{:db/id 1001 :person/name "alice" :person/age 30}
                    {:db/id 1002 :person/name "bob" :person/age 25}])

  ;; Open a DB value and query
  (def db (t/db conn))
  (t/q db '{:find [?name ?age]
            :where [[?e :person/name ?name]
                    [?e :person/age ?age]]})

  ;; Clean up
  (.close conn)

  )
