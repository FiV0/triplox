(ns dev
  (:require [xyz.triplox.api :as t]))

(comment
  (def conn (t/connect "localhost" 5490))

  (t/transact conn [{:db/ident :person/name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
                    {:db/ident :person/age :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])

  ;; Transact some data
  (t/transact conn [{:person/name "alice" :person/age 30}
                    {:person/name "bob" :person/age 25}])

  ;; Open a DB value and query
  (def db (t/db conn))
  (t/q db '{:find [?name ?age]
            :where [[?e :person/name ?name]
                    [?e :person/age ?age]]})
  ;; => [["bob" 25] ["alice" 30]]


  (def sub (t/subscribe conn '{:find [?name ?age]
                               :where [[?e :person/name ?name]
                                       [?e :person/age ?age]]}))


  (t/transact conn [{:person/name "eve" :person/age 99}])

  (t/take! sub 1000)

  ;; Clean up
  (.close conn)

  )
