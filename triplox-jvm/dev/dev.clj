(ns dev
  "REPL playground for Triplox client."
  (:require [triplox.client :as client]))

(comment
  (def conn (client/connect "localhost" 5490))

  (client/transact conn [{:db/id 50 :db/ident :person/name :db/valueType :db.type/string}
                         {:db/id 51 :db/ident :person/age :db/valueType :db.type/long}])

  ;; Transact some data
  (client/transact conn [{:db/id 1001 :person/name "alice" :person/age 30}
                         {:db/id 1002 :person/name "bob" :person/age 25}])

  ;; Open a DB snapshot and query
  (def db (client/open-db conn))

  (client/q db '{:find [?name ?age]
                 :where [[?e :person/name ?name]
                         [?e :person/age ?age]]})

  ;; Clean up
  (client/close-db db)
  (client/close conn)

  )
