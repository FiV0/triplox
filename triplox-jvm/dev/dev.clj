(ns dev
  "REPL playground for Triplox client."
  (:require [triplox.client :as client]))

(comment
  ;; Connect to a running Triplox server
  (def conn (client/connect "localhost" 5432))

  ;; Transact some data
  (client/transact conn [{:db/id 1 :person/name "alice" :person/age 30}
                          {:db/id 2 :person/name "bob" :person/age 25}])

  ;; Open a DB snapshot and query
  (def db (client/open-db conn))

  (client/q db '{:find [?name ?age]
                 :where [[?e :person/name ?name]
                         [?e :person/age ?age]]})

  ;; Clean up
  (client/close-db db)
  (client/close conn)
  ;;
  )
