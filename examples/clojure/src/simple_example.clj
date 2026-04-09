(require '[io.triplox.api :as tc])

(def host "localhost")
(def port 5490)

(println (str "Connecting to " host ":" port "..."))

(def conn (tc/connect host port))

;; 1. Transact a schema
(tc/transact conn [{:db/id 200
                    :db/ident :name
                    :db/valueType :db.type/string
                    :db/cardinality :db.cardinality/one}
                   {:db/id 201 :db/ident :age
                    :db/valueType :db.type/long
                    :db/cardinality :db.cardinality/one}])
;; => {:tx-id 0,
;;     :system-time 1775733871873835,
;;     :committed? true,
;;     :error-message nil}

;; 2. Transact some data
(tc/transact conn [{:db/id 100 :name "alice" :age 30}
                   {:db/id 101 :name "bob" :age 25}])
;; => {:tx-id 1,
;;     :system-time 1775733942779513,
;;     :committed? true,
;;     :error-message nil}



;; 3. Open a DB snapshot and query
(with-open [db (tc/db conn)]
  (tc/q db '{:find [?e ?name ?age]
             :where [[?e :name ?name]
                     [?e :age ?age]]}))
;; => [[101 "bob" 25] [100 "alice" 30]]
