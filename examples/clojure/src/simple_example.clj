(require '[io.triplox.api :as tc])

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



;; 3. Open a DB snapshot and query
(with-open [db (tc/db conn)]
  (tc/q db '{:find [?e ?name ?age]
             :where [[?e :name ?name]
                     [?e :age ?age]]}))
;; => [[8796093022209 "alice" 30] [8796093022208 "bob" 25]]
