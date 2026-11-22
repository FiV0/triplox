(require '[xyz.triplox.api :as tc])

(def conn (tc/connect "localhost" 5490))

;; define a :name attribute
(tc/transact conn [{:db/ident :name
                    :db/valueType :db.type/string
                    :db/cardinality :db.cardinality/one}])

;; subscribe to name changes
(def sub (tc/subscribe conn
                       '{:find [?e ?name]
                         :where [[?e :name ?name]]}))

;; take empty initial tuples
(tc/take! sub)
;; => []

;; transact a name
(tc/transact conn [{:name "Ivan"}])
;; => {:tx-id 2,
;;     :system-time
;;     #object[java.time.Instant 0x41c698cf "2026-11-22T13:23:22.647641Z"],
;;     :committed? true}

;; take the changes
(tc/take! sub)
;; => [[[8796093022208 "Ivan"] 1]]

;; The 2-arity bounds the wait and returns ::timeout when nothing changed.
(tc/take! sub 200)
;; => :xyz.triplox.api/timeout

;; close the subscription so server resources get released
(.close sub)
