(require '[xyz.triplox.api :as tc])

(def conn (tc/connect "localhost" 5490))

;; Define a :name attribute
(tc/transact conn [{:db/ident :name
                    :db/valueType :db.type/string
                    :db/cardinality :db.cardinality/one}])

;; 2. Subscribe to name changes
(def sub (tc/subscribe conn
                       '{:find [?e ?name]
                         :where [[?e :name ?name]]}))

(tc/tx-key sub)
;; => {:tx-id 1,
;;     :system-time
;;     #object[java.time.Instant 0x7c107312 "2026-09-02T12:31:24.534987Z"]}


;; Transact a name; the subscription receives a delta.
;; `take!` blocks for the next delta to arrive.
(do
  (tc/transact conn [{:name "Ivan"}])
  (tc/take! sub))
;; => [[[8796093022208 "Ivan"] 1]]

;; The 2-arity bounds the wait and returns ::timeout when nothing changed.
(tc/take! sub 200)
;; => :xyz.triplox.api/timeout

;; close the subscription so server resources get released
(.close sub)
