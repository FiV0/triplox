(require '[xyz.triplox.api :as tc])

(def host "localhost")
(def port 5490)

(def conn (tc/connect host port))

;; Define a :name attribute
(tc/transact conn [{:db/ident :name
                    :db/valueType :db.type/string
                    :db/cardinality :db.cardinality/one}])

;; 2. Subscribe to name changes
(def sub (tc/subscribe conn
                       '{:find [?e ?name]
                         :where [[?e :name ?name]]}))

(tc/basis sub)
;; => {:tx-id 1,
;;     :system-time
;;     #object[java.time.Instant 0x64cae1b "2026-06-02T13:52:43.058559Z"],
;;     :tx-eid 4398046511105}


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
