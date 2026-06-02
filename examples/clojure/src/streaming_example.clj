(require '[xyz.triplox.api :as tc])

(def host "localhost")
(def port 5490)

(def conn (tc/connect host port))

;; 1. Define a :name attribute
(tc/transact conn [{:db/ident :name
                    :db/valueType :db.type/string
                    :db/cardinality :db.cardinality/one}])

;; 2. Subscribe to all names at the latest indexed basis.
;;    `with-open` closes the subscription and unsubscribes.
(with-open [sub (tc/subscribe conn '{:find [?name] :where [[?e :name ?name]]})]

  (tc/basis sub)
  ;; => {:tx-id 0, :system-time ..., :tx-eid ...}

  ;; 3. Transact a name; the subscription receives a delta. `take!` blocks for the
  ;;    next delta and returns a vector of [row-values weight] pairs.
  (tc/transact conn [{:name "Ivan"}])
  (tc/take! sub)
  ;; => [[["Ivan"] 1]]

  ;; 4. The 2-arity bounds the wait and returns ::timeout when nothing changed.
  (tc/take! sub 200)
  ;; => :xyz.triplox.api/timeout
  )
