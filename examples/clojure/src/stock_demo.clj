(ns stock-demo
  (:require [xyz.triplox.api :as t])
  (:import [java.util UUID]))

(def low-stock-query
  '{:find [?stock ?sku ?name ?location ?available ?threshold]
    :where [[?stock :stock/product ?product]
            [?stock :stock/warehouse ?warehouse]
            [?stock :stock/available ?available]
            [?product :product/sku ?sku]
            [?product :product/name ?name]
            [?product :product/reorder-at ?threshold]
            [?warehouse :warehouse/name ?location]
            [(< ?available ?threshold)]]})

(def inventory-query
  (update low-stock-query :where pop))

(def schema
  [{:db/ident :product/sku
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :product/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :product/reorder-at
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident :warehouse/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :stock/id
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :stock/product
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :stock/warehouse
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :stock/available
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident :sale/id
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :sale/stock
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :sale/quantity
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident :refill/id
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :refill/stock
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :refill/quantity
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}])

(def products
  [{:product/sku "COF-01" :product/name "Coffee" :product/reorder-at 10}
   {:product/sku "TEA-02" :product/name "Tea" :product/reorder-at 8}
   {:product/sku "OAT-03" :product/name "Milk" :product/reorder-at 6}])

(defn seed-data []
  (vec (for [location ["Berlin" "Hamburg"]
             {:product/keys [sku reorder-at]} products]
         {:stock/id (str location "/" sku)
          :stock/product [:product/sku sku]
          :stock/warehouse [:warehouse/name location]
          :stock/available (if (= [location sku] ["Hamburg" "TEA-02"])
                             (dec reorder-at)
                             (+ reorder-at 2))})))

(defn transact! [conn tx-data]
  (let [result (t/transact conn tx-data)]
    (when-not (:committed? result)
      (throw (ex-info (str "The stock transaction failed: " (:error-message result)) result)))
    result))

(defn read-inventory [conn]
  (vec (sort-by (juxt #(nth % 3) second) (t/q (t/db conn) inventory-query))))

(defn apply-delta [weights delta]
  (reduce (fn [rows [row weight]]
            (let [total (+ (get rows row 0) weight)]
              (if (zero? total)
                (dissoc rows row)
                (assoc rows row total))))
          weights delta))

(defonce !state
  (atom {:inventory [] :weights {}}))

(defonce ^:private !session (atom nil))

(defn- receive! [{:keys [sub]}]
  (while true
    (if-let [delta (t/take! sub)]
      (swap! !state update :weights apply-delta delta)
      (throw (ex-info "The subscription closed." {})))))

(defn- advance! [{:keys [conn step]}]
  (let [inventory (read-inventory conn)
        n @step
        [stock _ _ _ available threshold]
        (nth inventory (mod (quot n 4) (count inventory)))
        refill? (= 3 (mod n 4))
        quantity (if refill?
                   (- (+ threshold 2) available)
                   (min available (nth [2 3 2] (mod n 4))))
        next-stock ((if refill? + -) available quantity)
        record (if refill?
                 {:refill/id (str (UUID/randomUUID))
                  :refill/stock stock :refill/quantity quantity}
                 {:sale/id (str (UUID/randomUUID))
                  :sale/stock stock :sale/quantity quantity})]
    (transact! conn [record [:db/add stock :stock/available next-stock]])
    (swap! step inc)
    (swap! !state assoc :inventory
           (mapv (fn [row]
                   (if (= stock (first row))
                     (assoc row 4 next-stock)
                     row)) inventory))))

(defn stop! []
  (when-let [{:keys [conn sub writer reader]} (first (reset-vals! !session nil))]
    (future-cancel writer)
    (future-cancel reader)
    (.close sub)
    (.close conn)
    :stopped))

;; Errors raised after stop! has run are shutdown noise, so only the first failure is printed.
(defn- fail! [error]
  (when (stop!)
    (.printStackTrace error)))

(defn start!
  ([] (start! {}))
  ([{:keys [host port tick-ms]
     :or {host "localhost" port 5490 tick-ms 500}}]
   (when @!session
     (throw (ex-info "The stock demo is already running." {})))
   (let [conn (t/connect host port)]
     (try
       (transact! conn schema)
       (transact! conn (concat products [{:warehouse/name "Berlin"}
                                         {:warehouse/name "Hamburg"}]))
       (transact! conn (seed-data))
       (let [inventory (read-inventory conn)
             sub (t/subscribe conn low-stock-query)
             session {:conn conn :sub sub :step (atom 0)}]
         (reset! !state {:inventory inventory :weights {}})
         (reset! !session
                 (assoc session
                        :reader (future
                                  (try (receive! session)
                                       (catch Exception error (fail! error))))
                        :writer (future
                                  (try
                                    (while true
                                      (Thread/sleep tick-ms)
                                      (advance! session))
                                    (catch Exception error (fail! error))))))
         :started)
       (catch Exception error
         (.close conn)
         (throw error))))))

(defn -main [& _]
  (let [serve! (requiring-resolve 'nextjournal.clerk/serve!)
        show! (requiring-resolve 'nextjournal.clerk/show!)
        halt! (requiring-resolve 'nextjournal.clerk/halt!)]
    (try
      (start! {:host (or (System/getenv "TRIPLOX_HOST") "localhost")
               :port (parse-long (or (System/getenv "TRIPLOX_PORT") "5490"))})
      (.addShutdownHook (Runtime/getRuntime) (Thread. ^Runnable stop!))
      (serve! {:host "localhost" :port 7777})
      (show! "notebooks/stock.clj")
      (println "Open http://localhost:7777 to watch stock changes. Press Ctrl-C to stop.")
      @(promise)
      (finally
        (stop!)
        (halt!)
        (shutdown-agents)))))
