(ns stock-demo
  (:require [xyz.triplox.api :as t])
  (:import [java.time LocalTime]
           [java.time.format DateTimeFormatter]
           [java.util UUID]))

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
  (mapv (fn [[ident value-type unique?]]
          (cond-> {:db/ident ident
                   :db/valueType value-type
                   :db/cardinality :db.cardinality/one}
            unique? (assoc :db/unique :db.unique/identity)))
        [[:product/sku :db.type/string true]
         [:product/name :db.type/string]
         [:product/reorder-at :db.type/long]
         [:warehouse/name :db.type/string true]
         [:stock/id :db.type/string true]
         [:stock/product :db.type/ref]
         [:stock/warehouse :db.type/ref]
         [:stock/available :db.type/long]
         [:sale/id :db.type/string true]
         [:sale/stock :db.type/ref]
         [:sale/quantity :db.type/long]
         [:refill/id :db.type/string true]
         [:refill/stock :db.type/ref]
         [:refill/quantity :db.type/long]]))

(def products
  [{:product/sku "COF-01" :product/name "Coffee" :product/reorder-at 10}
   {:product/sku "TEA-02" :product/name "Tea" :product/reorder-at 8}
   {:product/sku "OAT-03" :product/name "Oat milk" :product/reorder-at 6}])

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
  (atom {:status :stopped :inventory [] :weights {} :events [] :batches []
         :batch-count 0}))

(defonce ^:private !session (atom nil))
(defonce ^:private lifecycle-lock (Object.))

(defn- timestamp []
  (.format (LocalTime/now) (DateTimeFormatter/ofPattern "HH:mm:ss")))

(defn- prepend [items item limit]
  (vec (take limit (cons item items))))

(defn- receive! [{:keys [sub running]}]
  (while @running
    (let [delta (t/take! sub 200)]
      (cond
        (= delta ::t/timeout) nil
        (nil? delta) (when @running
                       (throw (ex-info "The subscription closed." {})))
        :else (swap! !state
                     (fn [state]
                       (let [number (inc (:batch-count state))
                             batch {:number number :time (timestamp) :rows delta}]
                         (-> state
                             (assoc :batch-count number)
                             (update :weights apply-delta delta)
                             (update :batches prepend batch 8)))))))))

(defn- advance! [{:keys [conn step write-lock running]}]
  (locking write-lock
    (when @running
      (let [inventory (read-inventory conn)
            n @step
            [stock sku product location available threshold]
            (nth inventory (mod (quot n 4) (count inventory)))
            refill? (= 3 (mod n 4))
            quantity (if refill?
                       (- (+ threshold 2) available)
                       (min available (nth [2 3 2] (mod n 4))))
            next-stock ((if refill? + -) available quantity)
            kind (if refill? :refill :sale)
            record (if refill?
                     {:refill/id (str (UUID/randomUUID))
                      :refill/stock stock :refill/quantity quantity}
                     {:sale/id (str (UUID/randomUUID))
                      :sale/stock stock :sale/quantity quantity})
            result (transact! conn
                              [record [:db/add stock :stock/available next-stock]])
            event {:tx-id (:tx-id result) :time (timestamp) :kind kind
                   :sku sku :product product :location location
                   :quantity quantity :before available :after next-stock}]
        (swap! step inc)
        (swap! !state
               (fn [state]
                 (-> state
                     (assoc :inventory
                            (mapv (fn [row]
                                    (if (= stock (first row))
                                      (assoc row 4 next-stock)
                                      row)) inventory))
                     (update :events prepend event 10))))
        event))))

(defn step! []
  (when-let [session @!session]
    (advance! session)))

(defn pause! []
  (when-let [{:keys [paused write-lock]} @!session]
    (locking write-lock
      (reset! paused true)
      (swap! !state assoc :status :paused))))

(defn resume! []
  (when-let [{:keys [paused]} @!session]
    (reset! paused false)
    (swap! !state assoc :status :running)))

(defn stop! []
  (locking lifecycle-lock
    (when-let [{:keys [conn sub running write-lock writer reader]} @!session]
      (reset! running false)
      (future-cancel writer)
      (locking write-lock
        (.close sub)
        (.close conn))
      (future-cancel reader)
      (reset! !session nil))
    (swap! !state assoc :status :stopped)))

(defn- fail! [session error]
  (when (compare-and-set! (:running session) true false)
    (locking (:write-lock session)
      (.close (:sub session))
      (.close (:conn session)))
    (swap! !state assoc :status :error :error (.getMessage error))))

(defn start!
  ([] (start! {}))
  ([{:keys [host port tick-ms automatic?]
     :or {host "localhost" port 5490 tick-ms 1500 automatic? true}}]
   (locking lifecycle-lock
     (stop!)
     (let [conn (t/connect host port)]
       (try
         (transact! conn schema)
         (transact! conn (concat products [{:warehouse/name "Berlin"}
                                           {:warehouse/name "Hamburg"}]))
         (transact! conn (seed-data))
         (let [inventory (read-inventory conn)
               sub (t/subscribe conn low-stock-query)
               session {:conn conn :sub sub :step (atom 0) :write-lock (Object.)
                        :running (atom true) :paused (atom (not automatic?))}]
           (reset! !state {:status (if automatic? :running :paused)
                           :inventory inventory :weights {} :events [] :batches []
                           :batch-count 0})
           (reset! !session
                   (assoc session
                          :reader (future
                                    (try (receive! session)
                                         (catch Exception error (fail! session error))))
                          :writer (future
                                    (try
                                      (while @(:running session)
                                        (Thread/sleep tick-ms)
                                        (locking (:write-lock session)
                                          (when-not @(:paused session)
                                            (advance! session))))
                                      (catch Exception error (fail! session error))))))
           :started)
         (catch Exception error
           (.close conn)
           (throw error)))))))

(defn -main [& _]
  (let [serve! (requiring-resolve 'nextjournal.clerk/serve!)
        show! (requiring-resolve 'nextjournal.clerk/show!)
        halt! (requiring-resolve 'nextjournal.clerk/halt!)]
    (.addShutdownHook (Runtime/getRuntime) (Thread. ^Runnable stop!))
    (try
      (start! {:host (or (System/getenv "TRIPLOX_HOST") "localhost")
               :port (parse-long (or (System/getenv "TRIPLOX_PORT") "5490"))})
      (serve! {:host "localhost" :port 7777})
      (show! "notebooks/stock.clj")
      (println "Open http://localhost:7777 to watch stock changes. Press Ctrl-C to stop.")
      @(promise)
      (finally (stop!) (halt!) (shutdown-agents)))))
