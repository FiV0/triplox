(ns auctionmark.measure
  "Match expected changes and consumed deltas, including delivery before transaction acknowledgement.")

(defn empty-tracker []
  {:known #{} :pending {} :early {} :samples [] :transactions [] :errors [] :peak-pending 0})

(defn record-delivery [tracker tx-id query-id rows applied-ns]
  (let [k [tx-id query-id]]
    (if (contains? (:known tracker) tx-id)
      (if-let [{:keys [expected start-ns measured? kind]} (get-in tracker [:pending k])]
        (cond-> (update tracker :pending dissoc k)
          (not= rows expected) (update :errors conj {:error :incorrect-result :tx-id tx-id :query-id query-id})
          measured? (update :samples conj {:ms (/ (- applied-ns start-ns) 1e6) :kind kind}))
        (update tracker :errors conj {:error :unexpected-delta :tx-id tx-id :query-id query-id}))
      (if (contains? (:early tracker) k)
        (update tracker :errors conj {:error :duplicate-delta :tx-id tx-id :query-id query-id})
        (assoc-in tracker [:early k] {:rows rows :applied-ns applied-ns})))))

(defn record-transaction [tracker tx-id timing changes subscription-count]
  (let [early (filter (fn [[[tx _] _]] (= tx tx-id)) (:early tracker))
        pending (into {} (for [[id {:keys [rows kind]}] changes]
                           [[tx-id id] (assoc timing :expected rows :kind kind)]))
        tracker (-> tracker
                    (update :known conj tx-id)
                    (update :pending merge pending)
                    (update :early #(apply dissoc % (map first early)))
                    (cond-> (:measured? timing)
                      (update :transactions conj
                              {:tx-id tx-id :ack-ms (/ (- (:ack-ns timing) (:start-ns timing)) 1e6)
                               :schedule-delay-ms (/ (max 0 (- (:start-ns timing) (:scheduled-ns timing))) 1e6)
                               :affected (count changes) :unchanged (- subscription-count (count changes))})))]
    (let [tracker (reduce (fn [t [[tx id] {:keys [rows applied-ns]}]]
                            (record-delivery t tx id rows applied-ns)) tracker early)]
      (update tracker :peak-pending max (count (:pending tracker))))))

(defn percentiles [samples]
  (let [values (vec (sort samples)) n (count values)
        at (fn [p] (when (pos? n) (nth values (dec (long (Math/ceil (* p n)))))))]
    {:count n :p50 (at 0.50) :p95 (at 0.95) :p99 (at 0.99) :max (last values)}))

(defn summary [tracker duration]
  {:latency-ms (percentiles (map :ms (:samples tracker)))
   :latency-by-query (into {} (for [[kind samples] (group-by :kind (:samples tracker))]
                               [kind (percentiles (map :ms samples))]))
   :committed-transactions (count (:transactions tracker))
   :transactions-per-second (/ (count (:transactions tracker)) duration)
   :ack-ms (percentiles (map :ack-ms (:transactions tracker)))
   :schedule-delay-ms (percentiles (map :schedule-delay-ms (:transactions tracker)))
   :affected-subscriptions (percentiles (map :affected (:transactions tracker)))
   :expected-deliveries (reduce + 0 (map :affected (:transactions tracker)))
   :applied-deliveries-per-second (/ (count (:samples tracker)) duration)
   :peak-pending-deliveries (:peak-pending tracker)
   :unchanged-query-transactions (reduce + 0 (map :unchanged (:transactions tracker)))
   :missing-deliveries (count (:pending tracker))
   :unmatched-deliveries (count (:early tracker))
   :errors (:errors tracker)})
