(ns auctionmark.measure
  "Match expected changes and consumed deltas, including delivery before transaction acknowledgement.")

(defn empty-tracker []
  {:known #{}
   :pending {}
   :early {}
   :samples []
   :transactions []
   :errors []
   :peak-pending 0})

(defn- add-error [tracker error tx-id query-id]
  (update tracker :errors conj {:error error :tx-id tx-id :query-id query-id}))

(defn record-delivery
  "Match a consumed delta against its expected change, or park it until its transaction is recorded."
  [tracker tx-id query-id rows applied-ns]
  (let [k [tx-id query-id]
        pending (get-in tracker [:pending k])]
    (cond
      (not (contains? (:known tracker) tx-id))
      (if (contains? (:early tracker) k)
        (add-error tracker :duplicate-delta tx-id query-id)
        (assoc-in tracker [:early k] {:rows rows :applied-ns applied-ns}))

      (nil? pending)
      (add-error tracker :unexpected-delta tx-id query-id)

      :else
      (let [{:keys [expected start-ns measured? kind]} pending]
        (cond-> (update tracker :pending dissoc k)
          (not= rows expected) (add-error :incorrect-result tx-id query-id)
          measured? (update :samples conj {:ms (/ (- applied-ns start-ns) 1e6) :kind kind}))))))

(defn record-transaction
  "Register a committed transaction's expected changes, then match deltas that arrived before it."
  [tracker tx-id timing changes subscription-count]
  (let [{:keys [start-ns ack-ns scheduled-ns measured?]} timing
        early (filter (fn [[[tx _] _]] (= tx tx-id)) (:early tracker))
        pending (into {} (for [[query-id {:keys [rows kind]}] changes]
                           [[tx-id query-id] (assoc timing :expected rows :kind kind)]))
        tracker (cond-> (-> tracker
                            (update :known conj tx-id)
                            (update :pending merge pending)
                            (update :early #(apply dissoc % (map first early))))
                  measured? (update :transactions conj
                                    {:tx-id tx-id
                                     :ack-ms (/ (- ack-ns start-ns) 1e6)
                                     :schedule-delay-ms (/ (max 0 (- start-ns scheduled-ns)) 1e6)
                                     :affected (count changes)
                                     :unchanged (- subscription-count (count changes))}))
        tracker (reduce (fn [tracker [[_ query-id] {:keys [rows applied-ns]}]]
                          (record-delivery tracker tx-id query-id rows applied-ns))
                        tracker early)]
    (update tracker :peak-pending max (count (:pending tracker)))))

(defn percentiles [samples]
  (let [values (vec (sort samples))
        n (count values)
        at (fn [p] (when (pos? n) (nth values (dec (long (Math/ceil (* p n)))))))]
    {:count n
     :p50 (at 0.50)
     :p95 (at 0.95)
     :p99 (at 0.99)
     :max (last values)}))

(defn summary [tracker duration]
  (let [{:keys [samples transactions]} tracker]
    {:latency-ms (percentiles (map :ms samples))
     :latency-by-query (update-vals (group-by :kind samples) #(percentiles (map :ms %)))
     :committed-transactions (count transactions)
     :transactions-per-second (/ (count transactions) duration)
     :ack-ms (percentiles (map :ack-ms transactions))
     :schedule-delay-ms (percentiles (map :schedule-delay-ms transactions))
     :affected-subscriptions (percentiles (map :affected transactions))
     :expected-deliveries (reduce + 0 (map :affected transactions))
     :applied-deliveries-per-second (/ (count samples) duration)
     :peak-pending-deliveries (:peak-pending tracker)
     :unchanged-query-transactions (reduce + 0 (map :unchanged transactions))
     :missing-deliveries (count (:pending tracker))
     :unmatched-deliveries (count (:early tracker))
     :errors (:errors tracker)}))
