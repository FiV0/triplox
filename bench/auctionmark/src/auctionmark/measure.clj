(ns auctionmark.measure
  "Measure how fresh the subscriptions are; result rows are checked elsewhere.

  The server applies one SlateDB WAL batch of transactions at a time, so one delta can
  cover several transactions and carries the tx-key of the last. A batch that leaves a
  result unchanged still sends an empty delta. A transaction that changes a result
  counts as delivered once its subscription consumes a delta at or after its tx-id.")

(defn empty-tracker []
  {:last-tx-id nil
   ;; query-id -> sorted {tx-id timing}, changes the subscription hasn't reached yet
   :pending {}
   ;; query-id -> [delivery], deltas whose tx-key no recorded transaction covers yet
   :early {}
   :samples []
   :transactions []
   :errors []
   :peak-pending 0})

(defn pending-deliveries
  "Timings of every change some subscription hasn't reached yet."
  [tracker]
  (mapcat vals (vals (:pending tracker))))

(defn- unmatched-deliveries [tracker]
  (reduce + 0 (map count (vals (:early tracker)))))

(defn- known? [tracker tx-id]
  (when-let [last-tx-id (:last-tx-id tracker)]
    (<= tx-id last-tx-id)))

(defn register-subscription
  "Note a subscription's registration tx-id, so deltas at or before it count as known."
  [tracker tx-id]
  (update tracker :last-tx-id #(if % (max % tx-id) tx-id)))

(defn record-delivery
  "Mark the changes a consumed delta reaches as delivered and sample their latency, or
  park the delta until the transaction it reaches is recorded."
  [tracker tx-id query-id applied-ns]
  (if-not (known? tracker tx-id)
    (update-in tracker [:early query-id] (fnil conj []) {:tx-id tx-id :applied-ns applied-ns})
    (let [pending (get-in tracker [:pending query-id] (sorted-map))
          reached (subseq pending <= tx-id)]
      (-> tracker
          (assoc-in [:pending query-id] (apply dissoc pending (map key reached)))
          (update :samples into (for [[_ {:keys [start-ns measured? kind]}] reached
                                      :when measured?]
                                  {:ms (/ (- applied-ns start-ns) 1e6) :kind kind}))))))

(defn- replay-early
  "Record parked deltas that the newly recorded transaction covers."
  [tracker]
  (reduce-kv (fn [tracker query-id deliveries]
               (let [{ready true waiting false} (group-by #(boolean (known? tracker (:tx-id %))) deliveries)
                     tracker (if (seq waiting)
                               (assoc-in tracker [:early query-id] waiting)
                               (update tracker :early dissoc query-id))]
                 (reduce (fn [tracker {:keys [tx-id applied-ns]}]
                           (record-delivery tracker tx-id query-id applied-ns))
                         tracker ready)))
             tracker (:early tracker)))

(defn record-transaction
  "Register the results a committed transaction changes, then record deltas that arrived
  before it. changes maps query-id to the query's kind."
  [tracker tx-id timing changes subscription-count]
  (let [{:keys [start-ns ack-ns scheduled-ns measured?]} timing
        tracker (cond-> (reduce-kv (fn [tracker query-id kind]
                                     (update-in tracker [:pending query-id] (fnil assoc (sorted-map))
                                                tx-id (assoc timing :kind kind)))
                                   tracker changes)
                  true (assoc :last-tx-id tx-id)
                  measured? (update :transactions conj
                                    {:tx-id tx-id
                                     :ack-ms (/ (- ack-ns start-ns) 1e6)
                                     :schedule-delay-ms (/ (max 0 (- start-ns scheduled-ns)) 1e6)
                                     :affected (count changes)
                                     :unchanged (- subscription-count (count changes))}))
        tracker (replay-early tracker)]
    (update tracker :peak-pending max (count (pending-deliveries tracker)))))

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
     :missing-deliveries (count (pending-deliveries tracker))
     :unmatched-deliveries (unmatched-deliveries tracker)
     :errors (:errors tracker)}))
