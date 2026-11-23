(ns auctionmark.measure
  "Check consumed deltas against the expected result history and measure freshness.

  The server applies one SlateDB WAL batch of transactions at a time, so one delta can
  cover several transactions and carries the tx-key of the last. Changes of a batch can
  cancel out, and a batch that leaves a result unchanged still sends an empty delta.
  The invariant checked is therefore: after consuming any delta with tx-key T, the
  client's rows equal the expected result as of T. A transaction that changes a result
  counts as delivered once the subscription reaches its tx-id.")

(defn empty-tracker []
  {:last-tx-id nil
   ;; query-id -> sorted {tx-id rows}, the expected result after each change
   :history {}
   ;; query-id -> tx-id of the latest consumed delta
   :reached {}
   ;; query-id -> sorted {tx-id timing}, changes the subscription hasn't reached yet
   :pending {}
   ;; query-id -> [delivery], deltas whose tx-key no recorded transaction covers yet
   :early {}
   :samples []
   :transactions []
   :errors []
   :peak-pending 0})

(defn- add-error [tracker error tx-id query-id]
  (update tracker :errors conj {:error error :tx-id tx-id :query-id query-id}))

(defn pending-deliveries
  "Timings of every change some subscription hasn't reached yet."
  [tracker]
  (mapcat vals (vals (:pending tracker))))

(defn- unmatched-deliveries [tracker]
  (reduce + 0 (map count (vals (:early tracker)))))

(defn register-subscription
  "Start a subscription's history from its primed rows at the registration tx-id."
  [tracker query-id tx-id rows]
  (-> tracker
      (assoc-in [:history query-id] (sorted-map tx-id rows))
      (assoc-in [:reached query-id] tx-id)
      (update :last-tx-id #(if % (max % tx-id) tx-id))))

(defn- known? [tracker tx-id]
  (when-let [last-tx-id (:last-tx-id tracker)]
    (<= tx-id last-tx-id)))

(defn- expected-at
  "The expected result as of tx-id, or nil when tx-id precedes the history."
  [tracker query-id tx-id]
  (some-> (get-in tracker [:history query-id])
          (rsubseq <= tx-id)
          first
          val))

(defn- forget-history-before
  "Drop expected results that later deltas can no longer be compared against."
  [history tx-id]
  (if-let [[floor] (first (rsubseq history <= tx-id))]
    (apply dissoc history (map key (subseq history < floor)))
    history))

(defn- record-reached
  "Remove the changes a subscription reached with tx-id and sample their latency."
  [tracker query-id tx-id applied-ns]
  (let [pending (get-in tracker [:pending query-id] (sorted-map))
        reached (subseq pending <= tx-id)]
    (-> tracker
        (assoc-in [:pending query-id] (apply dissoc pending (map key reached)))
        (update :samples into (for [[_ {:keys [start-ns measured? kind]}] reached
                                    :when measured?]
                                {:ms (/ (- applied-ns start-ns) 1e6) :kind kind})))))

(defn record-delivery
  "Check a consumed delta's rows against the expected result as of its tx-key, or park it
  until the transaction it reaches is recorded."
  [tracker tx-id query-id rows applied-ns]
  (if-not (known? tracker tx-id)
    (update-in tracker [:early query-id] (fnil conj []) {:tx-id tx-id :rows rows :applied-ns applied-ns})
    (let [expected (expected-at tracker query-id tx-id)]
      (cond-> (-> tracker
                  (record-reached query-id tx-id applied-ns)
                  (update-in [:history query-id] forget-history-before tx-id)
                  (update-in [:reached query-id] (fnil max tx-id) tx-id))
        (< tx-id (get-in tracker [:reached query-id] tx-id)) (add-error :tx-key-regressed tx-id query-id)
        (nil? expected) (add-error :delta-before-registration tx-id query-id)
        (and expected (not= rows expected)) (add-error :incorrect-result tx-id query-id)))))

(defn- replay-early
  "Check parked deltas that the newly recorded transaction covers."
  [tracker]
  (reduce-kv (fn [tracker query-id deliveries]
               (let [{ready true waiting false} (group-by #(known? tracker (:tx-id %)) deliveries)
                     tracker (if (seq waiting)
                               (assoc-in tracker [:early query-id] waiting)
                               (update tracker :early dissoc query-id))]
                 (reduce (fn [tracker {:keys [tx-id rows applied-ns]}]
                           (record-delivery tracker tx-id query-id rows applied-ns))
                         tracker ready)))
             tracker (:early tracker)))

(defn record-transaction
  "Register a committed transaction's changed results, then check deltas that arrived before it."
  [tracker tx-id timing changes subscription-count]
  (let [{:keys [start-ns ack-ns scheduled-ns measured?]} timing
        tracker (cond-> (reduce-kv (fn [tracker query-id {:keys [rows kind]}]
                                     (-> tracker
                                         (update-in [:history query-id] (fnil assoc (sorted-map)) tx-id rows)
                                         (update-in [:pending query-id] (fnil assoc (sorted-map)) tx-id
                                                    (assoc timing :kind kind))))
                                   tracker changes)
                  (known? tracker tx-id) (add-error :tx-id-not-increasing tx-id nil)
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
