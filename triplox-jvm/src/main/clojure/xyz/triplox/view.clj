(ns xyz.triplox.view
  "EXPERIMENTAL: Client-side materialized views backed by incremental queries."
  (:require [xyz.triplox.api :as api])
  (:import [java.io Closeable]
           [java.lang AutoCloseable]
           [java.util.concurrent TimeoutException]))

(defn- update-rows [rows delta]
  (reduce (fn [rows [tuple weight]]
            (let [weight (+ (get rows tuple 0) weight)]
              (when (neg? weight)
                (throw (ex-info "Subscription retracted an absent row" {:tuple tuple :weight weight})))
              (if (zero? weight) (dissoc rows tuple) (assoc rows tuple weight))))
          rows delta))

(defn- publish! [state monitor f]
  (locking monitor
    (swap! state f)
    (.notifyAll ^Object monitor)))

(defn- consume! [sub state monitor]
  (try
    (loop []
      (when-let [delta (api/take! sub)]
        (let [key (api/tx-key sub)]
          (publish! state monitor #(-> % (update :rows update-rows delta) (assoc :tx-key key))))
        (recur)))
    (catch Throwable error
      (publish! state monitor #(if (:closed? %) % (assoc % :error error))))
    (finally (publish! state monitor #(assoc % :closed? true)))))

(defrecord View [sub state monitor worker]
  Closeable
  (close [_]
    (publish! state monitor #(assoc % :closed? true))
    (.close ^AutoCloseable sub)
    (.join ^Thread worker)))

(defn ->view
  "Subscribe and continuously materialize result deltas. Close to unsubscribe."
  [conn query]
  (let [sub (api/subscribe conn query)
        state (atom {:rows {} :tx-key nil :closed? false})
        monitor (Object.)
        worker (doto (Thread. #(consume! sub state monitor) "triplox-view")
                 (.setDaemon true)
                 (.start))]
    (->View sub state monitor worker)))

(defn- snapshot [{:keys [rows tx-key error]}]
  (when error (throw error))
  {:rows (vec (keys rows)) :tx-key tx-key})

(defn get-view
  "Return current rows, or throw the subscription's terminal error."
  [{:keys [state]}]
  (:rows (snapshot @state)))

(defn tx-key
  "The transaction key applied to the view, or nil before its first delta."
  [{:keys [state]}]
  (:tx-key (snapshot @state)))

(defn await-tx
  "Wait for the view to apply through tx-key. Return {:rows ... :tx-key ...}.
  Throws on timeout, subscription failure, or closure before reaching the key."
  [{:keys [state monitor]} target timeout-ms]
  (when-not (and (integer? (:tx-id target)) (not (neg? timeout-ms)))
    (throw (IllegalArgumentException. "Expected a transaction key and nonnegative timeout")))
  (let [deadline (+ (System/nanoTime) (* (long timeout-ms) 1000000))]
    (locking monitor
      (loop []
        (let [{:keys [tx-key closed?] :as current} @state
              result (snapshot current)
              remaining (- deadline (System/nanoTime))]
          (cond
            (and tx-key (>= (:tx-id tx-key) (:tx-id target))) result
            closed? (throw (IllegalStateException. "View closed before reaching the transaction"))
            (not (pos? remaining)) (throw (TimeoutException. "View did not reach the transaction"))
            :else (do (.wait ^Object monitor (quot remaining 1000000)
                             (int (rem remaining 1000000)))
                      (recur))))))))
