(ns job.runner
  (:refer-clojure :exclude [run!])
  (:require [clojure.java.io :as io]
            [job.artifacts :as artifacts]
            [job.data :as data])
  (:import [java.lang AutoCloseable]
           [java.util.concurrent Callable ExecutorService Executors TimeUnit TimeoutException]))

(defn- milliseconds [start]
  (/ (- (System/nanoTime) start) 1e6))

(defn bounded [^ExecutorService executor timeout-ms f]
  (let [task (.submit executor ^Callable (reify Callable (call [_] (f))))]
    (try (.get task timeout-ms TimeUnit/MILLISECONDS)
         (catch TimeoutException error
           (.cancel task true)
           (throw (ex-info "Benchmark operation timed out" {:timeout-ms timeout-ms} error))))))

(defn- query-pass! [engine executor config measured? checkpoint]
  (let [{:keys [query-ids output timeout-ms verify-expected]} config
        pass-start (when measured? (System/nanoTime))
        db ((:db engine))
        results
        (try
          (mapv (fn [id]
                  (println (if measured? "measure" "warmup") checkpoint id)
                  (let [start (when measured? (System/nanoTime))]
                    (try
                      (let [rows (bounded executor timeout-ms #((:query engine) db id))]
                        [id rows (when measured? (milliseconds start))])
                      (catch Throwable error
                        (artifacts/append! output {:phase (if measured? "query" "warmup")
                                                  :checkpoint checkpoint :query id :status "failed"
                                                  :message (.getMessage error)})
                        (throw error))))) query-ids)
          (finally (when-let [close (:close-db! engine)] (close db))))
        pass-ms (when measured? (milliseconds pass-start))]
    (when measured?
      (doseq [[id rows elapsed] results]
        (let [answer (artifacts/save-result! output checkpoint id rows)]
          (artifacts/append! output (merge {:phase "query" :checkpoint checkpoint :query id
                                            :elapsed-ms elapsed :status "ok"} answer)))
        (when (and verify-expected (= checkpoint "initial"))
          (artifacts/verify-upstream! output id rows)))
      (artifacts/append! output {:phase "query-pass" :checkpoint checkpoint
                                :elapsed-ms pass-ms :status "ok"}))
    pass-ms))

(defn warmup! [engine executor {:keys [output engine-name] :as config}]
  (when (#{"datalevin" "datomic"} engine-name)
    (query-pass! engine executor config false "warmup")
    (artifacts/append! output {:phase "warmup" :status "ok"
                              :queries (count (:query-ids config))})))

(defn- await-views! [engine views tx timeout-ms]
  (let [deadline (+ (System/nanoTime) (* timeout-ms 1000000))]
    (mapv (fn [[id view]]
            (let [remaining (max 0 (quot (- deadline (System/nanoTime)) 1000000))]
              [id ((:await-view! engine) view tx remaining)])) views)))

(defn- save-views! [{:keys [output verify-expected]} checkpoint snapshots]
  (doseq [[id {:keys [rows tx-key]}] snapshots]
    (let [answer (artifacts/save-result! output checkpoint id rows)]
      (artifacts/append! output (merge {:phase "view" :checkpoint checkpoint :query id
                                        :applied-tx-id (:tx-id tx-key) :status "ok"} answer)))
    (when (and verify-expected (= checkpoint "initial"))
      (artifacts/verify-upstream! output id rows))))

(defn run! [engine {:keys [engine-name output data-dir workload timeout-ms query-ids]
                    :as config}]
  (let [executor (Executors/newSingleThreadExecutor)
        incremental? (= engine-name "triplox-incremental")
        views (atom [])]
    (try
      (bounded executor timeout-ms (:schema! engine))
      (when incremental?
        (let [start (System/nanoTime)]
          (doseq [id query-ids] (swap! views conj [id (bounded executor timeout-ms #((:view! engine) id))]))
          (artifacts/append! output {:phase "registration" :elapsed-ms (milliseconds start)
                                    :status "ok"})))
      (let [start (System/nanoTime)
            loaded (data/load-data! data-dir config
                                    #(bounded executor timeout-ms (fn [] ((:transact! engine) %))))
            ingestion-ms (milliseconds start)
            tx (:last-tx loaded)
            snapshots (when (and incremental? (pos? (:transactions loaded)))
                        (await-views! engine @views tx timeout-ms))
            initial-ms (when incremental? (milliseconds start))]
        (when-not (pos? (:transactions loaded))
          (throw (ex-info "Dataset contains no transactions" {})))
        (artifacts/append! output {:phase "ingestion" :elapsed-ms ingestion-ms :status "ok"
                                  :transactions (:transactions loaded)})
        (artifacts/write-json! (io/file output "load.json")
                               (dissoc loaded :samples :last-tx))
        (if incremental?
          (let [catchup-ms (- initial-ms ingestion-ms)]
            (artifacts/append! output {:phase "initial" :status "ok" :ingestion-ms ingestion-ms
                                      :catchup-ms catchup-ms :elapsed-ms initial-ms
                                      :final-tx-id (:tx-id tx)})
            (save-views! config "initial" snapshots))
          (do (warmup! engine executor config)
              (query-pass! engine executor config true "initial")))
        (when (= workload "maintenance")
          (let [trace (data/maintenance-trace loaded config)
                trace-file (io/file output "trace.edn")]
            (when (empty? trace)
              (throw (ex-info "Maintenance needs movie_companies or dated title rows" {})))
            (spit trace-file (pr-str trace))
            (artifacts/write-json! (io/file output "trace.json")
                                   {:sha256 (data/sha256 trace-file) :batches (count trace)})
            (doseq [[index {:keys [kind cycle source-rows tx-data]}] (map-indexed vector trace)]
              (let [checkpoint (str "batch-" index)
                    start (System/nanoTime)
                    tx (bounded executor timeout-ms #((:transact! engine) tx-data))
                    tx-ms (milliseconds start)]
                (if incremental?
                  (let [snapshots (await-views! engine @views tx timeout-ms)
                        elapsed (milliseconds start)]
                    (artifacts/append! output {:phase "refresh" :checkpoint checkpoint :status "ok"
                                              :kind kind :cycle cycle :source-rows source-rows
                                              :transaction-ms tx-ms :elapsed-ms elapsed})
                    (save-views! config checkpoint snapshots))
                  (let [query-ms (query-pass! engine executor config true checkpoint)]
                      (artifacts/append! output {:phase "refresh" :checkpoint checkpoint :status "ok"
                                                :kind kind :cycle cycle :source-rows source-rows
                                                :transaction-ms tx-ms :elapsed-ms (+ tx-ms query-ms)}))))))))
      (finally
        (doseq [[_ view] @views] (.close ^AutoCloseable view))
        (.shutdownNow executor)
        ((:close! engine))))))
