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

(defn- query-pass! [engine executor config measured?]
  (let [{:keys [query-ids output timeout-ms verify-expected]} config
        checkpoint (if measured? "final" "warmup")
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
        (let [answer (artifacts/save-result! output "final" id rows)]
          (artifacts/append! output (merge {:phase "query" :checkpoint checkpoint :query id
                                            :elapsed-ms elapsed :status "ok"} answer)))
        (when verify-expected
          (artifacts/verify-upstream! output id rows)))
      (artifacts/append! output {:phase "query-pass" :checkpoint checkpoint
                                :elapsed-ms pass-ms :status "ok"}))
    pass-ms))

(defn warmup! [engine executor {:keys [output engine-name] :as config}]
  (when (#{"datalevin" "datomic"} engine-name)
    (query-pass! engine executor config false)
    (artifacts/append! output {:phase "warmup" :status "ok"
                              :queries (count (:query-ids config))})))

(defn- await-quiet-views!
  "Waits until no view has changed for `quiet-ms`, then returns each view's rows and the
  time of the last change. Views do not report transaction progress, so a quiet period is
  the only sign that they have applied everything."
  [engine views quiet-ms timeout-ms]
  (let [start (System/nanoTime)
        deadline (+ start (* timeout-ms 1000000))
        quiet-ns (* quiet-ms 1000000)
        ;; A view without changes counts as quiet from before the wait.
        floor (- start quiet-ns)]
    (loop []
      (let [updated (reduce max floor (map (fn [[_ view]] ((:view-updated engine) view)) views))
            now (System/nanoTime)]
        (cond
          (>= (- now updated) quiet-ns)
          {:updated-ns updated
           :snapshots (mapv (fn [[id view]] [id {:rows ((:view-rows engine) view)}]) views)}
          (>= now deadline)
          (throw (ex-info "Incremental views did not go quiet"
                          {:quiet-ms quiet-ms :timeout-ms timeout-ms}))
          :else
          (do (Thread/sleep (max 1 (quot (min (- (+ updated quiet-ns) now) (- deadline now))
                                         1000000)))
              (recur)))))))

(defn- verify-views! [engine executor {:keys [output timeout-ms]} checkpoint snapshots]
  (let [db ((:db engine))]
    (try
      (doseq [[id {:keys [rows]}] snapshots]
        (let [standard (artifacts/normalized
                        (bounded executor timeout-ms #((:query engine) db id)))
              incremental (artifacts/normalized rows)]
          (when-not (= standard incremental)
            (let [file (io/file output "mismatches" checkpoint (str id ".edn"))]
              (io/make-parents file)
              (spit file (pr-str {:standard standard :incremental incremental})))
            (artifacts/append! output {:phase "verification" :checkpoint checkpoint
                                      :query id :status "failed"})
            (throw (ex-info "Incremental result differs from standard query"
                            {:query id :checkpoint checkpoint})))))
      (artifacts/append! output {:phase "verification" :checkpoint checkpoint
                                :queries (count snapshots) :status "ok"})
      (finally (when-let [close (:close-db! engine)] (close db))))))

(defn- save-views! [{:keys [output verify-expected]} checkpoint snapshots]
  (doseq [[id {:keys [rows]}] snapshots]
    (let [answer (artifacts/save-result! output checkpoint id rows)]
      (artifacts/append! output (merge {:phase "view" :checkpoint checkpoint :query id
                                        :status "ok"} answer)))
    (when verify-expected
      (artifacts/verify-upstream! output id rows))))

(defn- report-progress!
  "Prints ingestion progress to the runner log every 10 seconds until stopped.
  For Triplox, tx ids are file log offsets, so `behind` is the unindexed log size."
  [engine progress]
  (let [start (System/nanoTime)
        previous (atom [start 0])
        thread (Thread.
                (fn []
                  (try
                    (loop []
                      (Thread/sleep 10000)
                      (let [{:keys [table table-index tables rows transactions last-tx]} @progress
                            now (System/nanoTime)
                            [then rows-then] @previous
                            behind (when-let [indexed-tx (:indexed-tx engine)]
                                     (when last-tx
                                       (try (format " indexing %.1f MB behind"
                                                    (/ (- (:tx-id last-tx) (indexed-tx)) 1e6))
                                            (catch Exception _ " indexing lag unknown"))))]
                        (reset! previous [now (or rows 0)])
                        (println (format "progress %.0fs table %s (%s/%s) rows %d (%.0f/s) transactions %d%s"
                                         (/ (- now start) 1e9) (some-> table name) table-index tables
                                         (or rows 0) (/ (- (or rows 0) rows-then) (/ (- now then) 1e9))
                                         (or transactions 0) (or behind "")))
                        (flush))
                      (recur))
                    (catch InterruptedException _))))]
    (.setDaemon thread true)
    (.start thread)
    thread))

;; Engines that submit transactions asynchronously must wait for the last one to be
;; indexed before ingestion counts as finished.
(defn await-loaded! [engine loaded]
  (when-let [await-tx! (:await-tx! engine)]
    (when (pos? (:transactions loaded))
      (await-tx! (:last-tx loaded)))))

(defn load!
  "Loads the dataset with the engine's own bulk `:load!`, or else through `transact!`
  waiting until it is indexed, reporting progress to the runner log meanwhile."
  [engine {:keys [data-dir] :as config} transact!]
  (let [progress (atom {})
        reporter (report-progress! engine progress)]
    (try
      (if-let [bulk-load! (:load! engine)]
        (bulk-load! config progress)
        (let [loaded (data/load-data! data-dir config transact! progress)]
          (await-loaded! engine loaded)
          loaded))
      (finally (.interrupt ^Thread reporter)))))

(defn run! [engine {:keys [engine-name output data-dir timeout-ms catchup-timeout-ms quiet-ms
                           query-ids]
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
            loaded (load! engine config #(bounded executor timeout-ms (fn [] ((:transact! engine) %))))
            ingested (System/nanoTime)
            ingestion-ms (milliseconds start)
            tx (:last-tx loaded)
            {:keys [snapshots updated-ns]}
            (when (and incremental? (pos? (:transactions loaded)))
              (await-quiet-views! engine @views quiet-ms catchup-timeout-ms))
            ;; Catchup ends at the last view change, not when the quiet period confirms it.
            catchup-ms (/ (max 0 (- (or updated-ns ingested) ingested)) 1e6)]
        (when-not (pos? (reduce + (vals (:table-counts loaded))))
          (throw (ex-info "Dataset contains no rows" {})))
        (artifacts/append! output {:phase "ingestion" :elapsed-ms ingestion-ms :status "ok"
                                  :transactions (:transactions loaded)})
        (artifacts/write-json! (io/file output "load.json")
                               (dissoc loaded :last-tx))
        (if incremental?
          (do (artifacts/append! output {:phase "initial" :status "ok" :ingestion-ms ingestion-ms
                                        :catchup-ms catchup-ms :quiet-ms quiet-ms
                                        :elapsed-ms (+ ingestion-ms catchup-ms)
                                        :final-tx-id (:tx-id tx)})
              (verify-views! engine executor config "final" snapshots)
              (save-views! config "final" snapshots))
          (do (warmup! engine executor config)
              (query-pass! engine executor config true))))
      (finally
        (doseq [[_ view] @views] (.close ^AutoCloseable view))
        (.shutdownNow executor)
        ((:close! engine))))))
