(ns job.runner-test
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is]]
            [job.artifacts :as artifacts]
            [job.data :as data]
            [job.runner :as runner])
  (:import [java.nio.file Files]
           [java.util.concurrent Executors]))

(deftest warmup-completes-before-measurement
  (let [calls (atom [])
        executor (Executors/newSingleThreadExecutor)
        database (Object.)
        engine {:db (constantly database)
                :query (fn [db id] (swap! calls conj [:query id db (Thread/currentThread)]) [[id]])}
        config {:query-ids ["1a" "2a"] :output "unused" :engine-name "datomic" :timeout-ms 1000}]
    (try
      (with-redefs [artifacts/append! (fn [_ event] (swap! calls conj [:event event]))
                    artifacts/save-result! (fn [& _] {})]
        (runner/warmup! engine executor config)
        (#'runner/query-pass! engine executor config true)
        (let [queries (filter #(= :query (first %)) @calls)
              warmup-index (.indexOf @calls [:event {:phase "warmup" :status "ok" :queries 2}])]
          (is (= ["1a" "2a" "1a" "2a"] (mapv second queries)))
          (is (= 1 (count (set (map #(nth % 3) queries)))))
          (is (every? #(identical? database (nth % 2)) queries))
          (is (= 2 warmup-index))
          (is (not-any? #(contains? (second %) :elapsed-ms)
                        (filter #(= :event (first %)) (take 3 @calls))))))
      (finally (.shutdownNow executor)))))

(deftest warmup-failure-prevents-success-event
  (let [executor (Executors/newSingleThreadExecutor)
        events (atom [])]
    (try
      (with-redefs [artifacts/append! (fn [_ event] (swap! events conj event))]
        (is (thrown? Exception
                     (runner/warmup! {:db (constantly nil)
                                      :query (fn [& _] (throw (ex-info "failed" {})))}
                                     executor {:engine-name "datalevin" :query-ids ["1a"]
                                               :timeout-ms 1000})))
        (is (not-any? #(= "ok" (:status %)) @events)))
      (finally (.shutdownNow executor)))))

(deftest percentile-nearest-rank
  (is (= 2 (artifacts/percentile [1 2 3 4] 0.5)))
  (is (= 4 (artifacts/percentile [1 2 3 4] 0.95)))
  (is (nil? (artifacts/percentile [] 0.95))))

(deftest ingestion-and-catchup-have-no-artifact-io-between-them
  (let [calls (atom [])
        record! #(swap! calls conj %)
        engine {:db (constantly nil)
                :query (fn [& _] [])
                :schema! #(record! :schema)
                :view! (fn [_] (reify java.lang.AutoCloseable (close [_])))
                :await-view! (fn [_ tx _] (record! :await) {:rows [] :tx-key tx})
                :close! #(record! :close)}]
    (with-redefs [data/load-data! (fn [& _] (record! :load)
                                       {:transactions 1 :table-counts {:title 1} :last-tx {:tx-id 7}})
                  artifacts/append! (fn [& _] (record! :write))
                  artifacts/write-json! (fn [& _] (record! :write))
                  artifacts/save-result! (fn [& _] (record! :write) {})]
      (runner/run! engine {:engine-name "triplox-incremental" :query-ids ["1a"]
                           :output "unused" :timeout-ms 1000})
      (is (= :await (nth @calls (inc (.indexOf @calls :load))))))))

(defn with-loaded-run [engine-name standard-rows f]
  (let [directory (.toFile (Files/createTempDirectory "job-runner-test-"
                            (make-array java.nio.file.attribute.FileAttribute 0)))
        current (atom 0)
        queries (atom [])
        engine {:schema! (constantly nil)
                :transact! (fn [tx] {:tx-id (reset! current (:step (first tx)))})
                :db #(deref current)
                :query (fn [db id] (swap! queries conj [db id]) (standard-rows db id))
                :view! (fn [id]
                         (reify java.lang.AutoCloseable
                           (close [_])
                           Object
                           (toString [_] id)))
                :await-view! (fn [view tx _]
                               {:rows [[(str view) (:tx-id tx)]] :tx-key tx})
                :close! (constantly nil)}
        config {:engine-name engine-name :query-ids ["1a" "2a"]
                :output (str directory) :timeout-ms 1000}]
    (try
      (with-redefs [data/load-data! (fn [_ _ transact! & _]
                                     (transact! [{:step 1}])
                                     (transact! [{:step 2}])
                                     {:transactions 3 :table-counts {:title 3} :last-tx (transact! [{:step 3}])})]
        (f #(runner/run! engine config) directory queries))
      (finally
        (doseq [file (reverse (file-seq directory))] (io/delete-file file))))))

(deftest benchmark-checks-and-saves-only-final-loaded-results
  (doseq [engine ["datalevin" "datomic" "triplox-standard" "triplox-incremental"]]
    (with-loaded-run engine (fn [db id] [[id db]])
      (fn [run! directory queries]
        (run!)
        (let [answers (io/file directory "answers")
              events (artifacts/read-events directory)]
          (is (= ["final"] (vec (.list answers))))
          (doseq [id ["1a" "2a"]]
            (is (= [[id 3]]
                   (edn/read-string (slurp (io/file answers "final" (str id ".edn")))))))
          (is (every? #(= 3 (first %)) @queries))
          (is (not-any? #(= "refresh" (:phase %)) events))
          (when (= engine "triplox-incremental")
            (is (= [[3 "1a"] [3 "2a"]] @queries))
            (is (= ["final"]
                   (mapv :checkpoint (filter #(= "verification" (:phase %)) events))))))))))

(deftest incremental-mismatch-fails-with-diagnostic-answers
  (with-loaded-run "triplox-incremental" (fn [_ _] [["wrong"]])
    (fn [run! directory _]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                           #"Incremental result differs from standard query" (run!)))
      (is (= {:standard [["wrong"]] :incremental [["1a" 3]]}
             (edn/read-string (slurp (io/file directory "mismatches" "final" "1a.edn")))))
      (is (some #(and (= "verification" (:phase %)) (= "failed" (:status %))
                      (= "final" (:checkpoint %)))
                (artifacts/read-events directory))))))
