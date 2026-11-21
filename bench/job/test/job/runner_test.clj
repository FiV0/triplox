(ns job.runner-test
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is]]
            [job.artifacts :as artifacts]
            [job.data :as data]
            [job.queries :as queries]
            [job.runner :as runner])
  (:import [java.nio.file Files]))

(deftest ingestion-and-catchup-have-no-artifact-io-between-them
  (let [calls (atom [])
        record! #(swap! calls conj %)
        engine {:db-fn (constantly nil)
                :query-fn (fn [& _] [])
                :schema! #(record! :schema)
                :view! (fn [_] (reify java.lang.AutoCloseable (close [_])))
                :await-view! (fn [_ tx _] (record! :await) {:rows [] :tx-key tx})
                :close! #(record! :close)}]
    (with-redefs [data/load-data! (fn [& _] (record! :load)
                                       {:transactions 1 :table-counts {:title 1} :last-tx {:tx-id 7}})
                  artifacts/append! (fn [& _] (record! :write))
                  artifacts/write-json! (fn [& _] (record! :write))
                  artifacts/save-result! (fn [& _] (record! :write) {})]
      (runner/run! engine {:engine-name :triplox-incremental :query-ids ["1a"]
                           :output "unused" :timeout-ms 1000 :catchup-timeout-ms 1000})
      (is (= :await (nth @calls (inc (.indexOf @calls :load))))))))

(defn with-loaded-run [engine-name standard-rows f]
  (let [directory (.toFile (Files/createTempDirectory "job-runner-test-"
                            (make-array java.nio.file.attribute.FileAttribute 0)))
        current (atom 0)
        queries (atom [])
        engine {:schema! (constantly nil)
                :transact! (fn [tx] {:tx-id (reset! current (:step (first tx)))})
                :db-fn #(deref current)
                :query-fn (fn [db id] (swap! queries conj [db id]) (standard-rows db id))
                :view! (fn [id]
                         (reify java.lang.AutoCloseable
                           (close [_])
                           Object
                           (toString [_] id)))
                :await-view! (fn [view tx _]
                               {:rows [[(str view) (:tx-id tx)]] :tx-key tx})
                :close! (constantly nil)}
        config {:engine-name engine-name :query-ids ["1a" "2a"]
                :output (str directory) :measurements (io/file directory "measurements.jsonl")
                :timeout-ms 1000 :catchup-timeout-ms 1000}]
    (try
      (with-redefs [data/load-data! (fn [_ _ transact! & _]
                                     (transact! [{:step 1}])
                                     (transact! [{:step 2}])
                                     {:transactions 3 :table-counts {:title 3} :last-tx (transact! [{:step 3}])})]
        (f (fn run [& [overrides]] (runner/run! engine (merge config overrides))) directory queries))
      (finally
        (doseq [file (reverse (file-seq directory))] (io/delete-file file))))))

(deftest benchmark-checks-and-saves-only-final-loaded-results
  (doseq [engine [:datalevin :triplox-standard :triplox-incremental]]
    (with-loaded-run engine (fn [db id] [[id db]])
      (fn [run! directory queries]
        (run!)
        (let [answers (io/file directory "answers")
              events (artifacts/read-events (io/file directory "measurements.jsonl"))]
          (is (= #{"1a.edn" "2a.edn"} (set (.list answers))))
          (doseq [id ["1a" "2a"]]
            (is (= [[id 3]]
                   (edn/read-string (slurp (io/file answers (str id ".edn")))))))
          (is (every? #(= 3 (first %)) @queries))
          (when (= engine :triplox-incremental)
            (is (= [[3 "1a"] [3 "2a"]] @queries))
            (is (= [{:phase "verification" :against "standard" :queries 2 :status "ok"}]
                   (filter #(= "verification" (:phase %)) events)))))))))

(deftest incremental-with-expected-answers-skips-standard-queries
  (with-redefs [queries/expected {"1a" [["1a" 3]] "2a" [["2a" 3]]}]
    (with-loaded-run :triplox-incremental (fn [db id] [[id db]])
      (fn [run! directory standard-queries]
        (run! {:verify-expected true})
        (is (empty? @standard-queries))
        (is (= [{:phase "verification" :against "expected" :queries 2 :status "ok"}]
               (filter #(= "verification" (:phase %))
                       (artifacts/read-events (io/file directory "measurements.jsonl")))))))))

(deftest incremental-mismatch-fails-with-diagnostic-answers
  (with-loaded-run :triplox-incremental (fn [_ _] [["wrong"]])
    (fn [run! directory _]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                           #"Incremental result differs from standard query" (run!)))
      (is (= {:standard [["wrong"]] :incremental [["1a" 3]]}
             (edn/read-string (slurp (io/file directory "mismatches" "1a.edn")))))
      (is (some #(and (= "verification" (:phase %)) (= "failed" (:status %)))
                (artifacts/read-events (io/file directory "measurements.jsonl")))))))
