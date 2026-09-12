(ns job.runner-test
  (:require [clojure.test :refer [deftest is]]
            [job.artifacts :as artifacts]
            [job.data :as data]
            [job.runner :as runner])
  (:import [java.util.concurrent Executors]))

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
        (#'runner/query-pass! engine executor config true "initial")
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
        engine {:schema! #(record! :schema)
                :view! (fn [_] (reify java.lang.AutoCloseable (close [_])))
                :await-view! (fn [_ tx _] (record! :await) {:rows [] :tx-key tx})
                :close! #(record! :close)}]
    (with-redefs [data/load-data! (fn [& _] (record! :load)
                                       {:transactions 1 :last-tx {:tx-id 7}})
                  artifacts/append! (fn [& _] (record! :write))
                  artifacts/write-json! (fn [& _] (record! :write))
                  artifacts/save-result! (fn [& _] (record! :write) {})]
      (runner/run! engine {:engine-name "triplox-incremental" :query-ids ["1a"]
                           :output "unused" :timeout-ms 1000})
      (is (= :await (nth @calls (inc (.indexOf @calls :load))))))))
