(ns auctionmark.sync-test
  (:require [auctionmark.live :as live]
            [auctionmark.live-test :as fixture]
            [auctionmark.sync :as sync]
            [clojure.test :refer [deftest is]]
            [clojure.tools.cli :as cli])
  (:import [java.lang ProcessHandle]))

(def defaults (:options (cli/parse-opts [] sync/options)))

(deftest explicit-capacity-limits
  (is (= [5 20 50 100] (:stages (sync/validate-options defaults))))
  (doseq [changes [{:mode "capacity"} {:stages [101]} {:rate 0} {:rate Double/NaN}
                   {:duration -1} {:stages [0]} {:users 1} {:warmup Double/POSITIVE_INFINITY}]]
    (is (thrown? clojure.lang.ExceptionInfo (sync/validate-options (merge defaults changes)))))
  (is (= [200] (:stages (sync/validate-options (merge defaults {:mode "capacity" :stages [200] :users 40}))))))

(deftest measures-and-resets-two-stages
  (with-open [conn (fixture/connect)]
    (let [opts (merge defaults {:users 2 :items 4 :warmup 0 :duration 2 :sla-ms 5000})
          baseline (live/load-fixture! conn opts)]
      (doseq [n [5 10]]
        (let [result (sync/run-stage! conn baseline opts n {:client (.pid (ProcessHandle/current))})]
          (is (:passed? result) (pr-str result))
          (is (zero? (:missing-deliveries result)))
          (is (zero? (:unmatched-deliveries result)))
          (is (pos? (get-in result [:latency-ms :count])))
          (fixture/assert-oracle conn (:model baseline) (live/queries 2 4 10)))))))
