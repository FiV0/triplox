(ns auctionmark.main-test
  (:require [benchy.core :as bench]
            [clojure.test :refer [deftest is]]
            [xyz.triplox.api :as tc]
            [auctionmark.main :as main]))

(deftest benchmark-runs-test
  (with-open [conn (tc/connect (System/getProperty "triplox.host" "localhost")
                               (Integer/parseInt (System/getProperty "triplox.port" "5490")))]
    (let [result (bench/run-benchmark (main/benchmark {:scale-factor 0.0001 :threads 2 :duration 2 :seed 42})
                                      {:conn conn})]
      (is (= :completed (:status result)))
      (is (zero? (:error-count result)))
      (is (pos? (:transaction-count result)))
      (is (= #{:get-item :new-bid}
             (into #{} (comp (map :transaction) (filter #{:get-item :new-bid}))
                   (:transactions result)))))))
