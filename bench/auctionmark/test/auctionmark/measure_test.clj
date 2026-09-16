(ns auctionmark.measure-test
  (:require [auctionmark.measure :as m]
            [clojure.test :refer [deftest is]]))

(def timing {:start-ns 1000000 :ack-ns 3000000 :scheduled-ns 0 :measured? true})
(def changes {0 {:rows {["new"] 1} :kind :item}})

(deftest delivery-before-ack-is-measured-at-application
  (let [t (-> (m/empty-tracker)
              (m/record-delivery 1 0 {["new"] 1} 2000000)
              (m/record-transaction 1 timing changes 5))]
    (is (= [{:ms 1.0 :kind :item}] (:samples t)))
    (is (empty? (:pending t)))
    (is (empty? (:early t)))
    (is (= 4 (:unchanged-query-transactions (m/summary t 1))))))

(deftest missing-wrong-and-duplicate-results-do-not-pass
  (let [t (m/record-transaction (m/empty-tracker) 1 timing changes 1)]
    (is (= 1 (:missing-deliveries (m/summary t 1))))
    (is (= 1 (:peak-pending-deliveries (m/summary t 1))))
    (is (= 1 (:expected-deliveries (m/summary t 1))))
    (is (= :incorrect-result (-> (m/record-delivery t 1 0 {} 2000000) :errors first :error)))
    (is (= :unexpected-delta (-> t
                                (m/record-delivery 1 0 {["new"] 1} 2000000)
                                (m/record-delivery 1 0 {["new"] 1} 2000000)
                                :errors first :error)))))

(deftest unchanged-and-warmup-transactions-have-no-latency-samples
  (let [t (-> (m/empty-tracker)
              (m/record-transaction 1 timing {} 5)
              (m/record-transaction 2 (assoc timing :measured? false) changes 5)
              (m/record-delivery 2 0 {["new"] 1} 2000000))]
    (is (empty? (:samples t)))
    (is (= 1 (count (:transactions t))))
    (is (nil? (get-in (m/summary t 1) [:latency-ms :p95])))))
