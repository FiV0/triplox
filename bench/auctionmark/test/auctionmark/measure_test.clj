(ns auctionmark.measure-test
  (:require [auctionmark.measure :as m]
            [clojure.test :refer [deftest is]]))

(def timing {:start-ns 1000000 :ack-ns 3000000 :scheduled-ns 0 :measured? true})
(def changes {0 :item})

(defn- registered
  "A tracker with one subscription registered at tx 0."
  []
  (m/register-subscription (m/empty-tracker) 0))

(deftest delivery-before-ack-is-measured-at-application
  (let [t (-> (registered)
              (m/record-delivery 1 0 2000000)
              (m/record-transaction 1 timing changes 5))]
    (is (= [{:ms 1.0 :kind :item}] (:samples t)))
    (is (empty? (m/pending-deliveries t)))
    (is (empty? (:early t)))
    (is (= 4 (:unchanged-query-transactions (m/summary t 1))))))

(deftest missing-deliveries-are-counted
  (let [t (m/record-transaction (registered) 1 timing changes 1)]
    (is (= 1 (:missing-deliveries (m/summary t 1))))
    (is (= 1 (:peak-pending-deliveries (m/summary t 1))))
    (is (= 1 (:expected-deliveries (m/summary t 1))))
    (is (zero? (:missing-deliveries (m/summary (m/record-delivery t 1 0 2000000) 1))))))

(deftest one-delta-can-cover-several-transactions
  (let [t (-> (registered)
              (m/record-transaction 1 timing changes 1)
              (m/record-transaction 2 timing {} 1)
              (m/record-transaction 3 timing changes 1)
              (m/record-delivery 3 0 2000000))]
    (is (empty? (m/pending-deliveries t)))
    (is (= 2 (count (:samples t))))))

(deftest a-delta-before-a-change-does-not-deliver-it
  (let [t (-> (registered)
              (m/record-transaction 1 timing {} 1)
              (m/record-transaction 2 timing changes 1)
              (m/record-delivery 1 0 2000000))]
    (is (= 1 (count (m/pending-deliveries t))))
    (is (empty? (:samples t)))))

(deftest unchanged-and-warmup-transactions-have-no-latency-samples
  (let [t (-> (registered)
              (m/record-transaction 1 timing {} 5)
              (m/record-transaction 2 (assoc timing :measured? false) changes 5)
              (m/record-delivery 2 0 2000000))]
    (is (empty? (:samples t)))
    (is (= 1 (count (:transactions t))))
    (is (nil? (get-in (m/summary t 1) [:latency-ms :p95])))))
