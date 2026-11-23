(ns auctionmark.measure-test
  (:require [auctionmark.measure :as m]
            [clojure.test :refer [deftest is]]))

(def timing {:start-ns 1000000 :ack-ns 3000000 :scheduled-ns 0 :measured? true})
(def changes {0 {:rows {["new"] 1} :kind :item}})

(defn- registered
  "A tracker with subscription 0 primed to an empty result at tx 0."
  []
  (m/register-subscription (m/empty-tracker) 0 0 {}))

(defn- error-kinds [t]
  (map :error (:errors t)))

(deftest delivery-before-ack-is-measured-at-application
  (let [t (-> (registered)
              (m/record-delivery 1 0 {["new"] 1} 2000000)
              (m/record-transaction 1 timing changes 5))]
    (is (= [{:ms 1.0 :kind :item}] (:samples t)))
    (is (empty? (m/pending-deliveries t)))
    (is (empty? (:early t)))
    (is (empty? (:errors t)))
    (is (= 4 (:unchanged-query-transactions (m/summary t 1))))))

(deftest missing-and-wrong-results-do-not-pass
  (let [t (m/record-transaction (registered) 1 timing changes 1)]
    (is (= 1 (:missing-deliveries (m/summary t 1))))
    (is (= 1 (:peak-pending-deliveries (m/summary t 1))))
    (is (= 1 (:expected-deliveries (m/summary t 1))))
    (is (= [:incorrect-result] (error-kinds (m/record-delivery t 1 0 {} 2000000))))
    (is (= [:incorrect-result] (-> t
                                   (m/record-delivery 1 0 {["new"] 1} 2000000)
                                   (m/record-delivery 1 0 {["new"] 2} 2000000)
                                   error-kinds)))))

(deftest one-delta-can-cover-several-transactions
  (let [t (-> (registered)
              (m/record-transaction 1 timing changes 1)
              (m/record-transaction 2 timing {} 1)
              (m/record-transaction 3 timing {0 {:rows {["newer"] 1} :kind :item}} 1)
              (m/record-delivery 3 0 {["newer"] 1} 2000000))]
    (is (empty? (:errors t)))
    (is (empty? (m/pending-deliveries t)))
    (is (= 2 (count (:samples t))))))

(deftest changes-can-cancel-within-a-batch
  (let [t (-> (registered)
              (m/record-transaction 1 timing changes 1)
              (m/record-transaction 2 timing {0 {:rows {} :kind :item}} 1))]
    (is (empty? (:errors (m/record-delivery t 2 0 {} 2000000))))
    (is (empty? (m/pending-deliveries (m/record-delivery t 2 0 {} 2000000))))
    (is (= [:incorrect-result] (error-kinds (m/record-delivery t 2 0 {["new"] 1} 2000000))))))

(deftest empty-deltas-are-checked-against-the-unchanged-result
  (let [t (-> (registered)
              (m/record-transaction 1 timing changes 1)
              (m/record-delivery 1 0 {["new"] 1} 2000000)
              (m/record-transaction 2 timing {} 1))]
    (is (empty? (:errors (m/record-delivery t 2 0 {["new"] 1} 2000000))))
    (is (= [:incorrect-result] (error-kinds (m/record-delivery t 2 0 {} 2000000))))
    (is (= [:tx-key-regressed] (-> t
                                   (m/record-delivery 2 0 {["new"] 1} 2000000)
                                   (m/record-delivery 1 0 {["new"] 1} 2000000)
                                   error-kinds)))))

(deftest unchanged-and-warmup-transactions-have-no-latency-samples
  (let [t (-> (registered)
              (m/record-transaction 1 timing {} 5)
              (m/record-transaction 2 (assoc timing :measured? false) changes 5)
              (m/record-delivery 2 0 {["new"] 1} 2000000))]
    (is (empty? (:samples t)))
    (is (= 1 (count (:transactions t))))
    (is (nil? (get-in (m/summary t 1) [:latency-ms :p95])))))
