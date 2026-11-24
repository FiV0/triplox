(ns auctionmark.live-test
  (:require [auctionmark.live :as live]
            [clojure.test :refer [deftest is]]
            [xyz.triplox.api :as tc])
  (:import [java.util Random]))

(defn connect []
  (tc/connect (System/getProperty "triplox.host" "localhost")
              (Integer/parseInt (System/getProperty "triplox.port" "5490"))))

(defn- apply-next-delta! [{:keys [sub rows]}]
  (swap! rows live/apply-delta (tc/take! sub 5000)))

(defn- assert-primed
  "The first delta, also for an empty result, matches a standard query at the registration tx-key."
  [conn {:keys [sub rows query] :as s}]
  (apply-next-delta! s)
  (is (= (frequencies (tc/q (tc/db conn (tc/registration-tx-key sub)) query)) @rows)))

(defn- assert-current
  "The subscription's rows match a standard query at the tx-key it has reached."
  [conn {:keys [sub rows query]}]
  (is (= (frequencies (tc/q (tc/db conn (tc/tx-key sub)) query)) @rows)))

(defn- catch-up!
  "Apply deltas until the subscription reaches tx-id. One delta covers a whole WAL
  batch, so it can span several transactions, and it is empty when the batch leaves
  the result unchanged."
  [{:keys [sub] :as s} tx-id]
  (while (< (:tx-id (tc/tx-key sub)) tx-id)
    (apply-next-delta! s)))

(def auction-changes
  [[{:db/id [:item/id 0] :item/current-price 200.0 :item/num-bids 2}]
   [{:db/id [:item/id 0] :item/status :closed}]
   [{:db/id [:item/id 0] :item/status :open}]
   [{:db/id [:item-comment/id 0] :item-comment/response "Answered"}]
   [{:db/id [:item-comment/id 0] :item-comment/response ""}]
   [{:user-watch/id 900 :user-watch/user-id [:user/id 0] :user-watch/item-id [:item/id 0]}]
   [[:db/retractEntity [:user-watch/id 900]]]
   [{:user/id 9}]
   [{:user-watch/id 901 :user-watch/user-id [:user/id 9] :user-watch/item-id [:item/id 0]}]
   ;; Touches no queried attribute.
   [{:db/id [:user/id 0] :user/balance 17.0}]])

(deftest live-queries-track-auction-changes
  (with-open [conn (connect)]
    (live/load-fixture! conn {:users 2 :items 4 :seed 42})
    (let [;; Watchlist of user 9, who only appears in a later transaction.
          empty-watchlist (assoc (nth (live/queries 10 4 50) 46) :id 10)
          descriptors (conj (live/queries 2 4 10) empty-watchlist)
          subs (mapv #(assoc % :sub (tc/subscribe conn (:query %)) :rows (atom {})) descriptors)]
      (try
        (doseq [s subs]
          (assert-primed conn s))
        (doseq [tx auction-changes]
          (let [result (live/transact! conn tx)]
            (doseq [s subs]
              (catch-up! s (:tx-id result))
              (is (= (:tx-id result) (:tx-id (tc/tx-key (:sub s)))))
              (assert-current conn s))))
        (finally
          (doseq [{:keys [sub]} subs]
            (.close sub))))
      ;; A late subscriber starts from the current result.
      (with-open [sub (tc/subscribe conn (:query (first descriptors)))]
        (assert-primed conn {:sub sub :rows (atom {}) :query (:query (first descriptors))})))))

(deftest original-procedures-keep-subscriptions-current
  (with-open [conn (connect)]
    (let [state (live/load-fixture! conn {:users 2 :items 4 :seed 19})
          writes (atom 0)
          last-tx-id (atom 0)
          working-state (assoc state
                               :transact-fn (fn [c tx]
                                              (let [result (live/transact! c tx)]
                                                (reset! last-tx-id (:tx-id result))
                                                (swap! writes inc)
                                                result)))
          subs (mapv #(assoc % :sub (tc/subscribe conn (:query %)) :rows (atom {}))
                     (live/queries 2 4 10))
          rng (Random. 17)]
      (try
        (doseq [s subs]
          (assert-primed conn s))
        (dotimes [_ 30]
          (live/mutate! conn rng working-state)
          (doseq [s subs]
            (catch-up! s @last-tx-id)
            (assert-current conn s)))
        (finally
          (doseq [{:keys [sub]} subs]
            (.close sub))))
      (is (pos? @writes)))))

(deftest rejects-negative-client-weights
  (is (thrown? clojure.lang.ExceptionInfo (live/apply-delta {} [[[1] -1]]))))
