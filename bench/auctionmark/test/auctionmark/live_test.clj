(ns auctionmark.live-test
  (:require [auctionmark.live :as live]
            [auctionmark.procedures :as proc]
            [clojure.test :refer [deftest is]]
            [xyz.triplox.api :as tc])
  (:import [java.util Random]))

(defn connect []
  (tc/connect (System/getProperty "triplox.host" "localhost")
              (Integer/parseInt (System/getProperty "triplox.port" "5490"))
              {:subscription-thread-factory (.factory (Thread/ofVirtual))}))

(defn assert-oracle [conn model descriptors]
  (let [db (tc/db conn)]
    (doseq [d descriptors]
      (is (= (live/result-rows model d) (frequencies (tc/q db (:query d)))) (str (:kind d) " " (:param d))))))

(deftest live-queries-track-auction-changes
  (with-open [conn (connect)]
    (let [{baseline :model} (live/load-fixture! conn {:users 2 :items 4 :seed 42})
          empty-watch (assoc (nth (live/queries 10 4 50) 46) :id 10)
          descriptors (conj (live/queries 2 4 10) empty-watch)
          model (atom baseline)
          subs (mapv (fn [d] (assoc d :sub (tc/subscribe conn (:query d)) :rows (atom {}))) descriptors)]
      (try
        (doseq [{:keys [sub rows] :as d} subs]
          (let [expected (live/result-rows @model d)]
            (if (seq expected)
              (swap! rows live/apply-delta (tc/take! sub 5000))
              (is (= ::tc/timeout (tc/take! sub 20))))
            (is (= expected @rows))))
        (doseq [tx [[{:db/id [:item/id 0] :item/current-price 200.0 :item/num-bids 2}]
                    [{:db/id [:item/id 0] :item/status :closed}]
                    [{:db/id [:item/id 0] :item/status :open}]
                    [{:db/id [:item-comment/id 0] :item-comment/response "Answered"}]
                    [{:db/id [:item-comment/id 0] :item-comment/response ""}]
                    [{:user-watch/id 900 :user-watch/user-id [:user/id 0] :user-watch/item-id [:item/id 0]}]
                    [[:db/retractEntity [:user-watch/id 900]]]
                    [{:user/id 9}]
                    [{:user-watch/id 901 :user-watch/user-id [:user/id 9] :user-watch/item-id [:item/id 0]}]
                    [{:db/id [:user/id 0] :user/balance 17.0}]]]
          (let [before @model
                after (live/apply-tx before tx)
                result (live/transact! conn tx)]
            (doseq [{:keys [sub rows] :as d} subs]
              (let [expected (live/result-rows after d)]
                (if (= expected (live/result-rows before d))
                  (is (= ::tc/timeout (tc/take! sub 20)))
                  (do
                    (swap! rows live/apply-delta (tc/take! sub 5000))
                    (is (= (:tx-id result) (:tx-id (tc/tx-key sub))))))
                (is (= expected @rows))))
            (reset! model after)
            (assert-oracle conn after descriptors)))
        (finally (doseq [{:keys [sub]} subs] (.close sub))))
      (with-open [sub (tc/subscribe conn (:query (first descriptors)))]
        (is (= (live/result-rows @model (first descriptors))
               (live/apply-delta {} (tc/take! sub 5000)))))
      (live/restore! conn @model baseline)
      (assert-oracle conn baseline descriptors))))

(deftest original-procedures-and-model-agree
  (with-open [conn (connect)]
    (let [{baseline :model state :state} (live/load-fixture! conn {:users 2 :items 4 :seed 19})
          model (atom baseline)
          working-state (live/copy-state state)
          writes (atom 0)
          descriptors (live/queries 2 4 10)
          rng (Random. 17)]
      (binding [proc/*transact* (fn [c tx]
                                (let [result (live/transact! c tx)]
                                  (swap! model live/apply-tx tx)
                                  (swap! writes inc)
                                  result))]
        (dotimes [_ 30]
          (live/mutate! conn rng working-state @model)
          (assert-oracle conn @model descriptors)))
      (is (pos? @writes))
      (live/restore! conn @model baseline)
      (assert-oracle conn baseline descriptors))))

(deftest rejects-negative-client-weights
  (is (thrown? clojure.lang.ExceptionInfo (live/apply-delta {} [[[1] -1]]))))
