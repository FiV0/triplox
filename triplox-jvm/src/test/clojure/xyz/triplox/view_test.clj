(ns xyz.triplox.view-test
  (:require [clojure.test :refer [deftest is]]
            [xyz.triplox.api :as api]
            [xyz.triplox.view :as view])
  (:import [java.io Closeable]
           [java.util.concurrent LinkedBlockingQueue TimeoutException]))

(defn with-subscription [f]
  (let [queue (LinkedBlockingQueue.)
        consumed (atom nil)
        sub (reify Closeable (close [_] (.offer queue ::closed)))]
    (with-redefs [api/subscribe (fn [& _] sub)
                  api/take! (fn [_]
                              (let [event (.take queue)]
                                (cond
                                  (= ::closed event) nil
                                  (instance? Throwable event) (throw event)
                                  :else (do (reset! consumed (:key event)) (:rows event)))))
                  api/tx-key (fn [_] @consumed)]
      (with-open [v (view/->view nil nil)]
        (f v #(.put queue %))))))

(deftest view-publishes-rows-and-progress-together
  (with-subscription
    (fn [v send!]
      (is (nil? (view/tx-key v)))
      (is (thrown? TimeoutException (view/await-tx v {:tx-id 1} 0)))
      (send! {:key {:tx-id 1} :rows []})
      (is (= {:rows [] :tx-key {:tx-id 1}} (view/await-tx v {:tx-id 1} 1000)))
      (send! {:key {:tx-id 2} :rows [[["Alice"] 2]]})
      (send! {:key {:tx-id 3} :rows [[["Alice"] -1] [["Bob"] 1]]})
      (let [result (view/await-tx v {:tx-id 3} 1000)]
        (is (= #{["Alice"] ["Bob"]} (set (:rows result))))
        (is (= {:tx-id 3} (:tx-key result))))
      (send! {:key {:tx-id 4} :rows [[["Alice"] -1] [["Bob"] -1]]})
      (is (= {:rows [] :tx-key {:tx-id 4}} (view/await-tx v {:tx-id 4} 1000))))))

(deftest closure-and-errors-wake-waiters
  (with-subscription
    (fn [v send!]
      (send! ::closed)
      (is (thrown? IllegalStateException (view/await-tx v {:tx-id 1} 1000)))))
  (with-subscription
    (fn [v send!]
      (let [error (ex-info "subscription failed" {})]
        (send! error)
        (is (identical? error (try (view/await-tx v {:tx-id 1} 1000)
                                   (catch Throwable actual actual))))
        (is (identical? error (try (view/get-view v) (catch Throwable actual actual))))))))
