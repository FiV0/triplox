(ns stock-demo-test
  (:require [clojure.test :refer [deftest is run-tests testing]]
            [stock-demo :as demo]
            [xyz.triplox.api :as t]))

(defn eventually [pred]
  (let [deadline (+ (System/nanoTime) 10000000000)]
    (loop []
      (cond
        (pred) true
        (:error @demo/!state) (throw (ex-info "The demo failed." @demo/!state))
        (< (System/nanoTime) deadline) (do (Thread/sleep 20) (recur))
        :else false))))

(deftest sales-and-refills-update-the-subscription
  (let [host (or (System/getenv "TRIPLOX_HOST") "localhost")
        port (parse-long (or (System/getenv "TRIPLOX_PORT") "5490"))]
    (try
      (demo/start! {:host host :port port :automatic? false})
      (with-open [conn (t/connect host port)]
        (let [rows #(set (t/q (t/db conn) demo/low-stock-query))
              current #(set (keys (:weights @demo/!state)))
              coffee #(first (filter (fn [row] (= ["COF-01" "Berlin"]
                                                  [(second row) (nth row 3)]))
                                     (demo/read-inventory conn)))
              initial-sales (set (t/q (t/db conn)
                                      '{:find [?sale]
                                        :where [[?sale :sale/id _]]}))]
          (testing "The initial result arrives through the subscription."
            (is (eventually #(and (seq (current)) (= (rows) (current))))))
          (testing "A sale at the threshold does not add a low-stock row."
            (is (= {:kind :sale :quantity 2 :before 12 :after 10}
                   (select-keys (demo/step!) [:kind :quantity :before :after])))
            (is (= 10 (nth (coffee) 4)))
            (is (eventually #(= (rows) (current)))))
          (doseq [quantity [7 5]]
            (testing "A sale below the threshold updates the subscribed result."
              (demo/step!)
              (is (eventually #(and (= quantity (nth (coffee) 4))
                                    (contains? (current) (coffee))
                                    (= (rows) (current)))))))
          (testing "The refill removes the row and each sale has a stored record."
            (is (= :refill (:kind (demo/step!))))
            (is (eventually #(and (= 12 (nth (coffee) 4)) (= (rows) (current)))))
            (let [sales (t/q (t/db conn)
                             '{:find [?sale ?quantity]
                               :where [[?sale :sale/quantity ?quantity]]})
                  new-sales (remove #(contains? initial-sales [(first %)]) sales)]
              (is (= [2 2 3] (sort (map second new-sales))))))
          (testing "A row replacement arrives as a removal and an addition."
            (is (some #(= #{-1 1} (set (map second (:rows %))))
                      (:batches @demo/!state))))))
      (finally (demo/stop!)))
    (is (= :stopped (:status @demo/!state)))))

(deftest automatic-workload-can-pause-and-stop
  (try
    (demo/start! {:host (or (System/getenv "TRIPLOX_HOST") "localhost")
                  :port (parse-long (or (System/getenv "TRIPLOX_PORT") "5490"))
                  :tick-ms 80})
    (is (eventually #(>= (count (:events @demo/!state)) 4)))
    (demo/pause!)
    (let [before (:events @demo/!state)]
      (Thread/sleep 200)
      (is (= before (:events @demo/!state)))
      (demo/step!)
      (is (< (:tx-id (first before)) (:tx-id (first (:events @demo/!state))))))
    (demo/resume!)
    (let [before (:tx-id (first (:events @demo/!state)))]
      (is (eventually #(< before (:tx-id (first (:events @demo/!state)))))))
    (demo/stop!)
    (let [before (:events @demo/!state)]
      (Thread/sleep 200)
      (is (= :stopped (:status @demo/!state)))
      (is (= before (:events @demo/!state))))
    (finally (demo/stop!))))

(defn -main [& _]
  (let [{:keys [fail error]} (run-tests 'stock-demo-test)]
    (shutdown-agents)
    (System/exit (if (zero? (+ fail error)) 0 1))))
