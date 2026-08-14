(ns xyz.triplox.integration.view-test
  (:require
   [clojure.test :refer [deftest is]]
   [xyz.triplox.api :as api]
   [xyz.triplox.view :as view]))

(defn- connect []
  (let [host (System/getProperty "triplox.host" "localhost")
        port (Integer/parseInt (System/getProperty "triplox.port" "5490"))]
    (api/connect host port)))

(defn- await-row [materialized row]
  (let [deadline (+ (System/nanoTime) 5000000000)]
    (loop []
      (cond
        (some #{row} (view/get-view materialized)) true
        (< (System/nanoTime) deadline) (do (Thread/sleep 25) (recur))
        :else false))))

(deftest view-reflects-two-transactions
  (let [first-ident (keyword "view-test" (str (random-uuid)))
        second-ident (keyword "view-test" (str (random-uuid)))
        query '{:find [?ident]
                :where [[?entity :db/ident ?ident]]}]
    (with-open [conn (connect)
                materialized (view/->view conn query)]
      (is (:committed? (api/transact conn [{:db/ident first-ident}])))
      (is (await-row materialized [first-ident]))

      (is (:committed? (api/transact conn [{:db/ident second-ident}])))
      (is (await-row materialized [second-ident]))
      (is (some #{[first-ident]} (view/get-view materialized))))))
