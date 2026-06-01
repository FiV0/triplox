(ns xyz.triplox.integration.subscription-test
  (:require [clojure.test :as t :refer [deftest is]]
            [xyz.triplox.api :as api]))

(defn connect []
  (let [host (System/getProperty "triplox.host" "localhost")
        port (Integer/parseInt (System/getProperty "triplox.port" "5490"))]
    (api/connect host port)))

(def name-schema
  [{:db/ident :name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])

(def names-query '{:find [?name] :where [[?e :name ?name]]})

(deftest subscribe-returns-basis-and-times-out
  (with-open [conn (connect)]
    (api/transact conn name-schema)
    (with-open [sub (api/subscribe conn names-query)]
      (is (some? (:tx-id (api/basis sub))))
      ;; No transaction after the subscription -> bounded take! times out.
      (is (= ::api/timeout (api/take! sub 200))))))

(deftest subscribe-receives-delta
  (with-open [conn (connect)]
    (api/transact conn name-schema)
    (with-open [sub (api/subscribe conn names-query)]
      (api/transact conn [{:name "Ivan"}])
      (is (= [[["Ivan"] 1]] (api/take! sub 10000))))))
