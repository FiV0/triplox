(ns xyz.triplox.integration.basis-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [xyz.triplox.api :as tc]
            [xyz.triplox.integration.query-test :as query-test :refer [*conn*]]))

(use-fixtures :each query-test/with-conn query-test/with-people-schema)

(deftest test-standard-query-historical-basis
  (let [names-query '{:find [?name]
                      :where [[?e :name ?name]]}
        alice-basis (tc/transact *conn* [{:name "Alice"}])
        bob-basis (tc/transact *conn* [{:name "Bob"}])]
    (is (= [["Alice"]]
           (tc/q (tc/db *conn* alice-basis) names-query)))
    (is (= [["Bob"] ["Alice"]]
           (tc/q (tc/db *conn* bob-basis) names-query)))))
