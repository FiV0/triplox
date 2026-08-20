(ns xyz.triplox.integration.view-test
  (:require
   [clojure.test :as t :refer [deftest is]]
   [xyz.triplox.api :as api]
   [xyz.triplox.view :as view]
   [xyz.triplox.integration.query-test :as query-test :refer [*conn*]]))

(t/use-fixtures :each query-test/with-conn (query-test/with-schema query-test/people-schema))

(deftest view-reflects-two-transactions
  (with-open [mv (view/->view *conn* '{:find [?e ?name]
                                       :where [[?e :name ?name]]})]
    (is (:committed? (api/transact *conn* [{:name "Alice"}])))
    (Thread/sleep 500)
    (is (= [[8796093022208 "Alice"]] (view/get-view mv)))

    (is (:committed? (api/transact *conn* [{:name "Bob"}])))
    (Thread/sleep 500)
    (is (= [[8796093022208 "Alice"] [8796093022209 "Bob"]] (view/get-view mv)))))
