(ns xyz.triplox.integration.view-test
  (:require [clojure.test :as t :refer [deftest is]]
            [xyz.triplox.api :as api]
            [xyz.triplox.view :as view]
            [xyz.triplox.integration.query-test :as query-test :refer [*conn*]]))

(t/use-fixtures :each query-test/with-conn (query-test/with-schema query-test/people-schema))

(defn tx-key [tx]
  (is (:committed? tx))
  (select-keys tx [:tx-id :system-time]))

(deftest view-reflects-two-transactions
  (with-open [mv (view/->view *conn* '[:find ?name :where [?e :name ?name]])]
    (let [key (tx-key (api/transact *conn* [{:name "Alice"}]))]
      (is (= {:rows [["Alice"]] :tx-key key} (view/await-tx mv key 5000))))
    (let [key (tx-key (api/transact *conn* [{:name "Bob"}]))
          snapshot (view/await-tx mv key 5000)]
      (is (= #{["Alice"] ["Bob"]} (set (:rows snapshot))))
      (is (= key (:tx-key snapshot) (view/tx-key mv)))
      (is (= (set (:rows snapshot)) (set (view/get-view mv)))))))

(deftest empty-and-unchanged-results-still-catch-up
  (let [query '[:find (min ?age) :where [?e :age ?age]]]
    (with-open [mv (view/->view *conn* query)]
      (let [key (tx-key (api/transact *conn* [{:name "No age"}]))]
        (is (= {:rows [] :tx-key key} (view/await-tx mv key 5000)))
        (let [db (api/db *conn*)] (is (= [] (api/q db query)))))
      (let [key (tx-key (api/transact *conn* [{:name "Young" :age 10}]))]
        (is (= {:rows [[10]] :tx-key key} (view/await-tx mv key 5000))))
      (let [key (tx-key (api/transact *conn* [{:name "Older" :age 20}]))]
        (is (= {:rows [[10]] :tx-key key} (view/await-tx mv key 5000))))
      (let [entity (let [db (api/db *conn*)]
                     (ffirst (api/q db '[:find ?e :where [?e :name "Young"]])))
            key (tx-key (api/transact *conn* [[:db/retract entity :age 10]]))]
        (is (= {:rows [[20]] :tx-key key} (view/await-tx mv key 5000)))))))
