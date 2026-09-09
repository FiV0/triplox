(ns xyz.triplox.integration.view-test
  (:require
   [clojure.core.async :as async]
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

(deftest terminal-error-stops-view-worker-and-allows-close
  (api/transact *conn* [{:age 10}])
  (let [mv (view/->view *conn* '{:find [(sum ?value)]
                                :where [(or [?e :age ?value]
                                            [?e :name ?value])]})]
    (api/transact *conn* [{:name "Alice"}])
    (let [[_ channel] (async/alts!! [(:done-chan mv) (async/timeout 5000)])]
      (is (= (:done-chan mv) channel) "The worker finishes after a terminal error"))
    (let [closed (future (.close mv) :closed)]
      (try
        (is (= :closed (deref closed 5000 :timeout)) "Closing the failed view returns")
        (finally
          (future-cancel closed))))))
