(ns xyz.triplox.integration.tx-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [xyz.triplox.api :as api]
            [xyz.triplox.integration.query-test :as query-test :refer [*conn*]]))

(def tx-schema
  [{:db/ident :tx/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :tx/follows
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}])

(defn with-tx-schema [f]
  (api/transact *conn* tx-schema)
  (f))

(use-fixtures :each query-test/with-conn with-tx-schema)

(deftest tx-commited
  (is (true? (:committed? (api/transact *conn* [{:tx/name "Ivan"}])))))

(deftest rejects-explicit-unallocated-id
  ;; db/id
  (let [{:keys [committed? error-message] :as _tx-res} (api/transact *conn* [{:db/id 11111 :tx/name "Ivan"}])]
    (is (false? committed?))
    (is (some? (re-find #"^unallocated entity id \d+$" error-message))))

  ;; entity-id
  (let [{:keys [committed? error-message] :as _tx-res} (api/transact *conn* [[:db/add 11111 :tx/name "Ivan"]])]
    (is (false? committed?))
    (is (some? (re-find #"^unallocated entity id \d+$" error-message))))

  ;; ref
  (let [{:keys [committed? error-message] :as _tx-res} (api/transact *conn* [{:tx/name "Bob" :tx/follows 11111}])]
    (is (false? committed?))
    (is (some? (re-find #"^unallocated entity id \d+$" error-message)))))
