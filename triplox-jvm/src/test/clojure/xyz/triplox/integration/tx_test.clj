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
    :db/cardinality :db.cardinality/one}
   {:db/ident :tx/email
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/value}
   {:db/ident :tx/handle
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :tx/spouse
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}])

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

(deftest rejects-unknown-attribute
  (let [{:keys [committed? error-message] :as _tx-res} (api/transact *conn* [{:tx/nonexistent "Ivan"}])]
    (is (false? committed?))
    (is (some? (re-find #"^Unknown attribute" error-message)))))

(deftest rejects-type-mismatch
  ;; :tx/name is :db.type/string but a long is supplied
  (let [{:keys [committed? error-message] :as _tx-res} (api/transact *conn* [{:tx/name 42}])]
    (is (false? committed?))
    (is (some? (re-find #"^Type mismatch for attribute" error-message)))))

(deftest rejects-unknown-ident-entity
  (let [{:keys [committed? error-message] :as _tx-res} (api/transact *conn* [[:db/add :tx/does-not-exist :tx/name "Ivan"]])]
    (is (false? committed?))
    (is (some? (re-find #"^Unknown ident:" error-message)))))

(deftest rejects-unknown-ident-in-ref-value
  (let [{:keys [committed? error-message] :as _tx-res} (api/transact *conn* [{:tx/name "Bob" :tx/follows :tx/nope}])]
    (is (false? committed?))
    (is (some? (re-find #"^Unknown ident in ref value position" error-message)))))

(deftest rejects-cardinality-one-multiple-values
  ;; both ops share tempid "e", so they assert two values for a card-one attr on one entity
  (let [{:keys [committed? error-message] :as _tx-res}
        (api/transact *conn* [[:db/add "e" :tx/name "Ivan"]
                              [:db/add "e" :tx/name "Petr"]])]
    (is (false? committed?))
    (is (some? (re-find #"^Transaction cannot assert multiple values" error-message)))))

(deftest rejects-retract-of-unupserted-tempid
  (let [{:keys [committed? error-message] :as _tx-res} (api/transact *conn* [[:db/retract "e" :tx/name "Ivan"]])]
    (is (false? committed?))
    (is (some? (re-find #"referenced tempid that did not upsert" error-message)))))

(deftest rejects-lookup-ref-on-non-identity-attribute
  ;; :tx/email is :db.unique/value; lookup refs require :db.unique/identity
  (let [{:keys [committed? error-message] :as _tx-res}
        (api/transact *conn* [[:db/add [:tx/email "ivan@example.com"] :tx/name "Ivan"]])]
    (is (false? committed?))
    (is (some? (re-find #"must be :db.unique/identity" error-message)))))

(deftest rejects-lookup-ref-with-no-match
  ;; :tx/handle is :db.unique/identity but no entity owns this handle
  (let [{:keys [committed? error-message] :as _tx-res}
        (api/transact *conn* [[:db/add [:tx/handle "ghost"] :tx/name "Ivan"]])]
    (is (false? committed?))
    (is (some? (re-find #"^No entity found for lookup ref" error-message)))))

(deftest retract-entity-by-lookup-ref-releases-unique-value-in-same-tx
  (api/transact *conn* [{:tx/name "Alice"
                         :tx/handle "alice"
                         :tx/email "alice@example.com"}
                        {:tx/name "Bob"}])
  (let [bob-id (ffirst (query-test/q '{:find [?e]
                                      :where [[?e :tx/name "Bob"]]}))
        result (api/transact *conn*
                             [[:db/retractEntity [:tx/handle "alice"]]
                              [:db/add bob-id :tx/email "alice@example.com"]])]
    (is (true? (:committed? result)))
    (is (= #{[bob-id]}
           (query-test/q '{:find [?e]
                           :where [[?e :tx/email "alice@example.com"]]})))
    (is (= #{}
           (query-test/q '{:find [?e]
                           :where [[?e :tx/handle "alice"]]})))))

(deftest retract-entity-uses-normal-datom-semantics-with-other-operations
  (api/transact *conn* [{:tx/name "Alice"}])
  (let [entity-id (ffirst (query-test/q '{:find [?e]
                                         :where [[?e :tx/name "Alice"]]}))
        replace-result (api/transact *conn*
                                     [[:db/retractEntity entity-id]
                                      [:db/add entity-id :tx/name "Alicia"]])]
    (is (true? (:committed? replace-result)))
    (is (= #{["Alicia"]}
           (query-test/q '{:find [?name]
                           :where [[?e :tx/name ?name]]})))

    (let [{:keys [committed? error-message]}
          (api/transact *conn* [[:db/retractEntity entity-id]
                                [:db/add entity-id :tx/name "Alicia"]])]
      (is (false? committed?))
      (is (some? (re-find #"cannot both assert and retract" error-message)))
      (is (= #{["Alicia"]}
             (query-test/q '{:find [?name]
                             :where [[?e :tx/name ?name]]}))))))

(deftest rejects-unique-value-violation-within-tx
  ;; two distinct entities asserting the same :db.unique/value in one tx
  (let [{:keys [committed? error-message] :as _tx-res}
        (api/transact *conn* [{:tx/email "dup@example.com"}
                              {:tx/email "dup@example.com"}])]
    (is (false? committed?))
    (is (some? (re-find #"^Unique constraint violation" error-message)))))

(deftest rejects-unique-value-violation-against-stored
  (api/transact *conn* [{:tx/email "taken@example.com"}])
  (let [{:keys [committed? error-message] :as _tx-res}
        (api/transact *conn* [{:tx/email "taken@example.com"}])]
    (is (false? committed?))
    (is (some? (re-find #"already owns it" error-message)))))

(deftest rejects-conflicting-upserts
  ;; "t1" upserts to one entity via :tx/handle in step 0, then to a *different*
  ;; entity via the ref-identity :tx/spouse in step 1 (once "t2" is known),
  ;; so the two generations disagree on what "t1" resolves to.
  (api/transact *conn* [{:tx/handle "h1"}])                ; => E1
  (api/transact *conn* [{:tx/handle "t2h"}])               ; => T2 (the spouse target)
  (api/transact *conn* [{:tx/spouse [:tx/handle "t2h"]}])  ; => E2, identified by spouse=T2
  (let [{:keys [committed? error-message] :as _tx-res}
        (api/transact *conn* [{:db/id "t1" :tx/handle "h1" :tx/spouse "t2"}
                              {:db/id "t2" :tx/handle "t2h"}])]
    (is (false? committed?))
    (is (some? (re-find #"^Conflicting upserts" error-message)))))
