(ns xyz.triplox.integration.subscription-test
  (:require [clojure.test :as t :refer [deftest is testing]]
            [xyz.triplox.api :as api]))

(defn connect []
  (let [host (System/getProperty "triplox.host" "localhost")
        port (Integer/parseInt (System/getProperty "triplox.port" "5490"))]
    (api/connect host port)))

(def name-schema
  [{:db/ident :name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])

(def people-schema
  [{:db/ident :name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :last-name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :city :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])

(def residence-schema
  [{:db/ident :person/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :person/residence
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}])

(def edge-schema
  [{:db/ident :g/to :db/valueType :db.type/ref :db/cardinality :db.cardinality/many}])

(def triangle-relation-schema
  [{:db/ident :node/label :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :r/to :db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   {:db/ident :s/to :db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   {:db/ident :t/to :db/valueType :db.type/ref :db/cardinality :db.cardinality/many}])

(def names-query '{:find [?name] :where [[?e :name ?name]]})

(def delta-timeout-ms 1000)

(defn take-delta! [sub]
  (api/take! sub delta-timeout-ms))

(defn delta-set! [sub]
  (let [res (take-delta! sub)]
    (if (sequential? res)
      (set res)
      res)))

(defn q [conn query]
  (api/q (api/db conn) query))

(defn single-value [conn query]
  (let [rows (q conn query)]
    (is (= 1 (count rows)))
    (ffirst rows)))

(def graph-a "graph/a")
(def graph-b "graph/b")
(def graph-c "graph/c")
(def graph-d "graph/d")
(def graph-e "graph/e")

(def graph-nodes (mapv #(hash-map :db/id % :node/label %) [graph-a graph-b graph-c graph-d graph-e]))

(def first-user-entity-id 8796093022208)
(def user-entity-ids (range first-user-entity-id (+ first-user-entity-id 100)))


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
      (is (= [[["Ivan"] 1]] (take-delta! sub))))))

(deftest with-previous-value
  (with-open [conn (connect)]
    (api/transact conn people-schema)
    (api/transact conn [{:name "Ivan"}])
    (let [ivan-id (single-value conn '{:find [?e]
                                       :where [[?e :name "Ivan"]]})]
      (with-open [sub (api/subscribe conn {:find ['?name]
                                            :where [[ivan-id :name '?name]]})]
        (api/transact conn [[:db/add ivan-id :name "Ivanov"]])
        (is (= #{[["Ivan"] -1]
                 [["Ivanov"] 1]}
               (delta-set! sub)))))))

(deftest test-basic-query-1
  (with-open [conn (connect)]
    (api/transact conn people-schema)
    (with-open [sub (api/subscribe conn '{:find [?name]
                                           :where [[?e :name "Ivan"]
                                                   [?e :name ?name]]})]
      (api/transact conn [{:name "Ivan" :last-name "Ivanov"}
                          {:name "Petr" :last-name "Petrov"}])
      (is (= [[["Ivan"] 1]] (take-delta! sub))))))

(deftest test-basic-query-2
  (testing "Can query entity by single field"
    (with-open [conn (connect)]
      (api/transact conn people-schema)
      (with-open [sub (api/subscribe conn '{:find [?e]
                                             :where [[?e :name "Ivan"]]})]
        (api/transact conn [{:name "Ivan" :last-name "Ivanov"}
                            {:name "Petr" :last-name "Petrov"}])
        (let [[[row weight]] (take-delta! sub)]
          (is (= 1 weight))
          (is (integer? (first row))))))))

(deftest test-basic-query-3
  (testing "Can query using multiple terms"
    (with-open [conn (connect)]
      (api/transact conn people-schema)
      (with-open [sub (api/subscribe conn '{:find [?name ?last-name]
                                             :where [[?e :name ?name]
                                                     [?e :last-name ?last-name]
                                                     [?e :name "Ivan"]
                                                     [?e :last-name "Ivanov"]]})]
        (api/transact conn [{:name "Ivan" :last-name "Ivanov"}
                            {:name "Petr" :last-name "Petrov"}])
        (is (= [[["Ivan" "Ivanov"] 1]]
               (take-delta! sub)))))))

(deftest test-basic-query-4
  (testing "Negate query based on subsequent non-matching clause"
    (with-open [conn (connect)]
      (api/transact conn people-schema)
      (with-open [sub (api/subscribe conn '{:find [?e]
                                             :where [[?e :name "Ivan"]
                                                     [?e :last-name "Ivanov-does-not-match"]]})]
        (api/transact conn [{:name "Ivan" :last-name "Ivanov"}
                            {:name "Petr" :last-name "Petrov"}])
        (is (= ::api/timeout (api/take! sub 300)))))))

(deftest test-basic-query-5
  (testing "Can query for multiple results"
    (with-open [conn (connect)]
      (api/transact conn people-schema)
      (with-open [sub (api/subscribe conn '{:find [?name]
                                             :where [[?e :name ?name]]})]
        (api/transact conn [{:name "Ivan"}
                            {:name "Petr"}])
        (is (= #{[["Ivan"] 1]
                 [["Petr"] 1]}
               (delta-set! sub)))))))

(deftest test-basic-query-6
  (testing "Can query across fields for same value"
    (with-open [conn (connect)]
      (api/transact conn people-schema)
      (with-open [sub (api/subscribe conn '{:find [?p1]
                                             :where [[?p1 :name ?name]
                                                     [?p1 :last-name ?name]]})]
        (api/transact conn [{:name "Ivan" :last-name "Ivanov"}
                            {:name "Petr" :last-name "Petrov"}
                            {:name "Smith" :last-name "Smith"}])
        (let [[[row weight]] (take-delta! sub)]
          (is (= 1 weight))
          (is (integer? (first row))))))))

(deftest test-basic-query-7
  (testing "Can query across fields for same value when value is passed in"
    (with-open [conn (connect)]
      (api/transact conn people-schema)
      (with-open [sub (api/subscribe conn '{:find [?p1]
                                             :where [[?p1 :name ?name]
                                                     [?p1 :last-name ?name]
                                                     [?p1 :name "Smith"]]})]
        (api/transact conn [{:name "Ivan" :last-name "Ivanov"}
                            {:name "Petr" :last-name "Petrov"}
                            {:name "Smith" :last-name "Smith"}])
        (let [[[row weight]] (take-delta! sub)]
          (is (= 1 weight))
          (is (integer? (first row))))))))

(deftest test-basic-retractions-1
  (with-open [conn (connect)]
    (api/transact conn people-schema)
    (api/transact conn [{:name "Ivan" :last-name "Ivanov"}
                        {:name "Petr" :last-name "Petrov"}])
    (let [ivan-id (single-value conn '{:find [?e]
                                       :where [[?e :name "Ivan"]]})]
      (with-open [sub (api/subscribe conn '{:find [?name]
                                             :where [[?e :name ?name]]})]
        (api/transact conn [[:db/add ivan-id :name "Ivanova"]])
        (is (= #{[["Ivan"] -1]
                 [["Ivanova"] 1]}
               (delta-set! sub)))))))

(deftest test-basic-retractions-2
  (with-open [conn (connect)]
    (api/transact conn people-schema)
    (api/transact conn [{:name "Ivan" :last-name "Ivanov"}
                        {:name "Petr" :last-name "Petrov"}])
    (let [ivan-id (single-value conn '{:find [?e]
                                       :where [[?e :name "Ivan"]]})]
      (with-open [sub (api/subscribe conn '{:find [?name]
                                             :where [[?e :name ?name]]})]
        (api/transact conn [[:db/retract ivan-id :name "Ivan"]])
        (is (= [[["Ivan"] -1]]
               (take-delta! sub)))))))

(deftest test-dbsp-distinct-semantics-retractions
  (with-open [conn (connect)]
    (api/transact conn people-schema)
    (api/transact conn [{:name "Alice" :city "NYC"}
                        {:name "Bob" :city "NYC"}
                        {:name "Carol" :city "LA"}])
    (let [alice-id (single-value conn '{:find [?e]
                                        :where [[?e :name "Alice"]]})
          bob-id (single-value conn '{:find [?e]
                                      :where [[?e :name "Bob"]]})]
      (with-open [sub (api/subscribe conn '{:find [?city]
                                            :where [[?e :city ?city]]})]
        (api/transact conn [[:db/retract bob-id :city "NYC"]])
        (is (= [[["NYC"] -1]] (take-delta! sub)))
        (api/transact conn [[:db/retract alice-id :city "NYC"]])
        (is (= [[["NYC"] -1]] (take-delta! sub)))))))

(deftest test-prefix-stable-extension-addition
  (with-open [conn (connect)]
    (api/transact conn triangle-relation-schema)
    (let [[a-id b-id _c-id d-id] user-entity-ids]
      (api/transact conn (into graph-nodes
                               [[:db/add graph-a :r/to graph-b]
                                [:db/add graph-b :s/to graph-c]]))

      (with-open [sub (api/subscribe conn '{:find [?a ?b ?c]
                                            :where [[?a :r/to ?b]
                                                    [?b :s/to ?c]]})]
        (t/is (true? (:committed? (api/transact conn [[:db/add b-id :s/to d-id]]))))
        (is (= #{[[a-id b-id d-id] 1]}
               (delta-set! sub)))))))

(deftest test-prefix-stable-extension-retraction
  (with-open [conn (connect)]
    (api/transact conn triangle-relation-schema)
    (let [[a-id b-id _c-id d-id] user-entity-ids]
      (api/transact conn (into graph-nodes
                               [[:db/add graph-a :r/to graph-b]
                                [:db/add graph-b :s/to graph-c]
                                [:db/add graph-b :s/to graph-d]]))
      (with-open [sub (api/subscribe conn '{:find [?a ?b ?c]
                                            :where [[?a :r/to ?b]
                                                    [?b :s/to ?c]]})]
        (api/transact conn [[:db/retract b-id :s/to d-id]])
        (is (= #{[[a-id b-id d-id] -1]}
               (delta-set! sub)))))))

(deftest test-triangle-edge-deletion
  (with-open [conn (connect)]
    (api/transact conn triangle-relation-schema)
    (let [[a-id b-id c-id] user-entity-ids]
      (api/transact conn (into graph-nodes
                               [[:db/add graph-a :r/to graph-b]
                                [:db/add graph-b :s/to graph-c]
                                [:db/add graph-c :t/to graph-a]]))
      (with-open [sub (api/subscribe conn '{:find [?a ?b ?c]
                                            :where [[?a :r/to ?b]
                                                    [?b :s/to ?c]
                                                    [?c :t/to ?a]]})]
        (api/transact conn [[:db/retract b-id :s/to c-id]])
        (is (= [[[a-id b-id c-id] -1]] (take-delta! sub)))))))

(deftest e2e-triangle-test
  (with-open [conn (connect)]
    (api/transact conn edge-schema)
    (with-open [sub (api/subscribe conn '{:find [?a ?b ?c]
                                          :where [[?a :g/to ?b]
                                                  [?b :g/to ?c]
                                                  [?c :g/to ?a]]})]
      (api/transact conn [[:db/add graph-a :g/to graph-b]
                          [:db/add graph-b :g/to graph-c]
                          [:db/add graph-c :g/to graph-a]])
      (let [[a-id b-id c-id] user-entity-ids]
        (is (= #{[[a-id b-id c-id] 1]
                 [[b-id c-id a-id] 1]
                 [[c-id a-id b-id] 1]}
               (delta-set! sub)))))))

(deftest test-no-changes
  (with-open [conn (connect)]
    (api/transact conn triangle-relation-schema)
    (with-open [sub (api/subscribe conn '{:find [?a ?b ?c]
                                          :where [[?a :r/to ?b]
                                                  [?b :s/to ?c]
                                                  [?c :t/to ?a]]})]
      (api/transact conn (into graph-nodes
                               [[:db/add graph-b :s/to graph-c]]))
      (is (= ::api/timeout (api/take! sub 300))))))

(deftest residence-example
  (with-open [conn (connect)]
    (api/transact conn residence-schema)
    (api/transact conn [{:person/name "Ada Lovelace"
                         :person/residence "12 St. James's Square"}
                        {:person/name "Alan Turing"
                         :person/residence "Bletchley Park"}])
    (let [ada-id (single-value conn '{:find [?p]
                                      :where [[?p :person/name "Ada Lovelace"]]})]
      (with-open [sub (api/subscribe conn '{:find [?name ?residence]
                                             :where [[?p :person/name ?name]
                                                     [?p :person/residence ?residence]]})]
        (api/transact conn [[:db/add ada-id :person/residence "Buckingham Palace"]])
        (is (= #{[["Ada Lovelace" "12 St. James's Square"] -1]
                 [["Ada Lovelace" "Buckingham Palace"] 1]}
               (delta-set! sub)))))))
