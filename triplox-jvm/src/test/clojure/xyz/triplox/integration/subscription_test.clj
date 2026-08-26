(ns xyz.triplox.integration.subscription-test
  (:require [clojure.test :as t :refer [deftest is testing use-fixtures]]
            [xyz.triplox.api :as api]
            [xyz.triplox.integration.query-test :refer [*conn* people-schema q with-conn with-schema]])
  (:import (xyz.triplox.client TriploxException)))

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

(use-fixtures :each with-conn (with-schema people-schema))

(def names-query '{:find [?name] :where [[?e :name ?name]]})

(def default-delta-timeout-ms 1000)

(defn take-delta!
  ([sub] (take-delta! sub default-delta-timeout-ms))
  ([sub timeout] (api/take! sub timeout)))

(defn take-priming! [sub]
  (let [delta (take-delta! sub)]
    (is (seq delta))
    delta))

(defn single-value [query]
  (let [rows (q query)]
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

(deftest subscribe-returns-tx-key-and-times-out
  (with-open [sub (api/subscribe *conn* names-query)]
    (is (some? (:tx-id (api/tx-key sub))))
    ;; No transaction after the subscription -> bounded take! times out.
    (is (= ::api/timeout (api/take! sub 200)))))

(deftest subscribe-receives-delta
  (with-open [sub (api/subscribe *conn* names-query)]
    (api/transact *conn* [{:name "Ivan"}])
    (is (= [[["Ivan"] 1]] (take-delta! sub)))))

(deftest retract-entity-emits-retraction
  (api/transact *conn* [{:name "Alice" :age 30}])
  (let [alice-id (single-value '{:find [?e]
                                 :where [[?e :name "Alice"]]})]
    (with-open [sub (api/subscribe *conn* '{:find [?name ?age]
                                            :where [[?e :name ?name]
                                                    [?e :age ?age]]})]
      (take-priming! sub)
      (is (:committed? (api/transact *conn* [[:db/retractEntity alice-id]])))
      (is (= [[["Alice" 30] -1]] (take-delta! sub))))))

(deftest with-previous-value
  (api/transact *conn* [{:name "Ivan"}])
  (let [ivan-id (single-value '{:find [?e]
                                :where [[?e :name "Ivan"]]})]
    (with-open [sub (api/subscribe *conn* {:find ['?name]
                                           :where [[ivan-id :name '?name]]})]
      (take-priming! sub)
      (api/transact *conn* [[:db/add ivan-id :name "Ivanov"]])
      (is (= [[["Ivan"] -1]
              [["Ivanov"] 1]]
             (take-delta! sub))))))

(deftest aggregate-subscription-emits-priming-and-incremental-replacements
  (api/transact *conn* [{:name "Alice" :age 10}
                        {:name "Bob" :age 20}])
  (with-open [sub (api/subscribe *conn* '{:find [(sum ?age)
                                                 (min ?age)
                                                 (max ?age)
                                                 (count ?age)
                                                 (count-distinct ?age)
                                                 (avg ?age)]
                                          :where [[?e :age ?age]]})]
    (is (= [[[30 10 20 2 2 15.0] 1]]
           (take-priming! sub)))

    (api/transact *conn* [{:name "Carol" :age 30}])
    (is (= #{[[30 10 20 2 2 15.0] -1]
             [[60 10 30 3 3 20.0] 1]}
           (set (take-delta! sub))))))

(deftest aggregate-subscription-priming-error-is-reported
  (api/transact *conn* [{:name "Alice"}])
  (is (thrown-with-msg? TriploxException #"sum: cannot aggregate non-numeric value"
                        (api/subscribe *conn* '{:find [(sum ?name)]
                                                :where [[?e :name ?name]]}))))

(deftest live-aggregate-error-closes-only-the-affected-subscription
  (api/transact *conn* [{:age 10}])
  (with-open [aggregate-sub (api/subscribe *conn* '{:find [(sum ?value)]
                                                    :where [(or [?e :age ?value]
                                                                [?e :name ?value])]})
              names-sub (api/subscribe *conn* names-query)]
    (is (= [[[10] 1]] (take-priming! aggregate-sub)))

    (is (:committed? (api/transact *conn* [{:name "Alice"}])))
    (let [error (try
                  (take-delta! aggregate-sub)
                  nil
                  (catch TriploxException error error))]
      (is (some? error))
      (is (= 2001 (Short/toUnsignedInt (.code ^TriploxException error))))
      (is (re-find #"sum: cannot aggregate non-numeric value"
                   (.getMessage ^TriploxException error))))
    (is (.isDone aggregate-sub))
    (is (= [[["Alice"] 1]] (take-delta! names-sub)))

    (is (:committed? (api/transact *conn* [{:name "Bob"}])))
    (is (= [[["Bob"] 1]] (take-delta! names-sub)))))

(deftest grouped-aggregate-subscription-updates-one-of-multiple-groups
  (api/transact *conn* [{:name "Alice" :sex :female :age 10}
                        {:name "Bob" :sex :male :age 20}
                        {:name "Dave" :sex :male :age 30}])
  (with-open [sub (api/subscribe *conn* '{:find [(sum ?age)
                                                 ?sex
                                                 (count ?e)
                                                 (max ?age)]
                                          :where [[?e :sex ?sex]
                                                  [?e :age ?age]]})]
    (is (= #{[[10 :female 1 10] 1]
             [[50 :male 2 30] 1]}
           (set (take-priming! sub))))

    (api/transact *conn* [{:name "Carol" :sex :female :age 30}])
    (is (= #{[[10 :female 1 10] -1]
             [[40 :female 2 30] 1]}
           (set (take-delta! sub))))))

(deftest test-basic-query-1
  (with-open [sub (api/subscribe *conn* '{:find [?name]
                                          :where [[?e :name "Ivan"]
                                                  [?e :name ?name]]})]
    (api/transact *conn* [{:name "Ivan" :last-name "Ivanov"}
                          {:name "Petr" :last-name "Petrov"}])
    (is (= [[["Ivan"] 1]] (take-delta! sub)))))

(deftest test-basic-query-2
  (testing "Can query entity by single field"
    (with-open [sub (api/subscribe *conn* '{:find [?e]
                                            :where [[?e :name "Ivan"]]})]
      (api/transact *conn* [{:name "Ivan" :last-name "Ivanov"}
                            {:name "Petr" :last-name "Petrov"}])
      (let [[[row weight]] (take-delta! sub)]
        (is (= 1 weight))
        (is (integer? (first row)))))))

(deftest test-basic-query-3
  (testing "Can query using multiple terms"
    (with-open [sub (api/subscribe *conn* '{:find [?name ?last-name]
                                            :where [[?e :name ?name]
                                                    [?e :last-name ?last-name]
                                                    [?e :name "Ivan"]
                                                    [?e :last-name "Ivanov"]]})]
      (api/transact *conn* [{:name "Ivan" :last-name "Ivanov"}
                            {:name "Petr" :last-name "Petrov"}])
      (is (= [[["Ivan" "Ivanov"] 1]]
             (take-delta! sub))))))

(deftest test-basic-query-4
  (testing "Negate query based on subsequent non-matching clause"
    (with-open [sub (api/subscribe *conn* '{:find [?e]
                                            :where [[?e :name "Ivan"]
                                                    [?e :last-name "Ivanov-does-not-match"]]})]
      (api/transact *conn* [{:name "Ivan" :last-name "Ivanov"}
                            {:name "Petr" :last-name "Petrov"}])
      (is (= ::api/timeout (api/take! sub 300))))))

(deftest test-not-addition+retraction
  (with-open [sub (api/subscribe *conn* '{:find [?name]
                                          :where [[?e :name ?name]
                                                  (not [?e :age 30])]})]
    (api/transact *conn* [{:name "Alice"}])
    (is (= [[["Alice"] 1]] (take-delta! sub)))

    (let [alice-id (single-value '{:find [?e]
                                   :where [[?e :name "Alice"]]})]
      (api/transact *conn* [[:db/add alice-id :age 30]])
      (is (= [[["Alice"] -1]] (take-delta! sub)))

      (api/transact *conn* [[:db/retract alice-id :age 30]])
      (is (= [[["Alice"] 1]] (take-delta! sub))))))

(deftest not-suppresses-positive-side-addition+retraction
  (api/transact *conn* [{:db/ident :alias
                         :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/many}])
  (with-open [sub (api/subscribe *conn* '{:find [?alias]
                                          :where [[?e :alias ?alias]
                                                  (not [?e :age 30])]})]
    (api/transact *conn* [{:db/id "alice" :alias "Alice"}])
    (is (= [[["Alice"] 1]] (take-delta! sub)))

    (let [alice-id (single-value '{:find [?e]
                                   :where [[?e :alias "Alice"]]})]
      (api/transact *conn* [[:db/add alice-id :age 30]])
      (is (= [[["Alice"] -1]] (take-delta! sub)))

      (api/transact *conn* [[:db/add alice-id :alias "Alicia"]])
      (is (= ::api/timeout (take-delta! sub 300)))

      (api/transact *conn* [[:db/retract alice-id :alias "Alice"]])
      (is (= ::api/timeout (take-delta! sub 300)))

      (api/transact *conn* [[:db/retract alice-id :age 30]])
      (is (= [[["Alicia"] 1]] (take-delta! sub))))))

(deftest predicates-compose-with-and+or+not
  (with-open [sub (api/subscribe *conn* '{:find [?name]
                                          :where [[?e :name ?name]
                                                  [?e :age ?age]
                                                  (or
                                                   (and [(> ?age 20)]
                                                        [(< ?age 40)])
                                                   (not [(< ?age 30)]))]})]
    (api/transact *conn* [{:name "Alice" :age 20}
                          {:name "Bob" :age 25}
                          {:name "Cara" :age 35}
                          {:name "Dave" :age 50}])
    (is (= [[["Bob"] 1]
            [["Cara"] 1]
            [["Dave"] 1]]
           (take-delta! sub)))

    (let [cara-id (single-value '{:find [?e]
                                  :where [[?e :name "Cara"]]})]
      (api/transact *conn* [[:db/retract cara-id :age 35]])
      (is (= [[["Cara"] -1]]
             (take-delta! sub))))))

(deftest datalog-functions
  (testing "Function produces the variable"
    (with-open [sub (api/subscribe *conn* '{:find [?name ?half]
                                            :where [[?e :name ?name]
                                                    [?e :age ?age]
                                                    [(quot ?age 2) ?half]
                                                    [(< ?half 20)]]})]
      (api/transact *conn* [{:name "Ivan" :age 30}
                            {:name "Bob" :age 40}])
      (is (= [[["Ivan" 15] 1]] (take-delta! sub)))

      (let [ivan-id (single-value '{:find [?e]
                                    :where [[?e :name "Ivan"]]})]
        (api/transact *conn* [[:db/add ivan-id :age 31]])
        (is (= ::api/timeout (take-delta! sub 300)))

        (api/transact *conn* [[:db/add ivan-id :age 32]])
        (is (= #{[["Ivan" 15] -1]
                 [["Ivan" 16] 1]}
               (set (take-delta! sub)))))))

  (testing "function filters the variable"
    (with-open [sub (api/subscribe *conn* '{:find [?name]
                                            :where [[?e :age ?age]
                                                    [?e :name ?name]
                                                    [?e :salary ?salary]
                                                    [(quot ?age 2) ?salary]]})]
      (api/transact *conn* [{:name "Eq" :age 30 :salary 15}
                            {:name "Neq" :age 30 :salary 20}])
      (is (= [[["Eq"] 1]] (take-delta! sub)))

      (let [eq-id (single-value '{:find [?e] :where [[?e :name "Eq"]]})
            neq-id (single-value '{:find [?e] :where [[?e :name "Neq"]]})]
        (api/transact *conn* [[:db/add neq-id :salary 15]])
        (is (= [[["Neq"] 1]] (take-delta! sub)))

        (api/transact *conn* [[:db/add eq-id :salary 20]])
        (is (= [[["Eq"] -1]] (take-delta! sub)))))))

(deftest expression-failures-do-not-stop-subscription
  (with-open [sub (api/subscribe *conn* '{:find [?name ?result]
                                          :where [[?e :name ?name]
                                                  [?e :age ?age]
                                                  [?e :salary ?divisor]
                                                  [(quot ?age ?divisor) ?result]]})]
    (api/transact *conn* [{:name "Alice" :age 30 :salary 0}])
    (is (= ::api/timeout (take-delta! sub 300)))

    (let [alice-id (single-value '{:find [?e]
                                   :where [[?e :name "Alice"]]})]
      (api/transact *conn* [[:db/add alice-id :salary 2]])
      (is (= [[["Alice" 15] 1]] (take-delta! sub)))

      (api/transact *conn* [[:db/add alice-id :salary 0]])
      (is (= [[["Alice" 15] -1]] (take-delta! sub))))))

(deftest test-not-negative-scope-layouts
  (testing "Multi-clause negative scope"
    (api/transact *conn* [{:db/ident :friend
                           :db/valueType :db.type/ref
                           :db/cardinality :db.cardinality/one}])
    (api/transact *conn* [{:db/id "alice" :name "Alice" :friend "bob"}
                          {:db/id "bob" :name "Bob"}])
    (let [alice-id (single-value '{:find [?e]
                                   :where [[?e :name "Alice"]]})
          bob-id (single-value '{:find [?e]
                                 :where [[?e :name "Bob"]]})]
      (with-open [sub (api/subscribe *conn* '{:find [?name]
                                              :where [[?e :name ?name]
                                                      [?e :friend ?friend]
                                                      (not [?e :age 30]
                                                           [?friend :age 40])]})]
        (is (= [[["Alice"] 1]] (take-priming! sub)))

        (api/transact *conn* [[:db/add alice-id :age 30]])
        (is (= ::api/timeout (take-delta! sub 300)))

        (api/transact *conn* [[:db/add bob-id :age 40]])
        (is (= [[["Alice"] -1]] (take-delta! sub)))

        (api/transact *conn* [[:db/retract bob-id :age 40]])
        (is (= [[["Alice"] 1]] (take-delta! sub)))))))

(deftest test-or-inside-negative-scope
  (testing "OR inside negative scope"
    (with-open [sub (api/subscribe *conn* '{:find [?name]
                                            :where [[?e :name ?name]
                                                    (not (or [?e :age 30]
                                                             [?e :city "Berlin"]))]})]
      (api/transact *conn* [{:name "Alice"}])
      (is (= [[["Alice"] 1]] (take-delta! sub)))

      (let [alice-id (single-value '{:find [?e]
                                     :where [[?e :name "Alice"]]})]
        (api/transact *conn* [[:db/add alice-id :age 30]])
        (is (= [[["Alice"] -1]] (take-delta! sub)))

        (api/transact *conn* [[:db/add alice-id :city "Berlin"]])
        (is (= ::api/timeout (take-delta! sub 300)))

        (api/transact *conn* [[:db/retract alice-id :age 30]])
        (is (= ::api/timeout (take-delta! sub 300)))

        (api/transact *conn* [[:db/retract alice-id :city "Berlin"]])
        (is (= [[["Alice"] 1]] (take-delta! sub)))))))

(deftest test-basic-query-5
  (testing "Can query for multiple results"
    (with-open [sub (api/subscribe *conn* '{:find [?name]
                                            :where [[?e :name ?name]]})]
      (api/transact *conn* [{:name "Ivan"}
                            {:name "Petr"}])
      (is (= [[["Ivan"] 1]
              [["Petr"] 1]]
             (take-delta! sub))))))

(deftest test-basic-query-6
  (testing "Can query across fields for same value"
    (with-open [sub (api/subscribe *conn* '{:find [?p1]
                                            :where [[?p1 :name ?name]
                                                    [?p1 :last-name ?name]]})]
      (api/transact *conn* [{:name "Ivan" :last-name "Ivanov"}
                            {:name "Petr" :last-name "Petrov"}
                            {:name "Smith" :last-name "Smith"}])
      (let [[[row weight]] (take-delta! sub)]
        (is (= 1 weight))
        (is (integer? (first row)))))))

(deftest test-basic-query-7
  (testing "Can query across fields for same value when value is passed in"
    (with-open [sub (api/subscribe *conn* '{:find [?p1]
                                            :where [[?p1 :name ?name]
                                                    [?p1 :last-name ?name]
                                                    [?p1 :name "Smith"]]})]
      (api/transact *conn* [{:name "Ivan" :last-name "Ivanov"}
                            {:name "Petr" :last-name "Petrov"}
                            {:name "Smith" :last-name "Smith"}])
      (let [[[row weight]] (take-delta! sub)]
        (is (= 1 weight))
        (is (integer? (first row)))))))

(deftest test-basic-retractions-1
  (api/transact *conn* [{:name "Ivan" :last-name "Ivanov"}
                        {:name "Petr" :last-name "Petrov"}])
  (let [ivan-id (single-value '{:find [?e]
                                :where [[?e :name "Ivan"]]})]
    (with-open [sub (api/subscribe *conn* '{:find [?name]
                                            :where [[?e :name ?name]]})]
      (take-priming! sub)
      (api/transact *conn* [[:db/add ivan-id :name "Ivanova"]])
      (is (= [[["Ivan"] -1]
              [["Ivanova"] 1]]
             (take-delta! sub))))))

(deftest test-basic-retractions-2
  (api/transact *conn* [{:name "Ivan" :last-name "Ivanov"}
                        {:name "Petr" :last-name "Petrov"}])
  (let [ivan-id (single-value '{:find [?e]
                                :where [[?e :name "Ivan"]]})]
    (with-open [sub (api/subscribe *conn* '{:find [?name]
                                            :where [[?e :name ?name]]})]
      (take-priming! sub)
      (api/transact *conn* [[:db/retract ivan-id :name "Ivan"]])
      (is (= [[["Ivan"] -1]]
             (take-delta! sub))))))

(deftest test-dbsp-distinct-semantics-retractions
  (api/transact *conn* [{:name "Alice" :city "NYC"}
                        {:name "Bob" :city "NYC"}
                        {:name "Carol" :city "LA"}])
  (let [alice-id (single-value '{:find [?e]
                                 :where [[?e :name "Alice"]]})
        bob-id (single-value '{:find [?e]
                               :where [[?e :name "Bob"]]})]
    (with-open [sub (api/subscribe *conn* '{:find [?city]
                                            :where [[?e :city ?city]]})]
      (take-priming! sub)
      (api/transact *conn* [[:db/retract bob-id :city "NYC"]])
      (is (= [[["NYC"] -1]] (take-delta! sub)))
      (api/transact *conn* [[:db/retract alice-id :city "NYC"]])
      (is (= [[["NYC"] -1]] (take-delta! sub))))))

(deftest test-or-retraction
  (api/transact *conn* [{:name "Alice"}
                        {:name "Bob"}
                        {:name "Carol"}])
  (let [bob-id (single-value '{:find [?e]
                               :where [[?e :name "Bob"]]})]
    (with-open [sub (api/subscribe *conn* '{:find [?e]
                                            :where [(or [?e :name "Alice"]
                                                        [?e :name "Bob"])]})]
      (take-priming! sub)
      (api/transact *conn* [[:db/retract bob-id :name "Bob"]])
      (is (= [[[bob-id] -1]]
             (take-delta! sub))))))

(deftest test-or-multiple-matching-branches

  (with-open [sub (api/subscribe *conn* '{:find [?name]
                                          :where [[?e :name ?name]
                                                  (or [?e :name "Alice"]
                                                      [?e :age 40])]})]
    (api/transact *conn* [{:name "Alice" :age 40}
                          {:name "Bob" :age 30}])
    (is (= [[["Alice"] 1]] (take-delta! sub 300)))
    (is (= ::api/timeout (take-delta! sub 300)))

    (let [alice-id (single-value '{:find [?e]
                                   :where [[?e :name "Alice"]]})]

      (api/transact *conn* [[:db/retract alice-id :age 40]])
      (is (= ::api/timeout (take-delta! sub 300)))

      (api/transact *conn* [[:db/retract alice-id :name "Alice"]])
      (is (= [[["Alice"] -1]] (take-delta! sub 300))))))

(deftest test-or-joined-with-outer-pattern-retraction
  (api/transact *conn* [{:name "Alice" :city "Berlin"}
                        {:name "Bob" :city "Berlin"}
                        {:name "Carol" :city "Rome"}])
  (let [bob-id (single-value '{:find [?e]
                               :where [[?e :name "Bob"]]})]
    (with-open [sub (api/subscribe *conn* '{:find [?city]
                                            :where [(or [?e :name "Alice"]
                                                        [?e :name "Bob"])
                                                    [?e :city ?city]]})]
      (take-priming! sub)
      (api/transact *conn* [[:db/retract bob-id :name "Bob"]])
      (is (= [[["Berlin"] -1]]
             (take-delta! sub))))))

(deftest test-outer-relation-through-and+nested-or-addition+retraction
  (api/transact *conn* [{:db/ident :friend
                         :db/valueType :db.type/ref
                         :db/cardinality :db.cardinality/one}])
  (api/transact *conn* [{:db/id "alice" :name "Alice" :friend "bob"}
                        {:db/id "bob" :name "Bob"}])
  (let [bob-id (single-value '{:find [?e]
                               :where [[?e :name "Bob"]]})]
    (with-open [sub (api/subscribe *conn* '{:find [?name ?friend-name]
                                            :where [[?e :name ?name]
                                                    (or
                                                     (and [?e :friend ?friend]
                                                          (or [?friend :age 30]
                                                              [?friend :age 40])
                                                          [?friend :name ?friend-name])
                                                     (and [?e :friend ?friend]
                                                          [?friend :age 50]
                                                          [?friend :name ?friend-name]))]})]
      (api/transact *conn* [[:db/add bob-id :age 30]])
      (is (= [[["Alice" "Bob"] 1]]
             (take-delta! sub)))

      (api/transact *conn* [[:db/retract bob-id :age 30]])
      (is (= [[["Alice" "Bob"] -1]]
             (take-delta! sub))))))

(deftest test-prefix-stable-extension-addition
  (api/transact *conn* triangle-relation-schema)
  (let [[a-id b-id _c-id d-id] user-entity-ids]
    (api/transact *conn* (into graph-nodes
                               [[:db/add graph-a :r/to graph-b]
                                [:db/add graph-b :s/to graph-c]]))

    (with-open [sub (api/subscribe *conn* '{:find [?a ?b ?c]
                                            :where [[?a :r/to ?b]
                                                    [?b :s/to ?c]]})]
      (take-priming! sub)
      (t/is (true? (:committed? (api/transact *conn* [[:db/add b-id :s/to d-id]]))))
      (is (= [[[a-id b-id d-id] 1]]
             (take-delta! sub))))))

(deftest test-prefix-stable-extension-retraction
  (api/transact *conn* triangle-relation-schema)
  (let [[a-id b-id _c-id d-id] user-entity-ids]
    (api/transact *conn* (into graph-nodes
                               [[:db/add graph-a :r/to graph-b]
                                [:db/add graph-b :s/to graph-c]
                                [:db/add graph-b :s/to graph-d]]))
    (with-open [sub (api/subscribe *conn* '{:find [?a ?b ?c]
                                            :where [[?a :r/to ?b]
                                                    [?b :s/to ?c]]})]
      (take-priming! sub)
      (api/transact *conn* [[:db/retract b-id :s/to d-id]])
      (is (= [[[a-id b-id d-id] -1]]
             (take-delta! sub))))))

(deftest test-triangle-edge-deletion
  (api/transact *conn* triangle-relation-schema)
  (let [[a-id b-id c-id] user-entity-ids]
    (api/transact *conn* (into graph-nodes
                               [[:db/add graph-a :r/to graph-b]
                                [:db/add graph-b :s/to graph-c]
                                [:db/add graph-c :t/to graph-a]]))
    (with-open [sub (api/subscribe *conn* '{:find [?a ?b ?c]
                                            :where [[?a :r/to ?b]
                                                    [?b :s/to ?c]
                                                    [?c :t/to ?a]]})]
      (take-priming! sub)
      (api/transact *conn* [[:db/retract b-id :s/to c-id]])
      (is (= [[[a-id b-id c-id] -1]] (take-delta! sub))))))

(deftest e2e-triangle-test
  (api/transact *conn* edge-schema)
  (with-open [sub (api/subscribe *conn* '{:find [?a ?b ?c]
                                          :where [[?a :g/to ?b]
                                                  [?b :g/to ?c]
                                                  [?c :g/to ?a]]})]
    (api/transact *conn* [[:db/add graph-a :g/to graph-b]
                          [:db/add graph-b :g/to graph-c]
                          [:db/add graph-c :g/to graph-a]])
    (let [[a-id b-id c-id] user-entity-ids]
      (is (= #{[[a-id b-id c-id] 1]
               [[b-id c-id a-id] 1]
               [[c-id a-id b-id] 1]}
             (set (take-delta! sub)))))))

(deftest test-no-changes
  (api/transact *conn* triangle-relation-schema)
  (with-open [sub (api/subscribe *conn* '{:find [?a ?b ?c]
                                          :where [[?a :r/to ?b]
                                                  [?b :s/to ?c]
                                                  [?c :t/to ?a]]})]
    (api/transact *conn* (into graph-nodes
                               [[:db/add graph-b :s/to graph-c]]))
    (is (= ::api/timeout (api/take! sub 300)))))

(deftest residence-example
  (api/transact *conn* residence-schema)
  (api/transact *conn* [{:person/name "Ada Lovelace"
                         :person/residence "12 St. James's Square"}
                        {:person/name "Alan Turing"
                         :person/residence "Bletchley Park"}])
  (let [ada-id (single-value '{:find [?p]
                               :where [[?p :person/name "Ada Lovelace"]]})]
    (with-open [sub (api/subscribe *conn* '{:find [?name ?residence]
                                            :where [[?p :person/name ?name]
                                                    [?p :person/residence ?residence]]})]
      (take-priming! sub)
      (api/transact *conn* [[:db/add ada-id :person/residence "Buckingham Palace"]])
      (is (= [[["Ada Lovelace" "12 St. James's Square"] -1]
              [["Ada Lovelace" "Buckingham Palace"] 1]]
             (take-delta! sub))))))
