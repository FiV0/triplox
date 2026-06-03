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
  [{:db/ident :g/to :db/valueType :db.type/long :db/cardinality :db.cardinality/many}])

(def triangle-relation-schema
  [{:db/ident :r/to :db/valueType :db.type/long :db/cardinality :db.cardinality/many}
   {:db/ident :s/to :db/valueType :db.type/long :db/cardinality :db.cardinality/many}
   {:db/ident :t/to :db/valueType :db.type/long :db/cardinality :db.cardinality/many}])

(def names-query '{:find [?name] :where [[?e :name ?name]]})

(def delta-timeout-ms 10000)

(defn take-delta! [sub]
  (api/take! sub delta-timeout-ms))

(defn delta-set! [sub]
  (set (take-delta! sub)))

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
    (api/transact conn [{:db/id 101 :name "Ivan"}])
    (with-open [sub (api/subscribe conn '{:find [?name]
                                           :where [[101 :name ?name]]})]
      (api/transact conn [{:db/id 101 :name "Ivanov"}])
      (is (= #{[["Ivan"] -1]
               [["Ivanov"] 1]}
             (delta-set! sub))))))

(deftest test-basic-query-1
  (with-open [conn (connect)]
    (api/transact conn people-schema)
    (with-open [sub (api/subscribe conn '{:find [?name]
                                           :where [[?e :name "Ivan"]
                                                   [?e :name ?name]]})]
      (api/transact conn [{:db/id 111 :name "Ivan" :last-name "Ivanov"}
                          {:db/id 112 :name "Petr" :last-name "Petrov"}])
      (is (= [[["Ivan"] 1]] (take-delta! sub))))))

(deftest test-basic-query-2
  (testing "Can query entity by single field"
    (with-open [conn (connect)]
      (api/transact conn people-schema)
      (with-open [sub (api/subscribe conn '{:find [?e]
                                             :where [[?e :name "Ivan"]]})]
        (api/transact conn [{:db/id 121 :name "Ivan" :last-name "Ivanov"}
                            {:db/id 122 :name "Petr" :last-name "Petrov"}])
        (is (= [[[121] 1]] (take-delta! sub)))))))

(deftest test-basic-query-3
  (testing "Can query using multiple terms"
    (with-open [conn (connect)]
      (api/transact conn people-schema)
      (with-open [sub (api/subscribe conn '{:find [?name ?last-name]
                                             :where [[?e :name ?name]
                                                     [?e :last-name ?last-name]
                                                     [?e :name "Ivan"]
                                                     [?e :last-name "Ivanov"]]})]
        (api/transact conn [{:db/id 131 :name "Ivan" :last-name "Ivanov"}
                            {:db/id 132 :name "Petr" :last-name "Petrov"}])
        (is (= [[["Ivan" "Ivanov"] 1]]
               (take-delta! sub)))))))

(deftest test-basic-query-4
  (testing "Negate query based on subsequent non-matching clause"
    (with-open [conn (connect)]
      (api/transact conn people-schema)
      (with-open [sub (api/subscribe conn '{:find [?e]
                                             :where [[?e :name "Ivan"]
                                                     [?e :last-name "Ivanov-does-not-match"]]})]
        (api/transact conn [{:db/id 141 :name "Ivan" :last-name "Ivanov"}
                            {:db/id 142 :name "Petr" :last-name "Petrov"}])
        (is (= ::api/timeout (api/take! sub 300)))))))

(deftest test-basic-query-5
  (testing "Can query for multiple results"
    (with-open [conn (connect)]
      (api/transact conn people-schema)
      (with-open [sub (api/subscribe conn '{:find [?name]
                                             :where [[?e :name ?name]]})]
        (api/transact conn [{:db/id 151 :name "Ivan"}
                            {:db/id 152 :name "Petr"}])
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
        (api/transact conn [{:db/id 161 :name "Ivan" :last-name "Ivanov"}
                            {:db/id 162 :name "Petr" :last-name "Petrov"}
                            {:db/id 163 :name "Smith" :last-name "Smith"}])
        (is (= [[[163] 1]]
               (take-delta! sub)))))))

(deftest test-basic-query-7
  (testing "Can query across fields for same value when value is passed in"
    (with-open [conn (connect)]
      (api/transact conn people-schema)
      (with-open [sub (api/subscribe conn '{:find [?p1]
                                             :where [[?p1 :name ?name]
                                                     [?p1 :last-name ?name]
                                                     [?p1 :name "Smith"]]})]
        (api/transact conn [{:db/id 171 :name "Ivan" :last-name "Ivanov"}
                            {:db/id 172 :name "Petr" :last-name "Petrov"}
                            {:db/id 173 :name "Smith" :last-name "Smith"}])
        (is (= [[[173] 1]]
               (take-delta! sub)))))))

(deftest test-basic-retractions-1
  (with-open [conn (connect)]
    (api/transact conn people-schema)
    (api/transact conn [{:db/id 181 :name "Ivan" :last-name "Ivanov"}
                        {:db/id 182 :name "Petr" :last-name "Petrov"}])
    (with-open [sub (api/subscribe conn '{:find [?name]
                                           :where [[?e :name ?name]]})]
      (api/transact conn [[:db/add 181 :name "Ivanova"]])
      (is (= #{[["Ivan"] -1]
               [["Ivanova"] 1]}
             (delta-set! sub))))))

(deftest test-basic-retractions-2
  (with-open [conn (connect)]
    (api/transact conn people-schema)
    (api/transact conn [{:db/id 191 :name "Ivan" :last-name "Ivanov"}
                        {:db/id 192 :name "Petr" :last-name "Petrov"}])
    (with-open [sub (api/subscribe conn '{:find [?name]
                                           :where [[?e :name ?name]]})]
      (api/transact conn [[:db/retract 191 :name "Ivan"]])
      (is (= [[["Ivan"] -1]]
             (take-delta! sub))))))

(deftest test-dbsp-distinct-semantics-retractions
  (with-open [conn (connect)]
    (api/transact conn people-schema)
    (api/transact conn [{:db/id 201 :name "Alice" :city "NYC"}
                        {:db/id 202 :name "Bob" :city "NYC"}
                        {:db/id 203 :name "Carol" :city "LA"}])
    (with-open [sub (api/subscribe conn '{:find [?city]
                                           :where [[?e :city ?city]]})]
      (api/transact conn [[:db/retract 202 :city "NYC"]])
      (is (= [[["NYC"] -1]] (take-delta! sub)))
      (api/transact conn [[:db/retract 201 :city "NYC"]])
      (is (= [[["NYC"] -1]] (take-delta! sub))))))

(deftest test-prefix-stable-extension-addition
  (with-open [conn (connect)]
    (api/transact conn triangle-relation-schema)
    (api/transact conn [[:db/add 301 :r/to 302]
                        [:db/add 302 :s/to 304]])
    (with-open [sub (api/subscribe conn '{:find [?a ?b ?c]
                                           :where [[?a :r/to ?b]
                                                   [?b :s/to ?c]]})]
      (api/transact conn [[:db/add 302 :s/to 305]])
      (is (= #{[[301 302 305] 1]}
             (delta-set! sub))))))

(deftest test-prefix-stable-extension-retraction
  (with-open [conn (connect)]
    (api/transact conn triangle-relation-schema)
    (api/transact conn [[:db/add 311 :r/to 312]
                        [:db/add 312 :s/to 314]
                        [:db/add 312 :s/to 315]])
    (with-open [sub (api/subscribe conn '{:find [?a ?b ?c]
                                           :where [[?a :r/to ?b]
                                                   [?b :s/to ?c]]})]
      (api/transact conn [[:db/retract 312 :s/to 315]])
      (is (= #{[[311 312 315] -1]}
             (delta-set! sub))))))

(deftest test-triangle-edge-deletion
  (with-open [conn (connect)]
    (api/transact conn triangle-relation-schema)
    (api/transact conn [[:db/add 401 :r/to 402]
                        [:db/add 402 :s/to 403]
                        [:db/add 403 :t/to 401]])
    (with-open [sub (api/subscribe conn '{:find [?a ?b ?c]
                                           :where [[?a :r/to ?b]
                                                   [?b :s/to ?c]
                                                   [?c :t/to ?a]]})]
      (api/transact conn [[:db/retract 402 :s/to 403]])
      (is (= [[[401 402 403] -1]] (take-delta! sub))))))

(deftest e2e-triangle-test
  (with-open [conn (connect)]
    (api/transact conn edge-schema)
    (with-open [sub (api/subscribe conn '{:find [?a ?b ?c]
                                           :where [[?a :g/to ?b]
                                                   [?b :g/to ?c]
                                                   [?c :g/to ?a]]})]
      (api/transact conn [[:db/add 501 :g/to 502]
                          [:db/add 502 :g/to 503]
                          [:db/add 503 :g/to 501]])
      (is (= #{[[501 502 503] 1]
               [[502 503 501] 1]
               [[503 501 502] 1]}
             (delta-set! sub))))))

(deftest test-no-changes
  (with-open [conn (connect)]
    (api/transact conn triangle-relation-schema)
    (with-open [sub (api/subscribe conn '{:find [?a ?b ?c]
                                           :where [[?a :r/to ?b]
                                                   [?b :s/to ?c]
                                                   [?c :t/to ?a]]})]
      (api/transact conn [[:db/add 601 :s/to 602]])
      (is (= ::api/timeout (api/take! sub 300))))))

(deftest residence-example
  (with-open [conn (connect)]
    (api/transact conn residence-schema)
    (api/transact conn [{:db/id 701
                         :person/name "Ada Lovelace"
                         :person/residence "12 St. James's Square"}
                        {:db/id 702
                         :person/name "Alan Turing"
                         :person/residence "Bletchley Park"}])
    (with-open [sub (api/subscribe conn '{:find [?name ?residence]
                                           :where [[?p :person/name ?name]
                                                   [?p :person/residence ?residence]]})]
      (api/transact conn [[:db/add 701 :person/residence "Buckingham Palace"]])
      (is (= #{[["Ada Lovelace" "12 St. James's Square"] -1]
               [["Ada Lovelace" "Buckingham Palace"] 1]}
             (delta-set! sub))))))
