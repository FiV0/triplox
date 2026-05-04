(ns io.triplox.integration.query-test
  (:require [clojure.test :as t :refer [deftest is testing use-fixtures]]
            [io.triplox.api :as tc])
  (:import (io.triplox.client TriploxException)))

;; ---------------------------------------------------------------------------
;; Fixtures & helpers
;; ---------------------------------------------------------------------------

(def ^:dynamic *conn* nil)

(def people-schema
  [{:db/ident :name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :last-name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :sex :db/valueType :db.type/keyword :db/cardinality :db.cardinality/one}
   {:db/ident :age :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :salary :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :city :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])

(defn connect []
  (let [host (System/getProperty "triplox.host" "localhost")
        port (Integer/parseInt (System/getProperty "triplox.port" "5490"))]
    (tc/connect host port)))

(defn with-conn [f]
  (with-open [conn (connect)]
    (binding [*conn* conn]
      (f))))

(defn with-people-schema [f]
  (tc/transact *conn* people-schema)
  (f))

(use-fixtures :each with-conn with-people-schema)

(defn q
  "Open a DB, run query, close DB, return results as a set.
  Additional arguments bind `:in` variables as scalars."
  [query-edn & args]
  (with-open [db (tc/db *conn*)]
    (set (apply tc/q db query-edn args))))

(defn q-ordered
  "Open a DB, run query, close DB, return results as a vector (preserves order)."
  ([query-edn] (q-ordered *conn* query-edn))
  ([conn query-edn]
   (with-open [db (tc/db conn)]
     (vec (tc/q db query-edn)))))

;; ---------------------------------------------------------------------------
;; Tests — triple patterns
;; ---------------------------------------------------------------------------

(deftest test-sanity-check
  (tc/transact *conn* [{:name "Ivan"}])
  (is (= 1 (count (q '{:find [?e]
                       :where [[?e :name "Ivan"]]})))))

(deftest test-basic-query
  (tc/transact *conn* [{:name "Ivan" :last-name "Ivanov"}
                       {:name "Petr" :last-name "Petrov"}])

  (testing "Can query value by single field"
    (is (= #{["Ivan"]} (q '{:find [?name]
                            :where [[?e :name "Ivan"]
                                    [?e :name ?name]]})))
    (is (= #{["Petr"]} (q '{:find [?name]
                            :where [[?e :name "Petr"]
                                    [?e :name ?name]]}))))

  (testing "Can query entity by single field"
    (is (= 1 (count (q '{:find [?e]
                         :where [[?e :name "Ivan"]]}))))
    (is (= 1 (count (q '{:find [?e]
                         :where [[?e :name "Petr"]]})))))

  (testing "Can query using multiple terms"
    (is (= #{["Ivan" "Ivanov"]} (q '{:find [?name ?last-name]
                                     :where [[?e :name ?name]
                                             [?e :last-name ?last-name]
                                             [?e :name "Ivan"]
                                             [?e :last-name "Ivanov"]]}))))

  (testing "Negate query based on subsequent non-matching clause"
    (is (= #{} (q '{:find [?e]
                    :where [[?e :name "Ivan"]
                            [?e :last-name "Ivanov-does-not-match"]]}))))

  (testing "Can query for multiple results"
    (is (= #{["Ivan"] ["Petr"]}
           (q '{:find [?name] :where [[?e :name ?name]]}))))

  (tc/transact *conn* [{:name "Smith" :last-name "Smith"}])

  (testing "Can query across fields for same value"
    (is (= 1 (count (q '{:find [?p1] :where [[?p1 :name ?name]
                                             [?p1 :last-name ?name]]})))))

  (testing "Can query across fields for same value when value is passed in"
    (is (= 1 (count (q '{:find [?p1] :where [[?p1 :name ?name]
                                             [?p1 :last-name ?name]
                                             [?p1 :name "Smith"]]}))))))

(deftest test-multiple-results
  (tc/transact *conn* [{:name "Ivan" :last-name "1"}
                       {:name "Ivan" :last-name "2"}])

  (is (= 2
         (count (q '{:find [?e] :where [[?e :name "Ivan"]]})))))

(deftest test-query-using-keywords
  (tc/transact *conn* [{:name "Ivan" :sex :male}
                       {:name "Petr" :sex :male}
                       {:name "Doris" :sex :female}
                       {:name "Jane" :sex :female}])

  (testing "Can query by single field"
    (is (= #{["Ivan"] ["Petr"]} (q '{:find [?name]
                                     :where [[?e :name ?name]
                                             [?e :sex :male]]})))
    (is (= #{["Doris"] ["Jane"]} (q '{:find [?name]
                                      :where [[?e :name ?name]
                                              [?e :sex :female]]})))))

(deftest test-query-across-entities-using-join
  (tc/transact *conn* [{:name "Ivan" :age 30 :salary 50000}
                       {:name "Petr" :age 25 :salary 45000}
                       {:name "Sergei" :age 35 :salary 55000}
                       {:name "Denis" :age 28 :salary 48000}
                       {:name "Denis" :age 32 :salary 52000}])

  (testing "Five people, without a join"
    (is (= 5 (count (q '{:find [?p1]
                         :where [[?p1 :name ?name]
                                 [?p1 :age ?age]
                                 [?p1 :salary ?salary]]}))))))

;; ---------------------------------------------------------------------------
;; Tests — or
;; ---------------------------------------------------------------------------

(deftest test-or-query
  (tc/transact *conn* [{:name "Ivan" :last-name "Ivanov"}
                       {:name "Ivan" :last-name "Ivanov"}
                       {:name "Ivan" :last-name "Ivannotov"}
                       {:name "Bob" :last-name "Controlguy"}])

  (testing "Or works as expected"
    (is (= 3 (count (q '{:find [?e]
                         :where [[?e :name ?name]
                                 [?e :name "Ivan"]
                                 (or [?e :last-name "Ivanov"]
                                     [?e :last-name "Ivannotov"])]}))))

    (is (= 4 (count (q '{:find [?e]
                         :where [(or [?e :last-name "Ivanov"]
                                     [?e :last-name "Ivannotov"]
                                     [?e :last-name "Controlguy"])]}))))

    (is (= 0 (count (q '{:find [?e]
                         :where [(or [?e :last-name "Controlguy"])
                                 (or [?e :last-name "Ivanov"]
                                     [?e :last-name "Ivannotov"])]}))))

    (is (= 0 (count (q '{:find [?e]
                         :where [(or [?e :last-name "Ivanov"])
                                 (or [?e :last-name "Ivannotov"])]}))))

    (is (= 0 (count (q '{:find [?e]
                         :where [[?e :last-name "Controlguy"]
                                 (or [?e :last-name "Ivanov"]
                                     [?e :last-name "Ivannotov"])]}))))

    (is (= 3 (count (q '{:find [?e]
                         :where [[?e :name ?name]
                                 (or [?e :last-name "Ivanov"]
                                     [?e :name "Bob"])]})))))

  (testing "Or edge case - can take a single clause"
    (is (= 2 (count (q '{:find [?e]
                         :where [[?e :name ?name]
                                 [?e :name "Ivan"]
                                 (or [?e :last-name "Ivanov"])]}))))))

(deftest test-or-query-can-use-and
  (tc/transact *conn* [{:name "Ivan" :sex :male}
                       {:name "Bob" :sex :male}
                       {:name "Ivana" :sex :female}])

  (is (= #{["Ivan"]
           ["Ivana"]}
         (q '{:find [?name]
              :where [[?e :name ?name]
                      (or [?e :sex :female]
                          (and [?e :sex :male]
                               [?e :name "Ivan"]))]})))

  (is (= 1 (count (q '{:find [?e]
                       :where [(or [?e :name "Ivan"])]}))))

  (is (= #{}
         (q '{:find [?name]
              :where [[?e :name ?name]
                      (or (and [?e :sex :female]
                               [?e :name "Ivan"]))]}))))

(deftest test-ors-must-use-same-vars
  (is (thrown? TriploxException
               (q '{:find [?e]
                    :where [[?e :name ?name]
                            (or [?e1 :last-name "Ivanov"]
                                [?e2 :last-name "Ivanov"])]}))))

;; ---------------------------------------------------------------------------
;; Tests — not
;; ---------------------------------------------------------------------------

(deftest test-not-query
  (tc/transact *conn* [{:name "Ivan" :last-name "Ivanov"}
                       {:name "Ivan" :last-name "Ivanov"}
                       {:name "Ivan" :last-name "Ivannotov"}])

  (testing "literal v"
    (is (= 1 (count (q '{:find [?e]
                         :where [[?e :name ?name]
                                 [?e :name "Ivan"]
                                 (not [?e :last-name "Ivanov"])]}))))
    (is (= 1 (count (q '{:find [?e]
                         :where [[?e :name ?name]
                                 (not [?e :last-name "Ivanov"])]}))))

    (is (= 1 (count (q '{:find [?e]
                         :where [[?e :name "Ivan"]
                                 (not [?e :last-name "Ivanov"])]}))))

    (is (= 2 (count (q '{:find [?e]
                         :where [[?e :name ?name]
                                 [?e :name "Ivan"]
                                 (not [?e :last-name "Ivannotov"])]}))))

    (testing "multiple clauses in not"
      (is (= 2 (count (q '{:find [?e]
                           :where [[?e :name ?name]
                                   [?e :name "Ivan"]
                                   (not [?e :last-name "Ivannotov"]
                                        [?e :name "Ivan"])]}))))
      ;; string?/number? type predicates commented out — needs type predicate support
      #_(is (= 2 (count (q '{:find [?e]
                             :where [[?e :name ?name]
                                     [?e :name "Ivan"]
                                     (not [?e :last-name "Ivannotov"]
                                          [(string? ?name)])]}))))

      #_(is (= 3 (count (q '{:find [?e]
                             :where [[?e :name ?name]
                                     [?e :name "Ivan"]
                                     (not [?e :last-name "Ivannotov"]
                                          [(number? ?name)])]}))))

      (is (= 3 (count (q '{:find [?e]
                           :where [[?e :name ?name]
                                   [?e :name "Ivan"]
                                   (not [?e :last-name "Ivannotov"]
                                        [?e :name "Bob"])]}))))))

  (testing "variable v"
    (is (= 0 (count (q '{:find [?e]
                         :where [[?e :name ?name]
                                 [?e :name "Ivan"]
                                 (not [?e :name ?name])]}))))

    (is (= 0 (count (q '{:find [?e]
                         :where [[?e :name ?name]
                                 (not [?e :name ?name])]}))))

    ;; Use a join to find the entity with last-name "Ivannotov" and exclude
    ;; entities sharing that last-name
    (is (= 2 (count (q '{:find [?e]
                         :where [[?e :name ?name]
                                 [?ref :last-name "Ivannotov"]
                                 [?ref :last-name ?i-name]
                                 (not [?e :last-name ?i-name])]})))))

  (testing "literal entities — use discovered entity IDs"
    ;; Discover the entity ID for one of the "Ivan"/"Ivanov" entities
    (let [ivan-id (ffirst (q '{:find [?e]
                               :where [[?e :name "Ivan"]
                                       [?e :last-name "Ivanov"]]}))]
      ;; Exclude entities whose :name matches Ivan's :name
      (is (= 0 (count (q '{:find [?e]
                           :in [?ivan-id]
                           :where [[?e :name ?name]
                                   (not [?ivan-id :name ?name])]}
                         ivan-id)))))

    ;; Discover entity with last-name "Ivannotov"
    (let [ivannotov-id (ffirst (q '{:find [?e]
                                    :where [[?e :last-name "Ivannotov"]]}))]
      ;; Only entities whose last-name differs from Ivannotov's
      (is (= 2 (count (q '{:find [?e]
                           :in [?ivannotov-id]
                           :where [[?e :last-name ?last-name]
                                   (not [?ivannotov-id :last-name ?last-name])]}
                         ivannotov-id))))))

  (testing "not can come before positive clauses"
    (is (= 2 (count (q '{:find [?e]
                         :where [(not [?e :last-name "Ivannotov"])
                                 [?e :name ?name]
                                 [?e :name "Ivan"]]}))))))

;; ---------------------------------------------------------------------------
;; Tests — predicates & functions
;; ---------------------------------------------------------------------------

(deftest test-predicate-expression
  (tc/transact *conn* [{:name "Ivan" :last-name "Ivanov" :age 30}
                       {:name "Bob" :last-name "Ivanov" :age 40}
                       {:name "Dominic" :last-name "Monroe" :age 50}])

  (testing "range expressions"
    (is (= #{["Ivan"] ["Bob"]}
           (q '{:find [?name]
                :where [[?e :name ?name]
                        [?e :age ?age]
                        [(< ?age 50)]]})))

    (is (= #{["Dominic"]}
           (q '{:find [?name]
                :where [[?e :name ?name]
                        [?e :age ?age]
                        [(>= ?age 50)]]})))

    (testing "fallback to built in predicate for vars"
      (is (= #{["Ivan" 30 "Ivan" 30]
               ["Ivan" 30 "Bob" 40]
               ["Ivan" 30 "Dominic" 50]
               ["Bob" 40 "Bob" 40]
               ["Bob" 40 "Dominic" 50]
               ["Dominic" 50 "Dominic" 50]}
             (q '{:find [?name ?age1 ?name2 ?age2]
                  :where [[?e :name ?name]
                          [?e :age ?age1]
                          [?e2 :name ?name2]
                          [?e2 :age ?age2]
                          [(<= ?age1 ?age2)]]})))))

  ;; re-find tests commented out — see triplox-bwi for tracking
  #_(testing "re-find predicate"
      (is (= #{["Bob"] ["Dominic"]}
             (q '{:find [?name]
                  :where [[?e :name ?name]
                          [(re-find #"o" ?name)]]})))

      (testing "No results"
        (is (empty? (q '{:find [?name]
                         :where [[?e :name ?name]
                                 [(re-find #"X" ?name)]]}))))

      (testing "Not predicate"
        (is (= #{["Ivan"]}
               (q '{:find [?name]
                    :where [[?e :name ?name]
                            (not [(re-find #"o" ?name)])]})))))

  (testing "Entity variable"
    ;; Discover Ivan's entity ID, then use it in a predicate
    (let [ivan-id (ffirst (q '{:find [?e]
                               :where [[?e :name "Ivan"]]}))]
      (is (= #{["Ivan"]}
             (q '{:find [?name]
                  :in [?ivan-id]
                  :where [[?e :name ?name]
                          [(= ?ivan-id ?e)]]}
                ivan-id))))

    (testing "Filtered by value"
      (is (= 2 (count (q '{:find [?e]
                           :where [[?e :last-name ?last-name]
                                   [(= "Ivanov" ?last-name)]]}))))

      (is (= 1 (count (q '{:find [?e]
                           :where [[?e :last-name ?last-name]
                                   [?e :age ?age]
                                   [(= "Ivanov" ?last-name)]
                                   [(= 30 ?age)]]}))))))


  ;; re-find tests commented out — see triplox-bwi
  #_(testing "Several variables with re-find"
      (is (= #{["Bob"]}
             (q '{:find [?name]
                  :where [[?e :name ?name]
                          [?e :age ?age]
                          [(= 40 ?age)]
                          [(re-find #"o" ?name)]
                          [(not= ?age ?name)]]})))

      (is (= #{[1001 "Ivanov"]}
             (q '{:find [?e ?last-name]
                  :where [[?e :last-name ?last-name]
                          [?e :age ?age]
                          [(re-find #"ov$" ?last-name)]
                          (not [(= ?age 30)])]})))

      (testing "No results"
        (is (= #{}
               (q '{:find [?name]
                    :where [[?e :name ?name]
                            [?e :age ?age]
                            [(re-find #"o" ?name)]
                            [(= ?age ?name)]]})))))

  (testing "Bind result to var"
    (is (= #{["Dominic" 25] ["Ivan" 15] ["Bob" 20]}
           (q '{:find [?name ?half-age]
                :where [[?e :name ?name]
                        [?e :age ?age]
                        [(quot ?age 2) ?half-age]]})))

    (testing "Order of joins is rearranged to ensure arguments are bound"
      (is (= #{["Dominic" 25] ["Ivan" 15] ["Bob" 20]}
             (q '{:find [?name ?half-age]
                  :where [[?e :name ?name]
                          [?e :age ?real-age]
                          [(quot ?real-age 2) ?half-age]]}))))

    ;; Commented out — requires negative number literals in expressions (triplox-qns)
    #_(testing "Binding more than once intersects result"
        (is (= #{["Ivan" 15]}
               (q '{:find [?name ?half-age]
                    :where [[?e :name ?name]
                            [?e :age ?real-age]
                            [(quot ?real-age 2) ?half-age]
                            [(+ ?real-age -15) ?half-age]]}))))

    (testing "Binding can use range predicates"
      (is (= #{["Dominic" 25]}
             (q '{:find [?name ?half-age]
                  :where [[?e :name ?name]
                          [?e :age ?real-age]
                          [(quot ?real-age 2) ?half-age]
                          [(> ?half-age 20)]]}))))))

;; ---------------------------------------------------------------------------
;; Tests — additive transact
;; ---------------------------------------------------------------------------

(deftest datomic-style-addition
  (tc/transact *conn* [{:name "Alice"}])

  (is (= #{["Alice"]} (q '{:find [?name]
                           :where [[?p :name ?name]]})))

  ;; Discover entity ID, then add :city to the same entity
  (let [alice-id (ffirst (q '{:find [?e] :where [[?e :name "Alice"]]}))]
    (tc/transact *conn* [{:db/id alice-id :city "NYC"}])

    (is (= #{["Alice" "NYC"]} (q '{:find [?name ?city]
                                   :where [[?p :name ?name]
                                           [?p :city ?city]]})))))

(deftest test-upsert-with-resolved-entity-id
  (tc/transact *conn* [{:name "Alice" :age 30}])

  ;; Discover auto-assigned entity ID
  (let [alice-id (ffirst (q '{:find [?e] :where [[?e :name "Alice"]]}))]
    ;; Upsert: update age using the discovered entity ID
    (tc/transact *conn* [{:db/id alice-id :age 31}])

    ;; Verify: Alice should now have age 31 (cardinality-one retracted 30)
    (is (= #{["Alice" 31]} (q '{:find [?name ?age]
                                :where [[?e :name ?name]
                                        [?e :age ?age]]})))))

;; ---------------------------------------------------------------------------
;; Tests — aggregates
;; ---------------------------------------------------------------------------

(deftest test-aggregates-and-or
  (tc/transact *conn* [{:name "Ada" :last-name "Lovelace" :sex :female :age 21}
                       {:name "Alan" :last-name "Turing" :sex :male :age 22}
                       {:name "Adam" :last-name "Smith" :sex :male :age 23}])

  (is (= #{[1]} (q '{:find [(count ?p)]
                     :where [[?p :last-name "Lovelace"]
                             (or [?p :name "Ada"]
                                 [?p :sex :male])]})))

  (is (= #{[1]} (q '{:find [(count ?p)]
                     :where [[?p :last-name "Lovelace"]
                             (or [?p :name "Ada"]
                                 [?p :sex :female])]})))

  (is (= #{[3]} (q '{:find [(count ?p)]
                     :where [(or [?p :last-name "Lovelace"]
                                 [?p :sex :male])]})))

  (is (= #{[:male 2 45] [:female 1 21]}
         (q '{:find [?gender (count ?p) (sum ?age)]
              :where [[?p :sex ?gender]
                      [?p :age ?age]]}))
      "implicit grouping"))

(deftest test-aggregate-set-semantics
  (tc/transact *conn* [{:name "Alice" :city "NYC"}
                       {:name "Bob" :city "NYC"}
                       {:name "Carol" :city "LA"}])

  ;; TODO: do we want Datomic or XTDB semantics here?
  (is (= #{[3]} (q '{:find [(count ?city)]
                     :where [[?p :city ?city]]}))))

(deftest test-datascript-aggregates
  (tc/transact *conn* [{:db/ident :heads
                        :db/valueType :db.type/long
                        :db/cardinality :db.cardinality/one}])
  (tc/transact *conn* [{:db/id "cerberus" :heads 3}
                       {:db/id "medusa" :heads 1}
                       {:db/id "cyclops" :heads 1}
                       {:db/id "chimera" :heads 1}])

  (testing "Multiple aggregates, correct grouping"
    (is (= #{[6 1 3 4 2]}
           (q '{:find [(sum ?heads) (min ?heads) (max ?heads) (count ?heads) (count-distinct ?heads)]
                :where [[?monster :heads ?heads]]})))))

(deftest test-aggregate-avg
  (tc/transact *conn* [{:age 21}
                       {:age 22}
                       {:age 23}])

  (is (= #{[22.0]} (q '{:find [(avg ?age)]
                        :where [[?e :age ?age]]}))))

(deftest test-aggregate-min-max-strings
  (tc/transact *conn* [{:name "Charlie"}
                       {:name "Alice"}
                       {:name "Bob"}])

  (is (= #{["Alice" "Charlie"]}
         (q '{:find [(min ?name) (max ?name)]
              :where [[?e :name ?name]]}))))

(deftest test-aggregate-empty-result
  (is (= #{[0]} (q '{:find [(count ?e)]
                     :where [[?e :name "nobody"]]}))))

;; ---------------------------------------------------------------------------
;; Tests — lookup refs
;; ---------------------------------------------------------------------------

(deftest test-lookup-ref-in-entity-position
  (tc/transact *conn* [{:db/ident :lookup-id
                        :db/valueType :db.type/string
                        :db/cardinality :db.cardinality/one
                        :db/unique :db.unique/identity}])
  (tc/transact *conn* [{:lookup-id "alice" :name "Alice" :age 30}])
  (tc/transact *conn* [[:db/add [:lookup-id "alice"] :city "NYC"]])

  (is (= #{["Alice" "NYC"]}
         (q '{:find [?name ?city]
              :where [[?e :name ?name]
                      [?e :city ?city]]}))))

(deftest test-lookup-ref-in-value-position
  (tc/transact *conn* [{:db/ident :friend
                        :db/valueType :db.type/ref
                        :db/cardinality :db.cardinality/one}])
  (tc/transact *conn* [{:db/ident :lookup-id
                        :db/valueType :db.type/string
                        :db/cardinality :db.cardinality/one
                        :db/unique :db.unique/identity}])
  (tc/transact *conn* [{:lookup-id "alice" :name "Alice" :age 30}
                       {:lookup-id "bob" :name "Bob" :age 25}])

  (tc/transact *conn* [[:db/add [:lookup-id "bob"] :friend [:lookup-id "alice"]]])
  (is (= #{["Bob" "Alice"]}
         (q '{:find [?name ?friend-name]
              :where [[?e :name ?name]
                      [?e :friend ?f]
                      [?f :name ?friend-name]]}))))

;; ---------------------------------------------------------------------------
;; Tests — order-by & limit
;; ---------------------------------------------------------------------------

(deftest test-order-by-and-limit
  (tc/transact *conn* [{:name "Alice" :age 30}
                       {:name "Bob" :age 20}
                       {:name "Carol" :age 40}
                       {:name "Dave" :age 10}
                       {:name "Eve" :age 50}])

  (testing "order ascending with limit"
    (is (= [["Dave" 10] ["Bob" 20] ["Alice" 30]]
           (q-ordered '{:find [?name ?age]
                        :where [[?e :name ?name] [?e :age ?age]]
                        :order [[?age :asc]]
                        :limit 3}))))

  (testing "order descending with limit"
    (is (= [["Eve" 50] ["Carol" 40]]
           (q-ordered '{:find [?name ?age]
                        :where [[?e :name ?name] [?e :age ?age]]
                        :order [[?age :desc]]
                        :limit 2}))))

  (testing "limit only (no order)"
    (is (= 2 (count (q-ordered '{:find [?name ?age]
                                 :where [[?e :name ?name] [?e :age ?age]]
                                 :limit 2})))))

  (testing "order only (no limit)"
    (let [result (q-ordered '{:find [?name ?age]
                              :where [[?e :name ?name] [?e :age ?age]]
                              :order [[?age :asc]]})]
      (is (= 5 (count result)))
      (is (= ["Dave" 10] (first result)))
      (is (= ["Eve" 50] (last result))))))

(deftest test-query-with-arguments
  (tc/transact *conn* [{:name "Ivan" :last-name "Ivanov"}
                       {:name "Petr" :last-name "Petrov"}])

  (testing "Can query entity by single field"
    (is (= #{["Ivan" "Ivanov"]}
           (q '{:find [?name ?last-name]
                :in [?name]
                :where [[?e :name ?name]
                        [?e :last-name ?last-name]]}
              "Ivan")))

    (is (= #{["Petr" "Petrov"]}
           (q '{:find [?name ?last-name]
                :in [?name]
                :where [[?e :name ?name]
                        [?e :last-name ?last-name]]}
              "Petr"))))

  (testing "Can query entity by entity position"
    ;; Collection binding on entity position requires the entity id.
    ;; Re-enable these tests after something like https://github.com/FiV0/triplox/issues/58
    #_
    (is (= #{["Ivan"]
             ["Petr"]}
           (q '{:find [?name]
                :in [[?e ...]]
                :where [[?e :name ?name]]}
              [:ivan :petr])))

    #_
    (is (= #{["Ivan" "Ivanov"]
             ["Petr" "Petrov"]}
           (q '{:find [?name ?last-name]
                :in [[?e ...]]
                :where [[?e :name ?name]
                        [?e :last-name ?last-name]]}
              [:ivan :petr]))))

  (testing "Can match on both entity and value position"
    (is (= #{["Ivan" "Ivanov"]}
           (q '{:find [?name ?last-name]
                :in [?name ?last-name]
                :where [[?e :name ?name]
                        [?e :last-name ?last-name]]}
              "Ivan" "Ivanov")))

    ;; Inline `:args` clause — not yet supported.
    #_
    (is (= #{}
           (q '{:find [?name]
                :in [?e ?name]
                :where [[?e :name ?name]]
                :args [{:e :ivan :name "Petr"}]}
              :ivan "Petr"))))

  (testing "Can query entity by single field with several arguments"
    (is (= #{["Ivan"] ["Petr"]}
           (q '{:find [?name]
                :in [[?name ...]]
                :where [[?e :name ?name]]}
              ["Ivan" "Petr"]))))

  (testing "Can query entity by single field with literals"
    (is (= #{["Ivan"]}
           (q '{:find [?name]
                :in [[?name ...]]
                :where [[?e :name ?name]
                        [?e :last-name "Ivanov"]]}
              ["Ivan" "Petr"]))))

  (testing "Can query entity by non existent argument"
    (is (= #{}
           (q '{:find [?name]
                :in [?name]
                :where [[?e :name ?name]]}
              "Bob"))))

  (testing "Can query entity with empty arguments"
    (is (= #{["Ivan"] ["Petr"]}
           (q '{:find [?name]
                :in []
                :where [[?e :name ?name]]}))))

  (testing "Can query entity with tuple arguments"
    ;; Tuple binding `[[?name ?last-name]]` — not yet supported.
    #_
    (is (= #{["Ivan"]}
           (q '{:find [?name]
                :in [[?name ?last-name]]
                :where [[?e :name ?name]
                        [?e :last-name ?last-name]]}
              ["Ivan" "Ivanov"]))))

  (testing "Can query entity with collection arguments"
    ;; Relation binding `[[[?name ?last-name]]]` — not yet supported.
    #_
    (is (= #{["Ivan"] ["Petr"]}
           (q '{:find [?name]
                :in [[[?name ?last-name]]]
                :where [[?e :name ?name]
                        [?e :last-name ?last-name]]}
              [["Ivan" "Ivanov"] ["Petr" "Petrov"]]))))

  (testing "Can query predicates based on arguments alone"
    ;; Inline `:args` clause — not yet supported.
    #_
    (is (= #{["Ivan"]}
           (q '{:find [?name]
                :where [[(re-find #"I" ?name)]]
                :args [{:name "Ivan"} {:name "Petr"}]})))

    ;; Collection binding — not yet supported.
    #_
    (is (= #{["Ivan"] ["Petr"]}
           (q '{:find [?name]
                :in [[?name ...]]
                :where [[(string? ?name)]]}
              ["Ivan" "Petr"])))

    ;; Range constraints on a scalar `:in` binding — supported.
    (testing "Can use range constraints on arguments"
      (is (= #{}
             (q '{:find [?age]
                  :in [?age]
                  :where [[(>= ?age 21)]]}
                20)))

      (is (= #{[22]}
             (q '{:find [?age]
                  :in [?age]
                  :where [[(>= ?age 21)]]}
                22))))))
