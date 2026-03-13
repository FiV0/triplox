(ns triplox.integration.query-test
  (:require [clojure.test :as t :refer [deftest is testing use-fixtures]]
            [triplox.api :as tc])
  (:import (io.triplox.client TriploxException)))

;; ---------------------------------------------------------------------------
;; Fixtures & helpers
;; ---------------------------------------------------------------------------

(def ^:dynamic *conn* nil)

(def people-schema
  [{:db/id 200 :db/ident :name :db/valueType :db.type/string}
   {:db/id 201 :db/ident :last-name :db/valueType :db.type/string}
   {:db/id 202 :db/ident :sex :db/valueType :db.type/keyword}
   {:db/id 203 :db/ident :age :db/valueType :db.type/long}
   {:db/id 204 :db/ident :salary :db/valueType :db.type/long}
   {:db/id 205 :db/ident :city :db/valueType :db.type/string}])

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
  "Open a DB, run query, close DB, return results as a set."
  ([query-edn] (q *conn* query-edn))
  ([conn query-edn]
   (with-open [db (tc/db conn)]
     (set (tc/q db query-edn)))))

;; ---------------------------------------------------------------------------
;; Tests — triple patterns
;; ---------------------------------------------------------------------------

(deftest test-sanity-check
  (tc/transact *conn* [{:db/id 1000 :name "Ivan"}])
  (is (= #{[1000]} (q '{:find [?e]
                       :where [[?e :name "Ivan"]]}))))

(deftest test-basic-query
  (tc/transact *conn* [{:db/id 1000 :name "Ivan" :last-name "Ivanov"}
                       {:db/id 1001 :name "Petr" :last-name "Petrov"}])

  (testing "Can query value by single field"
    (is (= #{["Ivan"]} (q '{:find [?name]
                            :where [[?e :name "Ivan"]
                                    [?e :name ?name]]})))
    (is (= #{["Petr"]} (q '{:find [?name]
                            :where [[?e :name "Petr"]
                                    [?e :name ?name]]}))))

  (testing "Can query entity by single field"
    (is (= #{[1000]} (q '{:find [?e]
                         :where [[?e :name "Ivan"]]})))
    (is (= #{[1001]} (q '{:find [?e]
                         :where [[?e :name "Petr"]]}))))

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

  (tc/transact *conn* [{:db/id 1002 :name "Smith" :last-name "Smith"}])

  (testing "Can query across fields for same value"
    (is (= #{[1002]}
           (q '{:find [?p1] :where [[?p1 :name ?name]
                                    [?p1 :last-name ?name]]}))))

  (testing "Can query across fields for same value when value is passed in"
    (is (= #{[1002]}
           (q '{:find [?p1] :where [[?p1 :name ?name]
                                    [?p1 :last-name ?name]
                                    [?p1 :name "Smith"]]})))))

(deftest test-multiple-results
  (tc/transact *conn* [{:db/id 1000 :name "Ivan" :last-name "1"}
                       {:db/id 1001 :name "Ivan" :last-name "2"}])

  (is (= 2
         (count (q '{:find [?e] :where [[?e :name "Ivan"]]})))))

(deftest test-query-using-keywords
  (tc/transact *conn* [{:db/id 1000 :name "Ivan" :sex :male}
                       {:db/id 1001 :name "Petr" :sex :male}
                       {:db/id 1002 :name "Doris" :sex :female}
                       {:db/id 1003 :name "Jane" :sex :female}])

  (testing "Can query by single field"
    (is (= #{["Ivan"] ["Petr"]} (q '{:find [?name]
                                     :where [[?e :name ?name]
                                             [?e :sex :male]]})))
    (is (= #{["Doris"] ["Jane"]} (q '{:find [?name]
                                      :where [[?e :name ?name]
                                              [?e :sex :female]]})))))

(deftest test-query-across-entities-using-join
  (tc/transact *conn* [{:db/id 1000 :name "Ivan" :age 30 :salary 50000}
                       {:db/id 1001 :name "Petr" :age 25 :salary 45000}
                       {:db/id 1002 :name "Sergei" :age 35 :salary 55000}
                       {:db/id 1003 :name "Denis" :age 28 :salary 48000}
                       {:db/id 1004 :name "Denis" :age 32 :salary 52000}])

  (testing "Five people, without a join"
    (is (= 5 (count (q '{:find [?p1]
                         :where [[?p1 :name ?name]
                                 [?p1 :age ?age]
                                 [?p1 :salary ?salary]]}))))))

;; ---------------------------------------------------------------------------
;; Tests — or
;; ---------------------------------------------------------------------------

(deftest test-or-query
  (tc/transact *conn* [{:db/id 1000 :name "Ivan" :last-name "Ivanov"}
                       {:db/id 1001 :name "Ivan" :last-name "Ivanov"}
                       {:db/id 1002 :name "Ivan" :last-name "Ivannotov"}
                       {:db/id 1003 :name "Bob" :last-name "Controlguy"}])

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
  (tc/transact *conn* [{:db/id 1000 :name "Ivan" :sex :male}
                       {:db/id 1001 :name "Bob" :sex :male}
                       {:db/id 1002 :name "Ivana" :sex :female}])

  (is (= #{["Ivan"]
           ["Ivana"]}
         (q '{:find [?name]
              :where [[?e :name ?name]
                      (or [?e :sex :female]
                          (and [?e :sex :male]
                               [?e :name "Ivan"]))]})))

  (is (= #{[1000]}
         (q '{:find [?e]
              :where [(or [?e :name "Ivan"])]})))

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
  (tc/transact *conn* [{:db/id 1000 :name "Ivan" :last-name "Ivanov"}
                       {:db/id 1001 :name "Ivan" :last-name "Ivanov"}
                       {:db/id 1002 :name "Ivan" :last-name "Ivannotov"}])

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

    (is (= 2 (count (q '{:find [?e]
                         :where [[?e :name ?name]
                                 [1002 :last-name ?i-name]
                                 (not [?e :last-name ?i-name])]})))))

  (testing "literal entities"
    (is (= 0 (count (q '{:find [?e]
                         :where [[?e :name ?name]
                                 (not [1000 :name ?name])]}))))

    (is (= 1 (count (q '{:find [?e]
                         :where [[?e :last-name ?last-name]
                                 (not [1000 :last-name ?last-name])]})))))

  (testing "not can come before positive clauses"
    (is (= 2 (count (q '{:find [?e]
                         :where [(not [?e :last-name "Ivannotov"])
                                 [?e :name ?name]
                                 [?e :name "Ivan"]]}))))))

;; ---------------------------------------------------------------------------
;; Tests — predicates & functions
;; ---------------------------------------------------------------------------

(deftest test-predicate-expression
  (tc/transact *conn* [{:db/id 1000 :name "Ivan" :last-name "Ivanov" :age 30}
                       {:db/id 1001 :name "Bob" :last-name "Ivanov" :age 40}
                       {:db/id 1002 :name "Dominic" :last-name "Monroe" :age 50}])

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
    (is (= #{["Ivan"]}
           (q '{:find [?name]
                :where [[?e :name ?name]
                        [(= 1000 ?e)]]})))

    (testing "Filtered by value"
      (is (= #{[1001] [1000]}
             (q '{:find [?e]
                  :where [[?e :last-name ?last-name]
                          [(= "Ivanov" ?last-name)]]})))

      (is (= #{[1000]}
             (q '{:find [?e]
                  :where [[?e :last-name ?last-name]
                          [?e :age ?age]
                          [(= "Ivanov" ?last-name)]
                          [(= 30 ?age)]]})))))

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
  (tc/transact *conn* [{:db/id 1000 :name "Alice"}])

  (is (= #{["Alice"]} (q '{:find [?name]
                           :where [[?p :name ?name]]})))

  (tc/transact *conn* [{:db/id 1000 :city "NYC"}])

  (is (= #{["Alice" "NYC"]} (q '{:find [?name ?city]
                                 :where [[?p :name ?name]
                                         [?p :city ?city]]}))))
