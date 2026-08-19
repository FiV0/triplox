;; Vendored and slightly modified from:
;; https://github.com/tonsky/datascript/blob/100ab864f55e056df5837e77d44dfd0f8a447983/test/datascript/test/query_fns.cljc
;; Copyright © 2014–2025 Nikita Prokopov.
;; Licensed under the Eclipse Public License 1.0; see LICENSES/EPL-1.0.txt.

(ns xyz.triplox.datascript.query-fns
  (:require
    [clojure.test :as t :refer [is are deftest testing use-fixtures]]
    [xyz.triplox.api :as d]
    [xyz.triplox.datascript.test-util])
  (:import
    [xyz.triplox.client TriploxException]))

(def ^:dynamic *conn* nil)

(def schema
  [{:db/ident :name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :age :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :height :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :salary :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :parent :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/ident :pair :db/valueType :db.type/vector :db/cardinality :db.cardinality/one}
   {:db/ident :pred :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :weight :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])

(defn connect []
  (let [host (System/getProperty "triplox.host" "localhost")
        port (Integer/parseInt (System/getProperty "triplox.port" "5490"))]
    (d/connect host port)))

(defn with-conn [f]
  (with-open [conn (connect)]
    (binding [*conn* conn]
      (f))))

(defn with-schema [f]
  (d/transact *conn* schema)
  (f))

(use-fixtures :each with-conn with-schema)

(defn q [query-edn & args]
  (set (apply d/q (d/db *conn*) query-edn args)))

(deftest test-query-fns
  (testing "predicate without free variables"
    (is (= (d/q '[:find ?x
                  :in [?x ...]
                  :where [(> 2 1)]] [:a :b :c])
          #{[:a] [:b] [:c]})))

  (d/transact *conn* [{:db/id 1, :name  "Ivan",  :age   15}
                      {:db/id 2, :name  "Petr",  :age   22, :height 240, :parent 1}
                      {:db/id 3, :name  "Slava", :age   37, :parent 2}])
  (let [db (d/db *conn*)]

    (testing "ground"
      (is (= (q '[:find ?vowel
                  :where [(ground [:a :e :i :o :u]) [?vowel ...]]])
            #{[:a] [:e] [:i] [:o] [:u]})))

    (testing "get-else"
      (is (= (q '[:find ?e ?age ?height
                  :where [?e :age ?age]
                  [(get-else $ ?e :height 300) ?height]])
            #{[1 15 300] [2 22 240] [3 37 300]}))
      
      (is (thrown-with-msg? TriploxException #"get-else: nil default value is not supported"
            (q '[:find ?e ?height
                 :where [?e :age _]
                 [(get-else $ ?e :height nil) ?height]]))))

    (testing "get-some"
      (is (= (q '[:find ?e ?a ?v
                  :where [?e :name _]
                  [(get-some $ ?e :height :age) [?a ?v]]])
            #{[1 :age 15]
              [2 :height 240]
              [3 :age 37]})))

    (testing "missing?"
      (is (= (q '[:find ?e ?age
                  :where [?e :age ?age]
                  [(missing? $ ?e :height)]])
            #{[1 15] [3 37]})))

    (testing "missing? back-ref"
      (is (= (q '[:find ?e
                  :where [?e :age ?age]
                  [(missing? $ ?e :_parent)]])
            #{[3]})))

    (testing "Built-ins"
      (is (= (q '[:find  ?e1 ?e2
                  :where [?e1 :age ?a1]
                  [?e2 :age ?a2]
                  [(< ?a1 18 ?a2)]])
            #{[1 2] [1 3]}))
      (is (= (q '[:find  ?a1
                  :where [_ :age ?a1]
                  [(< ?a1 22)]])
            #{[15]}))
      (is (= (q '[:find  ?a1
                  :where [_ :age ?a1]
                  [(<= ?a1 22)]])
            #{[15] [22]}))
      (is (= (q '[:find  ?a1
                  :where [_ :age ?a1]
                  [(> ?a1 22)]])
            #{[37]}))
      (is (= (q '[:find  ?a1
                  :where [_ :age ?a1]
                  [(>= ?a1 22)]])
            #{[22] [37]}))

      (testing "compare values of different types"
        (is (= (d/q '[:find  ?e
                      :where [?e]
                      [(< ?e 1)]] [[0] [1] [""]])
              #{[0]}))
        (is (= (d/q '[:find  ?e
                      :where [?e]
                      [(<= ?e 1)]] [[0] [1] [""]])
              #{[0] [1]}))
        (is (= (d/q '[:find  ?e
                      :where [?e]
                      [(> ?e 1)]] [[0] [1] [""]])
              #{[""]}))
        (is (= (d/q '[:find  ?e
                      :where [?e]
                      [(>= ?e 1)]] [[0] [1] [""]])
              #{[1] [""]})))
      
      (is (= (d/q '[:find  ?x ?c
                    :in    [?x ...]
                    :where [(count ?x) ?c]]
               ["a" "abc"])
            #{["a" 1] ["abc" 3]})))

    (testing "Built-in vector, hashmap"
      (is (= (q '[:find [?tx-data ...]
                  :where
                  [(ground :db/add) ?op]
                  [(vector ?op -1 :attr 12) ?tx-data]])
            [[:db/add -1 :attr 12]]))

      (is (= (q '[:find [?tx-data ...]
                  :where
                  [(hash-map :db/id -1 :age 92 :name "Aaron") ?tx-data]])
            [{:db/id -1 :age 92 :name "Aaron"}])))

    (testing "Passing predicate as source"
      (is (= (q '[:find  ?e
                  :in    ?adult
                  :where [?e :age ?a]
                  [(?adult ?a)]]
               #(> % 18))
            #{[2] [3]})))

    (testing "Calling a function"
      (is (= (q '[:find  ?e1 ?e2 ?e3
                  :where [?e1 :age ?a1]
                  [?e2 :age ?a2]
                  [?e3 :age ?a3]
                  [(+ ?a1 ?a2) ?a12]
                  [(= ?a12 ?a3)]])
            #{[1 2 3] [2 1 3]})))

    (testing "Two conflicting function values for one binding."
      (is (= (q '[:find  ?n
                  :where
                  [(identity 1) ?n]
                  [(identity 2) ?n]])
            #{})))

    (testing "Destructured conflicting function values for two bindings."
      (is (= (q '[:find  ?n ?x
                  :where
                  [(identity [3 4]) [?n ?x]]
                  [(identity [1 2]) [?n ?x]]])
            #{})))

    (testing "Rule bindings interacting with function binding. (fn, rule)"
      (is (= (q '[:find  ?n
                  :in %
                  :where
                  [(identity 2) ?n]
                  (my-vals ?n)]
               '[[(my-vals ?x)
                  [(identity 1) ?x]]
                 [(my-vals ?x)
                  [(identity 2) ?x]]
                 [(my-vals ?x)
                  [(identity 3) ?x]]])
            #{[2]})))

    (testing "Rule bindings interacting with function binding. (rule, fn)"
      (is (= (q '[:find  ?n
                  :in %
                  :where (my-vals ?n)
                  [(identity 2) ?n]]
               '[[(my-vals ?x)
                  [(identity 1) ?x]]
                 [(my-vals ?x)
                  [(identity 2) ?x]]
                 [(my-vals ?x)
                  [(identity 3) ?x]]])
            #{[2]})))

    (testing "Conflicting relational bindings with function binding. (rel, fn)"
      (is (= (q '[:find  ?age
                  :where [_ :age ?age]
                  [(identity 100) ?age]])
            #{})))

    (testing "Conflicting relational bindings with function binding. (fn, rel)"
      (is (= (q '[:find  ?age
                  :where [(identity 100) ?age]
                  [_ :age ?age]])
            #{})))

    (testing "Function on empty rel"
      (is (= (d/q '[:find  ?e ?y
                    :where [?e :salary ?x]
                    [(+ ?x 100) ?y]]
               [[0 :age 15] [1 :age 35]])
            #{})))
    
    (testing "Returning nil from function filters out tuple from result"
      (is (= (d/q '[:find ?x
                    :in    [?in ...] ?f
                    :where [(?f ?in) ?x]]
               [1 2 3 4]
               #(when (even? %) %))
            #{[2] [4]})))

    (testing "Result bindings"
      (is (= (d/q '[:find ?a ?c
                    :in ?in
                    :where [(ground ?in) [?a _ ?c]]]
               [:a :b :c])
            #{[:a :c]}))

      (is (= (d/q '[:find ?in
                    :in ?in
                    :where [(ground ?in) _]]
               :a)
            #{[:a]}))

      (is (= (d/q '[:find ?x ?z
                    :in ?in
                    :where [(ground ?in) [[?x _ ?z]...]]]
               [[:a :b :c] [:d :e :f]])
            #{[:a :c] [:d :f]}))
      
      (is (= (d/q '[:find ?in
                    :in [?in ...]
                    :where [(ground ?in) _]]
               [])
            #{})))))

;; issue-490
(deftest test-fn-call-results-unification
  (is (= #{[[:a :a] :a]}
        (d/q '[:find ?pair ?x
               :in $ ?first ?second
               :where
               [_ _ ?pair]
               [(?first  ?pair) ?x]
               [(?second ?pair) ?x]]
          [[1 :pair [:a :a]]
           [2 :pair [:b :c]]]
          first
          second))))

(deftest test-predicates
  (let [entities [{:db/id 1 :name "Ivan" :age 10}
                  {:db/id 2 :name "Ivan" :age 20}
                  {:db/id 3 :name "Oleg" :age 10}
                  {:db/id 4 :name "Oleg" :age 20}]]
    (d/transact *conn* entities)
    (are [query res] (= (q (quote query)) res)
      ;; plain predicate
      [:find  ?e ?a
       :where [?e :age ?a]
       [(> ?a 10)]]
      #{[2 20] [4 20]}

      ;; join in predicate
      [:find  ?e ?e2
       :where [?e  :name _]
       [?e2 :name _]
       [(< ?e ?e2)]]
      #{[1 2] [1 3] [1 4] [2 3] [2 4] [3 4]}
         
      ;; join with extra symbols
      [:find  ?e ?e2
       :where [?e  :age ?a]
       [?e2 :age ?a2]
       [(< ?e ?e2)]]
      #{[1 2] [1 3] [1 4] [2 3] [2 4] [3 4]}

      ;; empty result
      [:find  ?e ?e2
       :where [?e  :name "Ivan"]
       [?e2 :name "Oleg"]
       [(= ?e ?e2)]]
      #{}

      ;; pred over const, true
      [:find  ?e
       :where [?e :name "Ivan"]
       [?e :age 20]
       [(= ?e 2)]]
      #{[2]}

      ;; pred over const, false
      [:find  ?e
       :where [?e :name "Ivan"]
       [?e :age 20]
       [(= ?e 1)]]
      #{})
    (let [db (d/db *conn*)
          pred (fn [db e a]
                 (= a (ffirst (d/q db '[:find ?age
                                         :in ?e
                                         :where [?e :age ?age]] e))))]
      (is (= (q '[:find ?e
                  :in ?pred
                  :where [?e :age ?a]
                  [(?pred $ ?e 10)]] pred)
            #{[1] [3]})))))

(deftest test-exceptions
  (is (thrown-msg? "Unknown predicate 'fun in [(fun ?e)]"
        (d/q '[:find ?e
               :in   [?e ...]
               :where [(fun ?e)]]
          [1])))
  
  (is (thrown-msg? "Unknown function 'fun in [(fun ?e) ?x]"
        (d/q '[:find ?e ?x
               :in   [?e ...]
               :where [(fun ?e) ?x]]
          [1])))

  (is (thrown-msg? "Insufficient bindings: #{?x} not bound in [(zero? ?x)]"
        (q '[:find ?x
             :where [(zero? ?x)]])))

  (is (thrown-msg? "Insufficient bindings: #{?x} not bound in [(inc ?x) ?y]"
        (q '[:find ?x
             :where [(inc ?x) ?y]])))

  (is (thrown-msg? "Where uses unknown source vars: [$2]"
        (q '[:find ?x
             :where [?x] [(zero? $2 ?x)]])))

  (is (thrown-msg? "Where uses unknown source vars: [$]"
        (q '[:find  ?x
             :in    $2
             :where [$2 ?x] [(zero? $ ?x)]]))))

(deftest test-issue-180
  (d/transact *conn* [[:db/add 1 :age 20]])
  (is (= #{}
        (q '[:find ?e ?a
             :where [_ :pred ?pred]
             [?e :age ?a]
             [(?pred ?a)]]))))

(defn sample-query-fn []
  42)

#?(:clj
   (deftest test-symbol-resolution
     (is (= 42 (q '[:find ?x .
                    :where [(xyz.triplox.datascript.query-fns/sample-query-fn) ?x]])))))

(deftest test-issue-445
  (d/transact *conn* [{:db/id 1 :name "Ivan" :age 15}
                      {:db/id 2 :name "Petr" :age 22 :height 240}])
  (testing "get-else using lookup ref"
    (is (= "Unknown"
          (q '[:find ?height .
               :in ?e
               :where [(get-else $ ?e :height "Unknown") ?height]]
             [:name "Ivan"]))))

  (testing "get-some using lookup ref"
    (is (= #{[[:name "Petr"] :age 22]}
          (q '[:find ?e ?a ?v
               :in ?e
               :where [(get-some $ ?e :weight :age :height) [?a ?v]]]
             [:name "Petr"])))))
