;; Vendored and slightly modified from:
;; https://github.com/tonsky/datascript/blob/100ab864f55e056df5837e77d44dfd0f8a447983/test/datascript/test/query.cljc
;; Copyright © 2014–2025 Nikita Prokopov.
;; Licensed under the Eclipse Public License 1.0; see LICENSES/EPL-1.0.txt.

(ns xyz.triplox.datascript.query
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
   {:db/ident :aka :db/valueType :db.type/string :db/cardinality :db.cardinality/many}
   {:db/ident :person/name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :s :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :a :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :b :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :c :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])

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

(deftest test-joins
  (d/transact *conn* [{:db/id 1, :name  "Ivan", :age   15}
                      {:db/id 2, :name  "Petr", :age   37}
                      {:db/id 3, :name  "Ivan", :age   37}
                      {:db/id 4, :age 15}])
  (is (= (q '[:find ?e
              :where [?e :name _]])
          #{[1] [2] [3]}))
  (is (= (q '[:find  ?e ?v
              :where [?e :name "Ivan"]
              [?e :age ?v]])
          #{[1 15] [3 37]}))
  (is (= (q '[:find  ?e1 ?e2
              :where [?e1 :name ?n]
              [?e2 :name ?n]])
          #{[1 1] [2 2] [3 3] [1 3] [3 1]}))
  (is (= (q '[:find  ?e ?e2 ?n
              :where [?e :name "Ivan"]
              [?e :age ?a]
              [?e2 :age ?a]
              [?e2 :name ?n]])
          #{[1 1 "Ivan"]
            [3 3 "Ivan"]
            [3 2 "Petr"]})))

(deftest test-q-many
  (d/transact *conn* [[:db/add 1 :name "Ivan"]
                      [:db/add 1 :aka  "ivolga"]
                      [:db/add 1 :aka  "pi"]
                      [:db/add 2 :name "Petr"]
                      [:db/add 2 :aka  "porosenok"]
                      [:db/add 2 :aka  "pi"]])
  (is (= (q '[:find  ?n1 ?n2
              :where [?e1 :aka ?x]
              [?e2 :aka ?x]
              [?e1 :name ?n1]
              [?e2 :name ?n2]])
          #{["Ivan" "Ivan"]
            ["Petr" "Petr"]
            ["Ivan" "Petr"]
            ["Petr" "Ivan"]})))

(deftest test-q-coll
  (let [db [[1 :name "Ivan"]
            [1 :age  19]
            [1 :aka  "dragon_killer_94"]
            [1 :aka  "-=autobot=-"]]]
    (is (= (d/q '[:find  ?n ?a
                  :where [?e :aka "dragon_killer_94"]
                  [?e :name ?n]
                  [?e :age  ?a]] db)
          #{["Ivan" 19]})))

  (testing "Query over long tuples"
    (let [db [[1 :name "Ivan" 945 :db/add]
              [1 :age  39     999 :db/retract]]]
      (is (= (d/q '[:find  ?e ?v
                    :where [?e :name ?v]] db)
            #{[1 "Ivan"]}))
      (is (= (d/q '[:find  ?e ?a ?v ?t
                    :where [?e ?a ?v ?t :db/retract]] db)
            #{[1 :age 39 999]})))))

(deftest test-q-in
  (d/transact *conn* [{:db/id 1, :name  "Ivan", :age   15}
                      {:db/id 2, :name  "Petr", :age   37}
                      {:db/id 3, :name  "Ivan", :age   37}])
  (let [query '{:find  [?e]
                :in    [?attr ?value]
                :where [[?e ?attr ?value]]}]
    (is (= (q query :name "Ivan")
          #{[1] [3]}))
    (is (= (q query :age 37)
          #{[2] [3]}))

    (testing "Named DB"
      (is (= (q '[:find  ?a ?v
                  :in    ?e
                  :where [?e ?a ?v]] 1)
            #{[:name "Ivan"]
              [:age 15]})))

    (testing "DB join with collection"
      (is (= (q '[:find  ?e ?email
                  :in    [[?n ?email]]
                  :where [?e :name ?n]]
               [["Ivan" "ivan@mail.ru"]
                ["Petr" "petr@gmail.com"]])
            #{[1 "ivan@mail.ru"]
              [2 "petr@gmail.com"]
              [3 "ivan@mail.ru"]})))
    
    (testing "Query without DB"
      (is (= (q '[:find ?a ?b
                  :in   ?a ?b]
             10 20)
            #{[10 20]})))

    (is (thrown-msg? "Extra inputs passed, expected: [], got: 1"
          (q '[:find ?e :where [(inc 1) ?e]])))

    (is (thrown-msg? "Too few inputs passed, expected: [$ $2], got: 1"
          (q '[:find ?e :in $2 :where [$2 ?e]])))

    (is (thrown-msg? "Extra inputs passed, expected: [$], got: 2"
          (q '[:find ?e :where [?e]] (d/db *conn*))))

    (is (thrown-msg? "Extra inputs passed, expected: [$ $2], got: 3"
          (q '[:find ?e :in $2 :where [?e]] (d/db *conn*) (d/db *conn*))))))

(deftest test-bindings
  (d/transact *conn* [{:db/id 1, :name  "Ivan", :age   15}
                      {:db/id 2, :name  "Petr", :age   37}
                      {:db/id 3, :name  "Ivan", :age   37}])
  (testing "Relation binding"
    (is (= (q '[:find  ?e ?email
                :in    [[?n ?email]]
                :where [?e :name ?n]]
               [["Ivan" "ivan@mail.ru"]
                ["Petr" "petr@gmail.com"]])
          #{[1 "ivan@mail.ru"]
            [2 "petr@gmail.com"]
            [3 "ivan@mail.ru"]})))

  (testing "Tuple binding"
    (is (= (q '[:find  ?e
                :in    [?name ?age]
                :where [?e :name ?name]
                [?e :age ?age]]
             ["Ivan" 37])
          #{[3]})))

  (testing "Collection binding"
    (is (= (q '[:find  ?attr ?value
                :in    ?e [?attr ...]
                :where [?e ?attr ?value]]
             1 [:name :age])
          #{[:name "Ivan"] [:age 15]})))

  (testing "Empty coll handling"
    (is (= (d/q '[:find ?id
                  :in $ [?id ...]
                  :where [?id :age _]]
             [[1 :name "Ivan"]
              [2 :name "Petr"]]
             [])
          #{}))
    (is (= (d/q '[:find ?id
                  :in $ [[?id]]
                  :where [?id :age _]]
             [[1 :name "Ivan"]
              [2 :name "Petr"]]
             [])
          #{})))
    
  (testing "Placeholders"
    (is (= (d/q '[:find ?x ?z
                  :in [?x _ ?z]]
             [:x :y :z])
          #{[:x :z]}))
    (is (= (d/q '[:find ?x ?z
                  :in [[?x _ ?z]]]
             [[:x :y :z] [:a :b :c]])
          #{[:x :z] [:a :c]})))
    
  (testing "Error reporting"
    (is (thrown-with-msg? TriploxException #"Cannot bind value :a to tuple \[\?a \?b\]"
          (d/q '[:find ?a ?b :in [?a ?b]] :a)))
    (is (thrown-with-msg? TriploxException #"Cannot bind value :a to collection \[\?a \.\.\.\]"
          (d/q '[:find ?a :in [?a ...]] :a)))
    (is (thrown-with-msg? TriploxException #"Not enough elements in a collection \[:a\] to bind tuple \[\?a \?b\]"
          (d/q '[:find ?a ?b :in [?a ?b]] [:a])))))
        
(deftest test-nested-bindings
  (is (= (d/q '[:find  ?k ?v
                :in    [[?k ?v] ...]
                :where [(> ?v 1)]]
           {:a 1, :b 2, :c 3})
        #{[:b 2] [:c 3]}))

  (is (= (d/q '[:find  ?k ?min ?max
                :in    [[?k ?v] ...] ?minmax
                :where [(?minmax ?v) [?min ?max]]
                [(> ?max ?min)]]
           {:a [1 2 3 4]
            :b [5 6 7]
            :c [3]}
           #(vector (reduce min %) (reduce max %)))
        #{[:a 1 4] [:b 5 7]}))

  (is (= (d/q '[:find  ?k ?x
                :in    [[?k [?min ?max]] ...] ?range
                :where [(?range ?min ?max) [?x ...]]
                [(even? ?x)]]
           {:a [1 7]
            :b [2 4]}
           range)
        #{[:a 2] [:a 4] [:a 6]
          [:b 2]})))

(deftest test-built-in-regex
  (is (= (d/q '[:find  ?name
                :in    [?name ...] ?key
                :where [(re-pattern ?key) ?pattern]
                [(re-find ?pattern ?name)]]
           #{"abc" "abcX" "aXb"}
           "X")
        #{["abcX"] ["aXb"]})))

(deftest test-built-in-get
  (is (= (d/q '[:find ?m ?m-value
                :in [[?k ?m] ...] ?m-key
                :where [(get ?m ?m-key) ?m-value]]
           {:a {:b 1}
            :c {:d 2}}
           :d)
        #{[{:d 2} 2]})))

(deftest ^{:doc "issue-385"} test-join-unrelated
  (d/transact *conn* [{:person/name "Joe"}])
  (is (= #{}
        (q '[:find ?name
             :in ?my-fn
             :where [?e :person/name ?name]
             [(?my-fn) ?result]
             [(< ?result 3)]]
           (fn [] 5)))))

(deftest ^{:doc "issue-425"} test-symbol-comparison
  (is (= [2]
        (d/q
          '[:find [?e ...]
            :where [?e :s b]]
          '[[1 :s a]
            [2 :s b]])))
  (d/transact *conn* '[{:db/id 1, :s a}
                       {:db/id 2, :s b}])
  (is (= [2]
        (q '[:find [?e ...]
             :where [?e :s b]]))))

(deftest ^{:doc "issue-462"} test-constant-substitution
  (d/transact *conn*
    (for [eid  (range 1 11)
          attr [:a :b :c]]
      [:db/add eid attr (str eid (name attr))]))
  (let [cnt+q (fn [query db & sources]
                (let [*cnt (volatile! 0)
                      res  (set (apply d/q db query sources))]
                  [@*cnt res]))
        db (d/db *conn*)]
    (is (= [1 #{["5b"]}] (cnt+q '[:find ?v :where [5 :b ?v]] db)))
    (is (= [1 #{[:b]}]   (cnt+q '[:find ?a :where [5 ?a "5b"]] db)))
    (is (= [1 #{[5]}]    (cnt+q '[:find ?e :where [?e :b "5b"]] db)))
    (is (= [1 #{[5 :b "5b"]}] (cnt+q '[:find ?e ?a ?v :in ?e ?a :where [?e ?a ?v]] db 5 :b)))
    (is (= [2 #{[5 :b "5b"]}] (cnt+q '[:find ?e2 ?a ?v :in ?a ?v :where [?e ?a ?v] [?e2 ?a ?v]] db :b "5b")))
    (is (= [3 #{[:a "5a"] [:b "5b"] [:c "5c"]}] (cnt+q '[:find ?a ?v :in ?e :where [?e ?a ?v]] db 5)))
    (is (= [1 #{[5 :b]}] (cnt+q '[:find ?e ?a :where [?e ?a "5b"]] db)))
    (is (= [1 #{[5 :b]}] (cnt+q '[:find ?e ?a :in ?v :where [?e ?a ?v]] db "5b")))
    (is (= [1 #{[5 :b]}] (cnt+q '[:find ?e ?a :in [?v ...] :where [?e ?a ?v]] db ["5b"])))
    (is (= [1 #{[5 :b]}] (cnt+q '[:find ?e ?a :where [(ground "5b") ?v] [?e ?a ?v]] db)))))
