;; Vendored and slightly modified from:
;; https://github.com/tonsky/datascript/blob/100ab864f55e056df5837e77d44dfd0f8a447983/test/datascript/test/query_not.cljc
;; Copyright © 2014–2025 Nikita Prokopov.
;; Licensed under the Eclipse Public License 1.0; see LICENSES/EPL-1.0.txt.

(ns xyz.triplox.datascript.query-not
  (:require [clojure.test :as t :refer [is are deftest testing use-fixtures]]
            [xyz.triplox.api :as d]
            [xyz.triplox.integration.query-test :as qt :refer [*conn*]]
            [xyz.triplox.datascript.test-util]))

(def schema
  [{:db/ident :id :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :age :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])

(def test-data
  [{:id 1 :name "Ivan" :age 10}
   {:id 2 :name "Ivan" :age 20}
   {:id 3 :name "Oleg" :age 10}
   {:id 4 :name "Oleg" :age 20}
   {:id 5 :name "Ivan" :age 10}
   {:id 6 :name "Ivan" :age 20}])

(use-fixtures :each qt/with-conn (qt/with-schema schema))

(defn q [query-edn & args]
  (let [res (apply d/q (d/db *conn*) query-edn args)]
    (println (type res))
    (println res)
    (set res)))

(deftest test-not
  (d/transact *conn* test-data)
  (are [clauses res] (= res
                        (q (vec (concat '[:find ?id :where] (quote clauses)))))
    ;; TODO placeholders
    [[?e :id ?id]
     (not [?e :name "Ivan"])]
    #{[3] [4]}

    [[?e :id ?id]
     (not
      [?e :name "Ivan"]
      [?e :age  10])]
    #{[2] [3] [4] [6]}

    [[?e :id ?id]
     (not [?e :name "Ivan"])
     (not [?e :age 10])]
    #{[4]}

    #_#_ ;; TODO need placeholders
    ;; full exclude
    [[?e :id ?id]
     (not [?e :age _])]
    #{}

    ;; not-intersecting rels
    [[?e :name "Ivan"]
     [?e :id ?id]
     (not [?e :name "Oleg"])]
    #{[1] [2] [5] [6]}

    ;; exclude empty set
    [[?e :id ?id]
     (not [?e :name "Ivan"]
          [?e :name "Oleg"])]
    #{[4] [6] [3] [5] [2] [1]}

    ;; nested excludes
    [[?e :id ?id]
     (not [?e :name "Ivan"]
          (not [?e :age 10]))]
    #{[1] [3] [4] [5]}

    #_#_
    ;; TODO
    ;; extra binding in not
    [[?e :name ?a]
     (not [?e :age ?f]
          [?e :age 10])]
    #{2 4 6}))

;; TODO not-join
#_
(deftest test-not-join
  (d/transact *conn* test-data)
  (are [clauses res] (= res (q (vec (concat '[:find ?e ?a :where] (quote clauses)))))
    [[?e :name _]
     [?e :age  ?a]
     (not-join [?e]
               [?e :name "Oleg"]
               [?e :age ?a])]
    #{[1 10] [2 20] [5 10] [6 20]}

    [[?e :age  ?a]
     [?e :age  10]
     (not-join [?e]
               [?e :name "Oleg"]
               [?e :age  ?a]
               [?e :age  10])]
    #{[1 10] [5 10]}

    ;; issue-481
    [[?e :age ?a]
     (not-join [?a]
               [?e :name "Petr"]
               [?e :age ?a])]
    #{[1 10] [2 20] [3 10] [4 20] [5 10] [6 20]}))

;; We currently only support a single source.
#_
(deftest test-default-source
  (d/transact *conn* [[:db/add 1 :name "Ivan"]
                      [:db/add 2 :name "Oleg"]])
  (let [db1 (d/db *conn*)]
    (d/transact *conn* [[:db/add 1 :age 10]
                        [:db/add 2 :age 20]])
    (let [db2 (d/db *conn*)]
      (are [clauses res] (= (set (d/q db1
                                      (concat '[:find [?e ...]
                                                :in   $2
                                                :where]
                                              (quote clauses))
                                      db2))
                            res)
        ;; NOT inherits default source
        [[?e :name _]
         (not [?e :name "Ivan"])]
        #{2}

        ;; NOT can reference any source
        [[?e :name _]
         (not [$2 ?e :age 10])]
        #{2}

        ;; NOT can change default source
        [[?e :name _]
         ($2 not [?e :age 10])]
        #{2}

        ;; even with another default source, it can reference any other source explicitly
        [[?e :name _]
         ($2 not [$ ?e :name "Ivan"])]
        #{2}

        ;; nested NOT keeps the default source
        [[?e :name _]
         ($2 not (not [?e :age 10]))]
        #{1}

        ;; can override nested NOT source
        [[?e :name _]
         ($2 not ($ not [?e :name "Ivan"]))]
        #{1}))))

(deftest test-impl-edge-cases
  (d/transact *conn* test-data)
  (are [query res] (= (q (quote query))
                      res)
    ;; const \ empty
    [:find ?id
     :where [?e :id ?id]
     [?e :name "Oleg"]
     [?e :age  10]
     (not [?e :age 20])]
    #{[3]}

    ;; const \ const
    [:find ?id
     :where [?e :id ?id]
     [?e :name "Oleg"]
     [?e :age  10]
     (not [?e :age 10])]
    #{}

    ;; rel \ const
    [:find ?id
     :where [?e :id ?id]
     [?e :name "Oleg"]
     (not [?e :age 10])]
    #{[4]}

    ;; 2 rels \ 2 rels
    [:find ?id ?id2
     :where [?e :id ?id]
     [?e2 :id ?id2]
     [?e  :name "Ivan"]
     [?e2 :name "Ivan"]
     (not [?e :age 10]
          [?e2 :age 20])]
    #{[2 1] [6 5] [1 1] [2 2] [5 5] [6 6] [2 5] [1 5] [2 6] [6 1] [5 1] [6 2]}

    ;; 2 rels \ rel + const
    [:find ?id ?id2
     :where [?e :id ?id]
     [?e2 :id ?id2]
     [?e  :name "Ivan"]
     [?e2 :name "Oleg"]
     (not [?e :age 10]
          [?e2 :age 20])]
    #{[2 3] [1 3] [2 4] [6 3] [5 3] [6 4]}

    ;; 2 rels \ 2 consts
    [:find ?id ?id2
     :where [?e :id ?id]
     [?e2 :id ?id2]
     [?e  :name "Oleg"]
     [?e2 :name "Oleg"]
     (not [?e :age 10]
          [?e2 :age 20])]
    #{[4 3] [3 3] [4 4]}))

(deftest test-insufficient-bindings
  (d/transact *conn* test-data)
  (are [clauses msg] (thrown-msg? msg
                                  (q (vec (concat '[:find ?e :where] (quote clauses)))))
    #_#_
    [[?e :name _]
     (not-join [?e]
               (not [1 :age ?a])
               [?e :age ?a])]
    "Insufficient bindings: none of #{?a} is bound in (not [1 :age ?a])"

    [[?e :name ?placeholder]
     (not [?a :name "Ivan"])]
    "Variable ?a in NOT clause is not bound by positive clauses"
    #_"Insufficient bindings: none of #{?a} is bound in (not [?a :name \"Ivan\"])"))
