;; Complete verbatim copy of DataScript's query_return_map.cljc.
;; Source: https://github.com/tonsky/datascript/blob/100ab864f55e056df5837e77d44dfd0f8a447983/test/datascript/test/query_return_map.cljc
;; Copyright © 2014–2025 Nikita Prokopov.
;; Licensed under the Eclipse Public License 1.0; see LICENSES/EPL-1.0.txt.
;; The upstream source is preserved between the markers below without edits.

(ns xyz.triplox.datascript-upstream.query-return-map)

;; Unsupported as an executable Triplox test file:
;; Uses DataScript vector queries and tuple find specifications; Triplox's query entry point accepts map queries.
;; BEGIN VERBATIM DATASCRIPT SOURCE
(comment
(ns datascript.test.query-return-map
  (:require
    [clojure.test :as t :refer [is are deftest testing]]
    [datascript.core :as d]
    [datascript.db :as db]
    [datascript.test.core :as tdc]))

(def *test-db
  (delay
    (d/db-with (d/empty-db)
      [[:db/add 1 :name "Petr"]
       [:db/add 1 :age 44]
       [:db/add 2 :name "Ivan"]
       [:db/add 2 :age 25]
       [:db/add 3 :name "Sergey"]
       [:db/add 3 :age 11]])))

(deftest test-find-specs
  (is (= (d/q '[:find ?name ?age
                :keys n a
                :where [?e :name ?name]
                [?e :age  ?age]]
           @*test-db)
        #{{:n "Petr" :a 44} {:n "Ivan" :a 25} {:n "Sergey" :a 11}}))
  (is (= (d/q '[:find ?name ?age
                :syms n a
                :where [?e :name ?name]
                [?e :age  ?age]]
           @*test-db)
        #{{'n "Petr" 'a 44} {'n "Ivan" 'a 25} {'n "Sergey" 'a 11}}))
  (is (= (d/q '[:find ?name ?age
                :strs n a
                :where [?e :name ?name]
                [?e :age  ?age]]
           @*test-db)
        #{{"n" "Petr" "a" 44} {"n" "Ivan" "a" 25} {"n" "Sergey" "a" 11}}))

  (is (= (d/q '[:find [?name ?age]
                :keys n a
                :where [?e :name ?name]
                [(= ?name "Ivan")]
                [?e :age  ?age]]
           @*test-db)
        {:n "Ivan" :a 25})))


)
;; END VERBATIM DATASCRIPT SOURCE
