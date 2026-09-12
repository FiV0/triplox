(ns job.queries-test
  (:require [clojure.test :refer [deftest is]]
            [job.queries :as q]))

(deftest complete-corpus
  (is (= 113 (count q/queries) (count q/expected)))
  (is (= (set (keys q/queries)) (set (keys q/expected)))))

(deftest query-translations
  (doseq [engine [:triplox :datomic :datalevin]
          id (keys q/queries)]
    (is (= :find (first (q/translate id engine)))))
  (is (some #{'(not [?mc :movie-companies/note _])} (q/translate "11a" :triplox)))
  (is (= '(if (<= 1950 ?year) (if (<= ?year 2000) true false) false)
         (q/expression '(<= 1950 ?year 2000) :triplox))))

(deftest patterns-and-datomic-predicates
  (is (q/like? "a\nb" "%a_b%"))
  (is (q/like? "(film)" "%(film)%"))
  (is (not (q/like? "film" "%(film)%")))
  (is (not (q/like? "prefixFilm" "Film%")))
  (is (q/predicate "(and (< ?x \"3.5\") (like ?title \"%Film%\"))"
                   "[?x ?title]" "2.0" "A Film"))
  (is (not (q/predicate "(in ?x [\"cast\" \"crew\"])" "[?x]" "actor"))))

(deftest datomic-variables-are-valid-compiler-locals
  (doseq [id (keys q/queries)]
    (is (not-any? #(and (symbol? %) (.contains (str %) "."))
                  (remove #{'job.queries/predicate}
                          (tree-seq coll? seq (q/translate id :datomic)))))))
