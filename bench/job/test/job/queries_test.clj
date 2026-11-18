(ns job.queries-test
  (:require [clojure.test :refer [deftest is testing]]
            [job.queries :as q]))

(deftest complete-corpus
  (testing "the full Join Order Benchmark has 113 queries, each with an expected result"
    (is (= 113 (count q/queries)))
    (is (= (set (keys q/queries)) (set (keys q/expected))))))

(defn- like? [value pattern]
  (boolean (re-matches (re-pattern (q/like-regex pattern)) value)))

(deftest like-patterns
  (testing "% matches any run of characters, including none"
    (is (like? "Film" "Film%"))
    (is (like? "Film noir" "Film%")))
  (testing "_ matches exactly one character"
    (is (like? "cat" "c_t"))
    (is (not (like? "ct" "c_t")))
    (is (not (like? "coat" "c_t"))))
  (testing "wildcards match newlines"
    (is (like? "a\nb" "a_b"))
    (is (like? "a\n\nb" "a%b")))
  (testing "the pattern must match the whole value"
    (is (not (like? "prefix Film" "Film%"))))
  (testing "matching is case sensitive"
    (is (not (like? "film" "Film%"))))
  (testing "regex metacharacters are literal"
    (is (like? "(film)" "%(film)%"))
    (is (not (like? "film" "%(film)%")))
    (is (like? "\\.^$|?*+()[]{}" "\\.^$|?*+()[]{}"))))

(deftest expressions
  (testing "and/or become if chains"
    (is (= '(if ?a (if ?b true false) false)
           (q/expression '(and ?a ?b))))
    (is (= '(if ?a true (if ?b true false))
           (q/expression '(or ?a ?b)))))
  (testing "in becomes an or over equalities"
    (is (= '(if (= ?x "a") true (if (= ?x "b") true false))
           (q/expression '(in ?x ["a" "b"])))))
  (testing "like/not-like become regexp_like"
    (is (= (list 'regexp_like '?x (q/like-regex "A%"))
           (q/expression '(like ?x "A%"))))
    (is (= (list 'not (list 'regexp_like '?x (q/like-regex "A%")))
           (q/expression '(not-like ?x "A%")))))
  (testing "chained comparisons become a conjunction of pairs"
    (is (= '(if (<= 1950 ?year) (if (<= ?year 2000) true false) false)
           (q/expression '(<= 1950 ?year 2000)))))
  (testing "other forms pass through"
    (is (= '(< 2010 ?year) (q/expression '(< 2010 ?year))))
    (is (= '(not= ?x "a") (q/expression '(not= ?x "a")))))
  (testing "nested forms are translated"
    (is (= (list 'if '(= ?g "m")
                 true
                 (list 'if (list 'if '(= ?g "f")
                                 (list 'if (list 'regexp_like '?n (q/like-regex "B%")) true false)
                                 false)
                       true
                       false))
           (q/expression '(or (= ?g "m")
                              (and (= ?g "f") (like ?n "B%"))))))))

(deftest query-translation
  (is (= '[:find (min ?title)
           :where
           [?t :title/title ?title]
           (not [?t :title/production-year _])
           [(regexp_like ?title "(?s)\\AA.*\\z")]]
         (q/translate-query
          '[:find (min ?title)
            :where
            [?t :title/title ?title]
            [(missing? $ ?t :title/production-year)]
            [(like ?title "A%")]]))))

(def ^:private untranslated-ops '#{and or in like not-like missing?})

(defn- calls [form]
  (filter #(and (seq? %) (symbol? (first %)))
          (tree-seq coll? seq form)))

(deftest corpus-translation
  (testing "every query translates to forms without source-only operators"
    (doseq [id (keys q/queries)
            call (calls (q/translate id))]
      (is (not (untranslated-ops (first call))) [id call])
      (when ('#{< <= > >=} (first call))
        (is (= 3 (count call)) [id call])))))
