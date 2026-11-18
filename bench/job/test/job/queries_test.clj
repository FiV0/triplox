(ns job.queries-test
  (:require [clojure.test :refer [deftest is]]
            [job.queries :as q]))

(deftest complete-corpus
  (is (= 113 (count q/queries) (count q/expected)))
  (is (= (set (keys q/queries)) (set (keys q/expected)))))

(deftest query-translations
  (doseq [id (keys q/queries)]
    (is (= :find (first (q/translate id)))))
  (is (some #(and (seq? %) (= 'not (first %))
                  (= [:movie-companies/note '_] (subvec (second %) 1)))
            (q/translate "11a")))
  (is (= '(if (<= 1950 ?year) (if (<= ?year 2000) true false) false)
         (q/expression '(<= 1950 ?year 2000)))))

(defn- like? [value pattern]
  (boolean (re-matches (re-pattern (q/like-regex pattern)) value)))

(deftest like-patterns
  (is (= "(?s)\\A.*\\(film\\).*\\z" (q/like-regex "%(film)%")))
  (is (like? "\\.^$|?*+()[]{}" "\\.^$|?*+()[]{}"))
  (is (like? "a\nb" "%a_b%"))
  (is (like? "(film)" "%(film)%"))
  (is (not (like? "film" "%(film)%")))
  (is (not (like? "prefixFilm" "Film%"))))
