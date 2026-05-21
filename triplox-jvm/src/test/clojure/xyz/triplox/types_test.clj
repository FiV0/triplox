(ns xyz.triplox.types-test
  (:require [clojure.test :refer [deftest is testing]]
            [xyz.triplox.types :as types])
  (:import [java.util TreeMap ArrayList]))

(deftest wire->clj-primitives
  (testing "Primitives pass through unchanged"
    (is (= 42 (types/wire->clj 42)))
    (is (= "hello" (types/wire->clj "hello")))
    (is (= true (types/wire->clj true)))
    (is (= 3.14 (types/wire->clj 3.14)))))

(deftest wire->clj-treemap
  (testing "TreeMap → Clojure map with keyword keys"
    (let [tm (TreeMap.)]
      (.put tm "name" "alice")
      (.put tm "age" 30)
      (is (= {:name "alice" :age 30} (types/wire->clj tm))))))

(deftest wire->clj-nested-treemap
  (testing "Nested TreeMap"
    (let [inner (TreeMap.)
          outer (TreeMap.)]
      (.put inner "x" 1)
      (.put outer "point" inner)
      (is (= {:point {:x 1}} (types/wire->clj outer))))))

(deftest wire->clj-list
  (testing "Java List → Clojure vector"
    (let [list (ArrayList. [1 "two" true])]
      (is (= [1 "two" true] (types/wire->clj list))))))
