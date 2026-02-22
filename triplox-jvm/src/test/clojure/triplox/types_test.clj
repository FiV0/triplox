(ns triplox.types-test
  (:require [clojure.test :refer [deftest is testing]]
            [triplox.types :as types])
  (:import [java.util TreeMap ArrayList]
           [io.triplox.client DataTypeCodec$TaggedTuple]))

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

(deftest wire->clj-tagged-tuple
  (testing "TaggedTuple → Clojure vector"
    (let [tuple (DataTypeCodec$TaggedTuple. (ArrayList. [42 true]))]
      (is (= [42 true] (types/wire->clj tuple))))))

(deftest clj->wire-map
  (testing "Clojure map → TreeMap with string keys"
    (let [tm (types/clj->wire {:name "alice" :age 30})]
      (is (instance? TreeMap tm))
      (is (= "alice" (.get ^TreeMap tm "name")))
      (is (= 30 (.get ^TreeMap tm "age"))))))

(deftest clj->wire-vector
  (testing "Clojure vector → ArrayList"
    (let [al (types/clj->wire [1 2 3])]
      (is (instance? ArrayList al))
      (is (= [1 2 3] (vec al))))))

(deftest clj->wire-primitives
  (testing "Primitives pass through unchanged"
    (is (= 42 (types/clj->wire 42)))
    (is (= "hello" (types/clj->wire "hello")))
    (is (= true (types/clj->wire true)))))

(deftest roundtrip-map
  (testing "Map round-trip: clj → wire → clj"
    (let [original {:person/name "alice" :person/age 30}
          wire (types/clj->wire original)
          back (types/wire->clj wire)]
      (is (= original back)))))
