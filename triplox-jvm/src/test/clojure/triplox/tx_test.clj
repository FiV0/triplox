(ns triplox.tx-test
  (:require [clojure.test :refer [deftest is testing]]
            [triplox.tx :as tx])
  (:import [io.triplox.client TxOp$Put TxOp$Add TxOp$Retract TxOp$Delete TxOp$Erase]))

(deftest map-to-put
  (testing "Map → TxOp.Put"
    (let [ops (tx/tx-data->ops [{:db/id 1 :person/name "alice"}])
          op (first ops)]
      (is (instance? TxOp$Put op))
      (is (= 1 (.get (.document ^TxOp$Put op) "db/id")))
      (is (= "alice" (.get (.document ^TxOp$Put op) "person/name"))))))

(deftest vec-to-add
  (testing "[:db/add e a v] → TxOp.Add"
    (let [ops (tx/tx-data->ops [[:db/add 42 :email "test@example.com"]])
          op (first ops)]
      (is (instance? TxOp$Add op))
      (is (= 42 (.entity ^TxOp$Add op)))
      (is (= "email" (.attribute ^TxOp$Add op)))
      (is (= "test@example.com" (.value ^TxOp$Add op))))))

(deftest vec-to-retract
  (testing "[:db/retract e a v] → TxOp.Retract"
    (let [ops (tx/tx-data->ops [[:db/retract 42 :email "old@example.com"]])
          op (first ops)]
      (is (instance? TxOp$Retract op))
      (is (= 42 (.entity ^TxOp$Retract op)))
      (is (= "email" (.attribute ^TxOp$Retract op)))
      (is (= "old@example.com" (.value ^TxOp$Retract op))))))

(deftest vec-to-delete
  (testing "[:db/delete eid] → TxOp.Delete"
    (let [ops (tx/tx-data->ops [[:db/delete 99]])
          op (first ops)]
      (is (instance? TxOp$Delete op))
      (is (= 99 (.entity ^TxOp$Delete op))))))

(deftest vec-to-erase
  (testing "[:db/erase eid] → TxOp.Erase"
    (let [ops (tx/tx-data->ops [[:db/erase 100]])
          op (first ops)]
      (is (instance? TxOp$Erase op))
      (is (= 100 (.entity ^TxOp$Erase op))))))

(deftest mixed-tx-data
  (testing "Mixed tx-data forms"
    (let [ops (tx/tx-data->ops [{:db/id 1 :name "bob"}
                                [:db/add 1 :age 30]
                                [:db/retract 1 :name "old-bob"]
                                [:db/delete 99]
                                [:db/erase 100]])]
      (is (= 5 (count ops)))
      (is (instance? TxOp$Put (nth ops 0)))
      (is (instance? TxOp$Add (nth ops 1)))
      (is (instance? TxOp$Retract (nth ops 2)))
      (is (instance? TxOp$Delete (nth ops 3)))
      (is (instance? TxOp$Erase (nth ops 4))))))

(deftest invalid-form-throws
  (testing "Invalid form throws"
    (is (thrown? clojure.lang.ExceptionInfo
                (tx/tx-data->ops ["not-a-map-or-vector"])))))
