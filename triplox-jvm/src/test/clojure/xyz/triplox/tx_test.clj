(ns xyz.triplox.tx-test
  (:require [clojure.test :refer [deftest is testing]]
            [xyz.triplox.tx :as tx])
  (:import [xyz.triplox.client EntityRef$Id EntityRef$TempId EntityRef$Ident EntityRef$LookupRef
            TxOp$Put TxOp$Add TxOp$Retract TxOp$RetractEntity TxOp$Erase]))

(deftest map-to-put
  (testing "Map -> TxOp.Put"
    (let [ops (tx/tx-data->ops [{:db/id 1 :person/name "alice"}])
          op (first ops)]
      (is (instance? TxOp$Put op))
      (let [doc (.document ^TxOp$Put op)]
        (is (= 1 (.get doc ":db/id")))
        (is (= "alice" (.get doc ":person/name")))))))

(deftest vec-to-add
  (testing "[:db/add e a v] -> TxOp.Add"
    (let [ops (tx/tx-data->ops [[:db/add 42 :email "test@example.com"]])
          op (first ops)]
      (is (instance? TxOp$Add op))
      (is (= 42 (.id ^EntityRef$Id (.entity ^TxOp$Add op))))
      (is (= ":email" (.attribute ^TxOp$Add op)))
      (is (= "test@example.com" (.value ^TxOp$Add op))))))

(deftest vec-to-retract
  (testing "[:db/retract e a v] -> TxOp.Retract"
    (let [ops (tx/tx-data->ops [[:db/retract 42 :email "old@example.com"]])
          op (first ops)]
      (is (instance? TxOp$Retract op))
      (is (= 42 (.id ^EntityRef$Id (.entity ^TxOp$Retract op))))
      (is (= ":email" (.attribute ^TxOp$Retract op)))
      (is (= "old@example.com" (.value ^TxOp$Retract op))))))

(deftest vec-to-retract-entity
  (testing "[:db/retractEntity eid] -> TxOp.RetractEntity"
    (let [ops (tx/tx-data->ops [[:db/retractEntity 99]])
          op (first ops)]
      (is (instance? TxOp$RetractEntity op))
      (is (= 99 (.id ^EntityRef$Id (.entity ^TxOp$RetractEntity op)))))))

(deftest vec-to-erase
  (testing "[:db/erase eid] -> TxOp.Erase"
    (let [ops (tx/tx-data->ops [[:db/erase 100]])
          op (first ops)]
      (is (instance? TxOp$Erase op))
      (is (= 100 (.id ^EntityRef$Id (.entity ^TxOp$Erase op)))))))

(deftest mixed-tx-data
  (testing "Mixed tx-data forms"
    (let [ops (tx/tx-data->ops [{:db/id 1 :name "bob"}
                                [:db/add 1 :age 30]
                                [:db/retract 1 :name "old-bob"]
                                [:db/retractEntity 99]
                                [:db/erase 100]])]
      (is (= 5 (count ops)))
      (is (instance? TxOp$Put (nth ops 0)))
      (is (instance? TxOp$Add (nth ops 1)))
      (is (instance? TxOp$Retract (nth ops 2)))
      (is (instance? TxOp$RetractEntity (nth ops 3)))
      (is (instance? TxOp$Erase (nth ops 4))))))

(deftest tempid-entity-ref
  (testing "String entity becomes TempId"
    (let [ops (tx/tx-data->ops [[:db/add "tempid-1" :name "alice"]])
          op (first ops)]
      (is (instance? EntityRef$TempId (.entity ^TxOp$Add op)))
      (is (= "tempid-1" (.tempId ^EntityRef$TempId (.entity ^TxOp$Add op)))))))

(deftest ident-entity-ref
  (testing "Keyword entity becomes Ident"
    (let [ops (tx/tx-data->ops [[:db/add :person/alice :name "Alice"]])
          op (first ops)]
      (is (instance? EntityRef$Ident (.entity ^TxOp$Add op)))
      (is (= ":person/alice" (.ident ^EntityRef$Ident (.entity ^TxOp$Add op)))))))

(deftest lookup-ref-entity-ref
  (testing "Lookup ref vector in entity position becomes LookupRef"
    (let [ops (tx/tx-data->ops [[:db/add [:email "test@example.com"] :name "Alice"]])
          op (first ops)]
      (is (instance? TxOp$Add op))
      (is (instance? EntityRef$LookupRef (.entity ^TxOp$Add op)))
      (is (= ":email" (.attr ^EntityRef$LookupRef (.entity ^TxOp$Add op))))
      (is (= "test@example.com" (.value ^EntityRef$LookupRef (.entity ^TxOp$Add op)))))))

(deftest invalid-form-throws
  (testing "Invalid form throws"
    (is (thrown? clojure.lang.ExceptionInfo
                 (tx/tx-data->ops ["not-a-map-or-vector"])))))
