(ns job.data-test
  (:require [clojure.test :refer [deftest is]]
            [job.data :as data]
            [job.source :as source])
  (:import [java.io StringReader]
           [job CSVReader]))

(deftest csv-escaping
  (let [reader (CSVReader. (StringReader. "1,\"a,\\\"b\\\"\nc\",\r\n2,plain,x\n"))]
    (is (= ["1" "a,\"b\"\nc" ""] (.next reader)))
    (is (= ["2" "plain" "x"] (.next reader)))
    (is (not (.hasNext reader)))))

(deftest portable-references
  (let [rows [{:job/id 20 :movie-companies/movie 80 :movie-companies/note "note"}]
        tx (data/transaction rows)]
    (is (= [{:db/id "20" :job/id 20 :movie-companies/movie "80" :movie-companies/note "note"}
            {:db/id "80" :job/id 80}] tx))))

(deftest transaction-size-bound
  (let [rows (mapv #(hash-map :job/id % :title/title (apply str (repeat 100 "a"))) (range 8))
        batches (data/split-batches rows 8 400)]
    (is (= rows (vec (mapcat identity batches))))
    (is (every? #(<= (data/encoded-size (data/transaction %)) 400) batches)))
  (is (thrown? Exception (doall (data/split-batches [{:job/id 1 :title/title "large"}] 1 1)))))

(deftest source-mapping
  (let [load-title (second (first (filter #(= :title (first %)) source/tables)))]
    (is (= [[80000001 :title/title "Movie"] [80000001 :title/kind 21]
            [80000001 :title/production-year 2000]]
           (vec (load-title (StringReader. "1,Movie,,1,2000,,,,,,\n")))))))
