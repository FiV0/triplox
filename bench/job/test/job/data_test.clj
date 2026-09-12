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
            {:db/id "80" :job/id 80}] tx))
    (is (= [[:db/retract [:job/id 20] :movie-companies/movie [:job/id 80]]
            [:db/retract [:job/id 20] :movie-companies/note "note"]]
           (data/retract-rows rows)))))

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

(deftest maintenance-restores-rows-and-honors-byte-budget
  (let [companies (mapv #(hash-map :job/id % :movie-companies/movie (+ % 100)
                                  :movie-companies/note (apply str (repeat 80 "a"))) (range 8))
        titles (mapv #(hash-map :job/id (+ % 100) :title/production-year 2000) (range 8))
        config {:batch-size 4 :max-bytes 400 :cycles 2}
        trace (data/maintenance-trace {:samples {:movie-companies companies :title titles}} config)]
    (is (= trace (data/maintenance-trace {:samples {:movie-companies companies :title titles}} config)))
    (is (every? #(<= (data/encoded-size (:tx-data %)) 400) trace))
    (is (= {:retract 8 :restore 8 :update 8 :restore-year 8}
           (reduce #(update %1 (:kind %2) (fnil + 0) (:source-rows %2)) {} trace)))
    (is (every? #(= 2000 (:title/production-year %))
                (mapcat :tx-data (filter #(= :restore-year (:kind %)) trace))))))
