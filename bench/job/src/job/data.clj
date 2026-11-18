(ns job.data
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [job.source :as source]))

(def schema
  (assoc source/schema :job/id {:db/valueType :db.type/long
                                :db/unique :db.unique/identity}))

(defn ref-attribute? [attribute]
  (= :db.type/ref (get-in schema [attribute :db/valueType])))

(defn schema-tx []
  (mapv (fn [[attribute properties]]
          (merge {:db/ident attribute :db/cardinality :db.cardinality/one}
                 properties))
        (sort-by key schema)))

(defn table-file [directory table]
  (io/file directory (str (str/replace (name table) "-" "_") ".csv")))

(defn reduce-table [directory [table read-triples] f initial]
  (with-open [reader (io/reader (table-file directory table) :encoding "UTF-8")]
    (reduce (fn [state triples]
              (f state (into {:job/id (ffirst triples)}
                             (map (fn [[_ attribute value]] [attribute value]))
                             triples)))
            initial (partition-by first (read-triples reader)))))

(defn transaction [rows]
  (let [entities (into (sorted-map) (map (juxt :job/id identity)) rows)
        references (into (sorted-set)
                         (mapcat (fn [row]
                                   (keep (fn [[a value]] (when (ref-attribute? a) value)) row)))
                         rows)
        entities (reduce #(if (contains? %1 %2) %1 (assoc %1 %2 {:job/id %2}))
                         entities references)]
    (mapv (fn [[id row]]
            (into {:db/id (str id)}
                  (map (fn [[attribute value]]
                         [attribute (if (ref-attribute? attribute) (str value) value)]))
                  row))
          entities)))

(defn load-data!
  "Loads every table, transacting up to `batch-size` source rows at a time.
  `progress`, when given, is an atom reset after each transaction to the table,
  rows and transactions loaded so far, and the last tx."
  [directory {:keys [batch-size]} transact! & [progress]]
  (let [counts (atom {})
        transactions (atom 0)
        datoms (atom 0)
        last-tx (atom nil)]
    (doseq [[[table :as descriptor] index] (map vector source/tables (range))]
      (let [flush! (fn [rows]
                     (let [tx (transaction rows)]
                       (reset! last-tx (transact! tx))
                       (swap! transactions inc)
                       (swap! datoms + (reduce + (map #(dec (count %)) tx)))
                       (when progress
                         (reset! progress {:table table :table-index (inc index)
                                           :tables (count source/tables)
                                           :rows (reduce + (vals @counts))
                                           :transactions @transactions
                                           :last-tx @last-tx}))))
            remaining
            (reduce-table
             directory descriptor
             (fn [pending row]
               (swap! counts update table (fnil inc 0))
               (let [pending (conj pending row)]
                 (if (= batch-size (count pending))
                   (do (flush! pending) []) pending))) [])]
        (when (seq remaining) (flush! remaining))))
    {:table-counts @counts :transactions @transactions :attempted-datoms @datoms
     :last-tx @last-tx}))
