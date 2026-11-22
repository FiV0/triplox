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

;; It is worth noting that the job/id is unique in the JOB Benchmark. That is the reason
;; we can go upsert against :job/id and use the string of it as a unique tempid identifier.

(defn- entity [triples]
  (into {:job/id (ffirst triples)}
        (map (fn [[_ attribute value]] [attribute value]))
        triples))

(defn reduce-table [directory [table read-triples] xform f init]
  (with-open [reader (io/reader (table-file directory table) :encoding "UTF-8")]
    (transduce (comp (partition-by first) (map entity) xform)
               (completing f) init (read-triples reader))))

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
                  ;; The idea here is that an id will get unified with the tempid for the job/id that gets
                  ;; upserted in the same transaction.
                  (map (fn [[attribute value]] [attribute (if (ref-attribute? attribute) (str value) value)]))
                  row))
          entities)))

(defn load-data!
  "Loads every table, transacting up to `batch-size` source rows at a time.
  `progress`, when given, is an atom reset after each transaction to the table,
  rows and transactions loaded so far, and the last tx."
  [directory {:keys [batch-size]} transact! & [progress]]
  (reduce
   (fn [stats [index [table :as descriptor]]]
     (reduce-table
      directory descriptor (partition-all batch-size)
      (fn [stats rows]
        (let [tx (transaction rows)
              stats (-> stats
                        (update-in [:table-counts table] (fnil + 0) (count rows))
                        (update :transactions inc)
                        (update :attempted-datoms + (transduce (map #(dec (count %))) + tx))
                        (assoc :last-tx (transact! tx)))]
          (when progress
            (reset! progress {:table table
                              :table-index (inc index)
                              :tables (count source/tables)
                              :rows (reduce + (vals (:table-counts stats)))
                              :transactions (:transactions stats)
                              :last-tx (:last-tx stats)}))
          stats))
      stats))
   {:table-counts {} :transactions 0 :attempted-datoms 0 :last-tx nil}
   (map-indexed vector source/tables)))
