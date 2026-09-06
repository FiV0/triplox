(ns tpch.data
  "TPC-H data loading via the io.airlift.tpch generator library."
  (:require [clojure.string :as str]
            [io.triplox.api :as tc]
            [tpch.schema :as schema])
  (:import (io.airlift.tpch GenerateUtils TpchColumn TpchColumnType$Base TpchEntity TpchTable)
           (java.time Instant LocalDate ZoneOffset)))

;; Column name prefix per table (stripped when building namespaced keywords).
(def ^:private table-prefix
  {"part"     "p_"
   "supplier" "s_"
   "partsupp" "ps_"
   "customer" "c_"
   "orders"   "o_"
   "lineitem" "l_"
   "nation"   "n_"
   "region"   "r_"})

;; Foreign key columns that should be stored as lookup refs.
;; Maps the namespaced attr of the FK to the namespaced attr of the referenced PK.
(def ^:private fk->lookup-attr
  {:lineitem/orderkey  :orders/orderkey
   :lineitem/partkey   :part/partkey
   :lineitem/suppkey   :supplier/suppkey
   :orders/custkey     :customer/custkey
   :customer/nationkey :nation/nationkey
   :supplier/nationkey :nation/nationkey
   :nation/regionkey   :region/regionkey
   :partsupp/partkey   :part/partkey
   :partsupp/suppkey   :supplier/suppkey})

(defn- col-name->keyword
  "Convert a TPC-H column name like 'l_shipdate' to a namespaced keyword like :lineitem/shipdate."
  [^String table-name ^String col-name]
  (let [prefix (get table-prefix table-name)
        short-name (if (str/starts-with? col-name prefix)
                     (subs col-name (count prefix))
                     col-name)]
    (keyword table-name short-name)))

(defn- parse-tpch-date
  "Parse a TPC-H date string (yyyy-MM-dd) into a java.time.Instant at start of day UTC."
  [^String s]
  (.toInstant (.atStartOfDay (LocalDate/parse s) ZoneOffset/UTC)))

(defn- tpch-entity->doc
  "Convert a TpchEntity to a Clojure map with namespaced keywords."
  [^TpchTable table ^TpchEntity entity]
  (let [table-name (.getTableName table)]
    (->> (.getColumns table)
         (reduce
          (fn [doc ^TpchColumn col]
            (let [col-name (.getColumnName col)
                  k (col-name->keyword table-name col-name)
                  v (condp = (.getBase (.getType col))
                      TpchColumnType$Base/IDENTIFIER
                      (let [id (.getIdentifier col entity)]
                        (if-let [ref-attr (get fk->lookup-attr k)]
                          [ref-attr id]
                          id))
                      TpchColumnType$Base/INTEGER
                      (long (.getInteger col entity))
                      TpchColumnType$Base/VARCHAR
                      (.getString col entity)
                      TpchColumnType$Base/DOUBLE
                      (.getDouble col entity)
                      TpchColumnType$Base/DATE
                      (parse-tpch-date (GenerateUtils/formatDate (.getDate col entity))))]
              (assoc doc k v)))
          {}))))

(def ^:private batch-size 1000)

(defn load-data!
  "Load all TPC-H tables into the Triplox node. Blocks until all data is indexed."
  [conn scale-factor]
  (println "Installing TPC-H schema...")
  (tc/transact conn schema/schema-tx)

  (println "Loading TPC-H data (scale factor:" (str scale-factor) ")...")
  (doseq [^TpchTable table (TpchTable/getTables)]
    (let [table-name (.getTableName table)
          docs (->> (.createGenerator table (double scale-factor) 1 1)
                    seq
                    (map (partial tpch-entity->doc table)))
          doc-count (reduce (fn [cnt chunk]
                              (tc/submit-tx conn (vec chunk))
                              (+ cnt (count chunk)))
                            0
                            (partition-all batch-size docs))]
      (println "  Submitted" doc-count table-name)))

  ;; Fence: wait for all data to be indexed.
  (println "Waiting for indexing...")
  (tc/transact conn [{:db/ident :tpch/fence}])
  (println "Data loading complete."))
