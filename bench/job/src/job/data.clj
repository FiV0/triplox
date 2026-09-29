(ns job.data
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [job.source :as source])
  (:import [java.nio.charset StandardCharsets]
           [java.security MessageDigest]
           [java.util HexFormat]))

(def schema
  (assoc source/schema :job/id {:db/valueType :db.type/long
                               :db/unique :db.unique/identity}))

(defn ref-attribute? [attribute]
  (= :db.type/ref (get-in schema [attribute :db/valueType])))

(defn schema-tx [engine]
  (mapv (fn [[attribute properties]]
          (cond-> (merge {:db/ident attribute :db/cardinality :db.cardinality/one}
                        properties)
            (= engine :datomic) (assoc :db/index true)))
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

(defn encoded-size [value]
  (alength (.getBytes (pr-str value) StandardCharsets/UTF_8)))

(defn split-batches [rows batch-size max-bytes]
  (letfn [(split [batch]
            (if (<= (encoded-size (transaction batch)) max-bytes)
              [batch]
              (if (= 1 (count batch))
                (throw (ex-info "A source row exceeds the transaction byte budget"
                                {:id (:job/id (first batch)) :max-bytes max-bytes}))
                (let [[left right] (split-at (quot (count batch) 2) batch)]
                  (concat (split left) (split right))))))]
    (mapcat split (partition-all batch-size rows))))

(defn load-data! [directory {:keys [batch-size max-bytes]} transact!]
  (let [counts (atom {})
        transactions (atom 0)
        datoms (atom 0)
        last-tx (atom nil)]
    (doseq [[table :as descriptor] source/tables]
      (let [flush! (fn [rows]
                     (doseq [batch (split-batches rows batch-size max-bytes)]
                       (let [tx (transaction batch)]
                         (reset! last-tx (transact! tx))
                         (swap! transactions inc)
                         (swap! datoms + (reduce + (map #(dec (count %)) tx))))))
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

(defn sha256 [file]
  (let [digest (MessageDigest/getInstance "SHA-256")
        buffer (byte-array 65536)]
    (with-open [input (io/input-stream file)]
      (loop []
        (let [length (.read input buffer)]
          (when (pos? length) (.update digest buffer 0 length) (recur)))))
    (.formatHex (HexFormat/of) (.digest digest))))

(defn dataset-manifest [directory]
  (mapv (fn [[table]]
          (let [file (table-file directory table)]
            (when-not (.isFile file)
              (throw (ex-info "Missing JOB CSV file" {:file (str file)})))
            {:table table :bytes (.length file) :sha256 (sha256 file)}))
        source/tables))
