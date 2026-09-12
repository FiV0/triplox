(ns job.artifacts
  (:require [clojure.data.json :as json]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [job.data :as data]
            [job.queries :as queries]))

(defn write-json! [file value]
  (io/make-parents file)
  (spit file (json/write-str value :value-fn (fn [_ value]
                                             (if (instance? java.time.Instant value)
                                               (str value) value)))))

(defn append! [directory event]
  (spit (io/file directory "measurements.jsonl")
        (str (json/write-str event) "\n") :append true))

(defn normalized [rows]
  (->> rows
       (map (fn [row] (mapv #(if (integer? %) (bigint %) %) row)))
       set
       (sort-by pr-str)
       vec))

(defn save-result! [directory checkpoint id rows]
  (let [rows (normalized rows)
        file (io/file directory "answers" (str checkpoint) (str id ".edn"))]
    (io/make-parents file)
    (spit file (pr-str rows))
    {:rows (count rows) :sha256 (data/sha256 file)}))

(defn verify-upstream! [directory id rows]
  (when-not (= (normalized (get queries/expected id)) (normalized rows))
    (let [file (io/file directory "mismatches" (str id ".edn"))]
      (io/make-parents file)
      (spit file (pr-str {:expected (get queries/expected id) :actual rows})))
    (throw (ex-info "JOB result differs from upstream expected answer" {:query id}))))

(defn read-events [directory]
  (let [file (io/file directory "measurements.jsonl")]
    (if-not (.isFile file) []
      (with-open [reader (io/reader file)]
        (mapv #(json/read-str % :key-fn keyword) (line-seq reader))))))

(defn percentile [values fraction]
  (when (seq values)
    (let [values (vec (sort values))]
      (nth values (min (dec (count values))
                       (max 0 (dec (long (Math/ceil (* fraction (count values)))))))))))
