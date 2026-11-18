(ns job.artifacts
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [job.queries :as queries]))

(defn write-json! [file value]
  (io/make-parents file)
  (spit file (json/write-str value)))

(defn measurements [directory]
  (io/file directory "measurements.jsonl"))

(defn append! [file event]
  (spit file (str (json/write-str event) "\n") :append true))

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
    {:rows (count rows)}))

(defn verify-expected [directory id rows]
  (when-not (= (normalized (get queries/expected id)) (normalized rows))
    (let [file (io/file directory "mismatches" (str id ".edn"))]
      (io/make-parents file)
      (spit file (pr-str {:expected (get queries/expected id) :actual rows})))
    (throw (ex-info "JOB result differs from upstream expected answer" {:query id}))))

(defn read-events [directory]
  (let [file (measurements directory)]
    (if-not (.isFile file) []
            (with-open [reader (io/reader file)]
              (mapv #(json/read-str % :key-fn keyword) (line-seq reader))))))
