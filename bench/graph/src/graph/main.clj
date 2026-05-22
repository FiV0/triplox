(ns graph.main
  "Graph ingestion benchmark entry point."
  (:require [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [graph.graph-gen :as gg]
            [xyz.triplox.api :as tc])
  (:import [java.lang AutoCloseable]))

(set! *print-namespace-maps* false)
(set! *warn-on-reflection* true)

(def cli-options
  [["-H" "--host HOST" "Triplox server host"
    :default (or (System/getenv "TRIPLOX_HOST") "localhost")]
   ["-p" "--port PORT" "Triplox server port"
    :default (Long/parseLong (or (System/getenv "TRIPLOX_PORT") "5490"))
    :parse-fn #(Long/parseLong %)]
   ["-v" "--vertices N" "Number of vertices in the complete graph"
    :default (Long/parseLong (or (System/getenv "VERTICES") "100"))
    :parse-fn #(Long/parseLong %)
    :validate [pos-int? "Must be a positive integer"]]
   ["-b" "--batch-size N" "Transaction batch size"
    :default (Long/parseLong (or (System/getenv "BATCH_SIZE") "1000"))
    :parse-fn #(Long/parseLong %)
    :validate [pos-int? "Must be a positive integer"]]
   ["-prob" "--probability p" "Probability of an edge in a random graph on N vertices"
    :default (Double/parseDouble (or (System/getenv "EDGE_PROBABILITY") "0.1"))
    :parse-fn #(Double/parseDouble %)
    :validate [double? "Must be a positive integer"]]
   ["-h" "--help" "Show this help message"]])

(defn parse-config [args]
  (let [{:keys [options errors summary]} (cli/parse-opts args cli-options)]
    (cond
      (:help options)
      (do (println "Usage: graph [options]")
          (println summary)
          (System/exit 0))

      errors
      (do (doseq [e errors] (log/error e))
          (System/exit 1))

      :else options)))


(defn vertex-docs [vertices]
  (map (fn [id] {:g/id (long id)}) (range vertices)))

(defn transact-batches!
  [conn tx-data batch-size]
  (doseq [[idx batch] (map-indexed vector (partition-all batch-size tx-data))]
    ;; TODO pass to async transactions
    (let [result (tc/transact conn (vec batch))]
      (when-not (:committed? result)
        (throw (ex-info "Failed to transact batch"
                        {:batch idx :result result}))))))

(defn ingest-graph! [conn {:keys [vertices batch-size probability]}]
  (tc/transact conn gg/graph-schema)
  (log/info "Ingesting" vertices "vertices")
  (transact-batches! conn (vertex-docs vertices) batch-size)
  (let [edges (gg/graph->ops (gg/random-graph vertices probability))]
    (log/info "Ingesting" (count edges) "edges")
    (transact-batches! conn edges batch-size)))

(defn print-report [{:keys [vertices probability batch-size ingest-secs triangle-count triangle-count-secs]}]
  (log/info "=== Graph Query Results ===")
  (log/info (format "Vertices: %d" vertices))
  (log/info (format "Edge probability: %.3f" probability))
  (log/info (format "Batch size: %d" batch-size))
  (log/info (format "Ingest: %.3f seconds" ingest-secs))
  (log/info (format "Number of triangles: %d"  triangle-count))
  (log/info (format "Triangle counting: %.3f seconds" triangle-count-secs)))

(defn -main [& args]
  (let [{:keys [host port] :as config} (parse-config args)]
    (log/info "=== Graph Query Benchmark for Triplox ===")
    (log/info (format "Vertices: %d" (:vertices config)))
    (log/info (format "Batch size: %d" (:batch-size config)))
    (let [conn (tc/connect host port)
          start (System/nanoTime)]
      (try
        (ingest-graph! conn config)
        (let [end-ingest (System/nanoTime)
              config (assoc config :ingest-secs (/ (- (System/nanoTime) start) 1e9))
              triangle-count (let [db (tc/db conn)]
                               (count (tc/q db gg/triangle-query)))]
          (print-report (assoc config
                               :ingest-secs (/ (- end-ingest start) 1e9)
                               :triangle-count triangle-count
                               :triangle-count-secs (/ (- (System/nanoTime) end-ingest) 1e9))))
        (finally
          (.close ^AutoCloseable conn))))
    (System/exit 0)))

(comment
  (def conn (tc/connect "localhost" 5490))

  (ingest-graph! conn {:vertices 10000 :probability 0.00182 :batch-size 1000})

  (time
   (let [db (tc/db conn)]
     (count (tc/q db gg/triangle-query))))

  )
