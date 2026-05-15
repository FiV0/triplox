(ns graph.main
  "Graph ingestion benchmark entry point."
  (:require [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [graph.graph-gen :as graph-gen]
            [graph.schema :as schema]
            [io.triplox.api :as tc])
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

(defn edge-count [vertices]
  (quot (* vertices (dec vertices)) 2))

(defn vertex-docs [vertices]
  (map (fn [id] {:g/id (long id)}) (range vertices)))

(defn transact-batches!
  [conn label tx-data batch-size]
  (doseq [[idx batch] (map-indexed vector (partition-all batch-size tx-data))]
    (let [result (tc/transact conn (vec batch))]
      (when-not (:committed? result)
        (throw (ex-info (str "Failed to transact " label " batch")
                        {:label label
                         :batch idx
                         :result result}))))))

(defn ingest-graph! [conn {:keys [vertices batch-size]}]
  (log/info "Installing graph schema")
  (transact-batches! conn "schema" schema/schema-tx batch-size)
  (log/info "Ingesting" vertices "vertices")
  (transact-batches! conn "vertices" (vertex-docs vertices) batch-size)
  (let [edges (graph-gen/complete-graph vertices)]
    (log/info "Ingesting" (edge-count vertices) "edges")
    (transact-batches! conn "edges" (graph-gen/graph->ops edges) batch-size)))

(defn print-report [{:keys [vertices batch-size elapsed-secs]}]
  (let [edges (edge-count vertices)]
    (log/info "=== Graph Ingestion Results ===")
    (log/info (format "Vertices: %d" vertices))
    (log/info (format "Edges: %d" edges))
    (log/info (format "Batch size: %d" batch-size))
    (log/info (format "Elapsed: %.3f seconds" elapsed-secs))
    (log/info (format "Edges/sec: %.1f" (/ edges elapsed-secs)))))

(defn -main [& args]
  (let [{:keys [host port] :as config} (parse-config args)]
    (log/info "=== Graph Ingestion Benchmark for Triplox ===")
    (log/info (format "Host: %s:%d" host port))
    (log/info (format "Vertices: %d" (:vertices config)))
    (log/info (format "Batch size: %d" (:batch-size config)))
    (let [conn (tc/connect host port)
          start (System/nanoTime)]
      (try
        (ingest-graph! conn config)
        (print-report (assoc config :elapsed-secs (/ (- (System/nanoTime) start) 1e9)))
        (finally
          (.close ^AutoCloseable conn))))
    (System/exit 0)))
