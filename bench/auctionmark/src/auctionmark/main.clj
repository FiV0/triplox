(ns auctionmark.main
  "AuctionMark benchmark entry point, workload driver, and reporting."
  (:require [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [io.triplox.api :as tc]
            [auctionmark.procedures :as proc])
  (:import [java.util Random]
           [java.util.concurrent Executors TimeUnit]
           [java.util.concurrent.atomic AtomicBoolean]))

;; ---------------------------------------------------------------------------
;; Config
;; ---------------------------------------------------------------------------

(def cli-options
  [["-H" "--host HOST" "Triplox server host"
    :default (or (System/getenv "TRIPLOX_HOST") "localhost")]
   ["-p" "--port PORT" "Triplox server port"
    :default (Long/parseLong (or (System/getenv "TRIPLOX_PORT") "5490"))
    :parse-fn #(Long/parseLong %)]
   ["-s" "--scale-factor SF" "Scale factor (fraction of full AuctionMark size)"
    :default (Double/parseDouble (or (System/getenv "SCALE_FACTOR") "0.1"))
    :parse-fn #(Double/parseDouble %)]
   ["-t" "--threads N" "Number of worker threads"
    :default (Long/parseLong (or (System/getenv "THREADS") "8"))
    :parse-fn #(Long/parseLong %)]
   ["-d" "--duration SECONDS" "Benchmark duration in seconds"
    :default (Long/parseLong (or (System/getenv "DURATION") "120"))
    :parse-fn #(Long/parseLong %)]
   ["-h" "--help" "Show this help message"]])

(defn parse-config
  "Parse CLI args (falling back to env vars and built-in defaults)."
  [args]
  (let [{:keys [options errors summary]} (cli/parse-opts args cli-options)]
    (cond
      (:help options)
      (do (println "Usage: auctionmark [options]")
          (println summary)
          (System/exit 0))

      errors
      (do (doseq [e errors] (log/error e))
          (System/exit 1))

      :else options)))

;; ---------------------------------------------------------------------------
;; Weighted procedure selection
;; ---------------------------------------------------------------------------

(def procedure-table
  ;; [name fn weight]
  [[:get-item           proc/proc-get-item           40]
   [:new-bid            proc/proc-new-bid            18]
   [:new-item           proc/proc-new-item           10]
   [:get-user-info      proc/proc-get-user-info      10]
   [:new-user           proc/proc-new-user            5]
   [:get-watched-items  proc/proc-get-watched-items   5]
   [:new-feedback       proc/proc-new-feedback        3]
   [:new-purchase       proc/proc-new-purchase        2]
   [:new-comment        proc/proc-new-comment         2]
   [:update-item        proc/proc-update-item         2]
   [:get-comment        proc/proc-get-comment         2]
   [:new-comment-response proc/proc-new-comment-response 1]])

(def total-weight (reduce + 0 (map #(nth % 2) procedure-table)))

(defn pick-procedure
  "Weighted random selection of a procedure."
  [^Random rng]
  (let [r (.nextInt rng (int total-weight))]
    (loop [remaining r
           [[name f w] & more] procedure-table]
      (if (or (nil? more) (< remaining (int w)))
        [name f]
        (recur (- remaining (int w)) more)))))

;; ---------------------------------------------------------------------------
;; Driver
;; ---------------------------------------------------------------------------

(defn run-worker
  "Run one worker thread."
  [conn ^Random rng state ^AtomicBoolean running?]
  (while (.get running?)
    (let [[proc-name proc-fn] (pick-procedure rng)]
      (try
        (proc-fn conn rng state)
        (catch Exception e
          (log/warn e "Error in" proc-name))))))

(defn run-benchmark
  "Run the OLTP phase with concurrent workers + background thread."
  [{:keys [host port threads duration] :as config} state]
  (let [pool (Executors/newFixedThreadPool (+ threads 1))
        running? (AtomicBoolean. true)
        ;; one connection per thread
        conns (mapv (fn [_] (tc/connect host port)) (range (+ threads 1)))
        start-time (System/nanoTime)]

    ;; start worker threads
    (doseq [i (range threads)]
      (.submit pool
        ^Runnable (fn []
                    (let [conn (nth conns i)
                          rng (Random. (+ 1000 i))]
                      (run-worker conn rng state running?)))))

    ;; start background thread for check-winning-bid + post-auction
    (.submit pool
      ^Runnable (fn []
                  (let [conn (last conns)
                        rng (Random. 9999)]
                    (while (.get running?)
                      (try
                        (Thread/sleep 10000)
                        (proc/proc-check-winning-bid conn rng state)
                        (proc/proc-post-auction conn rng state)
                        (catch InterruptedException _)
                        (catch Exception e
                          (log/warn e "Background error")))))))

    ;; wait for duration
    (log/info "Running benchmark for" duration "seconds...")
    (Thread/sleep (* duration 1000))
    (.set running? false)
    (.shutdown pool)
    (.awaitTermination pool 30 TimeUnit/SECONDS)

    (let [elapsed-ns (- (System/nanoTime) start-time)
          elapsed-secs (/ elapsed-ns 1e9)]

      ;; close connections
      (doseq [c conns] (.close c))

      {:elapsed-secs elapsed-secs})))

;; ---------------------------------------------------------------------------
;; Reporting
;; ---------------------------------------------------------------------------

(defn print-report [results]
  (log/info "=== AuctionMark Results ===")
  (log/info (format "Duration: %.1f seconds" (:elapsed-secs results))))

;; ---------------------------------------------------------------------------
;; Entry point
;; ---------------------------------------------------------------------------

(defn -main [& args]
  (let [config (parse-config args)]
    (log/info "=== AuctionMark Benchmark for Triplox ===")
    (log/info (format "Host: %s:%d" (:host config) (:port config)))
    (log/info (format "Scale factor: %.2f" (:scale-factor config)))
    (log/info (format "Threads: %d" (:threads config)))
    (log/info (format "Duration: %d seconds" (:duration config)))

    ;; Phase 1: Load data
    (log/info "--- Loading Phase ---")
    (let [load-conn (tc/connect (:host config) (:port config))
          state (proc/load-data! load-conn config)]
      (.close load-conn)

      ;; Phase 2: OLTP benchmark
      (log/info "--- OLTP Phase ---")
      (let [results (run-benchmark config state)]
        (print-report results)))

    (log/info "Done.")
    (System/exit 0)))
