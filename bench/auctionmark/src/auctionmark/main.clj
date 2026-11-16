(ns auctionmark.main
  "AuctionMark benchmark entry point, as a benchy benchmark."
  (:require [benchy.core :as bench]
            [clojure.pprint :as pp]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [xyz.triplox.api :as tc]
            [auctionmark.procedures :as proc])
  (:import [java.time Duration]))

;; TODO move to deps.edn
(set! *print-namespace-maps* false)
(set! *warn-on-reflection* true)

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
   [nil "--seed N" "Random seed for loading and the workload"
    :default 42
    :parse-fn #(Long/parseLong %)]
   [nil "--report PATH" "Append benchy events and the summary as JSON Lines to PATH"]
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
;; Benchmark
;; ---------------------------------------------------------------------------

(def procedure-table
  ;; [weight name fn]
  [[40 :get-item             proc/proc-get-item]
   [18 :new-bid              proc/proc-new-bid]
   [10 :new-item             proc/proc-new-item]
   [10 :get-user-info        proc/proc-get-user-info]
   [5  :new-user             proc/proc-new-user]
   [5  :get-watched-items    proc/proc-get-watched-items]
   [3  :new-feedback         proc/proc-new-feedback]
   [2  :new-purchase         proc/proc-new-purchase]
   [2  :new-comment          proc/proc-new-comment]
   [2  :update-item          proc/proc-update-item]
   [2  :get-comment          proc/proc-get-comment]
   [1  :new-comment-response proc/proc-new-comment-response]])

(defn- procedure-task [transaction f]
  {:t :call
   :transaction transaction
   :f (fn [{:keys [context random state]}] (f (:conn context) random state))})

(defn- load-task [[step f]]
  {:t :call
   :stage step
   :setup? true
   :f (fn [{:keys [context random state]}] (f (:conn context) state random))})

(def ^:private join-wait (Duration/ofSeconds 30))

(defn benchmark
  "The benchy benchmark map. Runs need a context with a :conn."
  [{:keys [threads duration seed report] :as config}]
  (let [duration (Duration/ofSeconds duration)]
    {:title "AuctionMark"
     :seed seed
     :parameters (select-keys config [:scale-factor :threads :duration])
     :bench-log-file report
     :->state proc/make-state
     :tasks [{:t :do
              :stage :load
              :setup? true
              :tasks (mapv load-task (proc/load-steps config))}
             {:t :call :f (fn [{:keys [state]}] (proc/log-item-counts state))}
             {:t :concurrently
              :stage :oltp
              :duration duration
              :join-wait join-wait
              :thread-tasks [{:t :pool
                              :thread-count threads
                              :duration duration
                              :join-wait join-wait
                              :pooled-task {:t :pick-weighted
                                            :choices (vec (for [[weight transaction f] procedure-table]
                                                            [weight (procedure-task transaction f)]))}}
                             {:t :freq-job
                              :duration duration
                              :freq (Duration/ofSeconds 10)
                              :job-task {:t :do
                                         :tasks [(procedure-task :check-winning-bid proc/proc-check-winning-bid)
                                                 (procedure-task :post-auction proc/proc-post-auction)]}}]}]}))

;; ---------------------------------------------------------------------------
;; Reporting
;; ---------------------------------------------------------------------------

(defn- format-number [v]
  (if (double? v) (format "%.3f" v) v))

(defn print-report [result]
  (log/info "=== AuctionMark Results ===")
  (log/info (format "Status: %s, measured %.1f s, setup %.1f s, %d transactions, %d errors, %.1f tx/s"
                    (name (:status result))
                    (/ (:time-taken-ms result) 1000)
                    (/ (:setup-time-ms result) 1000)
                    (:transaction-count result)
                    (:error-count result)
                    (double (:throughput result))))
  (pp/print-table [:transaction :count :error-count :mean-ms :p50-ms :p95-ms :p99-ms :max-ms]
                  (for [tx (:transactions result)]
                    (update-vals tx format-number)))
  (doseq [{:keys [transaction class message]} (:errors result)]
    (log/warn "Error in" transaction class message)))

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

    (with-open [conn (tc/connect (:host config) (:port config))]
      (print-report (bench/run-benchmark (benchmark config) {:conn conn})))

    (log/info "Done.")
    (System/exit 0)))


(comment
  (def host "localhost")
  (def port 5490)
  (def conn (tc/connect host 5490))

  (.close conn)

  (require '[auctionmark.schema :as schema])

  (time
   (def state (proc/load-data! conn {:scale-factor 0.01})))

  (-> (benchmark {:scale-factor 0.001 :threads 2 :duration 10 :seed 42})
      (bench/run-benchmark {:conn conn})
      print-report)

  (tc/transact conn [{:db/ident :name
                      :db/valueType :db.type/string
                      :db/cardinality :db.cardinality/one}
                     {:db/ident :age
                      :db/valueType :db.type/long
                      :db/cardinality :db.cardinality/one}])

  (let [db (tc/db conn)]
    (tc/q db '{:find [?ident, ?value-type, ?cardinality]
               :where [[?attr :db/ident ?ident]
                       [?attr :db/valueType ?value-ref]
                       [?attr :db/cardinality ?card-ref]
                       [?value-ref :db/ident ?value-type]
                       [?card-ref :db/ident ?cardinality]]}))

  )
