(ns tpch.main
  "TPC-H benchmark entry point: load data, execute all 22 queries."
  (:require [io.triplox.api :as tc]
            [tpch.data :as data]
            [tpch.queries :as queries]))

(defn parse-config []
  {:host         (or (System/getenv "TRIPLOX_HOST") "localhost")
   :port         (Long/parseLong (or (System/getenv "TRIPLOX_PORT") "5490"))
   :scale-factor (Double/parseDouble (or (System/getenv "SCALE_FACTOR") "0.05"))})

(defn run-all-queries [db]
  (doseq [[label query-fn] queries/all-queries]
    (print (str label ": "))
    (flush)
    (try
      (let [start (System/nanoTime)
            result (query-fn db)
            elapsed (/ (- (System/nanoTime) start) 1e6)]
        (println (format "%d rows (%.1f ms)" (count result) elapsed)))
      (catch Exception e
        (println (str "ERROR - " (.getMessage e)))))))

(defn -main [& _args]
  (let [config (parse-config)]
    (println "=== TPC-H Benchmark for Triplox ===")
    (println (format "Host: %s:%d" (:host config) (:port config)))
    (println (format "Scale factor: %.2f" (:scale-factor config)))

    (let [conn (tc/connect (:host config) (:port config))]
      (try
        ;; Load data
        (data/load-data! conn (:scale-factor config))

        ;; Run queries
        (println "\n--- Executing TPC-H Queries ---")
        (let [db (tc/db conn)]
          (try
            (run-all-queries db)
            (finally
              (.close db))))

        (println "\nDone.")
        (finally
          (.close conn))))

    (System/exit 0)))
