(ns tpch.ingest
  "TPC-H ingest-only entry point: load data and report timings, skip the queries."
  (:require [xyz.triplox.api :as tc]
            [tpch.data :as data]
            [tpch.main :as main]))

(defn -main [& _args]
  (let [{:keys [host port scale-factor]} (main/parse-config)]
    (println (format "=== TPC-H ingest: %s:%d, scale factor %.2f ===" host port scale-factor))
    (with-open [conn (tc/connect host port)]
      (data/load-data! conn scale-factor))
    (System/exit 0)))
