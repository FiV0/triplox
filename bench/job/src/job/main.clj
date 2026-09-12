(ns job.main
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [job.artifacts :as artifacts]
            [job.data :as data]
            [job.engines :as engines]
            [job.queries :as queries]
            [job.runner :as runner]))

(defn -main [config-file]
  (let [config (json/read-str (slurp config-file) :key-fn keyword)
        config (assoc config :engine-name (:engine config)
                             :query-ids (queries/selected (:queries config)))
        output (:output config)]
    (try
      (if (= "manifest" (:engine config))
        (artifacts/write-json! (io/file output "dataset.json") (data/dataset-manifest (:data-dir config)))
        (let [dialect (if (.startsWith ^String (:engine config) "triplox") :triplox
                          (keyword (:engine config)))
              query-forms (into (sorted-map)
                                (map (fn [id] [id (if (= dialect :datalevin)
                                                    (get queries/queries id)
                                                    (queries/translate id dialect))]))
                                (:query-ids config))
              _ (spit (io/file output "queries.edn") (pr-str query-forms))
              _ (artifacts/write-json! (io/file output "selection.json") (:query-ids config))
              engine (engines/open! config)]
          (if (:ingest-only config)
            (try
              ((:schema! engine))
              (artifacts/write-json! (io/file output "load.json")
                                     (dissoc (data/load-data! (:data-dir config) config (:transact! engine))
                                             :last-tx :samples))
              (finally ((:close! engine))))
            (runner/run! engine config))
          (artifacts/write-json! (io/file output "status.json") {:status "ok"})))
      (shutdown-agents)
      (System/exit 0)
      (catch Throwable error
        (.printStackTrace error)
        (artifacts/append! output {:phase "run" :status "failed" :message (.getMessage error)})
        (artifacts/write-json! (io/file output "status.json")
                               {:status "failed" :message (.getMessage error)})
        (shutdown-agents)
        (System/exit 1)))))
