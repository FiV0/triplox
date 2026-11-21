(ns job.integration-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [datalevin.core :as d]
            [datalevin.query :as datalevin-query]
            [job.artifacts :as artifacts]
            [job.data :as data]
            [job.engines :as engines]
            [job.queries :as queries]
            [job.runner :as runner]
            [job.witness :as witness])
  (:import [java.lang AutoCloseable]
           [java.nio.file Files]
           [java.util.concurrent Executors]))

;; The Datalevin engine bulk-loads CSVs, so the fixture goes through a transactional connection.
(defn- datalevin-baseline [directory]
  (let [conn (d/get-conn (str (io/file directory "database")) data/schema)]
    {:schema! (constantly nil)
     :transact! (fn [tx] (d/transact! conn tx) nil)
     :db-fn #(deref conn)
     :query-fn (fn [db id] (mapv vec (d/q (get queries/queries id) db)))
     :close! #(d/close conn)}))

(defn- check-queries! [baseline triplox views tx executor]
  (let [baseline-db ((:db-fn baseline))
        triplox-db ((:db-fn triplox))]
    (doseq [[id view] views]
      (testing (str "JOB " id)
        ;; Read uncached baseline answers for verification.
        (let [expected (binding [datalevin-query/*cache?* false]
                         (artifacts/normalized ((:query-fn baseline) baseline-db id)))
              actual (runner/bounded executor 30000 #((:query-fn triplox) triplox-db id))
              snapshot ((:await-view! triplox) view tx 30000)]
          (is (seq expected) "witness data gives an answer")
          (is (= expected (artifacts/normalized actual)) "standard query")
          (is (= expected (artifacts/normalized (:rows snapshot))) "incremental view")
          (is (<= (:tx-id tx) (get-in snapshot [:tx-key :tx-id])) "view freshness"))))))

(deftest ^:integration final-job-results-match-standard-queries
  (let [directory (.toFile (Files/createTempDirectory "triplox-job-test-"
                                                      (make-array java.nio.file.attribute.FileAttribute 0)))
        baseline (datalevin-baseline directory)
        triplox (engines/open! {:engine :triplox-incremental
                               :host (or (System/getenv "TRIPLOX_HOST") "127.0.0.1")
                               :port (Integer/parseInt (or (System/getenv "TRIPLOX_PORT") "5490"))})
        executor (Executors/newSingleThreadExecutor)
        views (atom [])]
    (try
      ((:schema! baseline))
      ((:schema! triplox))
      (doseq [id (queries/selected "all")]
        (testing (str "register JOB " id " before ingestion")
          (swap! views conj [id (runner/bounded executor 30000 #((:view! triplox) id))])))
      (is (= 113 (count @views)))
      (let [last-tx (reduce (fn [_ rows]
                             (let [tx (data/transaction rows)]
                               ((:transact! baseline) tx)
                               ((:transact! triplox) tx)))
                           nil (partition-all 25 (witness/read-resource)))]
        ((:await-tx! triplox) last-tx)
        (check-queries! baseline triplox @views last-tx executor))
      (finally
        (doseq [[_ view] @views] (.close ^AutoCloseable view))
        (.shutdownNow executor)
        ((:close! triplox))
        ((:close! baseline))
        (doseq [file (reverse (file-seq directory))] (io/delete-file file))))))
