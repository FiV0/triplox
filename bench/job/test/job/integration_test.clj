(ns job.integration-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t :refer [deftest is testing]]
            [datalevin.query :as datalevin-query]
            [job.artifacts :as artifacts]
            [job.data :as data]
            [job.engines :as engines]
            [job.queries :as queries]
            [job.runner :as runner])
  (:import [java.lang AutoCloseable]
           [java.nio.file Files]
           [java.util.concurrent Executors]))

(def fixture
  [{:job/id 1 :company-type/kind "production companies"}
   {:job/id 2 :company-name/name "Film Company" :company-name/country-code "[us]"}
   {:job/id 3 :keyword/keyword "sequel"}
   {:job/id 4 :link-type/link "follows"}
   {:job/id 5 :title/title "Money Movie" :title/production-year 1998}
   {:job/id 6 :title/title "Second Movie" :title/production-year 2001}
   {:job/id 7 :movie-link/movie 5 :movie-link/linked-movie 6 :movie-link/link-type 4}
   {:job/id 8 :movie-keyword/movie 5 :movie-keyword/keyword 3}
   {:job/id 9 :movie-companies/movie 5 :movie-companies/company-type 1
    :movie-companies/company 2}])

(def matching-answer [["Film Company" "follows" "Money Movie"]])

(defn- check-queries! [baseline triplox views tx executor expected-11b]
  (let [baseline-db ((:db baseline))
        triplox-db ((:db triplox))]
    (doseq [[id view] views]
      (testing (str "JOB " id)
        ;; Datalevin 1.1.0 can cache stale missing? results after note-only writes.
        (let [expected (binding [datalevin-query/*cache?* false]
                         (artifacts/normalized ((:query baseline) baseline-db id)))
              actual (runner/bounded executor 30000 #((:query triplox) triplox-db id))
              snapshot ((:await-view! triplox) view tx 30000)]
          (is (= expected (artifacts/normalized actual)) "standard query")
          (is (= expected (artifacts/normalized (:rows snapshot))) "incremental view")
          (is (<= (:tx-id tx) (get-in snapshot [:tx-key :tx-id])) "view freshness")
          (when (= id "11b")
            (is (= expected-11b expected) "fixture exercises missing-attribute negation")))))))

(deftest all-job-queries-follow-transactions
  (let [directory (.toFile (Files/createTempDirectory "triplox-job-test-"
                                                      (make-array java.nio.file.attribute.FileAttribute 0)))
        baseline (engines/open! {:engine "datalevin" :state-dir (str directory)})
        triplox (engines/open! {:engine "triplox-incremental"
                               :host (or (System/getenv "TRIPLOX_HOST") "127.0.0.1")
                               :port (Integer/parseInt (or (System/getenv "TRIPLOX_PORT") "5490"))})
        executor (Executors/newSingleThreadExecutor)
        views (atom [])]
    (try
      ((:schema! baseline))
      (let [schema-tx ((:schema! triplox))]
        (doseq [id (queries/selected "all")]
          (testing (str "register JOB " id " before ingestion")
            (swap! views conj [id (runner/bounded executor 30000 #((:view! triplox) id))])))
        (is (= 113 (count @views)))
        (testing "empty database"
          (check-queries! baseline triplox @views schema-tx executor [])))
      (doseq [rows (partition-all 2 fixture)]
        (let [tx (data/transaction rows)]
          ((:transact! baseline) tx)
          ((:transact! triplox) tx)))
      (doseq [[label tx expected]
              [["loaded" [{:job/id 5 :title/production-year 1998}] matching-answer]
               ["add missing attribute" [[:db/add [:job/id 9] :movie-companies/note "(presents)"]] []]
               ["change existing attribute" [[:db/add [:job/id 9] :movie-companies/note "(distributes)"]] []]
               ["remove attribute" [[:db/retract [:job/id 9] :movie-companies/note "(distributes)"]] matching-answer]
               ["retract join" [[:db/retract [:job/id 9] :movie-companies/movie [:job/id 5]]] []]
               ["restore join" [[:db/add [:job/id 9] :movie-companies/movie [:job/id 5]]] matching-answer]
               ["update year" [{:job/id 5 :title/production-year 1999}] []]
               ["restore year" [{:job/id 5 :title/production-year 1998}] matching-answer]]]
        (testing label
          (println "check all JOB queries:" label)
          ((:transact! baseline) tx)
          (check-queries! baseline triplox @views ((:transact! triplox) tx) executor expected)))
      (finally
        (doseq [[_ view] @views] (.close ^AutoCloseable view))
        (.shutdownNow executor)
        ((:close! triplox))
        ((:close! baseline))
        (doseq [file (reverse (file-seq directory))] (io/delete-file file))))))

(defn -main [& _]
  (let [{:keys [fail error]} (t/run-tests 'job.integration-test)]
    (shutdown-agents)
    (System/exit (if (zero? (+ fail error)) 0 1))))
