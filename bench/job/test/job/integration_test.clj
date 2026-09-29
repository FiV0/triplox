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
        ;; Read uncached baseline answers for verification.
        (let [expected (binding [datalevin-query/*cache?* false]
                         (artifacts/normalized ((:query baseline) baseline-db id)))
              actual (runner/bounded executor 30000 #((:query triplox) triplox-db id))
              snapshot ((:await-view! triplox) view tx 30000)]
          (is (= expected (artifacts/normalized actual)) "standard query")
          (is (= expected (artifacts/normalized (:rows snapshot))) "incremental view")
          (is (<= (:tx-id tx) (get-in snapshot [:tx-key :tx-id])) "view freshness")
          (when (= id "11b")
            (is (= expected-11b expected) "fixture exercises missing-attribute negation")))))))

(deftest final-job-results-match-standard-queries
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
      ((:schema! triplox))
      (doseq [id (queries/selected "all")]
        (testing (str "register JOB " id " before ingestion")
          (swap! views conj [id (runner/bounded executor 30000 #((:view! triplox) id))])))
      (is (= 113 (count @views)))
      (let [last-tx (reduce (fn [_ rows]
                             (let [tx (data/transaction rows)]
                               ((:transact! baseline) tx)
                               ((:transact! triplox) tx)))
                           nil (partition-all 2 fixture))]
        (check-queries! baseline triplox @views last-tx executor matching-answer))
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
