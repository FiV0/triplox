(ns job.witness-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [datalevin.core :as d]
            [job.data :as data]
            [job.queries :as queries]
            [job.witness :as witness])
  (:import [java.nio.file Files]))

(deftest resource-is-current
  (is (= (witness/generate) (witness/read-resource))
      "regenerate with `clojure -M -m job.witness`"))

(deftest every-query-has-an-answer
  (let [directory (.toFile (Files/createTempDirectory "triplox-job-witness-"
                                                      (make-array java.nio.file.attribute.FileAttribute 0)))
        conn (d/get-conn (str (io/file directory "database")) data/schema)]
    (try
      (d/transact! conn (data/transaction (witness/read-resource)))
      (doseq [id (queries/selected "all")]
        (testing (str "JOB " id)
          (is (seq (d/q (get queries/queries id) @conn)))))
      (finally
        (d/close conn)
        (doseq [file (reverse (file-seq directory))] (io/delete-file file))))))
