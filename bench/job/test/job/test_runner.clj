(ns job.test-runner
  (:require [clojure.test :as t]
            [job.queries-test]
            [job.data-test]
            [job.runner-test]))

(defn -main [& _]
  (let [{:keys [fail error]} (t/run-all-tests #"job\..*-test")]
    (shutdown-agents)
    (System/exit (if (zero? (+ fail error)) 0 1))))
