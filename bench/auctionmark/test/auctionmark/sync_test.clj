(ns auctionmark.sync-test
  (:require [auctionmark.live :as live]
            [auctionmark.live-test :as fixture]
            [auctionmark.sync :as sync]
            [clojure.test :refer [deftest is]]
            [clojure.tools.cli :as cli]))

(def defaults (:options (cli/parse-opts [] sync/options)))

(deftest validates-options
  (is (= defaults (sync/validate-options defaults)))
  (doseq [changes [{:subscriptions 1001} {:subscriptions 0} {:items 199} {:rate 0} {:rate Double/NaN}
                   {:duration -1} {:probe-ms 0} {:warmup Double/POSITIVE_INFINITY}]]
    (is (thrown? clojure.lang.ExceptionInfo (sync/validate-options (merge defaults changes))))))

(deftest runs-and-probes
  (with-open [conn (fixture/connect)]
    (let [opts (merge defaults {:users 2 :items 4 :subscriptions 10 :warmup 0 :duration 2 :sla-ms 5000})
          state (live/load-fixture! conn opts)
          result (sync/run-benchmark! conn state opts)]
      (is (:passed? result) (pr-str result))
      (is (pos? (get-in result [:catch-up-ms :count])))
      (is (pos? (:final-catch-up-ms result)))
      (is (< (:skipped-operations result) (:committed-transactions result))))))
