(ns xyz.triplox.view-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is]]
            [xyz.triplox.api :as api]
            [xyz.triplox.view :as view]))

(deftest terminal-error-allows-view-to-close
  (let [closed (promise)
        sub (reify java.lang.AutoCloseable (close [_] (deliver closed true)))]
    (with-redefs [api/take! (fn [& _] (throw (ex-info "subscription failed" {})))]
      (let [{:keys [stop done]} (#'view/start-worker sub (atom {}))
            mv (view/->View sub (atom {}) stop done)
            [_ channel] (async/alts!! [done (async/timeout 5000)])]
        (is (= done channel))
        (when (= done channel)
          (.close mv)
          (is (= true (deref closed 1000 false))))))))

(deftest end-of-stream-stops-view-worker
  (with-redefs [api/take! (fn [& _] nil)]
    (let [{:keys [done]} (#'view/start-worker nil (atom {}))
          [_ channel] (async/alts!! [done (async/timeout 5000)])]
      (is (= done channel)))))
