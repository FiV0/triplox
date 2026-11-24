(ns auctionmark.sync
  "Bounded live-query experiment. Subscription results are checked against standard
  queries when primed and at the end of the run. Freshness is the time until the
  last subscription reaches a probed transaction's tx-key."
  (:refer-clojure :exclude [run!])
  (:require [auctionmark.live :as live]
            [clojure.data.json :as json]
            [clojure.java.shell :as shell]
            [clojure.string :as str]
            [clojure.tools.cli :as cli]
            [xyz.triplox.api :as tc])
  (:import [java.time Instant]
           [java.util Random]))

;; ---------------------------------------------------------------------------
;; Options

(def options
  [[nil "--host HOST" "Triplox host" :default "localhost"]
   [nil "--port PORT" "Triplox port" :default 5477 :parse-fn parse-long]
   [nil "--users N" "Fixture users" :default 200 :parse-fn parse-long]
   [nil "--items N" "Fixture items" :default 1000 :parse-fn parse-long]
   [nil "--subscriptions N" "Live queries, five per user" :default 1000 :parse-fn parse-long]
   [nil "--seed N" "Workload seed" :default 42 :parse-fn parse-long]
   [nil "--rate N" "Target committed transactions/second" :default 5.0 :parse-fn parse-double]
   [nil "--warmup SECONDS" "Writes before measurement" :default 10.0 :parse-fn parse-double]
   [nil "--duration SECONDS" "Measured writes" :default 30.0 :parse-fn parse-double]
   [nil "--probe-ms MS" "Interval between catch-up probes" :default 500 :parse-fn parse-long]
   [nil "--output PATH" "JSON report" :default "sync-report.json"]
   ["-h" "--help"]])

(defn validate-options [opts]
  (let [{:keys [users items subscriptions warmup]} opts
        positive? #(and (number? %) (Double/isFinite (double %)) (pos? %))]
    (when-not (and (every? positive? (map opts [:port :users :items :subscriptions :rate :duration :probe-ms]))
                   (number? warmup)
                   (Double/isFinite (double warmup))
                   (<= 0 warmup)
                   (<= users items)
                   (<= subscriptions (* 5 users)))
      (throw (ex-info "Invalid options: need items >= users >= subscriptions/5" {:options opts})))
    opts))

;; ---------------------------------------------------------------------------
;; Subscriptions

(defn error! [tracker error]
  (swap! tracker update :errors conj error))

(defn consume!
  "Apply deltas as they arrive and publish the tx-id each one reaches."
  [{:keys [sub rows applied id]} tracker running]
  (try
    (while @running
      (let [delta (tc/take! sub 100)]
        (cond
          (= ::tc/timeout delta) nil
          (nil? delta) (throw (ex-info "Subscription ended" {:query-id id}))
          ;; Empty deltas count too: they move the subscription's tx-key past
          ;; batches that leave the result unchanged.
          :else (do (swap! rows live/apply-delta delta)
                    (reset! applied (:tx-id (tc/tx-key sub)))))))
    (catch Exception e
      (when @running
        (error! tracker {:error :consumer-failed :query-id id :message (.getMessage e)})))))

(defn- prime!
  "Apply the subscription's initial result and check it against a standard query at the
  registration tx-key."
  [conn {:keys [id sub rows query applied]}]
  (let [delta (tc/take! sub 10000)]
    (when (or (nil? delta) (= ::tc/timeout delta))
      (throw (ex-info "Priming timed out" {:query-id id})))
    (let [primed (swap! rows live/apply-delta delta)
          tx-key (tc/registration-tx-key sub)]
      (when-not (= (frequencies (tc/q (tc/db conn tx-key) query)) primed)
        (throw (ex-info "Incorrect priming result" {:query-id id :tx-key tx-key})))
      (reset! applied (:tx-id (tc/tx-key sub))))))

(defn open-subscriptions!
  "Subscribe and prime one query at a time, then start its consumer."
  [conn descriptors entries tracker running]
  (doseq [d descriptors]
    (let [start (System/nanoTime)
          entry (assoc d :sub (tc/subscribe conn (:query d)) :rows (atom {}) :applied (atom nil))
          index (count @entries)]
      ;; Register before priming so a failure still closes this subscription.
      (swap! entries conj entry)
      (prime! conn entry)
      (swap! entries assoc index
             (assoc entry
                    :initialization-ms (/ (- (System/nanoTime) start) 1e6)
                    :consumer (.start (Thread/ofVirtual) ^Runnable #(consume! entry tracker running)))))))

(defn- stop-consumers!
  "Stop consuming deltas, so each subscription's rows stay at its tx-key."
  [entries running]
  (reset! running false)
  (doseq [{:keys [consumer]} entries :when consumer]
    (.join ^Thread consumer 2000)))

(defn close-subscriptions! [entries running]
  (reset! running false)
  (doseq [{:keys [sub]} entries]
    (.close sub))
  (stop-consumers! entries running))

(defn- caught-up? [entries tx-id]
  (every? #(<= tx-id @(:applied %)) entries))

(defn- await-catch-up!
  "Poll every millisecond until every subscription has reached tx-id, however long that
  takes. Returns false only if the consumers stop or a subscription fails first, since
  then the subscriptions may never reach tx-id."
  [entries tx-id tracker running]
  (loop []
    (cond
      (caught-up? entries tx-id) true
      (or (not @running) (seq (:errors @tracker))) false
      :else (do (Thread/sleep 1) (recur)))))

;; ---------------------------------------------------------------------------
;; Workload

(defn pause-until! [deadline]
  (let [ms (/ (- deadline (System/nanoTime)) 1e6)]
    (when (pos? ms)
      (Thread/sleep (long (Math/ceil ms))))))

(defn check-errors! [tracker]
  (when (seq (:errors @tracker))
    (throw (ex-info "Subscription error" {}))))

(defn- jittered-interval-ns
  "Time to the next transaction at the target rate, with +-10% jitter."
  [rate ^Random rng]
  (long (* (/ 1e9 rate) (+ 0.9 (* 0.2 (.nextDouble rng))))))

(defn- probe!
  "Record how long after its submission the last subscription reached the transaction."
  [entries tracker running {:keys [tx-id start-ns]}]
  (when (await-catch-up! entries tx-id tracker running)
    (swap! tracker update :catch-up-ms conj (/ (- (System/nanoTime) start-ns) 1e6))))

(defn tracked-transactor
  "A transact fn that paces writes and, in the measured window, probes one transaction
  every probe-ms."
  [{:keys [entries tracker running opts measured-start end next-due next-probe pacing-rng probes]}]
  (fn [conn tx]
    (check-errors! tracker)
    (let [scheduled @next-due]
      (pause-until! scheduled)
      (let [start (System/nanoTime)
            result (live/transact! conn tx)]
        (reset! next-due (+ (max scheduled start) (jittered-interval-ns (:rate opts) pacing-rng)))
        (swap! tracker assoc :last-tx-id (:tx-id result) :last-tx-start-ns start)
        (when (<= measured-start start (dec end))
          (swap! tracker update :transactions inc)
          (when (<= @next-probe start)
            (reset! next-probe (+ start (* 1000000 (:probe-ms opts))))
            (swap! probes conj (.start (Thread/ofVirtual)
                                       ^Runnable #(probe! entries tracker running {:tx-id (:tx-id result)
                                                                                   :start-ns start})))))
        result))))

(defn- run-workload!
  "Run random mutations through the tracked transactor for warmup plus duration."
  [conn state {:keys [tracker skipped opts] :as ctx}]
  (let [begin (System/nanoTime)
        measured-start (+ begin (long (* 1e9 (:warmup opts))))
        end (+ measured-start (long (* 1e9 (:duration opts))))
        rng (Random. (:seed opts))
        state (assoc state :transact-fn (tracked-transactor (assoc ctx
                                                                   :measured-start measured-start
                                                                   :end end
                                                                   :next-due (atom begin)
                                                                   :next-probe (atom measured-start)
                                                                   :pacing-rng (Random. (inc (:seed opts))))))]
    (while (< (System/nanoTime) end)
      (let [before (:last-tx-id @tracker)]
        (live/mutate! conn rng state)
        ;; Procedures with nothing to do (e.g. no waiting items) don't write.
        (when (= before (:last-tx-id @tracker))
          (swap! skipped inc)
          (Thread/sleep 10)))
      (check-errors! tracker))))

(defn- drain!
  "Wait for every subscription to reach the last transaction and record how long after
  its submission the last subscription got there."
  [entries tracker running]
  (let [{:keys [last-tx-id last-tx-start-ns]} @tracker]
    (when (and last-tx-id (await-catch-up! entries last-tx-id tracker running))
      (swap! tracker assoc :final-catch-up-ms (/ (- (System/nanoTime) last-tx-start-ns) 1e6)))))

(defn verify-final!
  "Stop the consumers, then check each subscription's client-side result against a
  standard query at the tx-key the subscription has reached."
  [conn entries tracker running]
  (stop-consumers! entries running)
  (doseq [{:keys [id sub query rows]} entries]
    (let [tx-key (tc/tx-key sub)]
      (when-not (= (frequencies (tc/q (tc/db conn tx-key) query)) @rows)
        (error! tracker {:error :final-result-mismatch :query-id id :tx-key tx-key})))))

;; ---------------------------------------------------------------------------
;; Run

(defn percentiles [samples]
  (let [values (vec (sort samples))
        n (count values)
        at (fn [p] (when (pos? n) (nth values (dec (long (Math/ceil (* p n)))))))]
    {:count n
     :p50 (at 0.50)
     :p95 (at 0.95)
     :p99 (at 0.99)
     :max (last values)}))

(defn- failed-checks [entries metrics opts]
  (cond-> []
    (not= (:subscriptions opts) (count entries)) (conj :initialization)
    (seq (:errors metrics)) (conj :correctness-or-runtime)
    (zero? (get-in metrics [:catch-up-ms :count])) (conj :no-probes)
    (< (:transactions-per-second metrics) (* 0.9 (:rate opts))) (conj :write-rate)))

(defn run-benchmark!
  "Open the subscriptions, write through warmup and the measured period, then verify."
  [conn state opts]
  (let [descriptors (live/queries (:users opts) (:items opts) (:subscriptions opts))
        tracker (atom {:last-tx-id nil :transactions 0 :catch-up-ms [] :errors []})
        entries (atom [])
        running (atom true)
        probes (atom [])
        skipped (atom 0)
        start (System/nanoTime)
        initialization-ms (atom nil)]
    (try
      (open-subscriptions! conn descriptors entries tracker running)
      (reset! initialization-ms (/ (- (System/nanoTime) start) 1e6))
      (run-workload! conn state {:entries @entries
                                 :tracker tracker
                                 :running running
                                 :probes probes
                                 :skipped skipped
                                 :opts opts})
      (drain! @entries tracker running)
      (verify-final! conn @entries tracker running)
      (catch Exception e
        (error! tracker {:error :run-aborted :message (.getMessage e)}))
      (finally
        ;; After a full drain every probe has finished; after an abort, stopping the
        ;; consumers makes the remaining probes give up.
        (close-subscriptions! @entries running)
        (doseq [^Thread probe @probes]
          (.join probe))))
    (let [{:keys [transactions catch-up-ms final-catch-up-ms errors]} @tracker
          metrics {:committed-transactions transactions
                   :transactions-per-second (/ transactions (:duration opts))
                   :catch-up-ms (percentiles catch-up-ms)
                   :final-catch-up-ms final-catch-up-ms
                   :errors errors}
          failures (failed-checks @entries metrics opts)]
      (merge metrics
             {:subscriptions (:subscriptions opts)
              :unique-queries (count (set (map :query descriptors)))
              :passed? (empty? failures)
              :failed-checks failures
              :initialization-ms @initialization-ms
              :per-query-initialization-ms (percentiles (keep :initialization-ms @entries))
              :final-result-rows (percentiles (map #(reduce + 0 (vals @(:rows %))) @entries))
              :skipped-operations @skipped}))))

;; ---------------------------------------------------------------------------
;; Entry point

(defn- print-result [result]
  (println (format "%4d subscriptions  %.2f tx/s  catch-up p95=%s max=%s ms (%d probes)  final catch-up=%s ms  %s"
                   (:subscriptions result)
                   (:transactions-per-second result)
                   (get-in result [:catch-up-ms :p95])
                   (get-in result [:catch-up-ms :max])
                   (get-in result [:catch-up-ms :count])
                   (:final-catch-up-ms result)
                   (if (:passed? result) "PASS" "FAIL")))
  (when (seq (:failed-checks result))
    (println "Failed checks:" (:failed-checks result))))

(defn run!
  "Load the fixture into a fresh database and run the benchmark once."
  [opts]
  (let [report {:created-at (str (Instant/now))
                :options opts
                :revision (str/trim (:out (shell/sh "git" "rev-parse" "HEAD")))
                :feldera-revision "1d81c0323e96a0cbc9e50e584babc112e5cfdb16"
                :environment (some-> (System/getenv "AUCTIONMARK_ENVIRONMENT") (json/read-str :key-fn keyword))}]
    (with-open [conn (tc/connect (:host opts) (:port opts))]
      (let [result (run-benchmark! conn (live/load-fixture! conn opts) opts)]
        (print-result result)
        (merge report result)))))

(defn -main [& args]
  (let [{:keys [options errors summary]} (cli/parse-opts args options)]
    (if (:help options)
      (println summary)
      (let [report (try
                     (when (seq errors)
                       (throw (ex-info (str/join "; " errors) {})))
                     (run! (validate-options options))
                     (catch Exception e
                       {:passed? false :error (.getMessage e) :options options}))]
        (spit (:output options) (json/write-str report :escape-slash false))
        (println "Report:" (:output options))
        (when-let [error (:error report)]
          (binding [*out* *err*] (println error)))
        (shutdown-agents)
        (System/exit (if (:passed? report) 0 1))))))
