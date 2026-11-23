(ns auctionmark.sync
  "Bounded live-query experiment. Subscription results are checked against standard
  queries when primed and at the end of each stage. Freshness is the time until the
  last subscription reaches a probed transaction's tx-key."
  (:refer-clojure :exclude [run!])
  (:require [auctionmark.live :as live]
            [auctionmark.resources :as resources]
            [clojure.data.json :as json]
            [clojure.java.shell :as shell]
            [clojure.string :as str]
            [clojure.tools.cli :as cli]
            [xyz.triplox.api :as tc])
  (:import [java.lang ProcessHandle]
           [java.time Instant]
           [java.util Random]))

;; ---------------------------------------------------------------------------
;; Options

(def options
  [[nil "--host HOST" "Triplox host" :default "localhost"]
   [nil "--port PORT" "Triplox port" :default 5477 :parse-fn parse-long]
   [nil "--mode MODE" "smoke or capacity" :default "smoke"]
   [nil "--stages COUNTS" "Comma-separated subscription counts" :parse-fn #(mapv parse-long (str/split % #","))]
   [nil "--users N" "Fixture users" :default 20 :parse-fn parse-long]
   [nil "--items N" "Fixture items" :default 100 :parse-fn parse-long]
   [nil "--seed N" "Workload seed" :default 42 :parse-fn parse-long]
   [nil "--rate N" "Target committed transactions/second" :default 5.0 :parse-fn parse-double]
   [nil "--warmup SECONDS" "Warmup per stage" :default 10.0 :parse-fn parse-double]
   [nil "--duration SECONDS" "Measurement per stage" :default 30.0 :parse-fn parse-double]
   [nil "--sla-ms MS" "Maximum p95 time until every subscription reaches a transaction" :default 250.0 :parse-fn parse-double]
   [nil "--probe-ms MS" "Interval between catch-up probes" :default 500 :parse-fn parse-long]
   [nil "--drain-ms MS" "Maximum wait for every subscription to catch up" :default 5000 :parse-fn parse-long]
   [nil "--memory-mib N" "Combined sampled RSS limit" :default 8192 :parse-fn parse-long]
   [nil "--server-pid PID" "Server process to sample" :parse-fn parse-long]
   [nil "--storage-pid PID" "Object storage process to sample" :parse-fn parse-long]
   [nil "--output PATH" "JSON report" :default "sync-report.json"]
   ["-h" "--help"]])

(defn validate-options [opts]
  (let [{:keys [mode users items warmup]} opts
        stages (or (:stages opts) [5 20 50 100])
        positive? #(and (number? %) (Double/isFinite (double %)) (pos? %))]
    (when-not (and (#{"smoke" "capacity"} mode)
                   (or (= "smoke" mode) (:stages opts))
                   (seq stages)
                   (every? positive? stages)
                   (every? positive? (map opts [:port :users :items :rate :duration :sla-ms :probe-ms :drain-ms :memory-mib]))
                   (number? warmup)
                   (Double/isFinite (double warmup))
                   (<= 0 warmup)
                   (<= users items)
                   (<= (apply max stages) (* 5 users))
                   (or (= "capacity" mode) (<= (apply max stages) 100)))
      (throw (ex-info "Invalid limits: capacity requires explicit stages; need items >= users >= subscriptions/5"
                      {:options opts})))
    (assoc opts :stages stages)))

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
  "Poll every millisecond until every subscription has reached tx-id. Returns false
  after timeout-ms."
  [entries tx-id timeout-ms]
  (let [deadline (+ (System/nanoTime) (* 1000000 timeout-ms))]
    (loop []
      (cond
        (caught-up? entries tx-id) true
        (< (System/nanoTime) deadline) (do (Thread/sleep 1) (recur))
        :else false))))

;; ---------------------------------------------------------------------------
;; Workload

(defn pause-until! [deadline]
  (let [ms (/ (- deadline (System/nanoTime)) 1e6)]
    (when (pos? ms)
      (Thread/sleep (long (Math/ceil ms))))))

(defn check-limits! [tracker samples opts]
  (when (seq (:errors @tracker))
    (throw (ex-info "Subscription error" {})))
  (when (> (resources/combined-rss (last @samples)) (:memory-mib opts))
    (throw (ex-info "Combined process RSS exceeded memory limit" {}))))

(defn- jittered-interval-ns
  "Time to the next transaction at the target rate, with +-10% jitter."
  [rate ^Random rng]
  (long (* (/ 1e9 rate) (+ 0.9 (* 0.2 (.nextDouble rng))))))

(defn- probe!
  "Record how long after its submission the last subscription reached the transaction."
  [entries tracker opts {:keys [tx-id start-ns]}]
  (if (await-catch-up! entries tx-id (:drain-ms opts))
    (swap! tracker update :catch-up-ms conj (/ (- (System/nanoTime) start-ns) 1e6))
    (error! tracker {:error :catch-up-timeout :tx-id tx-id})))

(defn tracked-transactor
  "A transact fn that paces writes and, in the measured window, probes one transaction
  every probe-ms."
  [{:keys [entries tracker opts samples measured-start end next-due next-probe pacing-rng probes]}]
  (fn [conn tx]
    (check-limits! tracker samples opts)
    (let [scheduled @next-due]
      (pause-until! scheduled)
      (let [start (System/nanoTime)
            result (live/transact! conn tx)]
        (reset! next-due (+ (max scheduled start) (jittered-interval-ns (:rate opts) pacing-rng)))
        (swap! tracker assoc :last-tx-id (:tx-id result))
        (when (<= measured-start start (dec end))
          (swap! tracker update :transactions inc)
          (when (<= @next-probe start)
            (reset! next-probe (+ start (* 1000000 (:probe-ms opts))))
            (swap! probes conj (.start (Thread/ofVirtual)
                                       ^Runnable #(probe! entries tracker opts {:tx-id (:tx-id result)
                                                                                :start-ns start})))))
        result))))

(defn- run-workload!
  "Run random mutations through the tracked transactor for warmup plus duration."
  [conn state {:keys [tracker samples skipped opts] :as ctx}]
  (let [begin (System/nanoTime)
        measured-start (+ begin (long (* 1e9 (:warmup opts))))
        end (+ measured-start (long (* 1e9 (:duration opts))))
        rng (Random. (:seed opts))
        state (assoc state :transact (tracked-transactor (assoc ctx
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
      (check-limits! tracker samples opts))))

(defn- drain!
  "Wait for every subscription to reach the last transaction."
  [entries tracker opts]
  (when-let [tx-id (:last-tx-id @tracker)]
    (when-not (await-catch-up! entries tx-id (:drain-ms opts))
      (error! tracker {:error :not-caught-up :tx-id tx-id}))))

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
;; Stages

(defn- start-sampler!
  "Sample process resources every 500ms while sampling is true."
  [samples sampling pids]
  (.start (Thread/ofVirtual)
          ^Runnable #(while @sampling
                       (Thread/sleep 500)
                       (swap! samples conj (resources/sample pids)))))

(defn percentiles [samples]
  (let [values (vec (sort samples))
        n (count values)
        at (fn [p] (when (pos? n) (nth values (dec (long (Math/ceil (* p n)))))))]
    {:count n
     :p50 (at 0.50)
     :p95 (at 0.95)
     :p99 (at 0.99)
     :max (last values)}))

(defn- failed-checks [n entries metrics resources opts]
  (let [p95 (get-in metrics [:catch-up-ms :p95])]
    (cond-> []
      (not= n (count entries)) (conj :initialization)
      (seq (:errors metrics)) (conj :correctness-or-runtime)
      (nil? p95) (conj :no-probes)
      (and p95 (>= p95 (:sla-ms opts))) (conj :freshness)
      (< (:transactions-per-second metrics) (* 0.9 (:rate opts))) (conj :write-rate)
      (> (:combined-peak-rss-mib resources) (:memory-mib opts)) (conj :memory))))

(defn run-stage!
  "Run one stage with n subscriptions, then restore the fixture."
  [conn fixture opts n pids]
  (let [descriptors (live/queries (:users opts) (:items opts) n)
        state (live/copy-state (:state fixture))
        tracker (atom {:last-tx-id nil :transactions 0 :catch-up-ms [] :errors []})
        entries (atom [])
        running (atom true)
        probes (atom [])
        samples (atom [(resources/sample pids)])
        sampling (atom true)
        sampler (start-sampler! samples sampling pids)
        skipped (atom 0)
        start (System/nanoTime)
        initialization-ms (atom nil)]
    (try
      (open-subscriptions! conn descriptors entries tracker running)
      (reset! initialization-ms (/ (- (System/nanoTime) start) 1e6))
      (run-workload! conn state {:entries @entries
                                 :tracker tracker
                                 :probes probes
                                 :samples samples
                                 :skipped skipped
                                 :opts opts})
      (drain! @entries tracker opts)
      (verify-final! conn @entries tracker running)
      (catch Exception e
        (error! tracker {:error :stage-aborted :message (.getMessage e)}))
      (finally
        (reset! sampling false)
        (.join sampler 2000)
        ;; Probes give up after drain-ms.
        (doseq [^Thread probe @probes]
          (.join probe))
        (close-subscriptions! @entries running)
        (try
          (live/restore! conn (:tx-key fixture))
          (catch Exception e
            (error! tracker {:error :restore-failed :message (.getMessage e)})))))
    (let [{:keys [transactions catch-up-ms errors]} @tracker
          metrics {:committed-transactions transactions
                   :transactions-per-second (/ transactions (:duration opts))
                   :catch-up-ms (percentiles catch-up-ms)
                   :errors errors}
          resources (resources/summarize @samples)
          failures (failed-checks n @entries metrics resources opts)]
      (merge metrics
             {:subscriptions n
              :unique-queries (count (set (map :query descriptors)))
              :passed? (empty? failures)
              :failed-checks failures
              :initialization-ms @initialization-ms
              :per-query-initialization-ms (percentiles (keep :initialization-ms @entries))
              :final-result-rows (percentiles (map #(reduce + 0 (vals @(:rows %))) @entries))
              :skipped-operations @skipped
              :resources resources}))))

;; ---------------------------------------------------------------------------
;; Entry point

(defn- print-stage [n result]
  (println (format "%4d subscriptions  %.2f tx/s  catch-up p95=%s ms (%d probes)  RSS=%.0f MiB  %s"
                   n
                   (:transactions-per-second result)
                   (get-in result [:catch-up-ms :p95])
                   (get-in result [:catch-up-ms :count])
                   (get-in result [:resources :combined-peak-rss-mib])
                   (if (:passed? result) "PASS" "STOP")))
  (when (seq (:failed-checks result))
    (println "Failed checks:" (:failed-checks result))))

(defn run!
  "Run stages in order, stopping at the first that fails."
  [opts]
  (let [pids (cond-> {:client (.pid (ProcessHandle/current))}
               (:server-pid opts) (assoc :server (:server-pid opts))
               (:storage-pid opts) (assoc :storage (:storage-pid opts)))
        report {:created-at (str (Instant/now))
                :options opts
                :revision (str/trim (:out (shell/sh "git" "rev-parse" "HEAD")))
                :feldera-revision "1d81c0323e96a0cbc9e50e584babc112e5cfdb16"
                :environment (some-> (System/getenv "AUCTIONMARK_ENVIRONMENT") (json/read-str :key-fn keyword))
                :sampled-processes (keys pids)}]
    (with-open [conn (tc/connect (:host opts) (:port opts))]
      (let [fixture (live/load-fixture! conn opts)]
        (loop [[n & more] (:stages opts)
               results []]
          (if-not n
            (assoc report :stages results :passed? true)
            (let [result (run-stage! conn fixture opts n pids)
                  results (conj results result)]
              (print-stage n result)
              (if (:passed? result)
                (recur more results)
                (assoc report :stages results :passed? false)))))))))

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
