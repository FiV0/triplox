(ns auctionmark.sync
  "Bounded live-query experiment with a transaction-based delivery oracle."
  (:refer-clojure :exclude [run!])
  (:require [auctionmark.live :as live]
            [auctionmark.measure :as measure]
            [auctionmark.procedures :as proc]
            [auctionmark.resources :as resources]
            [clojure.data.json :as json]
            [clojure.java.shell :as shell]
            [clojure.string :as str]
            [clojure.tools.cli :as cli]
            [xyz.triplox.api :as tc])
  (:import [java.lang ProcessHandle]
           [java.time Instant]
           [java.util Random]))

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
   [nil "--sla-ms MS" "Maximum p95 submission-to-applied latency" :default 250.0 :parse-fn parse-double]
   [nil "--drain-ms MS" "Maximum delivery backlog age and final drain" :default 5000 :parse-fn parse-long]
   [nil "--memory-mib N" "Combined sampled RSS limit" :default 8192 :parse-fn parse-long]
   [nil "--server-pid PID" "Server process to sample" :parse-fn parse-long]
   [nil "--minio-pid PID" "MinIO process to sample" :parse-fn parse-long]
   [nil "--output PATH" "JSON report" :default "sync-report.json"]
   ["-h" "--help"]])

(defn validate-options [opts]
  (let [stages (or (:stages opts) [5 20 50 100])
        positive? #(and (number? %) (Double/isFinite (double %)) (pos? %))]
    (when-not (and (#{"smoke" "capacity"} (:mode opts))
                   (or (= "smoke" (:mode opts)) (:stages opts))
                   (seq stages) (every? positive? stages)
                   (every? positive? (map opts [:port :users :items :rate :duration :sla-ms :drain-ms :memory-mib]))
                   (number? (:warmup opts)) (Double/isFinite (double (:warmup opts))) (<= 0 (:warmup opts))
                   (<= (:users opts) (:items opts)) (<= (apply max stages) (* 5 (:users opts)))
                   (or (= "capacity" (:mode opts)) (<= (apply max stages) 100)))
      (throw (ex-info "Invalid limits: capacity requires explicit stages; need items >= users >= subscriptions/5" {:options opts})))
    (assoc opts :stages stages)))

(defn pause-until! [deadline]
  (let [ms (/ (- deadline (System/nanoTime)) 1e6)]
    (when (pos? ms) (Thread/sleep (long (Math/ceil ms))))))

(defn error! [tracker error] (swap! tracker update :errors conj error))

(defn consume! [{:keys [sub rows id]} tracker running]
  (try
    (while @running
      (let [delta (tc/take! sub 100)]
        (cond
          (= ::tc/timeout delta) nil
          (nil? delta) (throw (ex-info "Subscription ended" {:query-id id}))
          :else (let [updated (swap! rows live/apply-delta delta)
                      applied (System/nanoTime)]
                  (swap! tracker measure/record-delivery (:tx-id (tc/tx-key sub)) id updated applied)))))
    (catch Exception e
      (when @running (error! tracker {:error :consumer-failed :query-id id :message (.getMessage e)})))))

(defn open-subscriptions! [conn descriptors model entries tracker running]
  (doseq [d descriptors]
    (let [start (System/nanoTime) sub (tc/subscribe conn (:query d))
          entry (assoc d :sub sub :rows (atom {}))]
      (swap! entries conj entry)
      (let [expected (live/result-rows model d)]
        (when (seq expected)
          (let [delta (tc/take! sub 10000)]
            (when (or (nil? delta) (= ::tc/timeout delta))
              (throw (ex-info "Priming timed out" {:query-id (:id d)})))
            (swap! (:rows entry) live/apply-delta delta)))
        (when-not (= expected @(:rows entry))
          (throw (ex-info "Incorrect priming result" {:query-id (:id d)}))))
      (swap! entries assoc (dec (count @entries))
             (assoc entry :initialization-ms (/ (- (System/nanoTime) start) 1e6)
                    :consumer (.start (Thread/ofVirtual) ^Runnable #(consume! entry tracker running)))))))

(defn close-subscriptions! [entries running]
  (reset! running false)
  (doseq [{:keys [sub]} entries] (.close sub))
  (doseq [{:keys [consumer]} entries :when consumer] (.join ^Thread consumer 2000)))

(defn check-limits! [tracker samples opts]
  (when (seq (:errors @tracker)) (throw (ex-info "Subscription error" {})))
  (when (> (resources/combined-rss (last @samples)) (:memory-mib opts))
    (throw (ex-info "Combined process RSS exceeded memory limit" {})))
  (when (some #(< (:start-ns %) (- (System/nanoTime) (* 1e6 (:drain-ms opts)))) (vals (:pending @tracker)))
    (throw (ex-info "Expected delivery exceeded backlog age limit" {}))))

(defn tracked-transactor [model descriptors tracker opts measured-start end next-due ^Random pacing-rng samples]
  (fn [conn tx]
    (check-limits! tracker samples opts)
    (let [before @model after (live/apply-tx before tx)
          changes (into {} (for [d descriptors :let [rows (live/result-rows after d)]
                                 :when (not= rows (live/result-rows before d))]
                             [(:id d) {:rows rows :kind (:kind d)}]))
          scheduled @next-due]
      (pause-until! scheduled)
      (let [start (System/nanoTime) result (live/transact! conn tx) ack (System/nanoTime)]
        (reset! next-due (+ (max scheduled start) (long (* (/ 1e9 (:rate opts)) (+ 0.9 (* 0.2 (.nextDouble pacing-rng)))))))
        (reset! model after)
        (swap! tracker measure/record-transaction (:tx-id result)
               {:start-ns start :ack-ns ack :scheduled-ns scheduled :measured? (<= measured-start start (dec end))}
               changes (count descriptors))
        result))))

(defn drain! [tracker timeout-ms]
  (let [deadline (+ (System/nanoTime) (* 1000000 timeout-ms))]
    (while (and (seq (:pending @tracker)) (< (System/nanoTime) deadline) (empty? (:errors @tracker)))
      (Thread/sleep 10))))

(defn verify-final! [conn model entries tracker]
  (let [db (tc/db conn)]
    (doseq [{:keys [id query rows] :as entry} entries]
      (let [expected (live/result-rows model entry)]
        (when-not (= expected @rows) (error! tracker {:error :final-client-mismatch :query-id id}))
        (when-not (= expected (frequencies (tc/q db query)))
          (error! tracker {:error :final-database-mismatch :query-id id}))))))

(defn run-stage! [conn fixture opts n pids]
  (let [descriptors (live/queries (:users opts) (:items opts) n)
        model (atom (:model fixture)) state (live/copy-state (:state fixture))
        tracker (atom (measure/empty-tracker)) entries (atom []) running (atom true)
        samples (atom [(resources/sample pids)]) sampling (atom true)
        sampler (.start (Thread/ofVirtual) ^Runnable #(while @sampling
                                                       (Thread/sleep 500)
                                                       (swap! samples conj (resources/sample pids))))
        start (System/nanoTime) initialization (atom nil) skipped (atom 0)]
    (try
      (open-subscriptions! conn descriptors @model entries tracker running)
      (reset! initialization (/ (- (System/nanoTime) start) 1e6))
      (let [begin (System/nanoTime) measured-start (+ begin (long (* 1e9 (:warmup opts))))
            end (+ measured-start (long (* 1e9 (:duration opts))))
            next-due (atom begin) rng (Random. (:seed opts))]
        (binding [proc/*transact* (tracked-transactor model descriptors tracker opts measured-start end next-due (Random. (+ 1 (:seed opts))) samples)]
          (while (< (System/nanoTime) end)
            (let [before (count (:known @tracker))]
              (live/mutate! conn rng state @model)
              (when (= before (count (:known @tracker))) (swap! skipped inc) (Thread/sleep 10)))
            (check-limits! tracker samples opts)))
        (drain! tracker (:drain-ms opts))
        (verify-final! conn @model @entries tracker))
      (catch Exception e (error! tracker {:error :stage-aborted :message (.getMessage e)}))
      (finally
        (reset! sampling false)
        (.join sampler 2000)
        (close-subscriptions! @entries running)
        (try (live/restore! conn @model (:model fixture))
             (catch Exception e (error! tracker {:error :restore-failed :message (.getMessage e)})))))
    (let [metrics (measure/summary @tracker (:duration opts)) resources (resources/summarize @samples)
          p95 (get-in metrics [:latency-ms :p95])
          failures (cond-> []
                     (not= n (count @entries)) (conj :initialization)
                     (seq (:errors metrics)) (conj :correctness-or-runtime)
                     (pos? (:missing-deliveries metrics)) (conj :missing-deliveries)
                     (pos? (:unmatched-deliveries metrics)) (conj :unmatched-deliveries)
                     (nil? p95) (conj :no-measured-deliveries)
                     (and p95 (>= p95 (:sla-ms opts))) (conj :freshness)
                     (< (:transactions-per-second metrics) (* 0.9 (:rate opts))) (conj :write-rate)
                     (> (:combined-peak-rss-mib resources) (:memory-mib opts)) (conj :memory))]
      (merge metrics {:subscriptions n :unique-queries (count (set (map :query descriptors)))
                      :passed? (empty? failures) :failed-checks failures :initialization-ms @initialization
                      :per-query-initialization-ms (measure/percentiles (keep :initialization-ms @entries))
                      :final-result-rows (measure/percentiles (map #(reduce + 0 (vals @(:rows %))) @entries))
                      :skipped-operations @skipped :resources resources}))))

(defn run! [opts]
  (let [pids (cond-> {:client (.pid (ProcessHandle/current))}
               (:server-pid opts) (assoc :server (:server-pid opts))
               (:minio-pid opts) (assoc :minio (:minio-pid opts)))
        report {:created-at (str (Instant/now)) :options opts
                :revision (str/trim (:out (shell/sh "git" "rev-parse" "HEAD")))
                :feldera-revision "ded0d390b64bdfe82a9afac9084404ad3809547e"
                :environment (some-> (System/getenv "AUCTIONMARK_ENVIRONMENT") (json/read-str :key-fn keyword))
                :sampled-processes (keys pids)}]
    (with-open [conn (tc/connect (:host opts) (:port opts) {:subscription-thread-factory (.factory (Thread/ofVirtual))})]
      (let [fixture (live/load-fixture! conn opts)]
        (loop [stages (:stages opts) results []]
          (if-let [n (first stages)]
            (let [result (run-stage! conn fixture opts n pids) results (conj results result)]
              (println (format "%4d subscriptions  %.2f tx/s  p95=%s ms  missing=%d  RSS=%.0f MiB  %s"
                               n (:transactions-per-second result) (get-in result [:latency-ms :p95])
                               (:missing-deliveries result) (get-in result [:resources :combined-peak-rss-mib])
                               (if (:passed? result) "PASS" "STOP")))
              (when (seq (:failed-checks result)) (println "Failed checks:" (:failed-checks result)))
              (if (:passed? result) (recur (next stages) results) (assoc report :stages results :passed? false)))
            (assoc report :stages results :passed? true)))))))

(defn -main [& args]
  (let [{:keys [options errors summary]} (cli/parse-opts args options)]
    (if (:help options) (println summary)
      (let [report (try
                     (when (seq errors) (throw (ex-info (str/join "; " errors) {})))
                     (run! (validate-options options))
                     (catch Exception e {:passed? false :error (.getMessage e) :options options}))]
        (spit (:output options) (json/write-str report :escape-slash false))
        (println "Report:" (:output options))
        (when-let [error (:error report)] (binding [*out* *err*] (println error)))
        (shutdown-agents)
        (System/exit (if (:passed? report) 0 1))))))
