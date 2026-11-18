(ns job.engines
  (:require [clojure.java.io :as io]
            [job.data :as data]
            [job.queries :as queries]
            [job.source :as source])
  (:import [java.lang AutoCloseable]))

(defn- function [ns-name name]
  (requiring-resolve (symbol ns-name name)))

;; Loads like Datalevin's own JOB-bench: the source's numeric entity ids go straight
;; into `fill-db` as trusted datoms, bypassing transaction processing, then `analyze`.
(defn- datalevin [{:keys [state-dir]}]
  (let [api #(function "datalevin.core" %)
        db (atom ((api "empty-db") (str (io/file state-dir "database")) source/schema
                  {:closed-schema? true
                   :kv-opts {:mapsize 100000 :key-compress :hu}}))
        load! (fn [{:keys [data-dir]} progress]
                (let [counts (atom {})
                      datoms (atom 0)]
                  (doseq [[[table read-triples] index] (map vector source/tables (range))]
                    (with-open [reader (io/reader (data/table-file data-dir table) :encoding "UTF-8")]
                      (let [rows (fn [triples]
                                   (swap! counts update table (fnil inc 0))
                                   (when (zero? (mod (get @counts table) 10000))
                                     (reset! progress {:table table :table-index (inc index)
                                                       :tables (count source/tables)
                                                       :rows (reduce + (vals @counts))
                                                       :transactions 0}))
                                   triples)]
                        (swap! db (api "fill-db")
                               (sequence (comp (partition-by first)
                                               (map rows)
                                               cat
                                               (map (fn [[e a v]] (swap! datoms inc) ((api "datom") e a v))))
                                         (read-triples reader))))))
                  ((api "analyze") @db)
                  {:table-counts @counts :attempted-datoms @datoms}))]
    {:schema! (constantly nil)
     :load! load!
     :db #(deref db)
     :query (fn [db id] (mapv vec ((api "q") (get queries/queries id) db)))
     :close! #((api "close-db") @db)}))

(defn- triplox [{:keys [host port]}]
  (let [api #(function "xyz.triplox.api" %)
        view #(function "xyz.triplox.view" %)
        conn ((api "connect") host port)
        transact! (fn [tx]
                    (let [result ((api "transact") conn tx)]
                      (when-not (:committed? result)
                        (throw (ex-info "Triplox transaction rejected" result)))
                      (select-keys result [:tx-id :system-time])))
        await-tx! (fn [tx]
                    ;; Opening a db as of `tx` waits until `tx` is indexed, but the server
                    ;; gives up after its own timeout; retry those until the backlog drains.
                    (let [db (loop []
                               (let [result (try ((api "db") conn tx)
                                                 (catch Exception e
                                                   (if (some-> (ex-message e) (.contains "was not indexed within"))
                                                     ::not-indexed
                                                     (throw e))))]
                                 (if (= ::not-indexed result) (recur) result)))
                          rejected ((api "q") db '[:find ?tx ?error :where [?tx :db/txError ?error]])]
                      (when (seq rejected)
                        (throw (ex-info "Triplox transactions rejected"
                                        {:count (count rejected) :first (first rejected)})))))]
    {:schema! #(transact! (data/schema-tx))
     :transact! #((api "submit-tx") conn %)
     :await-tx! await-tx!
     :indexed-tx #(.txId (.txKey ((api "db") conn)))
     :db #((api "db") conn)
     :query (fn [db id] ((api "q") db (queries/translate id)))
     ;; Every batch sends a delta, empty if unchanged, so a watch on the rows sees each applied tx-key.
     :view! (fn [id]
              (let [v ((view "->view") conn (queries/translate id))
                    applied (atom nil)]
                ;; The watch runs on the view's worker right after it applies a delta.
                (add-watch (:view v) ::applied (fn [& _] (reset! applied ((api "tx-key") (:sub v)))))
                (assoc v ::applied applied)))
     :await-view! (fn [v tx timeout-ms]
                    (let [deadline (+ (System/nanoTime) (* timeout-ms 1000000))]
                      (loop []
                        (let [applied @(::applied v)]
                          (cond
                            (and applied (<= (:tx-id tx) (:tx-id applied)))
                            {:rows ((view "get-view") v) :tx-key applied}
                            ((view "done?") v) (throw (ex-info "Triplox view stopped" {}))
                            (<= deadline (System/nanoTime))
                            (throw (ex-info "Triplox view did not apply the transaction"
                                            {:tx tx :applied applied :timeout-ms timeout-ms}))
                            :else (do (Thread/sleep 1) (recur)))))))
     :close! #(.close ^AutoCloseable conn)}))

(defn open! [{:keys [engine] :as config}]
  (case engine
    "datalevin" (datalevin config)
    ("triplox-standard" "triplox-incremental") (triplox config)
    (throw (ex-info "Unknown benchmark engine" {:engine engine}))))
