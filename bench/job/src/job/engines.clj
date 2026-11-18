(ns job.engines
  (:require [clojure.java.io :as io]
            [datalevin.core :as d]
            [job.data :as data]
            [job.queries :as queries]
            [job.source :as source]
            [xyz.triplox.api :as triplox]
            [xyz.triplox.view :as view])
  (:import [java.lang AutoCloseable]))

;; Loads like Datalevin's own JOB-bench: the source's numeric entity ids go straight
;; into `fill-db` as trusted datoms, bypassing transaction processing, then `analyze`.
(defn- datalevin [{:keys [state-dir]}]
  (let [db (atom (d/empty-db (str (io/file state-dir "database")) source/schema
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
                        (swap! db d/fill-db
                               (sequence (comp (partition-by first)
                                               (map rows)
                                               cat
                                               (map (fn [[e a v]] (swap! datoms inc) (d/datom e a v))))
                                         (read-triples reader))))))
                  (d/analyze @db)
                  {:table-counts @counts :attempted-datoms @datoms}))]
    {:schema! (constantly nil)
     :load! load!
     :db #(deref db)
     :query (fn [db id] (mapv vec (d/q (get queries/queries id) db)))
     :close! #(d/close-db @db)}))

(defn- triplox [{:keys [host port]}]
  (let [conn (triplox/connect host port)
        transact! (fn [tx]
                    (let [result (triplox/transact conn tx)]
                      (when-not (:committed? result)
                        (throw (ex-info "Triplox transaction rejected" result)))
                      (select-keys result [:tx-id :system-time])))
        await-tx! (fn [tx]
                    (let [db (triplox/db conn tx)
                          rejected (triplox/q db '[:find ?tx ?error :where [?tx :db/txError ?error]])]
                      (when (seq rejected)
                        (throw (ex-info "Triplox transactions rejected"
                                        {:count (count rejected) :first (first rejected)})))))]
    {:schema! #(transact! (data/schema-tx))
     :transact! #(triplox/submit-tx conn %)
     :await-tx! await-tx!
     :indexed-tx-id #(.txId (.txKey (triplox/db conn)))
     :db #(triplox/db conn)
     :query (fn [db id] (triplox/q db (queries/translate id)))
     ;; Every batch sends a delta, empty if unchanged, so a watch on the rows sees each applied tx-key.
     :view! (fn [id]
              (let [v (view/->view conn (queries/translate id))
                    applied (atom nil)]
                ;; The watch runs on the view's worker right after it applies a delta.
                (add-watch (:view v) ::applied (fn [& _] (reset! applied (triplox/tx-key (:sub v)))))
                (assoc v ::applied applied)))
     :await-view! (fn [v tx timeout-ms]
                    (let [deadline (+ (System/nanoTime) (* timeout-ms 1000000))]
                      (loop []
                        (let [applied @(::applied v)]
                          (cond
                            (and applied (<= (:tx-id tx) (:tx-id applied)))
                            {:rows (view/get-view v) :tx-key applied}
                            (view/done? v) (throw (ex-info "Triplox view stopped" {}))
                            (<= deadline (System/nanoTime))
                            (throw (ex-info "Triplox view did not apply the transaction"
                                            {:tx tx :applied applied :timeout-ms timeout-ms}))
                            :else (do (Thread/sleep 1) (recur)))))))
     :close! #(.close ^AutoCloseable conn)}))

(defn open! [{:keys [engine] :as config}]
  (case engine
    :datalevin (datalevin config)
    (:triplox-standard :triplox-incremental) (triplox config)
    (throw (ex-info "Unknown benchmark engine" {:engine engine}))))
