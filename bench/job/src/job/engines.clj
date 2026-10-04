(ns job.engines
  (:require [clojure.java.io :as io]
            [job.data :as data]
            [job.queries :as queries])
  (:import [java.lang AutoCloseable]))

(defn- function [ns-name name]
  (requiring-resolve (symbol ns-name name)))

(defn- datalevin [{:keys [state-dir]}]
  (let [api #(function "datalevin.core" %)
        conn ((api "get-conn") (str (io/file state-dir "database")) data/schema)]
    {:schema! (constantly nil)
     :transact! (fn [tx] ((api "transact!") conn tx) nil)
     :db #(deref conn)
     :query (fn [db id] (mapv vec ((api "q") (get queries/queries id) db)))
     :close! #((api "close") conn)}))

(defn- datomic [{:keys [datomic-uri timeout-ms]}]
  (let [api #(function "datomic.api" %)]
    (when-not ((api "create-database") datomic-uri)
      (throw (ex-info "Datomic benchmark database already exists" {:uri datomic-uri})))
    (let [conn ((api "connect") datomic-uri)
          current (atom ((api "db") conn))
          transact! (fn [tx]
                      (let [result (deref ((api "transact") conn tx) timeout-ms ::timeout)]
                        (when (= result ::timeout)
                          (throw (ex-info "Datomic transaction timed out" {})))
                        (reset! current (:db-after result))
                        {:tx-id ((api "basis-t") @current)}))]
      {:schema! #(transact! (data/schema-tx :datomic))
       :transact! transact!
       :db #(deref current)
       :query (fn [db id] (mapv vec ((api "q") (queries/translate id :datomic) db)))
       :close! #((api "release") conn)})))

(defn- triplox [{:keys [host port]}]
  (let [api #(function "xyz.triplox.api" %)
        view #(function "xyz.triplox.view" %)
        conn ((api "connect") host port)
        transact! (fn [tx]
                    (let [result ((api "transact") conn tx)]
                      (when-not (:committed? result)
                        (throw (ex-info "Triplox transaction rejected" result)))
                      (select-keys result [:tx-id :system-time])))
        await-tx! (fn [tx timeout-ms]
                    ;; Opening a db as of `tx` waits until `tx` is indexed, but the server
                    ;; gives up after its own timeout; retry those until the backlog drains
                    ;; or our own deadline passes.
                    (let [deadline (+ (System/nanoTime) (* timeout-ms 1000000))
                          db (loop []
                               (let [result (try ((api "db") conn tx)
                                                 (catch Exception e
                                                   (if (some-> (ex-message e) (.contains "was not indexed within"))
                                                     ::not-indexed
                                                     (throw e))))]
                                 (cond
                                   (not= ::not-indexed result) result
                                   (< (System/nanoTime) deadline) (recur)
                                   :else (throw (ex-info "Triplox transaction was not indexed in time"
                                                         {:tx tx :timeout-ms timeout-ms})))))
                          rejected ((api "q") db '[:find ?tx ?error :where [?tx :db/txError ?error]])]
                      (when (seq rejected)
                        (throw (ex-info "Triplox transactions rejected"
                                        {:count (count rejected) :first (first rejected)})))))]
    {:schema! #(transact! (data/schema-tx :triplox))
     :transact! #((api "submit-tx") conn %)
     :await-tx! await-tx!
     :indexed-tx #(.txId (.txKey ((api "db") conn)))
     :db #((api "db") conn)
     :query (fn [db id] ((api "q") db (queries/translate id :triplox)))
     :view! (fn [id] ((view "->view") conn (queries/translate id :triplox)))
     :await-view! (fn [v tx timeout-ms] ((view "await-tx") v tx timeout-ms))
     :close! #(.close ^AutoCloseable conn)}))

(defn open! [{:keys [engine] :as config}]
  (case engine
    "datalevin" (datalevin config)
    "datomic" (datomic config)
    ("triplox-standard" "triplox-incremental") (triplox config)
    (throw (ex-info "Unknown benchmark engine" {:engine engine}))))
