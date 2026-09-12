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
                      (select-keys result [:tx-id :system-time])))]
    {:schema! #(transact! (data/schema-tx :triplox))
     :transact! transact!
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
