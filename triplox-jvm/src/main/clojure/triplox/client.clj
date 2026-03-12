(ns triplox.client
  "Clojure client API for Triplox."
  (:require [triplox.types :as types]
            [triplox.tx :as tx])
  (:import [io.triplox.client TriploxNode Db QueryResult TxKeyResult TxResultValue]))

(defn connect
  "Connect to a Triplox server. Returns a TriploxNode (AutoCloseable)."
  (^TriploxNode [host port]
   (connect host port {}))
  (^TriploxNode [host port params]
   (let [str-params (java.util.TreeMap.)]
     (doseq [[k v] params]
       (.put str-params (name k) (str v)))
     (TriploxNode/connect host (int port) str-params))))

(defn open-db
  "Open a DB snapshot. Returns a Db (AutoCloseable)."
  (^Db [conn]
   (open-db conn nil))
  (^Db [conn opts]
   (let [basis-tx-id (when-let [b (:basis-tx-id opts)] (Long/valueOf (long b)))]
     (.openDb ^TriploxNode conn basis-tx-id))))

(defn q
  "Execute a Datalog query. Returns a vector of vectors."
  [db query-edn]
  (let [query-str (pr-str query-edn)
        ^QueryResult result (.query ^Db db query-str)]
    (mapv (fn [row] (mapv types/wire->clj row))
          (.rows result))))

(defn transact
  "Execute a transaction and wait for indexing. Returns result map."
  [conn tx-data]
  (let [ops (tx/tx-data->ops tx-data)
        ^TxResultValue result (.executeTx ^TriploxNode conn ops)]
    {:tx-id (.txId result)
     :system-time (.systemTime result)
     :committed? (.isCommitted result)
     :seq-num (.seqNum result)
     :error-message (.errorMessage result)}))

(defn submit-tx
  "Submit a fire-and-forget transaction. Returns result map."
  [conn tx-data]
  (let [ops (tx/tx-data->ops tx-data)
        ^TxKeyResult result (.submitTx ^TriploxNode conn ops)]
    {:tx-id (.txId result)
     :system-time (.systemTime result)}))
