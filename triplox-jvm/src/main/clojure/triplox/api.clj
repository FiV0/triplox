(ns triplox.api
  "Clojure client API for Triplox."
  (:require [triplox.types :as types]
            [triplox.tx :as tx])
  (:import [io.triplox.client TriploxNode Db TxKeyResult TxResultValue]))

(defn connect
  "Connect to a Triplox server. Returns a TriploxNode (AutoCloseable)."
  (^TriploxNode [host port]
   (connect host port {}))
  (^TriploxNode [host port params]
   (TriploxNode/connect host (int port) (into {} (map (fn [[k v]] [(name k) (str v)])) params))))

(defn db
  "Open a DB snapshot. Returns a Db (AutoCloseable)."
  (^Db [conn]
   (db conn nil))
  (^Db [conn {:keys [basis-tx-id] :as _opts}]
   (.openDb ^TriploxNode conn (when basis-tx-id (long basis-tx-id)))))

(defn q
  "Execute a Datalog query. Returns a vector of vectors."
  [db query]
  (mapv (fn [row] (mapv types/wire->clj row))
        (.query ^Db db (pr-str query))))

(defn transact
  "Execute a transaction and wait for indexing. Returns result map."
  [conn tx-data]
  (let [^TxResultValue result (.executeTx ^TriploxNode conn (tx/tx-data->ops tx-data))]
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
