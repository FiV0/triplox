(ns triplox.client
  "Clojure client API for Triplox."
  (:require [triplox.types :as types]
            [triplox.tx :as tx])
  (:import [io.triplox.client TriploxNode Db QueryResult TxKeyResult TxResultValue]))

(defn connect
  "Connect to a Triplox server. Returns a connection map."
  ([host port]
   (connect host port {}))
  ([host port params]
   (let [str-params (java.util.TreeMap.)]
     (doseq [[k v] params]
       (.put str-params (name k) (str v)))
     {:node (TriploxNode/connect host (int port) str-params)})))

(defn close
  "Close the connection."
  [conn]
  (.close ^TriploxNode (:node conn)))

(defn open-db
  "Open a DB snapshot. Returns a db map with :conn and :handle."
  ([conn]
   (open-db conn nil))
  ([conn opts]
   (let [basis-tx-id (when-let [b (:basis-tx-id opts)] (Long/valueOf (long b)))
         ^Db handle (.openDb ^TriploxNode (:node conn) basis-tx-id)]
     {:conn conn
      :handle handle})))

(defn close-db
  "Release a DB snapshot."
  [db]
  (.close ^Db (:handle db)))

(defn q
  "Execute a Datalog query. Returns a vector of vectors."
  [db query-edn]
  (let [query-str (pr-str query-edn)
        ^QueryResult result (.query ^Db (:handle db) query-str)]
    (mapv (fn [row] (mapv types/wire->clj row))
          (.rows result))))

(defn transact
  "Execute a transaction and wait for indexing. Returns result map."
  [conn tx-data]
  (let [ops (tx/tx-data->ops tx-data)
        ^TxResultValue result (.executeTx ^TriploxNode (:node conn) ops)]
    {:tx-id (.txId result)
     :system-time (.systemTime result)
     :committed? (.isCommitted result)
     :seq-num (.seqNum result)
     :error-message (.errorMessage result)}))

(defn submit-tx
  "Submit a fire-and-forget transaction. Returns result map."
  [conn tx-data]
  (let [ops (tx/tx-data->ops tx-data)
        ^TxKeyResult result (.submitTx ^TriploxNode (:node conn) ops)]
    {:tx-id (.txId result)
     :system-time (.systemTime result)}))

(defn subscribe
  "Stub — not yet supported."
  [_conn _db _query]
  (throw (UnsupportedOperationException. "subscribe is not yet supported")))
