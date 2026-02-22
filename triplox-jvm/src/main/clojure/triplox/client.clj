(ns triplox.client
  "Clojure client API for Triplox."
  (:require [triplox.types :as types]
            [triplox.tx :as tx])
  (:import [io.triplox.client TriploxNode DbHandle QueryResult TxKeyResult TxResultValue]))

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
  "Open a DB snapshot. Returns a db map with :conn, :db-id, :tx-id."
  ([conn]
   (open-db conn nil))
  ([conn opts]
   (let [basis-tx-id (when-let [b (:basis-tx-id opts)] (Long/valueOf (long b)))
         ^DbHandle handle (.openDb ^TriploxNode (:node conn) basis-tx-id)]
     {:conn conn
      :db-id (.dbId handle)
      :tx-id (.txId handle)})))

(defn close-db
  "Release a DB snapshot."
  [conn db]
  (.closeDb ^TriploxNode (:node conn) (DbHandle. (:db-id db) (:tx-id db))))

(defn q
  "Execute a Datalog query. Returns a vector of vectors."
  [db query-edn]
  (let [query-str (pr-str query-edn)
        ^QueryResult result (.query ^TriploxNode (get-in db [:conn :node])
                                    (DbHandle. (:db-id db) (:tx-id db))
                                    query-str)]
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
