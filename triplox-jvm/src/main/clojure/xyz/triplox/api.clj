(ns xyz.triplox.api
  "Clojure client API for Triplox."
  (:require [xyz.triplox.types :as types]
            [xyz.triplox.tx :as tx])
  (:import [xyz.triplox.client TriploxNode Db TxBasis TxKey TxResult QueryArg QueryArg$Scalar QueryArg$Collection]))

(defn connect
  "Connect to a Triplox server. Returns a TriploxNode (AutoCloseable)."
  ^TriploxNode [host port]
  (TriploxNode/connect host (int port)))

(defn db
  "Open a DB value. Returns a Db."
  (^Db [conn]
   (db conn nil))
  (^Db [conn {:keys [tx-basis] :as _opts}]
   (if tx-basis
     (.openDbAsOf ^TriploxNode conn (TxBasis. (long (:tx-id tx-basis)) (:system-time tx-basis) (long (:tx-eid tx-basis))))
     (.openDb ^TriploxNode conn))))

(defn q
  "Execute a Datalog query. Returns a vector of vectors."
  [db query & args]
  (if (seq args)
    (let [query-args (mapv (fn [a]
                            (if (sequential? a)
                              (QueryArg$Collection. (vec a))
                              (QueryArg$Scalar. a)))
                          args)]
      (mapv (fn [row] (mapv types/wire->clj row))
            (.query ^Db db (pr-str query) query-args)))
    (mapv (fn [row] (mapv types/wire->clj row))
          (.query ^Db db (pr-str query)))))

(defn transact
  "Execute a transaction and wait for indexing. Returns result map."
  [conn tx-data]
  (let [^TxResult result (.executeTx ^TriploxNode conn (tx/tx-data->ops tx-data))]
    {:tx-id (.txId result)
     :system-time (.systemTime result)
     :tx-eid (.txEid result)
     :committed? (.isCommitted result)
     :error-message (.errorMessage result)}))

(defn submit-tx
  "Submit a fire-and-forget transaction. Returns result map."
  [conn tx-data]
  (let [ops (tx/tx-data->ops tx-data)
        ^TxKey result (.submitTx ^TriploxNode conn ops)]
    {:tx-id (.txId result)
     :system-time (.systemTime result)}))
