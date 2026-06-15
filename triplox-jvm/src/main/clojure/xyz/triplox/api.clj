(ns xyz.triplox.api
  "Clojure client API for Triplox."
  (:require
   [xyz.triplox.tx :as tx]
   [xyz.triplox.types :as types])
  (:import
   [java.util.concurrent TimeUnit]
   [xyz.triplox.client Db QueryArg$Collection QueryArg$Scalar Subscription TriploxNode TxKey TxResult]))

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
     (.openDbAsOf ^TriploxNode conn (TxKey. (long (:tx-id tx-basis)) (:system-time tx-basis)))
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
    (cond-> {:tx-id (.txId result)
             :system-time (.systemTime result)
             :committed? (.isCommitted result)}
      (not (.isCommitted result))
      (assoc :error-message (.errorMessage result)))))

(defn submit-tx
  "Submit a fire-and-forget transaction. Returns result map."
  [conn tx-data]
  (let [ops (tx/tx-data->ops tx-data)
        ^TxKey result (.submitTx ^TriploxNode conn ops)]
    {:tx-id (.txId result)
     :system-time (.systemTime result)}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Incremental subscriptions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn subscribe
  "Register an incremental query and stream its result deltas. Returns a
  Subscription (Closeable); use with `with-open`. Closing unsubscribes."
  ^Subscription [conn query & args]
  (if (seq args)
    (let [query-args (mapv (fn [a]
                             (if (sequential? a)
                               (QueryArg$Collection. (vec a))
                               (QueryArg$Scalar. a)))
                           args)]
      (.subscribe ^TriploxNode conn (pr-str query) query-args))
    (.subscribe ^TriploxNode conn (pr-str query))))

(defn take!
  "Block for the next delta. Returns a vector of `[row-values weight]` pairs
  (values via `types/wire->clj`), or nil when the stream is closed. The 2-arity
  bounds the wait, returning `::timeout` on expiry."
  ([sub] (types/delta->clj (.take ^Subscription sub)))
  ([sub timeout-ms]
   (let [delta (.poll ^Subscription sub (long timeout-ms) TimeUnit/MILLISECONDS)]
     (cond
       (some? delta) (types/delta->clj delta)
       (.isDone ^Subscription sub) nil
       :else ::timeout))))

(defn basis
  "The registration basis of a subscription, as a map."
  [^Subscription sub]
  (let [b (.basis ^Subscription sub)]
    {:tx-id (.txId b)
     :system-time (.systemTime b)}))
