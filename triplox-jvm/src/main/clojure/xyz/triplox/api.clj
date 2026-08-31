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
  (^Db [conn {:keys [tx-id system-time] :as tx-key}]
   (if tx-key
     (.openDbAsOf ^TriploxNode conn (TxKey. (long tx-id) system-time))
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
  "Execute a transaction and wait for indexing. Returns a result map of

   :tx-id - the transaction id
   :system-time - the time the transaction was registered on the log
   :committed? - whether the transaction was committed or not
   :error-message - optional error message if the transaction was not committed"
  [conn tx-data]
  (let [^TxResult result (.executeTx ^TriploxNode conn (tx/tx-data->ops tx-data))]
    (cond-> {:tx-id (.txId result)
             :system-time (.systemTime result)
             :committed? (.isCommitted result)}
      (not (.isCommitted result))
      (assoc :error-message (.errorMessage result)))))

(defn submit-tx
  "Submits a transaction without waiting for indexing. Returns a tx-key as map."
  [conn tx-data]
  (let [ops (tx/tx-data->ops tx-data)
        ^TxKey result (.submitTx ^TriploxNode conn ops)]
    {:tx-id (.txId result)
     :system-time (.systemTime result)}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Incremental subscriptions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn subscribe
  "Register an incremental query and stream its result deltas. A non-empty
  initial result is the first delta. Returns a Subscription (Closeable); use
  with `with-open`. Closing unsubscribes."
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
  or nil when the stream is closed. The second argument bounds the wait,
  returning `::timeout` on expiry.

  A terminal subscription error throws TriploxException."
  ([sub] (types/delta->clj (.take ^Subscription sub)))
  ([sub timeout-ms]
   (let [delta (.poll ^Subscription sub (long timeout-ms) TimeUnit/MILLISECONDS)]
     (cond
       (some? delta) (types/delta->clj delta)
       (.isDone ^Subscription sub) nil
       :else ::timeout))))

(defn tx-key
  "The registration tx-key of a subscription, as a map."
  [^Subscription sub]
  (let [b (.txKey ^Subscription sub)]
    {:tx-id (.txId b)
     :system-time (.systemTime b)}))

(defn ^:no-doc dbsp-q [conn query timeout-ms & args]
  (with-open [sub (apply subscribe conn query args)]
    (take! sub (or timeout-ms 5000))))
