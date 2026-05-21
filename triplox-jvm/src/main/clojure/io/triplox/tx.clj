(ns io.triplox.tx
  "Convert Datomic-style transaction data to TxOp objects."
  (:import [clojure.lang MapEntry]
           [io.triplox.client EntityRef$Id EntityRef$TempId EntityRef$Ident EntityRef$LookupRef
            TxOp$Put TxOp$Add TxOp$Retract TxOp$Delete TxOp$Erase]))

(defn- ->entity-ref
  "Convert a Clojure value to an EntityRef."
  [v]
  (cond
    (integer? v)  (EntityRef$Id. (long v))
    (string? v)   (EntityRef$TempId. v)
    (keyword? v)  (EntityRef$Ident. (str v))
    (and (vector? v) (= 2 (count v)) (keyword? (first v)))
    (EntityRef$LookupRef. (str (first v)) (second v))
    :else (throw (ex-info "Cannot convert to EntityRef" {:value v}))))

(defn- map->put
  "Convert a Clojure map to a TxOp.Put."
  [m]
  (when-not (every? keyword? (keys m))
    (throw (ex-info "TxOp.Put map keys must be keywords" {:map m})))
  (TxOp$Put. (into {} (map (fn [[k v]] (MapEntry/create (str k) v)) m))))

(defn- vec->tx-op
  "Convert a Datomic-style tx-data vector to a TxOp."
  [[op :as v]]
  (case op
    :db/add     (let [[_ e a val] v] (TxOp$Add. (->entity-ref e) (str a) val))
    :db/retract (let [[_ e a val] v] (TxOp$Retract. (->entity-ref e) (str a) val))
    :db/delete  (let [[_ eid] v] (TxOp$Delete. (->entity-ref eid)))
    :db/erase   (let [[_ eid] v] (TxOp$Erase. (->entity-ref eid)))
    (throw (ex-info (str "Unknown tx-data op: " op) {:op op :form v}))))

(defn tx-data->ops
  "Convert a sequence of Datomic-style tx-data forms to a List<TxOp>.
   Maps → TxOp.Put, vectors → TxOp.Add/Retract/Delete/Erase."
  [tx-data]
  (mapv (fn [form]
          (cond
            (map? form) (map->put form)
            (vector? form) (vec->tx-op form)
            :else (throw (ex-info "tx-data form must be a map or vector" {:form form}))))
        tx-data))
