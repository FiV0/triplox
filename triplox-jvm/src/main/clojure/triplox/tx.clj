(ns triplox.tx
  "Convert Datomic-style transaction data to TxOp objects."
  (:import [io.triplox.client TxOp$Put TxOp$Add TxOp$Retract TxOp$Delete TxOp$Erase]))

(defn- keyword->attr
  "Convert a keyword to a string attribute name.
   :person/name → \"person/name\"
   :name → \"name\""
  [kw]
  (subs (str kw) 1))

(defn- map->put
  "Convert a Clojure map to a TxOp.Put."
  [m]
  (TxOp$Put. (into {} (map (fn [[k v]] [(keyword->attr k) v])) m)))

(defn- vec->tx-op
  "Convert a Datomic-style tx-data vector to a TxOp."
  [v]
  (let [op (first v)]
    (case op
      :db/add     (let [[_ e a val] v] (TxOp$Add. (long e) (keyword->attr a) val))
      :db/retract (let [[_ e a val] v] (TxOp$Retract. (long e) (keyword->attr a) val))
      :db/delete  (let [[_ eid] v]     (TxOp$Delete. (long eid)))
      :db/erase   (let [[_ eid] v]     (TxOp$Erase. (long eid)))
      (throw (ex-info (str "Unknown tx-data op: " op) {:op op :form v})))))

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
