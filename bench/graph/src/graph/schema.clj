(ns graph.schema
  "Schema for the graph ingestion benchmark.")

(def schema-tx
  [{:db/ident :g/id
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :g/to
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many}])
