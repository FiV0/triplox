(ns tpch.schema
  "TPC-H schema attribute definitions for Triplox.")

(defn- attr
  ([ident value-type]
   (attr ident value-type :db.cardinality/one))
  ([ident value-type cardinality]
   {:db/ident ident
    :db/valueType value-type
    :db/cardinality cardinality}))

(def schema-tx
  (vec
   (concat
    ;; --- Region ---
    [(attr :region/regionkey :db.type/long)
     (attr :region/name :db.type/string)
     (attr :region/comment :db.type/string)]

    ;; --- Nation ---
    [(attr :nation/nationkey :db.type/long)
     (attr :nation/name :db.type/string)
     (attr :nation/regionkey :db.type/ref)
     (attr :nation/comment :db.type/string)]

    ;; --- Supplier ---
    [(attr :supplier/suppkey :db.type/long)
     (attr :supplier/name :db.type/string)
     (attr :supplier/address :db.type/string)
     (attr :supplier/nationkey :db.type/ref)
     (attr :supplier/phone :db.type/string)
     (attr :supplier/acctbal :db.type/double)
     (attr :supplier/comment :db.type/string)]

    ;; --- Customer ---
    [(attr :customer/custkey :db.type/long)
     (attr :customer/name :db.type/string)
     (attr :customer/address :db.type/string)
     (attr :customer/nationkey :db.type/ref)
     (attr :customer/phone :db.type/string)
     (attr :customer/acctbal :db.type/double)
     (attr :customer/mktsegment :db.type/string)
     (attr :customer/comment :db.type/string)]

    ;; --- Part ---
    [(attr :part/partkey :db.type/long)
     (attr :part/name :db.type/string)
     (attr :part/mfgr :db.type/string)
     (attr :part/brand :db.type/string)
     (attr :part/type :db.type/string)
     (attr :part/size :db.type/long)
     (attr :part/container :db.type/string)
     (attr :part/retailprice :db.type/double)
     (attr :part/comment :db.type/string)]

    ;; --- PartSupp ---
    [(attr :partsupp/partkey :db.type/ref)
     (attr :partsupp/suppkey :db.type/ref)
     (attr :partsupp/availqty :db.type/long)
     (attr :partsupp/supplycost :db.type/double)
     (attr :partsupp/comment :db.type/string)]

    ;; --- Orders ---
    [(attr :orders/orderkey :db.type/long)
     (attr :orders/custkey :db.type/ref)
     (attr :orders/orderstatus :db.type/string)
     (attr :orders/totalprice :db.type/double)
     (attr :orders/orderdate :db.type/instant)
     (attr :orders/orderpriority :db.type/string)
     (attr :orders/clerk :db.type/string)
     (attr :orders/shippriority :db.type/long)
     (attr :orders/comment :db.type/string)]

    ;; --- LineItem ---
    [(attr :lineitem/orderkey :db.type/ref)
     (attr :lineitem/partkey :db.type/ref)
     (attr :lineitem/suppkey :db.type/ref)
     (attr :lineitem/linenumber :db.type/long)
     (attr :lineitem/quantity :db.type/double)
     (attr :lineitem/extendedprice :db.type/double)
     (attr :lineitem/discount :db.type/double)
     (attr :lineitem/tax :db.type/double)
     (attr :lineitem/returnflag :db.type/string)
     (attr :lineitem/linestatus :db.type/string)
     (attr :lineitem/shipdate :db.type/instant)
     (attr :lineitem/commitdate :db.type/instant)
     (attr :lineitem/receiptdate :db.type/instant)
     (attr :lineitem/shipinstruct :db.type/string)
     (attr :lineitem/shipmode :db.type/string)
     (attr :lineitem/comment :db.type/string)])))
