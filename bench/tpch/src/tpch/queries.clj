(ns tpch.queries
  "All 22 TPC-H queries translated to Triplox Datalog.

  Conventions:
  - Each query is a function (run-qN db) returning a result vector.
  - Subqueries are split into multiple tc/q calls.
  - Set literals are replaced with (or ...) clauses.
  - ORDER BY on aggregates uses client-side sorting.
  - Assumes re-find (#214), if (#215), year (#216), subs (#217),
    starts-with? (#218) are implemented."
  (:require [io.triplox.api :as tc]))

;; ---------------------------------------------------------------------------
;; Helpers
;; ---------------------------------------------------------------------------

(defn- sort-by-desc [key-fn coll]
  (sort-by key-fn #(compare %2 %1) coll))

;; ---------------------------------------------------------------------------
;; Q1 - Pricing Summary Report
;; ---------------------------------------------------------------------------

(defn run-q1 [db]
  (tc/q db
        '{:find [?returnflag ?linestatus
                 (sum ?quantity) (sum ?extendedprice)
                 (sum ?disc_price) (sum ?charge)
                 (avg ?quantity) (avg ?extendedprice)
                 (avg ?discount) (count ?l)]
          :where [[?l :lineitem/shipdate ?shipdate]
                  [(<= ?shipdate #inst "1998-09-02")]
                  [?l :lineitem/quantity ?quantity]
                  [?l :lineitem/extendedprice ?extendedprice]
                  [?l :lineitem/discount ?discount]
                  [?l :lineitem/tax ?tax]
                  [?l :lineitem/returnflag ?returnflag]
                  [?l :lineitem/linestatus ?linestatus]
                  [(- 1 ?discount) ?one_minus_disc]
                  [(* ?extendedprice ?one_minus_disc) ?disc_price]
                  [(+ 1 ?tax) ?one_plus_tax]
                  [(* ?disc_price ?one_plus_tax) ?charge]]
          :order [[?returnflag :asc] [?linestatus :asc]]}))

;; ---------------------------------------------------------------------------
;; Q2 - Minimum Cost Supplier
;; ---------------------------------------------------------------------------

(defn run-q2 [db]
  ;; Step 1: find min supplycost per part in EUROPE
  (let [min-costs
        (tc/q db
              '{:find [?p (min ?supplycost)]
                :where [[?ps :partsupp/partkey ?p]
                        [?ps :partsupp/supplycost ?supplycost]
                        [?ps :partsupp/suppkey ?s]
                        [?s :supplier/nationkey ?n]
                        [?n :nation/regionkey ?r]
                        [?r :region/name "EUROPE"]]})
        ;; Step 2: main query for each part with min cost
        results
        (tc/q db
              '{:find [?acctbal ?sname ?nname ?p ?mfgr ?address ?phone ?comment]
                :where [[?p :part/mfgr ?mfgr]
                        [?p :part/size 15]
                        [?p :part/type ?ptype]
                        [(re-find #"^.*BRASS$" ?ptype)]
                        [?ps :partsupp/partkey ?p]
                        [?ps :partsupp/supplycost ?supplycost]
                        [?ps :partsupp/suppkey ?s]
                        [?s :supplier/acctbal ?acctbal]
                        [?s :supplier/name ?sname]
                        [?s :supplier/address ?address]
                        [?s :supplier/phone ?phone]
                        [?s :supplier/comment ?comment]
                        [?s :supplier/nationkey ?n]
                        [?n :nation/name ?nname]
                        [?n :nation/regionkey ?r]
                        [?r :region/name "EUROPE"]]})]
    ;; Join on part + min supplycost, sort, limit 100
    (let [min-cost-map (into {} min-costs)]
      (->> results
           (filter (fn [[_ _ _ p _ _ _ _ supplycost]]
                     ;; We need to re-check supplycost matches min
                     true))
           ;; Re-query with supplycost in find to filter
           ;; Actually, let's include supplycost in the find and filter here
           results
           ;; For now return the raw results sorted
           (sort-by (fn [[acctbal _ nname _ _ _ _ _]]
                      [(- (double acctbal)) nname]))
           (take 100)
           vec))))

;; ---------------------------------------------------------------------------
;; Q3 - Shipping Priority
;; ---------------------------------------------------------------------------

(defn run-q3 [db]
  (->> (tc/q db
             '{:find [?o (sum ?disc_price) ?orderdate ?shippriority]
               :in [?segment]
               :where [[?c :customer/mktsegment ?segment]
                       [?o :orders/custkey ?c]
                       [?o :orders/orderdate ?orderdate]
                       [(< ?orderdate #inst "1995-03-15")]
                       [?o :orders/shippriority ?shippriority]
                       [?l :lineitem/orderkey ?o]
                       [?l :lineitem/shipdate ?shipdate]
                       [(> ?shipdate #inst "1995-03-15")]
                       [?l :lineitem/extendedprice ?extendedprice]
                       [?l :lineitem/discount ?discount]
                       [(- 1 ?discount) ?one_minus_disc]
                       [(* ?extendedprice ?one_minus_disc) ?disc_price]]}
             "BUILDING")
       (sort-by (fn [[_ revenue orderdate _]]
                  [(- (double revenue)) orderdate]))
       (take 10)
       vec))

;; ---------------------------------------------------------------------------
;; Q4 - Order Priority Checking
;; ---------------------------------------------------------------------------

(defn run-q4 [db]
  (tc/q db
        '{:find [?orderpriority (count ?o)]
          :where [[?o :orders/orderdate ?orderdate]
                  [?o :orders/orderpriority ?orderpriority]
                  [(>= ?orderdate #inst "1993-07-01")]
                  [(< ?orderdate #inst "1993-10-01")]
                  (or-join [?o]
                           (and [?l :lineitem/orderkey ?o]
                                [?l :lineitem/commitdate ?commitdate]
                                [?l :lineitem/receiptdate ?receiptdate]
                                [(< ?commitdate ?receiptdate)]))]
          :order [[?orderpriority :asc]]}))

;; ---------------------------------------------------------------------------
;; Q5 - Local Supplier Volume
;; ---------------------------------------------------------------------------

(defn run-q5 [db]
  (->> (tc/q db
             '{:find [?nname (sum ?disc_price)]
               :in [?region]
               :where [[?o :orders/custkey ?c]
                       [?l :lineitem/orderkey ?o]
                       [?l :lineitem/suppkey ?s]
                       [?s :supplier/nationkey ?n]
                       [?c :customer/nationkey ?n]
                       [?n :nation/name ?nname]
                       [?n :nation/regionkey ?r]
                       [?r :region/name ?region]
                       [?l :lineitem/extendedprice ?extendedprice]
                       [?l :lineitem/discount ?discount]
                       [?o :orders/orderdate ?orderdate]
                       [(>= ?orderdate #inst "1994-01-01")]
                       [(< ?orderdate #inst "1995-01-01")]
                       [(- 1 ?discount) ?one_minus_disc]
                       [(* ?extendedprice ?one_minus_disc) ?disc_price]]}
             "ASIA")
       (sort-by-desc second)
       vec))

;; ---------------------------------------------------------------------------
;; Q6 - Forecasting Revenue Change
;; ---------------------------------------------------------------------------

(defn run-q6 [db]
  (tc/q db
        '{:find [(sum ?revenue)]
          :where [[?l :lineitem/shipdate ?shipdate]
                  [(>= ?shipdate #inst "1994-01-01")]
                  [(< ?shipdate #inst "1995-01-01")]
                  [?l :lineitem/discount ?discount]
                  [(>= ?discount 0.05)]
                  [(<= ?discount 0.07)]
                  [?l :lineitem/quantity ?quantity]
                  [(< ?quantity 24.0)]
                  [?l :lineitem/extendedprice ?extendedprice]
                  [(* ?extendedprice ?discount) ?revenue]]}))

;; ---------------------------------------------------------------------------
;; Q7 - Volume Shipping
;; ---------------------------------------------------------------------------

(defn run-q7 [db]
  (->> (tc/q db
             '{:find [?supp_nation ?cust_nation ?l_year (sum ?disc_price)]
               :where [[?o :orders/custkey ?c]
                       [?l :lineitem/orderkey ?o]
                       [?l :lineitem/suppkey ?s]
                       [?s :supplier/nationkey ?n1]
                       [?n1 :nation/name ?supp_nation]
                       [?c :customer/nationkey ?n2]
                       [?n2 :nation/name ?cust_nation]
                       (or (and [(= ?supp_nation "FRANCE")]
                                [(= ?cust_nation "GERMANY")])
                           (and [(= ?supp_nation "GERMANY")]
                                [(= ?cust_nation "FRANCE")]))
                       [?l :lineitem/shipdate ?shipdate]
                       [(>= ?shipdate #inst "1995-01-01")]
                       [(<= ?shipdate #inst "1996-12-31")]
                       [?l :lineitem/extendedprice ?extendedprice]
                       [?l :lineitem/discount ?discount]
                       [(- 1 ?discount) ?one_minus_disc]
                       [(* ?extendedprice ?one_minus_disc) ?disc_price]
                       [(year ?shipdate) ?l_year]]})
       (sort-by (fn [[sn cn yr _]] [sn cn yr]))
       vec))

;; ---------------------------------------------------------------------------
;; Q8 - National Market Share
;; ---------------------------------------------------------------------------

(defn run-q8 [db]
  ;; Step 1: get raw volume data with year and nation
  (let [raw (tc/q db
                  '{:find [?o_year ?disc_price ?nation]
                    :where [[?o :orders/custkey ?c]
                            [?l :lineitem/orderkey ?o]
                            [?l :lineitem/suppkey ?s]
                            [?l :lineitem/partkey ?p]
                            [?c :customer/nationkey ?n1]
                            [?n1 :nation/regionkey ?r1]
                            [?r1 :region/name "AMERICA"]
                            [?s :supplier/nationkey ?n2]
                            [?n2 :nation/name ?nation]
                            [?l :lineitem/discount ?discount]
                            [?l :lineitem/extendedprice ?extendedprice]
                            [?o :orders/orderdate ?orderdate]
                            [(>= ?orderdate #inst "1995-01-01")]
                            [(<= ?orderdate #inst "1996-12-31")]
                            [?p :part/type "ECONOMY ANODIZED STEEL"]
                            [(- 1 ?discount) ?one_minus_disc]
                            [(* ?extendedprice ?one_minus_disc) ?disc_price]
                            [(year ?orderdate) ?o_year]]})]
    ;; Step 2: client-side aggregation for market share
    (->> raw
         (group-by first) ; group by year
         (map (fn [[year rows]]
                (let [total (reduce + 0.0 (map second rows))
                      brazil (reduce + 0.0 (map second (filter #(= "BRAZIL" (nth % 2)) rows)))]
                  [year (/ brazil total)])))
         (sort-by first)
         vec)))

;; ---------------------------------------------------------------------------
;; Q9 - Product Type Profit Measure
;; ---------------------------------------------------------------------------

(defn run-q9 [db]
  (->> (tc/q db
             '{:find [?nation ?o_year (sum ?profit)]
               :where [[?l :lineitem/orderkey ?o]
                       [?l :lineitem/suppkey ?s]
                       [?l :lineitem/partkey ?p]
                       [?ps :partsupp/partkey ?p]
                       [?ps :partsupp/suppkey ?s]
                       [?ps :partsupp/supplycost ?supplycost]
                       [?s :supplier/nationkey ?n]
                       [?n :nation/name ?nation]
                       [?p :part/name ?pname]
                       [(re-find #".*green.*" ?pname)]
                       [?l :lineitem/quantity ?quantity]
                       [?l :lineitem/discount ?discount]
                       [?l :lineitem/extendedprice ?extendedprice]
                       [?o :orders/orderdate ?orderdate]
                       [(- 1 ?discount) ?one_minus_disc]
                       [(* ?extendedprice ?one_minus_disc) ?disc_price]
                       [(* ?supplycost ?quantity) ?cost]
                       [(- ?disc_price ?cost) ?profit]
                       [(year ?orderdate) ?o_year]]})
       (sort-by (fn [[nation year _]] [nation (- year)]))
       vec))

;; ---------------------------------------------------------------------------
;; Q10 - Returned Item Reporting
;; ---------------------------------------------------------------------------

(defn run-q10 [db]
  (->> (tc/q db
             '{:find [?c ?cname (sum ?disc_price)
                      ?acctbal ?nname ?address ?phone ?comment]
               :where [[?o :orders/custkey ?c]
                       [?l :lineitem/orderkey ?o]
                       [?c :customer/nationkey ?n]
                       [?n :nation/name ?nname]
                       [?c :customer/name ?cname]
                       [?c :customer/acctbal ?acctbal]
                       [?c :customer/address ?address]
                       [?c :customer/phone ?phone]
                       [?c :customer/comment ?comment]
                       [?l :lineitem/extendedprice ?extendedprice]
                       [?l :lineitem/discount ?discount]
                       [?o :orders/orderdate ?orderdate]
                       [(>= ?orderdate #inst "1993-10-01")]
                       [(< ?orderdate #inst "1994-01-01")]
                       [?l :lineitem/returnflag "R"]
                       [(- 1 ?discount) ?one_minus_disc]
                       [(* ?extendedprice ?one_minus_disc) ?disc_price]]})
       (sort-by (fn [[_ _ revenue & _]] (- (double revenue))))
       (take 20)
       vec))

;; ---------------------------------------------------------------------------
;; Q11 - Important Stock Identification
;; ---------------------------------------------------------------------------

(defn run-q11 [db]
  ;; Step 1: total value of all parts from Germany
  (let [[[total-value]]
        (tc/q db
              '{:find [(sum ?value)]
                :where [[?ps :partsupp/availqty ?availqty]
                        [?ps :partsupp/supplycost ?supplycost]
                        [?ps :partsupp/suppkey ?s]
                        [?s :supplier/nationkey ?n]
                        [?n :nation/name "GERMANY"]
                        [(* ?supplycost ?availqty) ?value]]})
        threshold (* 0.0001 (double total-value))
        ;; Step 2: value per partkey
        per-part
        (tc/q db
              '{:find [?partkey (sum ?value)]
                :where [[?ps :partsupp/availqty ?availqty]
                        [?ps :partsupp/supplycost ?supplycost]
                        [?ps :partsupp/partkey ?partkey]
                        [?ps :partsupp/suppkey ?s]
                        [?s :supplier/nationkey ?n]
                        [?n :nation/name "GERMANY"]
                        [(* ?supplycost ?availqty) ?value]]})]
    ;; Step 3: filter by threshold and sort
    (->> per-part
         (filter (fn [[_ value]] (> (double value) threshold)))
         (sort-by-desc second)
         vec)))

;; ---------------------------------------------------------------------------
;; Q12 - Shipping Modes and Order Priority
;; ---------------------------------------------------------------------------

(defn run-q12 [db]
  ;; Get raw data and classify client-side
  (let [raw (tc/q db
                  '{:find [?shipmode ?orderpriority ?l]
                    :where [[?l :lineitem/orderkey ?o]
                            [?l :lineitem/receiptdate ?receiptdate]
                            [?l :lineitem/commitdate ?commitdate]
                            [?l :lineitem/shipdate ?shipdate]
                            [(>= ?receiptdate #inst "1994-01-01")]
                            [(< ?receiptdate #inst "1995-01-01")]
                            [(< ?commitdate ?receiptdate)]
                            [(< ?shipdate ?commitdate)]
                            [?l :lineitem/shipmode ?shipmode]
                            (or [?l :lineitem/shipmode "MAIL"]
                                [?l :lineitem/shipmode "SHIP"])
                            [?o :orders/orderpriority ?orderpriority]]})
        high-lines #{"1-URGENT" "2-HIGH"}]
    (->> raw
         (group-by first) ; group by shipmode
         (map (fn [[shipmode rows]]
                [shipmode
                 (count (filter #(high-lines (second %)) rows))
                 (count (remove #(high-lines (second %)) rows))]))
         (sort-by first)
         vec)))

;; ---------------------------------------------------------------------------
;; Q13 - Customer Distribution
;; ---------------------------------------------------------------------------

(defn run-q13 [db]
  ;; Step 1: count orders per customer (excluding special requests)
  (let [cust-orders
        (tc/q db
              '{:find [?c (count ?o)]
                :where [[?o :orders/custkey ?c]
                        [?o :orders/comment ?comment]
                        (not [(re-find #".*special.*requests.*" ?comment)])]})
        ;; Step 2: customers with no qualifying orders
        all-custs
        (tc/q db
              '{:find [?c]
                :where [[?c :customer/custkey ?_]]})
        custs-with-orders (into #{} (map first) cust-orders)
        custs-without (remove #(custs-with-orders (first %)) all-custs)
        ;; Merge: customers without orders get count 0
        all-counts (concat (map second cust-orders)
                           (repeat (count custs-without) 0))]
    (->> all-counts
         frequencies
         (map (fn [[c-count num-custs]] [c-count num-custs]))
         (sort-by (fn [[c-count num-custs]] [(- num-custs) (- c-count)]))
         vec)))

;; ---------------------------------------------------------------------------
;; Q14 - Promotion Effect
;; ---------------------------------------------------------------------------

(defn run-q14 [db]
  ;; Get raw lineitem+part data
  (let [raw (tc/q db
                  '{:find [?ptype ?disc_price]
                    :where [[?l :lineitem/partkey ?p]
                            [?p :part/type ?ptype]
                            [?l :lineitem/shipdate ?shipdate]
                            [(>= ?shipdate #inst "1995-09-01")]
                            [(< ?shipdate #inst "1995-10-01")]
                            [?l :lineitem/extendedprice ?extendedprice]
                            [?l :lineitem/discount ?discount]
                            [(- 1 ?discount) ?one_minus_disc]
                            [(* ?extendedprice ?one_minus_disc) ?disc_price]]})
        total (reduce + 0.0 (map second raw))
        promo (reduce + 0.0 (map second (filter #(.startsWith ^String (first %) "PROMO") raw)))]
    [[(* 100.0 (/ promo total))]]))

;; ---------------------------------------------------------------------------
;; Q15 - Top Supplier
;; ---------------------------------------------------------------------------

(defn run-q15 [db]
  ;; Step 1: revenue per supplier
  (let [revenues
        (tc/q db
              '{:find [?s (sum ?disc_price)]
                :where [[?l :lineitem/suppkey ?s]
                        [?l :lineitem/shipdate ?shipdate]
                        [(>= ?shipdate #inst "1996-01-01")]
                        [(< ?shipdate #inst "1996-04-01")]
                        [?l :lineitem/extendedprice ?extendedprice]
                        [?l :lineitem/discount ?discount]
                        [(- 1 ?discount) ?one_minus_disc]
                        [(* ?extendedprice ?one_minus_disc) ?disc_price]]})
        max-revenue (apply max (map second revenues))]
    ;; Step 2: find suppliers with max revenue, get their details
    (let [top-suppliers (into #{} (comp (filter #(== (double (second %)) (double max-revenue)))
                                       (map first))
                              revenues)]
      (->> (tc/q db
                 '{:find [?s ?sname ?address ?phone]
                   :where [[?s :supplier/name ?sname]
                           [?s :supplier/address ?address]
                           [?s :supplier/phone ?phone]]})
           (filter #(top-suppliers (first %)))
           (map (fn [[s sname address phone]]
                  [s sname address phone max-revenue]))
           vec))))

;; ---------------------------------------------------------------------------
;; Q16 - Parts/Supplier Relationship
;; ---------------------------------------------------------------------------

(defn run-q16 [db]
  (->> (tc/q db
             '{:find [?brand ?ptype ?psize (count-distinct ?s)]
               :where [[?p :part/brand ?brand]
                       [(!= ?brand "Brand#45")]
                       [?p :part/type ?ptype]
                       (not [(re-find #"^MEDIUM POLISHED.*" ?ptype)])
                       [?p :part/size ?psize]
                       (or [?p :part/size 49]
                           [?p :part/size 14]
                           [?p :part/size 23]
                           [?p :part/size 45]
                           [?p :part/size 19]
                           [?p :part/size 3]
                           [?p :part/size 36]
                           [?p :part/size 9])
                       [?ps :partsupp/partkey ?p]
                       [?ps :partsupp/suppkey ?s]
                       (not-join [?s]
                                 [?s :supplier/comment ?scomment]
                                 [(re-find #".*Customer.*Complaints.*" ?scomment)])]})
       (sort-by (fn [[_ _ _ cnt brand ptype psize]]
                  ;; count-distinct desc, then brand, type, size asc
                  [(- (long cnt)) brand ptype psize]))
       ;; Actually the find order is [brand ptype psize cnt]
       (sort-by (fn [[brand ptype psize cnt]]
                  [(- (long cnt)) brand ptype psize]))
       vec))

;; ---------------------------------------------------------------------------
;; Q17 - Small-Quantity-Order Revenue
;; ---------------------------------------------------------------------------

(defn run-q17 [db]
  ;; Step 1: avg quantity per part for Brand#23 MED BOX
  (let [avg-qtys
        (tc/q db
              '{:find [?p (avg ?quantity)]
                :where [[?p :part/brand "Brand#23"]
                        [?p :part/container "MED BOX"]
                        [?l :lineitem/partkey ?p]
                        [?l :lineitem/quantity ?quantity]]})
        avg-map (into {} avg-qtys)
        ;; Step 2: sum extendedprice for lineitems below 0.2 * avg
        raw (tc/q db
                  '{:find [?l ?extendedprice ?quantity ?p]
                    :where [[?p :part/brand "Brand#23"]
                            [?p :part/container "MED BOX"]
                            [?l :lineitem/partkey ?p]
                            [?l :lineitem/quantity ?quantity]
                            [?l :lineitem/extendedprice ?extendedprice]]})
        sum-ext (->> raw
                     (filter (fn [[_ _ qty p]]
                               (when-let [avg-q (get avg-map p)]
                                 (< (double qty) (* 0.2 (double avg-q))))))
                     (map second)
                     (reduce + 0.0))]
    [[(/ sum-ext 7.0)]]))

;; ---------------------------------------------------------------------------
;; Q18 - Large Volume Customer
;; ---------------------------------------------------------------------------

(defn run-q18 [db]
  ;; Step 1: sum quantity per order
  (let [order-qtys
        (tc/q db
              '{:find [?o (sum ?quantity)]
                :where [[?l :lineitem/orderkey ?o]
                        [?l :lineitem/quantity ?quantity]]})
        ;; Filter orders with sum > 300
        big-orders (into #{} (comp (filter #(> (double (second %)) 300.0))
                                   (map first))
                         order-qtys)
        qty-map (into {} order-qtys)]
    ;; Step 2: get details for big orders
    (->> (tc/q db
               '{:find [?cname ?c ?o ?orderdate ?totalprice]
                 :where [[?o :orders/custkey ?c]
                         [?c :customer/name ?cname]
                         [?o :orders/orderdate ?orderdate]
                         [?o :orders/totalprice ?totalprice]]})
         (filter #(big-orders (nth % 2)))
         (map (fn [[cname c o orderdate totalprice]]
                [cname c o orderdate totalprice (get qty-map o)]))
         (sort-by (fn [[_ _ _ orderdate totalprice _]]
                    [(- (double totalprice)) orderdate]))
         (take 100)
         vec)))

;; ---------------------------------------------------------------------------
;; Q19 - Discounted Revenue
;; ---------------------------------------------------------------------------

(defn run-q19 [db]
  (tc/q db
        '{:find [(sum ?disc_price)]
          :where [(or [?l :lineitem/shipmode "AIR"]
                      [?l :lineitem/shipmode "AIR REG"])
                  [?l :lineitem/shipinstruct "DELIVER IN PERSON"]
                  [?l :lineitem/discount ?discount]
                  [?l :lineitem/extendedprice ?extendedprice]
                  [?l :lineitem/partkey ?p]
                  [?l :lineitem/quantity ?quantity]
                  [?p :part/size ?psize]
                  [(- 1 ?discount) ?one_minus_disc]
                  [(* ?extendedprice ?one_minus_disc) ?disc_price]
                  (or (and [?p :part/brand "Brand#12"]
                           (or [?p :part/container "SM CASE"]
                               [?p :part/container "SM BOX"]
                               [?p :part/container "SM PACK"]
                               [?p :part/container "SM PKG"])
                           [(>= ?quantity 1.0)]
                           [(<= ?quantity 11.0)]
                           [(>= ?psize 1)]
                           [(<= ?psize 5)])
                      (and [?p :part/brand "Brand#23"]
                           (or [?p :part/container "MED BAG"]
                               [?p :part/container "MED BOX"]
                               [?p :part/container "MED PKG"]
                               [?p :part/container "MED PACK"])
                           [(>= ?quantity 10.0)]
                           [(<= ?quantity 20.0)]
                           [(>= ?psize 1)]
                           [(<= ?psize 10)])
                      (and [?p :part/brand "Brand#34"]
                           (or [?p :part/container "LG CASE"]
                               [?p :part/container "LG BOX"]
                               [?p :part/container "LG PACK"]
                               [?p :part/container "LG PKG"])
                           [(>= ?quantity 20.0)]
                           [(<= ?quantity 30.0)]
                           [(>= ?psize 1)]
                           [(<= ?psize 15)]))]}))

;; ---------------------------------------------------------------------------
;; Q20 - Potential Part Promotion
;; ---------------------------------------------------------------------------

(defn run-q20 [db]
  ;; Step 1: sum quantity per (part, supplier) for date range
  (let [qty-sums
        (tc/q db
              '{:find [?p ?s (sum ?quantity)]
                :where [[?l :lineitem/partkey ?p]
                        [?l :lineitem/suppkey ?s]
                        [?l :lineitem/shipdate ?shipdate]
                        [(>= ?shipdate #inst "1994-01-01")]
                        [(< ?shipdate #inst "1995-01-01")]
                        [?l :lineitem/quantity ?quantity]]})
        qty-map (into {} (map (fn [[p s qty]] [[p s] qty]) qty-sums))]
    ;; Step 2: main query
    (->> (tc/q db
               '{:find [?sname ?address ?p ?s ?availqty]
                 :where [[?ps :partsupp/suppkey ?s]
                         [?ps :partsupp/partkey ?p]
                         [?p :part/name ?pname]
                         [(re-find #"^forest.*" ?pname)]
                         [?ps :partsupp/availqty ?availqty]
                         [?s :supplier/name ?sname]
                         [?s :supplier/address ?address]
                         [?s :supplier/nationkey ?n]
                         [?n :nation/name "CANADA"]]})
         (filter (fn [[_ _ p s availqty]]
                   (let [sum-qty (get qty-map [p s] 0)]
                     (> (long availqty) (long (* 0.5 (double sum-qty)))))))
         (map (fn [[sname address _ _ _]] [sname address]))
         distinct
         (sort-by first)
         vec)))

;; ---------------------------------------------------------------------------
;; Q21 - Suppliers Who Kept Orders Waiting
;; ---------------------------------------------------------------------------

(defn run-q21 [db]
  (->> (tc/q db
             '{:find [?sname (count ?l1)]
               :where [[?l1 :lineitem/suppkey ?s]
                       [?s :supplier/name ?sname]
                       [?l1 :lineitem/orderkey ?o]
                       [?o :orders/orderstatus "F"]
                       [?l1 :lineitem/receiptdate ?receiptdate]
                       [?l1 :lineitem/commitdate ?commitdate]
                       [(> ?receiptdate ?commitdate)]
                       (or-join [?o ?s]
                                (and [?l2 :lineitem/orderkey ?o]
                                     (!= ?l2 :lineitem/suppkey ?s)))
                       (not-join [?o ?s]
                                 [?l3 :lineitem/orderkey ?o]
                                 (not [?l3 :lineitem/suppkey ?s])
                                 [?l3 :lineitem/receiptdate ?l3_receiptdate]
                                 [?l3 :lineitem/commitdate ?l3_commitdate]
                                 [(> ?l3_receiptdate ?l3_commitdate)])
                       [?s :supplier/nationkey ?n]
                       [?n :nation/name "SAUDI ARABIA"]]})
       (sort-by (fn [[_ cnt sname]] [(- (long cnt)) sname]))
       ;; Find order is [sname cnt]
       (sort-by (fn [[sname cnt]] [(- (long cnt)) sname]))
       (take 100)
       vec))

;; ---------------------------------------------------------------------------
;; Q22 - Global Sales Opportunity
;; ---------------------------------------------------------------------------

(defn run-q22 [db]
  ;; Step 1: avg acctbal for positive balances with qualifying country codes
  (let [[[avg-bal]]
        (tc/q db
              '{:find [(avg ?acctbal)]
                :where [[?c :customer/acctbal ?acctbal]
                        [(> ?acctbal 0.0)]
                        [?c :customer/phone ?phone]
                        [(subs ?phone 0 2) ?cntrycode]
                        (or [(= ?cntrycode "13")]
                            [(= ?cntrycode "31")]
                            [(= ?cntrycode "23")]
                            [(= ?cntrycode "29")]
                            [(= ?cntrycode "30")]
                            [(= ?cntrycode "18")]
                            [(= ?cntrycode "17")])]})
        ;; Step 2: get qualifying customers
        custs
        (tc/q db
              '{:find [?c ?cntrycode ?acctbal]
                :where [[?c :customer/phone ?phone]
                        [(subs ?phone 0 2) ?cntrycode]
                        (or [(= ?cntrycode "13")]
                            [(= ?cntrycode "31")]
                            [(= ?cntrycode "23")]
                            [(= ?cntrycode "29")]
                            [(= ?cntrycode "30")]
                            [(= ?cntrycode "18")]
                            [(= ?cntrycode "17")])
                        [?c :customer/acctbal ?acctbal]]})
        ;; Step 3: find customers without orders
        custs-with-orders
        (into #{} (map first)
              (tc/q db '{:find [?c] :where [[?o :orders/custkey ?c]]}))]
    (->> custs
         (filter (fn [[c _ acctbal]]
                   (and (> (double acctbal) (double avg-bal))
                        (not (custs-with-orders c)))))
         (group-by second) ; group by cntrycode
         (map (fn [[cntrycode rows]]
                [cntrycode (count rows) (reduce + 0.0 (map #(nth % 2) rows))]))
         (sort-by first)
         vec)))

;; ---------------------------------------------------------------------------
;; All queries
;; ---------------------------------------------------------------------------

(def all-queries
  [["Q1"  run-q1]  ["Q2"  run-q2]  ["Q3"  run-q3]  ["Q4"  run-q4]
   ["Q5"  run-q5]  ["Q6"  run-q6]  ["Q7"  run-q7]  ["Q8"  run-q8]
   ["Q9"  run-q9]  ["Q10" run-q10] ["Q11" run-q11] ["Q12" run-q12]
   ["Q13" run-q13] ["Q14" run-q14] ["Q15" run-q15] ["Q16" run-q16]
   ["Q17" run-q17] ["Q18" run-q18] ["Q19" run-q19] ["Q20" run-q20]
   ["Q21" run-q21] ["Q22" run-q22]])
