;; # Incremental stock queries
(ns stock
  {:nextjournal.clerk/visibility {:code :hide :result :hide}}
  (:require [clojure.pprint :as pprint]
            [nextjournal.clerk :as clerk]
            [stock-demo :as demo]))

;; This query returns stock below each product's reorder threshold.
;; It joins stock records with their products and warehouses.
;; The client subscribes once and applies the returned additions and removals to Low Stock.
^{::clerk/visibility {:result :show}}
(clerk/code (with-out-str (pprint/pprint demo/low-stock-query)))

;; ## Products and transactions
;; The example has coffee, tea, and milk in Berlin and Hamburg, with one stock record per product and warehouse.
;; Their reorder thresholds are 10, 8, and 6 units, respectively.
;; Each stock record starts two units above its threshold, except Hamburg tea, which starts at 7 units.

;; Every 500 ms, the example commits a sale or refill transaction.
;; A sale creates a record with the stock reference and quantity, and reduces stock by that quantity in the same transaction.
;; A refill records the added quantity and increases stock in the same transaction.

;; For Berlin coffee, three sales change stock from 12 to 10, then 7, then 5.
;; The row enters Low Stock at 7, changes at 5, and leaves when a refill restores stock to 12.
;; The loop then moves to the next stock record and repeats.
;; All Stock shows the quantities from committed transactions.

^{::clerk/sync true}
(defonce !dashboard demo/!state)

(def stock-viewer
  {:render-fn
   '(fn [_]
      (let [{:keys [inventory weights]} @stock/!dashboard
            low-stock (sort-by (juxt #(nth % 3) second) (keys weights))
            stock-table
            (fn [rows]
              [:div.stock-scroll
               [:table
                [:thead
                 [:tr (for [label ["Product" "Warehouse" "Available" "Reorder below"]]
                        ^{:key label} [:th label])]]
                [:tbody
                 (for [[id sku product location available threshold] rows]
                   ^{:key id}
                   [:tr
                    [:td [:div product] [:small sku]]
                    [:td location]
                    [:td {:class (when (< available threshold) "stock-low")} available]
                    [:td threshold]])]]])]
        [:div.stock-view
         [:style "
          .stock-view{font-family:system-ui,sans-serif;color:#253549;width:100%;box-sizing:border-box}
          .stock-view h2{font-size:20px;font-weight:600;margin:24px 0 12px}
          .stock-scroll{overflow-x:auto;border:1px solid #dce6e3;border-radius:8px}
          .stock-view table{width:100%;border-collapse:collapse;margin:0;font-size:14px;white-space:nowrap}
          .stock-view th,.stock-view td{padding:12px 16px;text-align:left;border-bottom:1px solid #edf1ef}
          .stock-view th{font-size:12px;color:#617471;background:#f6f9f8}
          .stock-view tbody tr:last-child td{border-bottom:0}
          .stock-view small{color:#71817d;font-size:11px}
          .stock-view .stock-low{color:#b45309;font-weight:600}
         "]
         [:h2 "All Stock"]
         (stock-table inventory)
         [:h2 "Low Stock"]
         (stock-table low-stock)]))})

^{::clerk/visibility {:result :show} ::clerk/width :wide}
(clerk/with-viewer stock-viewer nil)

^{::clerk/visibility {:result :show}}
(clerk/html [:div {:style {:height "70vh"} :aria-hidden true}])
