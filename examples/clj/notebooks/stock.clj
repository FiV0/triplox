;; # Stock that changes while you watch
;; Each sale records a purchase and reduces stock in the same transaction.
;; Refills record a delivery and add stock back.
(ns stock
  {:nextjournal.clerk/visibility {:code :hide :result :hide}}
  (:require [clojure.pprint :as pprint]
            [nextjournal.clerk :as clerk]
            [stock-demo :as demo]))

;; The client subscribes once and applies complete batches to the table below.
;; A weight of `+1` adds a row, and `-1` removes a row.
;; One received batch can cover several transactions.

^{::clerk/sync true}
(defonce !dashboard demo/!state)

(def dashboard-viewer
  {:render-fn
   '(fn [_]
      (let [{:keys [status inventory weights events batches batch-count error]} @stock/!dashboard
            low-stock (sort-by (juxt #(nth % 3) second) (keys weights))
            paused? (= status :paused)
            active? (contains? #{:running :paused} status)
            latest-rows (set (map first (:rows (first batches))))
            table-head (fn [labels]
                         [:thead [:tr (for [label labels]
                                        ^{:key label} [:th label])]])]
        [:div.stock-board
         [:style "
          .stock-board{font-family:system-ui,sans-serif;color:#253549;font-size:14px;max-width:1200px;margin:auto;padding:0 16px}
          .stock-board *{box-sizing:border-box}
          .stock-hero{background:#122c34;color:#f2faf8;padding:26px;border-radius:16px;margin:20px 0}
          .stock-kicker{font-size:11px;letter-spacing:.13em;text-transform:uppercase;color:#a7d9c8}
          .stock-title{font-size:28px;font-weight:650;margin:8px 0}
          .stock-subtitle{color:#c1d7d5;margin-bottom:20px}
          .stock-controls{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
          .stock-controls button{border:1px solid #66858a;background:#20434c;color:white;padding:8px 14px;border-radius:7px;cursor:pointer}
          .stock-controls button:disabled{opacity:.4;cursor:default}
          .stock-status{font-size:12px;margin-right:auto;text-transform:capitalize}
          .stock-metrics{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin:16px 0}
          .stock-metric{border:1px solid #dce6e3;border-radius:10px;padding:16px;background:#f5f9f7}
          .stock-metric strong{display:block;font-size:26px;color:#154e43}
          .stock-metric span{font-size:12px;color:#56716a}
          .stock-panel{border:1px solid #dce6e3;border-radius:12px;overflow:hidden;margin:16px 0;background:white}
          .stock-panel h3{font-size:16px;margin:0;padding:15px 18px;border-bottom:1px solid #e5ece9}
          .stock-panel .stock-note{padding:0 18px;color:#617471;font-size:12px}
          .stock-scroll{overflow-x:auto}
          .stock-board table{width:100%;border-collapse:collapse;margin:0;font-size:13px;white-space:nowrap}
          .stock-board th{font-size:11px;text-transform:uppercase;letter-spacing:.04em;color:#657775;background:#f6f9f8;text-align:left}
          .stock-board th,.stock-board td{padding:11px 15px;border-bottom:1px solid #edf1ef}
          .stock-board tbody tr:last-child td{border-bottom:0}
          .stock-board .stock-recent{background:#edf8f0}
          .stock-board .stock-danger{color:#b45309;font-weight:650}
          .stock-board .stock-muted{color:#71817d;font-size:11px}
          .stock-grid{display:grid;grid-template-columns:1fr 1fr;gap:16px}
          .stock-grid .stock-panel{margin:0;max-height:440px;overflow-y:auto}
          .stock-event,.stock-batch{padding:12px 16px;border-bottom:1px solid #e9efec}
          .stock-event:last-child,.stock-batch:last-child{border-bottom:0}
          .stock-event-title{display:flex;justify-content:space-between;gap:8px;margin:4px 0;font-weight:550}
          .stock-tag{border-radius:4px;padding:2px 6px;font-size:10px;background:#edf2f0;color:#49675d}
          .stock-tag-sale{background:#fff1e5;color:#92400e}
          .stock-tag-refill{background:#dcf4ec;color:#16634e}
          .stock-change{margin:6px 0;padding:7px 9px;border-radius:5px;font-size:12px}
          .stock-add{background:#e6f5ed;color:#16634e}
          .stock-remove{background:#fff0e8;color:#9a4820}
          .stock-change code{display:block;font-size:10px;white-space:pre-wrap;overflow-wrap:anywhere;margin-top:4px}
          .stock-empty{padding:20px;color:#71817d}
          .stock-bar{width:85px;height:5px;background:#e4ebe7;border-radius:4px;margin-top:5px;overflow:hidden}
          .stock-bar div{height:100%;border-radius:4px}
          .stock-error{background:#fff0e8;color:#9a4820;padding:16px;border-radius:10px}
          @media(max-width:750px){.stock-grid{grid-template-columns:1fr}.stock-hero{padding:20px}.stock-title{font-size:23px}}
         "]
         [:div.stock-hero
          [:div.stock-kicker "Triplox / incremental queries"]
          [:div.stock-title "A live view of low stock"]
          [:div.stock-subtitle "Sales reduce stock. Refills restore it. The query sends the changes."]
          [:div.stock-controls
           [:span.stock-status (str (if (= status :running) "● " "○ ") (name status))]
           [:button {:disabled (not active?)
                     :on-click (fn [_]
                                 (nextjournal.clerk.render/clerk-eval
                                  (if paused? '(stock-demo/resume!) '(stock-demo/pause!))))}
            (if paused? "Resume" "Pause")]
           [:button {:disabled (not paused?)
                     :on-click (fn [_]
                                 (nextjournal.clerk.render/clerk-eval '(stock-demo/step!)))}
            "Next transaction"]
           [:button {:disabled (not active?)
                     :on-click (fn [_]
                                 (nextjournal.clerk.render/clerk-eval '(stock-demo/stop!)))}
            "Stop"]]]
         (when error [:div.stock-error error])
         [:div.stock-metrics
          [:div.stock-metric [:strong (count inventory)] [:span "Stock records"]]
          [:div.stock-metric [:strong (count low-stock)] [:span "Rows in the query result"]]
          [:div.stock-metric [:strong batch-count] [:span "Received batches"]]]
         [:div.stock-panel
          [:h3 "Low stock / maintained from subscription changes"]
          [:div.stock-scroll
           (if (seq low-stock)
             [:table
              (table-head ["Product" "Warehouse" "Available" "Reorder below"])
              [:tbody
               (for [[id sku product location available threshold :as row] low-stock]
                 ^{:key id}
                 [:tr {:class (when (contains? latest-rows row) "stock-recent")}
                  [:td [:div product] [:div.stock-muted sku]]
                  [:td location]
                  [:td.stock-danger available]
                  [:td threshold]])]]
             [:div.stock-empty "All stock meets its reorder threshold."])]]
         [:div.stock-grid
          [:div.stock-panel
           [:h3 "Transactions / newest first"]
           [:p.stock-note "Each record and its stock change commit together."]
           (if (seq events)
             (for [{:keys [tx-id time kind product location quantity before after]} events]
               ^{:key tx-id}
               [:div.stock-event
                [:div.stock-muted (str time " · transaction " tx-id)]
                [:div.stock-event-title
                 [:span (str product " / " location)]
                 [:span.stock-tag {:class (str "stock-tag-" (name kind))}
                  (str (if (= kind :sale) "SOLD " "REFILLED ") quantity)]]
                [:div (str "Stock " before " → " after)]])
             [:div.stock-empty "The first sale arrives shortly."])]
          [:div.stock-panel
           [:h3 "Query changes / newest batch first"]
           [:p.stock-note "A row update removes the old row and adds the new row."]
           (if (seq batches)
             (for [{:keys [number time rows]} batches]
               ^{:key number}
               [:div.stock-batch
                [:div.stock-muted (str time " · batch " number)]
                (for [[[_ sku product location available _ :as row] weight] rows]
                  ^{:key (str row "/" weight)}
                  [:div.stock-change {:class (if (pos? weight) "stock-add" "stock-remove")}
                   [:div (str (if (pos? weight) "+" "") weight " · " product
                              " / " location " · " available " available")]
                   [:code (pr-str [row weight])]])])
             [:div.stock-empty "Waiting for the initial result."])]]
         [:div.stock-panel
          [:h3 "All stock / quantities from committed transactions"]
          [:div.stock-scroll
           [:table
            (table-head ["Product" "Warehouse" "Available" "Reorder below"])
            [:tbody
             (for [[id sku product location available threshold] inventory]
               ^{:key id}
               [:tr
                [:td [:div product] [:div.stock-muted sku]]
                [:td location]
                [:td {:class (when (< available threshold) "stock-danger")}
                 available
                 [:div.stock-bar
                  [:div {:style {:width (str (min 100 (* 100 (/ available (+ threshold 2)))) "%")
                                 :background (if (< available threshold) "#d69a48" "#4d9c82")}}]]]
                [:td threshold]])]]]]]))})

^{::clerk/visibility {:result :show} ::clerk/width :full}
(clerk/with-viewer dashboard-viewer nil)

;; ## The subscribed query
;; A row qualifies when its available quantity is below its product's reorder threshold.
;; Sales above the threshold do not change the query result.
^{::clerk/visibility {:result :show}}
(clerk/code (with-out-str (pprint/pprint demo/low-stock-query)))

;; The loop sells two units, then three, then two, and finally refills to two units above the threshold.
;; It then moves to the next stock record and repeats.
;; Pause the loop and select Next transaction to follow one change at a time.
