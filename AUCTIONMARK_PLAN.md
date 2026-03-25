# AuctionMark Benchmark in Datomic-Style Queries

## Context

The [AuctionMark benchmark](https://hstore.cs.brown.edu/projects/auctionmark/) is an OLTP benchmark simulating an online auction site. We want to model it as Datomic-style schema, transactions, and datalog queries for Triplox, written in Clojure using the existing `triplox.api` client.

Inspired by: [XTDB AuctionMark implementation](https://github.com/wotbrew/xtdb/blob/auctionmark/bench/src/xtdb/bench2/auctionmark.clj)

Initial version: **schema + queries + transactions only** (no data generation or workload driver).

---

## File Structure

```
triplox-jvm/src/main/clojure/triplox/bench/
  auctionmark.clj        -- schema, queries, transaction functions
```

The bench code lives inside `triplox-jvm` to reuse the existing Gradle/Clojurephant build and the `triplox.api` / `triplox.tx` namespaces. No new build config needed.

### Key existing files to reuse
- `triplox-jvm/src/main/clojure/triplox/api.clj` — `connect`, `db`, `q`, `transact`, `submit-tx`
- `triplox-jvm/src/main/clojure/triplox/tx.clj` — Datomic-style tx-data conversion (maps → Put, vectors → Add/Retract/Delete/Erase)
- `triplox-jvm/src/main/clojure/triplox/types.clj` — wire type conversion

---

## 1. Schema

All attributes use namespaced keywords following Datomic convention. Each schema attribute is a map with `:db/id`, `:db/ident`, `:db/valueType`, `:db/cardinality`.

Entity IDs for schema attributes start at 100 (above bootstrap range 1-41).

```clojure
(def auctionmark-schema
  [;; --- Region ---
   {:db/id 100 :db/ident :region/name
    :db/valueType :db.type/string :db/cardinality :db.cardinality/one}

   ;; --- Category ---
   {:db/id 101 :db/ident :category/name
    :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/id 102 :db/ident :category/parent
    :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}

   ;; --- Global Attribute Group ---
   {:db/id 103 :db/ident :gag/name
    :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/id 104 :db/ident :gag/category
    :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}

   ;; --- Global Attribute Value ---
   {:db/id 105 :db/ident :gav/name
    :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/id 106 :db/ident :gav/group
    :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}

   ;; --- User ---
   {:db/id 110 :db/ident :user/rating
    :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/id 111 :db/ident :user/balance
    :db/valueType :db.type/double :db/cardinality :db.cardinality/one}
   {:db/id 112 :db/ident :user/created
    :db/valueType :db.type/instant :db/cardinality :db.cardinality/one}
   {:db/id 113 :db/ident :user/region
    :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   ;; user/sattr0 through user/sattr7
   {:db/id 114 :db/ident :user/sattr0
    :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/id 115 :db/ident :user/sattr1
    :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/id 116 :db/ident :user/sattr2
    :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/id 117 :db/ident :user/sattr3
    :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/id 118 :db/ident :user/sattr4
    :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/id 119 :db/ident :user/sattr5
    :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/id 120 :db/ident :user/sattr6
    :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/id 121 :db/ident :user/sattr7
    :db/valueType :db.type/string :db/cardinality :db.cardinality/one}

   ;; --- User Attribute (flexible key-value) ---
   {:db/id 122 :db/ident :ua/user
    :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/id 123 :db/ident :ua/name
    :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/id 124 :db/ident :ua/value
    :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/id 125 :db/ident :ua/created
    :db/valueType :db.type/instant :db/cardinality :db.cardinality/one}

   ;; --- Item ---
   {:db/id 130 :db/ident :item/seller
    :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/id 131 :db/ident :item/category
    :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/id 132 :db/ident :item/name
    :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/id 133 :db/ident :item/description
    :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/id 134 :db/ident :item/initial-price
    :db/valueType :db.type/double :db/cardinality :db.cardinality/one}
   {:db/id 135 :db/ident :item/current-price
    :db/valueType :db.type/double :db/cardinality :db.cardinality/one}
   {:db/id 136 :db/ident :item/num-bids
    :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/id 137 :db/ident :item/num-images
    :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/id 138 :db/ident :item/num-global-attrs
    :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/id 139 :db/ident :item/start-date
    :db/valueType :db.type/instant :db/cardinality :db.cardinality/one}
   {:db/id 140 :db/ident :item/end-date
    :db/valueType :db.type/instant :db/cardinality :db.cardinality/one}
   {:db/id 141 :db/ident :item/status
    :db/valueType :db.type/keyword :db/cardinality :db.cardinality/one}

   ;; --- Item Image ---
   {:db/id 142 :db/ident :ii/item
    :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/id 143 :db/ident :ii/path
    :db/valueType :db.type/string :db/cardinality :db.cardinality/one}

   ;; --- Item Comment ---
   {:db/id 144 :db/ident :ic/item
    :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/id 145 :db/ident :ic/seller
    :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/id 146 :db/ident :ic/buyer
    :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/id 147 :db/ident :ic/date
    :db/valueType :db.type/instant :db/cardinality :db.cardinality/one}
   {:db/id 148 :db/ident :ic/question
    :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/id 149 :db/ident :ic/response
    :db/valueType :db.type/string :db/cardinality :db.cardinality/one}

   ;; --- Item Feedback ---
   {:db/id 150 :db/ident :if/item
    :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/id 151 :db/ident :if/seller
    :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/id 152 :db/ident :if/buyer
    :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/id 153 :db/ident :if/rating
    :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/id 154 :db/ident :if/date
    :db/valueType :db.type/instant :db/cardinality :db.cardinality/one}
   {:db/id 155 :db/ident :if/comment
    :db/valueType :db.type/string :db/cardinality :db.cardinality/one}

   ;; --- Item Bid ---
   {:db/id 156 :db/ident :ib/item
    :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/id 157 :db/ident :ib/seller
    :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/id 158 :db/ident :ib/buyer
    :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/id 159 :db/ident :ib/bid
    :db/valueType :db.type/double :db/cardinality :db.cardinality/one}
   {:db/id 160 :db/ident :ib/max-bid
    :db/valueType :db.type/double :db/cardinality :db.cardinality/one}
   {:db/id 161 :db/ident :ib/created
    :db/valueType :db.type/instant :db/cardinality :db.cardinality/one}
   {:db/id 162 :db/ident :ib/updated
    :db/valueType :db.type/instant :db/cardinality :db.cardinality/one}

   ;; --- Item Max Bid ---
   {:db/id 163 :db/ident :imb/item
    :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/id 164 :db/ident :imb/seller
    :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/id 165 :db/ident :imb/bid
    :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/id 166 :db/ident :imb/created
    :db/valueType :db.type/instant :db/cardinality :db.cardinality/one}
   {:db/id 167 :db/ident :imb/updated
    :db/valueType :db.type/instant :db/cardinality :db.cardinality/one}

   ;; --- Item Purchase ---
   {:db/id 168 :db/ident :ip/bid
    :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/id 169 :db/ident :ip/item
    :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/id 170 :db/ident :ip/seller
    :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/id 171 :db/ident :ip/date
    :db/valueType :db.type/instant :db/cardinality :db.cardinality/one}

   ;; --- User Watch ---
   {:db/id 172 :db/ident :uw/user
    :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/id 173 :db/ident :uw/item
    :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/id 174 :db/ident :uw/created
    :db/valueType :db.type/instant :db/cardinality :db.cardinality/one}

   ;; --- User Item (purchased items, buyer view) ---
   {:db/id 175 :db/ident :ui/buyer
    :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/id 176 :db/ident :ui/item
    :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/id 177 :db/ident :ui/seller
    :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/id 178 :db/ident :ui/created
    :db/valueType :db.type/instant :db/cardinality :db.cardinality/one}])
```

### Design Decisions
- **Status as keyword**: `:item.status/open`, `:item.status/waiting-for-purchase`, `:item.status/closed` — idiomatic, self-documenting, works with Triplox keyword queries (verified in tests).
- **Refs for relationships**: All cross-entity references use `:db.type/ref`.
- **No composite IDs**: Triplox assigns entity IDs; we use `db/id` with explicit long values.
- **USER_WATCH included**: Inferred from `get-watched-items` procedure (undocumented in spec but required).

---

## 2. Queries

Each query is a Clojure function returning results. Uses `triplox.api/q` with EDN datalog.

Note: `:in` parameters are not yet supported. We pass known entity IDs as constants directly in where-clause patterns.

```clojure
;; get-item (40%) — retrieve item details by entity ID
(defn get-item [db item-id]
  (tc/q db
    {:find '[?seller ?init-price ?curr-price]
     :where [[item-id :item/seller '?seller]
             [item-id :item/initial-price '?init-price]
             [item-id :item/current-price '?curr-price]
             [item-id :item/status :item.status/open]]}))

;; get-user-info (10%) — user profile + seller items + buyer items + feedback
(defn get-user-info [db user-id]
  {:user    (tc/q db {:find '[?rating ?balance ?created]
                      :where [[user-id :user/rating '?rating]
                              [user-id :user/balance '?balance]
                              [user-id :user/created '?created]]})
   :seller-items (tc/q db {:find '[?i ?name ?price ?end ?status]
                           :where [['?i :item/seller user-id]
                                   ['?i :item/name '?name]
                                   ['?i :item/current-price '?price]
                                   ['?i :item/end-date '?end]
                                   ['?i :item/status '?status]]})
   :buyer-items  (tc/q db {:find '[?i ?name ?price ?end ?status]
                           :where [['?ui :ui/buyer user-id]
                                   ['?ui :ui/item '?i]
                                   ['?i :item/name '?name]
                                   ['?i :item/current-price '?price]
                                   ['?i :item/end-date '?end]
                                   ['?i :item/status '?status]]})
   :feedback     (tc/q db {:find '[?if-ent ?rating ?comment ?date]
                           :where [['?if-ent :if/seller user-id]
                                   ['?if-ent :if/rating '?rating]
                                   ['?if-ent :if/comment '?comment]
                                   ['?if-ent :if/date '?date]]})})

;; get-comment (2%) — unanswered comments for a seller
(defn get-comment [db seller-id]
  (tc/q db
    {:find '[?ic ?item ?buyer ?date ?question]
     :where [['?ic :ic/seller seller-id]
             ['?ic :ic/item '?item]
             ['?ic :ic/buyer '?buyer]
             ['?ic :ic/date '?date]
             ['?ic :ic/question '?question]
             '(not [?ic :ic/response _])]}))

;; get-watched-items (5%)
(defn get-watched-items [db user-id]
  (tc/q db
    {:find '[?i ?seller ?name ?price ?end ?status ?created]
     :where [['?uw :uw/user user-id]
             ['?uw :uw/item '?i]
             ['?uw :uw/created '?created]
             ['?i :item/seller '?seller]
             ['?i :item/name '?name]
             ['?i :item/current-price '?price]
             ['?i :item/end-date '?end]
             ['?i :item/status '?status]]}))

;; check-winning-bids (system) — find expired open auctions with their winning bids
(defn check-winning-bids [db]
  (tc/q db
    {:find '[?i ?seller ?imb-bid ?buyer]
     :where [['?i :item/status :item.status/open]
             ['?i :item/seller '?seller]
             ['?i :item/end-date '?end-date]
             ['?imb :imb/item '?i]
             ['?imb :imb/bid '?imb-bid]
             ['?imb-bid :ib/buyer '?buyer]]}))
;; Note: time-window filtering (end-date < now) requires predicate support:
;; [(< ?end-date <now-instant>)]
```

---

## 3. Transactions

Each benchmark operation is a Clojure function returning tx-data (vector of maps/vectors) for `tc/transact`.

```clojure
;; new-user (5%)
(defn new-user-tx [user-id region-id now sattrs]
  [{:db/id user-id
    :user/rating 0
    :user/balance 0.0
    :user/created now
    :user/region region-id
    :user/sattr0 (nth sattrs 0)
    :user/sattr1 (nth sattrs 1)
    ;; ... sattr2-7
    }])

;; new-item (10%)
(defn new-item-tx [item-id seller-id cat-id name desc price end-date now image-paths]
  (into [{:db/id item-id
          :item/seller seller-id
          :item/category cat-id
          :item/name name
          :item/description desc
          :item/initial-price price
          :item/current-price price
          :item/num-bids 0
          :item/num-images (count image-paths)
          :item/start-date now
          :item/end-date end-date
          :item/status :item.status/open}]
        (map-indexed (fn [i path]
                       {:db/id (+ (* item-id 100) i 1)  ;; derived image ID
                        :ii/item item-id
                        :ii/path path}))
        image-paths))

;; new-bid (18%) — read-then-write (query first, then transact)
(defn new-bid-tx [bid-id item-id seller-id buyer-id bid-amount max-bid
                  new-price new-num-bids imb-id now]
  [{:db/id bid-id
    :ib/item item-id
    :ib/seller seller-id
    :ib/buyer buyer-id
    :ib/bid bid-amount
    :ib/max-bid max-bid
    :ib/created now
    :ib/updated now}
   [:db/add item-id :item/current-price new-price]
   [:db/add item-id :item/num-bids new-num-bids]
   {:db/id imb-id
    :imb/item item-id
    :imb/seller seller-id
    :imb/bid bid-id
    :imb/updated now}])

;; new-comment (2%)
(defn new-comment-tx [comment-id item-id seller-id buyer-id question now]
  [{:db/id comment-id
    :ic/item item-id
    :ic/seller seller-id
    :ic/buyer buyer-id
    :ic/date now
    :ic/question question}])

;; new-comment-response (1%)
(defn new-comment-response-tx [comment-id response]
  [[:db/add comment-id :ic/response response]])

;; new-purchase (2%)
(defn new-purchase-tx [purchase-id bid-id item-id seller-id buyer-id now]
  [[:db/add item-id :item/status :item.status/closed]
   {:db/id purchase-id
    :ip/bid bid-id
    :ip/item item-id
    :ip/seller seller-id
    :ip/date now}
   {:db/id (+ purchase-id 1000000)  ;; derived user-item ID
    :ui/buyer buyer-id
    :ui/item item-id
    :ui/seller seller-id
    :ui/created now}])

;; new-feedback (3%)
(defn new-feedback-tx [feedback-id item-id seller-id buyer-id
                       rating comment new-seller-rating now]
  [{:db/id feedback-id
    :if/item item-id
    :if/seller seller-id
    :if/buyer buyer-id
    :if/rating rating
    :if/date now
    :if/comment comment}
   [:db/add seller-id :user/rating new-seller-rating]])

;; update-item (2%)
(defn update-item-tx [item-id new-description]
  [[:db/add item-id :item/description new-description]])

;; post-auction (system) — close expired items
(defn post-auction-tx [item-id has-bids? winner-id seller-id now]
  (if has-bids?
    [[:db/add item-id :item/status :item.status/waiting-for-purchase]
     {:db/id (+ item-id 2000000)  ;; derived user-item ID
      :ui/buyer winner-id
      :ui/item item-id
      :ui/seller seller-id
      :ui/created now}]
    [[:db/add item-id :item/status :item.status/closed]]))
```

---

## 4. Triplox Limitations (workarounds)

| Feature | Status | Workaround |
|---------|--------|------------|
| `:in` parameters | TODO | Inline entity IDs as constants in where-patterns |
| `:order-by` | TODO | Sort results in Clojure (`sort-by`) |
| `:limit` / `:offset` | TODO | `(take n results)` in Clojure |
| `pull` expressions | TODO | Explicit `:find` with all needed attributes |
| Timestamp predicates | Predicates exist, instant comparison untested | Test `[(< ?end-date <instant>)]`; fall back to client-side filter |

---

## 5. Verification

1. Start Triplox server: `cargo run` from project root
2. In a REPL or test:
   ```clojure
   (require '[triplox.api :as tc])
   (def conn (tc/connect "localhost" 5490))
   ;; Install schema
   (tc/transact conn auctionmark-schema)
   ;; Seed a region, user, item
   (tc/transact conn [{:db/id 10000 :region/name "US-East"}])
   (tc/transact conn [(new-user-tx 20000 10000 (java.time.Instant/now) (repeat 8 "attr"))])
   ;; ... seed more data, run queries, verify results
   ```
3. Run `cargo test` to verify no regressions in server
4. Run `cd triplox-jvm && ./gradlew test` to verify Clojure compilation
