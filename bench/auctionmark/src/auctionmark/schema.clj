(ns auctionmark.schema
  "AuctionMark schema attribute definitions for Triplox.

  Primary entity ids (`:*/id`) are declared `:db.unique/identity`. That constraint
  is required for an attribute to be usable in lookup refs (e.g. `[:user/id 0]`)
  and identity upserts (e.g. `{:db/id [:item/id 0] ...}`), both of which the
  procedures rely on. All other attributes are cardinality-one.")

(def schema-tx
  [;; --- Region ---
   {:db/ident :region/id
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :region/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}

   ;; --- Category ---
   {:db/ident :category/id
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :category/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :category/parent-id
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}

   ;; --- Global Attribute Group ---
   {:db/ident :gag/id
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :gag/category-id
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :gag/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}

   ;; --- Global Attribute Value ---
   {:db/ident :gav/id
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :gav/gag-id
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :gav/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}

   ;; --- User ---
   {:db/ident :user/id
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :user/region-id
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :user/rating
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident :user/balance
    :db/valueType :db.type/double
    :db/cardinality :db.cardinality/one}
   {:db/ident :user/created
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}
   {:db/ident :user/sattr0
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :user/sattr1
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :user/sattr2
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :user/sattr3
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :user/sattr4
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :user/sattr5
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :user/sattr6
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :user/sattr7
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}

   ;; --- User Attribute ---
   {:db/ident :user-attribute/id
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :user-attribute/user-id
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :user-attribute/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :user-attribute/value
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :user-attribute/created
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}

   ;; --- Item ---
   {:db/ident :item/id
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :item/user-id
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :item/category-id
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :item/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :item/description
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :item/initial-price
    :db/valueType :db.type/double
    :db/cardinality :db.cardinality/one}
   {:db/ident :item/current-price
    :db/valueType :db.type/double
    :db/cardinality :db.cardinality/one}
   {:db/ident :item/num-bids
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident :item/num-images
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident :item/start-date
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}
   {:db/ident :item/end-date
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}
   {:db/ident :item/status
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one}

   ;; --- Item Image ---
   {:db/ident :item-image/id
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :item-image/item-id
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :item-image/user-id
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :item-image/path
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}

   ;; --- Item Bid ---
   {:db/ident :item-bid/id
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :item-bid/item-id
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :item-bid/user-id
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :item-bid/buyer-id
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :item-bid/bid
    :db/valueType :db.type/double
    :db/cardinality :db.cardinality/one}
   {:db/ident :item-bid/max-bid
    :db/valueType :db.type/double
    :db/cardinality :db.cardinality/one}
   {:db/ident :item-bid/created-at
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}
   {:db/ident :item-bid/updated
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}

   ;; --- Item Max Bid ---
   {:db/ident :item-max-bid/id
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :item-max-bid/item-id
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :item-max-bid/user-id
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :item-max-bid/bid-id
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :item-max-bid/buyer-id
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :item-max-bid/created
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}
   {:db/ident :item-max-bid/updated
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}

   ;; --- Item Comment ---
   {:db/ident :item-comment/id
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :item-comment/item-id
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :item-comment/user-id
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :item-comment/buyer-id
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :item-comment/question
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :item-comment/response
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :item-comment/created
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}
   {:db/ident :item-comment/updated
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}

   ;; --- Item Feedback ---
   {:db/ident :item-feedback/id
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :item-feedback/item-id
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :item-feedback/user-id
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :item-feedback/buyer-id
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :item-feedback/rating
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident :item-feedback/comment
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :item-feedback/date
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}

   ;; --- Item Purchase ---
   {:db/ident :item-purchase/id
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :item-purchase/bid-id
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :item-purchase/item-id
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :item-purchase/user-id
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :item-purchase/date
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}

   ;; --- User Watch ---
   {:db/ident :user-watch/id
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :user-watch/user-id
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :user-watch/item-id
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :user-watch/created
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}])
