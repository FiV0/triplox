(ns auctionmark.schema
  "AuctionMark schema attribute definitions for Triplox.")

(defn- attr
  ([ident value-type]
   (attr ident value-type :db.cardinality/one))
  ([ident value-type cardinality]
   {:db/ident ident
    :db/valueType value-type
    :db/cardinality cardinality}))

(def schema-tx
  [
   ;; --- Region ---
   (attr :region/id :db.type/long)
   (attr :region/name :db.type/string)

   ;; --- Category ---
   (attr :category/id :db.type/long)
   (attr :category/name :db.type/string)
   (attr :category/parent-id :db.type/ref)

   ;; --- Global Attribute Group ---
   (attr :gag/id :db.type/long)
   (attr :gag/category-id :db.type/ref)
   (attr :gag/name :db.type/string)

   ;; --- Global Attribute Value ---
   (attr :gav/id :db.type/long)
   (attr :gav/gag-id :db.type/ref)
   (attr :gav/name :db.type/string)

   ;; --- User ---
   [(attr :user/id :db.type/long)
    (attr :user/region-id :db.type/ref)
    (attr :user/rating :db.type/long)
    (attr :user/balance :db.type/double)
    (attr :user/created :db.type/instant)
    (attr :user/sattr0 :db.type/string)
    (attr :user/sattr1 :db.type/string)
    (attr :user/sattr2 :db.type/string)
    (attr :user/sattr3 :db.type/string)
    (attr :user/sattr4 :db.type/string)
    (attr :user/sattr5 :db.type/string)
    (attr :user/sattr6 :db.type/string)
    (attr :user/sattr7 :db.type/string)]

   ;; --- User Attribute ---
   (attr :user-attribute/id :db.type/long)
   (attr :user-attribute/user-id :db.type/ref)
   (attr :user-attribute/name :db.type/string)
   (attr :user-attribute/value :db.type/string)
   (attr :user-attribute/created :db.type/instant)

   ;; --- Item ---
   (attr :item/id :db.type/long)
   (attr :item/user-id :db.type/ref)
   (attr :item/category-id :db.type/ref)
   (attr :item/name :db.type/string)
   (attr :item/description :db.type/string)
   (attr :item/initial-price :db.type/double)
   (attr :item/current-price :db.type/double)
   (attr :item/num-bids :db.type/long)
   (attr :item/num-images :db.type/long)
   (attr :item/start-date :db.type/instant)
   (attr :item/end-date :db.type/instant)
   (attr :item/status :db.type/keyword)

   ;; --- Item Image ---
   (attr :item-image/id :db.type/long)
   (attr :item-image/item-id :db.type/ref)
   (attr :item-image/user-id :db.type/ref)
   (attr :item-image/path :db.type/string)

   ;; --- Item Bid ---
   (attr :item-bid/id :db.type/long)
   (attr :item-bid/item-id :db.type/ref)
   (attr :item-bid/user-id :db.type/ref)
   (attr :item-bid/buyer-id :db.type/ref)
   (attr :item-bid/bid :db.type/double)
   (attr :item-bid/max-bid :db.type/double)
   (attr :item-bid/created-at :db.type/instant)
   (attr :item-bid/updated :db.type/instant)

   ;; --- Item Max Bid ---
   (attr :item-max-bid/id :db.type/long)
   (attr :item-max-bid/item-id :db.type/ref)
   (attr :item-max-bid/user-id :db.type/ref)
   (attr :item-max-bid/bid-id :db.type/ref)
   (attr :item-max-bid/buyer-id :db.type/ref)
   (attr :item-max-bid/created :db.type/instant)
   (attr :item-max-bid/updated :db.type/instant)


   ;; --- Item Comment ---
   (attr :item-comment/id :db.type/long)
   (attr :item-comment/item-id :db.type/ref)
   (attr :item-comment/user-id :db.type/ref)
   (attr :item-comment/buyer-id :db.type/ref)
   (attr :item-comment/question :db.type/string)
   (attr :item-comment/response :db.type/string)
   (attr :item-comment/created :db.type/instant)
   (attr :item-comment/updated :db.type/instant)

   ;; --- Item Feedback ---
   (attr :item-feedback/id :db.type/long)
   (attr :item-feedback/item-id :db.type/ref)
   (attr :item-feedback/user-id :db.type/ref)
   (attr :item-feedback/buyer-id :db.type/ref)
   (attr :item-feedback/rating :db.type/long)
   (attr :item-feedback/comment :db.type/string)
   (attr :item-feedback/date :db.type/instant)

   ;; --- Item Purchase ---
   (attr :item-purchase/id :db.type/long)
   (attr :item-purchase/bid-id :db.type/ref)
   (attr :item-purchase/item-id :db.type/ref)
   (attr :item-purchase/user-id :db.type/ref)
   (attr :item-purchase/date :db.type/instant)

   ;; --- User Watch ---
   (attr :user-watch/id :db.type/long)
   (attr :user-watch/user-id :db.type/ref)
   (attr :user-watch/item-id :db.type/ref)
   (attr :user-watch/created :db.type/instant)])
