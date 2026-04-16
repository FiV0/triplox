(ns auctionmark.procedures
  "AuctionMark data generation, loading, and all 14 benchmark procedures."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [io.triplox.api :as tc]
            [auctionmark.schema :as schema])
  (:import [java.util Random]
           [java.util.concurrent ConcurrentLinkedQueue]
           [java.util.concurrent.atomic AtomicLong]
           [java.time Instant]))

;; ---------------------------------------------------------------------------
;; State
;; ---------------------------------------------------------------------------

(defrecord ItemSample [item-id seller-id])

(defn make-state
  "Create shared benchmark state. Thread-safe."
  []
  {:user-counter     (AtomicLong. 0)
   :region-counter   (AtomicLong. 0)
   :category-counter (AtomicLong. 0)
   :gag-counter      (AtomicLong. 0)
   :gav-counter      (AtomicLong. 0)
   :ua-counter       (AtomicLong. 0)
   :item-counter     (AtomicLong. 0)
   :image-counter    (AtomicLong. 0)
   :bid-counter      (AtomicLong. 0)
   :max-bid-counter  (AtomicLong. 0)
   :comment-counter  (AtomicLong. 0)
   :feedback-counter (AtomicLong. 0)
   :purchase-counter (AtomicLong. 0)
   :watch-counter    (AtomicLong. 0)
   ;; populated during loading
   :users            (atom [])            ; vector of user app-ids (longs)
   :regions          (atom [])            ; vector of region app-ids
   :categories       (atom [])            ; vector of {:id :name :weight}
   :category-ids     (atom [])            ; flat vector of category app-ids
   :gag-ids          (atom [])
   :gav-ids          (atom [])
   ;; item status queues — thread-safe
   :items-open       (ConcurrentLinkedQueue.)
   :items-waiting    (ConcurrentLinkedQueue.)
   :items-closed     (ConcurrentLinkedQueue.)})

;; ---------------------------------------------------------------------------
;; Helpers
;; ---------------------------------------------------------------------------

(defn next-id [^AtomicLong counter]
  (.getAndIncrement counter))

(defn rand-string
  "Random alphanumeric string of length n."
  [^Random rng n]
  (let [chars "abcdefghijklmnopqrstuvwxyz0123456789"
        sb (StringBuilder. n)]
    (dotimes [_ n]
      (.append sb (.charAt chars (.nextInt rng (count chars)))))
    (.toString sb)))

(defn rand-nth-vec
  "Random element from a vector using the given RNG."
  [^Random rng v]
  (when (seq v)
    (nth v (.nextInt rng (count v)))))

(defn rand-gaussian
  "Sample from a Gaussian distribution clamped to [0, n)."
  [^Random rng n]
  (let [raw (Math/abs (.nextGaussian rng))
        scaled (int (* (/ raw 3.0) n))]
    (min (max scaled 0) (dec n))))

(defn weighted-sample
  "Select a random index according to weights (doubles).
   Uses linear scan — fine for the category count we have."
  [^Random rng weights]
  (let [total (reduce + 0.0 weights)
        r (* (.nextDouble rng) total)]
    (loop [i 0
           acc 0.0]
      (if (>= i (count weights))
        (dec (count weights))
        (let [acc' (+ acc (double (nth weights i)))]
          (if (>= acc' r) i (recur (inc i) acc')))))))

(defn batch-transact!
  "Submit tx-data in batches of batch-size. Uses submit-tx (fire-and-forget)."
  [conn docs batch-size]
  (doseq [chunk (partition-all batch-size docs)]
    (tc/submit-tx conn (vec chunk))))

(defn now-instant []
  (Instant/now))

(defn instant->millis [^Instant inst]
  (.toEpochMilli inst))

(defn millis->instant [^long ms]
  (Instant/ofEpochMilli ms))

;; ---------------------------------------------------------------------------
;; Categories TSV
;; ---------------------------------------------------------------------------

(defn load-categories-tsv
  "Parse categories.tsv. Returns vector of {:name string :weight long}."
  []
  (let [lines (-> (io/resource "categories.tsv")
                  slurp
                  (str/split #"\n"))]
    (mapv (fn [line]
            (let [cols (str/split line #"\t" -1)
                  ;; cols: [top sub leaf _ _ count]
                  cat-name (str/join " > " (remove str/blank? (take 3 cols)))
                  weight (Long/parseLong (str/trim (last cols)))]
              {:name cat-name :weight weight}))
          lines)))

;; ---------------------------------------------------------------------------
;; Data Generation
;; ---------------------------------------------------------------------------

(defn generate-regions!
  "Generate region entities. Returns vector of region app-ids."
  [conn state ^Random rng n]
  (let [ids (mapv (fn [_]
                    (let [id (next-id (:region-counter state))]
                      {:region/id id
                       :region/name (str "Region-" id)}))
                  (range n))]
    (batch-transact! conn ids 512)
    (let [region-ids (mapv :region/id ids)]
      (reset! (:regions state) region-ids)
      region-ids)))

(defn generate-categories!
  "Generate category entities from parsed TSV data."
  [conn state categories]
  (let [docs (mapv (fn [cat]
                     (let [id (next-id (:category-counter state))]
                       {:category/id id
                        :category/name (:name cat)}))
                   categories)]
    (batch-transact! conn docs 512)
    (let [cat-ids (mapv :category/id docs)]
      (reset! (:category-ids state) cat-ids)
      (reset! (:categories state)
              (mapv (fn [cat doc] (assoc cat :id (:category/id doc)))
                    categories docs))
      cat-ids)))

(defn generate-gags!
  "Generate global attribute groups."
  [conn state ^Random rng n]
  (let [cat-ids @(:category-ids state)
        docs (mapv (fn [_]
                     (let [id (next-id (:gag-counter state))]
                       {:gag/id id
                        :gag/category-id [:category/id (rand-nth-vec rng cat-ids)]
                        :gag/name (str "GAG-" id)}))
                   (range n))]
    (batch-transact! conn docs 512)
    (let [ids (mapv :gag/id docs)]
      (reset! (:gag-ids state) ids)
      ids)))

(defn generate-gavs!
  "Generate global attribute values."
  [conn state ^Random rng n]
  (let [gag-ids @(:gag-ids state)
        docs (mapv (fn [_]
                     (let [id (next-id (:gav-counter state))]
                       {:gav/id id
                        :gav/gag-id [:gag/id (rand-nth-vec rng gag-ids)]
                        :gav/name (str "GAV-" id)}))
                   (range n))]
    (batch-transact! conn docs 512)
    (let [ids (mapv :gav/id docs)]
      (reset! (:gav-ids state) ids)
      ids)))

(defn generate-users!
  "Generate user entities."
  [conn state ^Random rng n]
  (let [region-ids @(:regions state)
        now-ms (instant->millis (now-instant))
        docs (mapv (fn [_]
                     (let [id (next-id (:user-counter state))]
                       {:user/id id
                        :user/region-id [:region/id (rand-nth-vec rng region-ids)]
                        :user/rating 0
                        :user/balance 0.0
                        :user/created (millis->instant (- now-ms (.nextInt rng 86400000)))
                        :user/sattr0 (rand-string rng 16)
                        :user/sattr1 (rand-string rng 16)
                        :user/sattr2 (rand-string rng 16)
                        :user/sattr3 (rand-string rng 16)
                        :user/sattr4 (rand-string rng 16)
                        :user/sattr5 (rand-string rng 16)
                        :user/sattr6 (rand-string rng 16)
                        :user/sattr7 (rand-string rng 16)}))
                   (range n))]
    (batch-transact! conn docs 512)
    (let [ids (mapv :user/id docs)]
      (reset! (:users state) ids)
      ids)))

(defn generate-user-attributes!
  "Generate user attribute entities."
  [conn state ^Random rng n]
  (let [user-ids @(:users state)
        docs (mapv (fn [_]
                     (let [id (next-id (:ua-counter state))
                           uid (rand-nth-vec rng user-ids)]
                       {:user-attribute/id id
                        :user-attribute/user-id [:user/id uid]
                        :user-attribute/name (str "attr-" (.nextInt rng 100))
                        :user-attribute/value (rand-string rng 32)
                        :user-attribute/created (now-instant)}))
                   (range n))]
    (batch-transact! conn docs 512)))

(defn- sample-status
  "Random item status with distribution: 25% open, 25% waiting, 50% closed."
  [^Random rng]
  (let [r (.nextInt rng 100)]
    (cond
      (< r 25) :open
      (< r 50) :waiting-for-purchase
      :else    :closed)))

(defn generate-items!
  "Generate item entities with images."
  [conn state ^Random rng n]
  (let [user-ids @(:users state)
        categories @(:categories state)
        cat-weights (mapv :weight categories)
        now-ms (instant->millis (now-instant))
        day-ms (* 24 60 60 1000)]
    ;; generate in chunks to avoid holding all docs in memory
    (doseq [chunk-start (range 0 n 512)]
      (let [chunk-end (min n (+ chunk-start 512))
            chunk-docs
            (reduce
             (fn [acc _]
               (let [item-id (next-id (:item-counter state))
                     seller-id (nth user-ids (rand-gaussian rng (count user-ids)))
                     cat-idx (weighted-sample rng cat-weights)
                     cat-id (:id (nth categories cat-idx))
                     status (sample-status rng)
                     num-images (inc (.nextInt rng 5))
                     initial-price (+ 1.0 (* (.nextDouble rng) 100.0))
                     duration-days (+ 1 (.nextInt rng 43))
                     start-ms (- now-ms (* (.nextInt rng 30) day-ms))
                     end-ms (+ start-ms (* duration-days day-ms))
                     item-doc {:item/id item-id
                               :item/user-id [:user/id seller-id]
                               :item/category-id [:category/id cat-id]
                               :item/name (str "Item-" item-id)
                               :item/description (str "Description for item " item-id)
                               :item/initial-price initial-price
                               :item/current-price initial-price
                               :item/num-bids 0
                               :item/num-images num-images
                               :item/start-date (millis->instant start-ms)
                               :item/end-date (millis->instant end-ms)
                               :item/status status}
                     image-docs (mapv (fn [i]
                                        (let [img-id (next-id (:image-counter state))]
                                          {:item-image/id img-id
                                           :item-image/item-id [:item/id item-id]
                                           :item-image/user-id [:user/id seller-id]
                                           :item-image/path (str "/img/" item-id "/" i ".jpg")}))
                                      (range num-images))
                     sample (->ItemSample item-id seller-id)]
                 ;; track in status queues
                 (case status
                   :open (.add ^ConcurrentLinkedQueue (:items-open state) sample)
                   :waiting-for-purchase (.add ^ConcurrentLinkedQueue (:items-waiting state) sample)
                   :closed (.add ^ConcurrentLinkedQueue (:items-closed state) sample))
                 (into (conj acc item-doc) image-docs)))
             []
             (range chunk-start chunk-end))]
        (tc/submit-tx conn chunk-docs)))))

(defn generate-user-watches!
  "Each user watches 0-5 random open items."
  [conn state ^Random rng]
  (let [user-ids @(:users state)
        open-items (vec (.toArray ^ConcurrentLinkedQueue (:items-open state)))]
    (when (seq open-items)
      (let [docs (into []
                       (mapcat
                        (fn [uid]
                          (let [n-watches (.nextInt rng 6)]
                            (repeatedly n-watches
                                        (fn []
                                          (let [item (rand-nth-vec rng open-items)]
                                            {:user-watch/id (next-id (:watch-counter state))
                                             :user-watch/user-id [:user/id uid]
                                             :user-watch/item-id [:item/id (:item-id item)]
                                             :user-watch/created (now-instant)})))))
                        user-ids))]
        (batch-transact! conn docs 512)))))

;; ---------------------------------------------------------------------------
;; Loading orchestration
;; ---------------------------------------------------------------------------

(defn load-data!
  "Load all AuctionMark data. Returns state."
  [conn {:keys [scale-factor] :as _config}]
  (let [state (make-state)
        rng (Random. 42)
        num-users (long (* scale-factor 1000000))
        num-user-attrs (long (* scale-factor 1300000))
        num-items (long (* scale-factor 10000000))
        categories-data (load-categories-tsv)]
    (println "Installing schema...")
    (tc/transact conn schema/schema-tx)

    (println (str "Generating " (count categories-data) " categories..."))
    (generate-categories! conn state categories-data)

    (println "Generating 75 regions...")
    (generate-regions! conn state rng 75)

    (println "Generating 100 global attribute groups...")
    (generate-gags! conn state rng 100)

    (println "Generating 1000 global attribute values...")
    (generate-gavs! conn state rng 1000)

    (println (str "Generating " num-users " users..."))
    (generate-users! conn state rng num-users)

    (println (str "Generating " num-user-attrs " user attributes..."))
    (generate-user-attributes! conn state rng num-user-attrs)

    (println (str "Generating " num-items " items + images..."))
    (generate-items! conn state rng num-items)

    (println "Generating user watches...")
    (generate-user-watches! conn state rng)

    ;; fence: wait for all submit-tx to be indexed
    (println "Waiting for indexing to complete...")
    (tc/transact conn [[:db/add [:region/id 0] :region/name
                        (str "Region-0-fence-" (System/currentTimeMillis))]])

    (println (str "Load complete. Items: open="
                  (.size ^ConcurrentLinkedQueue (:items-open state))
                  " waiting=" (.size ^ConcurrentLinkedQueue (:items-waiting state))
                  " closed=" (.size ^ConcurrentLinkedQueue (:items-closed state))))
    state))

;; ---------------------------------------------------------------------------
;; Helper: pick random items from queues
;; ---------------------------------------------------------------------------

(defn- queue->vec [^ConcurrentLinkedQueue q]
  (vec (.toArray q)))

(defn pick-random-open [^Random rng state]
  (let [v (queue->vec (:items-open state))]
    (when (seq v) (rand-nth-vec rng v))))

(defn pick-random-waiting [^Random rng state]
  (let [v (queue->vec (:items-waiting state))]
    (when (seq v) (rand-nth-vec rng v))))

(defn pick-random-closed [^Random rng state]
  (let [v (queue->vec (:items-closed state))]
    (when (seq v) (rand-nth-vec rng v))))

(defn pick-random-user [^Random rng state]
  (rand-nth-vec rng @(:users state)))

(defn pick-random-item
  "Pick from any status queue."
  [^Random rng state]
  (let [choice (.nextInt rng 3)]
    (case choice
      0 (pick-random-open rng state)
      1 (pick-random-waiting rng state)
      2 (pick-random-closed rng state))))

;; ---------------------------------------------------------------------------
;; Procedure 1: get-item (40%)
;; ---------------------------------------------------------------------------

(defn proc-get-item
  "Read item details for an open item."
  [conn ^Random rng state]
  (when-let [item (pick-random-open rng state)]
    (with-open [db (tc/db conn)]
      (tc/q db
            '{:find [?name ?desc ?price ?nbids ?status ?start ?end]
              :in [?item-id]
              :where [[?e :item/id ?item-id]
                      [?e :item/name ?name]
                      [?e :item/description ?desc]
                      [?e :item/current-price ?price]
                      [?e :item/num-bids ?nbids]
                      [?e :item/status ?status]
                      [?e :item/start-date ?start]
                      [?e :item/end-date ?end]]}
            (:item-id item)))))

;; ---------------------------------------------------------------------------
;; Procedure 2: new-bid (18%)
;; ---------------------------------------------------------------------------

(defn proc-new-bid
  "Place a bid on an open item. Client-side read-then-write (no tx functions)."
  [conn ^Random rng state]
  (when-let [item (pick-random-open rng state)]
    (let [buyer-id (pick-random-user rng state)
          item-id (:item-id item)]
      ;; read current state
      (let [[current-price num-bids]
            (with-open [db (tc/db conn)]
              (first (tc/q db
                           '{:find [?price ?nbids]
                             :in [?item-id]
                             :where [[?e :item/id ?item-id]
                                     [?e :item/current-price ?price]
                                     [?e :item/num-bids ?nbids]
                                     [?e :item/status :open]]}
                           item-id)))]
        (when current-price
          (let [new-price (* (double current-price) (+ 1.0 (* (.nextDouble rng) 0.1)))
                bid-id (next-id (:bid-counter state))
                max-bid-id (next-id (:max-bid-counter state))
                now (now-instant)]
            (tc/transact conn
              [{:item-bid/id bid-id
                :item-bid/item-id [:item/id item-id]
                :item-bid/user-id [:user/id (:seller-id item)]
                :item-bid/buyer-id [:user/id buyer-id]
                :item-bid/bid new-price
                :item-bid/max-bid new-price
                :item-bid/created-at now
                :item-bid/updated now}
               {:db/id [:item/id item-id]
                :item/current-price new-price
                :item/num-bids (inc (long num-bids))}
               ;; always create a new max-bid record (first bid has no existing record to upsert)
               {:item-max-bid/id max-bid-id
                :item-max-bid/item-id [:item/id item-id]
                :item-max-bid/user-id [:user/id (:seller-id item)]
                :item-max-bid/bid-id [:item-bid/id bid-id]
                :item-max-bid/buyer-id [:user/id buyer-id]
                :item-max-bid/created now
                :item-max-bid/updated now}])))))))

;; ---------------------------------------------------------------------------
;; Procedure 3: new-item (10%)
;; ---------------------------------------------------------------------------

(defn proc-new-item
  "List a new auction item with images."
  [conn ^Random rng state]
  (let [seller-id (pick-random-user rng state)
        categories @(:categories state)
        cat-weights (mapv :weight categories)
        cat-idx (weighted-sample rng cat-weights)
        cat-id (:id (nth categories cat-idx))
        item-id (next-id (:item-counter state))
        num-images (+ 1 (.nextInt rng 5))
        initial-price (+ 1.0 (* (.nextDouble rng) 100.0))
        now (now-instant)
        now-ms (instant->millis now)
        end-ms (+ now-ms (* (+ 1 (.nextInt rng 14)) 24 60 60 1000))
        image-docs (mapv (fn [i]
                           {:item-image/id (next-id (:image-counter state))
                            :item-image/item-id [:item/id item-id]
                            :item-image/user-id [:user/id seller-id]
                            :item-image/path (str "/img/" item-id "/" i ".jpg")})
                         (range num-images))
        item-doc {:item/id item-id
                  :item/user-id [:user/id seller-id]
                  :item/category-id [:category/id cat-id]
                  :item/name (str "Item-" item-id)
                  :item/description (str "New listing " item-id)
                  :item/initial-price initial-price
                  :item/current-price initial-price
                  :item/num-bids 0
                  :item/num-images num-images
                  :item/start-date now
                  :item/end-date (millis->instant end-ms)
                  :item/status :open}]
    (tc/transact conn (into [item-doc] image-docs))
    ;; apply seller fee — read current balance then decrement
    (let [balance (with-open [db (tc/db conn)]
                    (or (ffirst (tc/q db
                                     '{:find [?b]
                                       :in [?uid]
                                       :where [[?e :user/id ?uid]
                                               [?e :user/balance ?b]]}
                                     seller-id))
                        0.0))]
      (tc/transact conn
        [{:db/id [:user/id seller-id]
          :user/balance (- (double balance) 1.0)}]))
    (.add ^ConcurrentLinkedQueue (:items-open state)
          (->ItemSample item-id seller-id))))

;; ---------------------------------------------------------------------------
;; Procedure 4: get-user-info (10%)
;; ---------------------------------------------------------------------------

(defn proc-get-user-info
  "Read user profile."
  [conn ^Random rng state]
  (when-let [uid (pick-random-user rng state)]
    (with-open [db (tc/db conn)]
      (tc/q db
            '{:find [?rating ?balance ?created]
              :in [?uid]
              :where [[?e :user/id ?uid]
                      [?e :user/rating ?rating]
                      [?e :user/balance ?balance]
                      [?e :user/created ?created]]}
            uid))))

;; ---------------------------------------------------------------------------
;; Procedure 5: new-user (5%)
;; ---------------------------------------------------------------------------

(defn proc-new-user
  "Register a new user."
  [conn ^Random rng state]
  (let [uid (next-id (:user-counter state))
        region-ids @(:regions state)]
    (tc/transact conn
      [{:user/id uid
        :user/region-id [:region/id (rand-nth-vec rng region-ids)]
        :user/rating 0
        :user/balance 0.0
        :user/created (now-instant)
        :user/sattr0 (rand-string rng 16)
        :user/sattr1 (rand-string rng 16)
        :user/sattr2 (rand-string rng 16)
        :user/sattr3 (rand-string rng 16)
        :user/sattr4 (rand-string rng 16)
        :user/sattr5 (rand-string rng 16)
        :user/sattr6 (rand-string rng 16)
        :user/sattr7 (rand-string rng 16)}])
    (swap! (:users state) conj uid)))

;; ---------------------------------------------------------------------------
;; Procedure 6: get-watched-items (5%)
;; ---------------------------------------------------------------------------

(defn proc-get-watched-items
  "Get items being watched by a user."
  [conn ^Random rng state]
  (when-let [uid (pick-random-user rng state)]
    (with-open [db (tc/db conn)]
      (tc/q db
            '{:find [?item-name ?price ?status]
              :in [?uid]
              :where [[?w :user-watch/user-id ?u]
                      [?u :user/id ?uid]
                      [?w :user-watch/item-id ?item]
                      [?item :item/name ?item-name]
                      [?item :item/current-price ?price]
                      [?item :item/status ?status]]}
            uid))))

;; ---------------------------------------------------------------------------
;; Procedure 7: new-feedback (3%)
;; ---------------------------------------------------------------------------

(defn proc-new-feedback
  "Leave feedback on a completed purchase."
  [conn ^Random rng state]
  (when-let [item (pick-random-closed rng state)]
    (let [buyer-id (pick-random-user rng state)]
      (tc/transact conn
        [{:item-feedback/id (next-id (:feedback-counter state))
          :item-feedback/item-id [:item/id (:item-id item)]
          :item-feedback/user-id [:user/id (:seller-id item)]
          :item-feedback/buyer-id [:user/id buyer-id]
          :item-feedback/rating (+ 1 (.nextInt rng 5))
          :item-feedback/comment (str "Feedback " (.nextInt rng 10000))
          :item-feedback/date (now-instant)}]))))

;; ---------------------------------------------------------------------------
;; Procedure 8: new-purchase (2%)
;; ---------------------------------------------------------------------------

(defn proc-new-purchase
  "Complete a purchase for a waiting item."
  [conn ^Random rng state]
  (when-let [item (.poll ^ConcurrentLinkedQueue (:items-waiting state))]
    (let [item-id (:item-id item)]
      ;; find the highest bid for this item
      (let [bids (with-open [db (tc/db conn)]
                   (tc/q db
                         '{:find [?bid-id ?bid-amount]
                           :in [?item-id]
                           :where [[?b :item-bid/item-id ?i]
                                   [?i :item/id ?item-id]
                                   [?b :item-bid/id ?bid-id]
                                   [?b :item-bid/bid ?bid-amount]]
                           :order [[?bid-amount :desc]]
                           :limit 1}
                         item-id))
            winning-bid-id (ffirst bids)]
        (tc/transact conn
          [(merge
            {:item-purchase/id (next-id (:purchase-counter state))
             :item-purchase/item-id [:item/id item-id]
             :item-purchase/user-id [:user/id (:seller-id item)]
             :item-purchase/date (now-instant)}
            (when winning-bid-id
              {:item-purchase/bid-id [:item-bid/id winning-bid-id]}))
           {:db/id [:item/id item-id]
            :item/status :closed}])
        (.add ^ConcurrentLinkedQueue (:items-closed state) item)))))

;; ---------------------------------------------------------------------------
;; Procedure 9: new-comment (2%)
;; ---------------------------------------------------------------------------

(defn proc-new-comment
  "Post a question about an item."
  [conn ^Random rng state]
  (when-let [item (pick-random-open rng state)]
    (let [buyer-id (pick-random-user rng state)]
      (tc/transact conn
        [{:item-comment/id (next-id (:comment-counter state))
          :item-comment/item-id [:item/id (:item-id item)]
          :item-comment/user-id [:user/id (:seller-id item)]
          :item-comment/buyer-id [:user/id buyer-id]
          :item-comment/question (str "Question about item " (:item-id item))
          :item-comment/response ""
          :item-comment/created (now-instant)
          :item-comment/updated (now-instant)}]))))

;; ---------------------------------------------------------------------------
;; Procedure 10: update-item (2%)
;; ---------------------------------------------------------------------------

(defn proc-update-item
  "Update item description."
  [conn ^Random rng state]
  (when-let [item (pick-random-open rng state)]
    (tc/transact conn
      [{:db/id [:item/id (:item-id item)]
        :item/description (str "Updated description " (.nextInt rng 100000))}])))

;; ---------------------------------------------------------------------------
;; Procedure 11: get-comment (2%)
;; ---------------------------------------------------------------------------

(defn proc-get-comment
  "Read comments for an item."
  [conn ^Random rng state]
  (when-let [item (pick-random-item rng state)]
    (with-open [db (tc/db conn)]
      (tc/q db
            '{:find [?cid ?question ?response ?created]
              :in [?item-id]
              :where [[?c :item-comment/item-id ?i]
                      [?i :item/id ?item-id]
                      [?c :item-comment/id ?cid]
                      [?c :item-comment/question ?question]
                      [?c :item-comment/response ?response]
                      [?c :item-comment/created ?created]]}
            (:item-id item)))))

;; ---------------------------------------------------------------------------
;; Procedure 12: new-comment-response (1%)
;; ---------------------------------------------------------------------------

(defn proc-new-comment-response
  "Seller responds to a question."
  [conn ^Random _rng state]
  (let [unanswered
        (with-open [db (tc/db conn)]
          (tc/q db
                '{:find [?comment-id]
                  :where [[?c :item-comment/response ""]
                          [?c :item-comment/id ?comment-id]]
                  :limit 10}))]
    (when (seq unanswered)
      (let [comment-id (first (rand-nth unanswered))]
        (tc/transact conn
          [{:db/id [:item-comment/id comment-id]
            :item-comment/response "Thanks for asking!"
            :item-comment/updated (now-instant)}])))))

;; ---------------------------------------------------------------------------
;; Procedure 13: check-winning-bid (periodic)
;; ---------------------------------------------------------------------------

(defn proc-check-winning-bid
  "Find expired open items, transition to waiting-for-purchase."
  [conn _rng state]
  (let [now-ms (instant->millis (now-instant))
        expired
        (with-open [db (tc/db conn)]
          (tc/q db
                '{:find [?item-id ?seller-id]
                  :in [?now]
                  :where [[?e :item/status :open]
                          [?e :item/end-date ?end]
                          [(< ?end ?now)]
                          [?e :item/id ?item-id]
                          [?e :item/user-id ?seller]
                          [?seller :user/id ?seller-id]]
                  :limit 100}
                (millis->instant now-ms)))]
    (when (seq expired)
      (let [tx-data (mapv (fn [[item-id _]]
                            {:db/id [:item/id item-id]
                             :item/status :waiting-for-purchase})
                          expired)]
        (tc/transact conn tx-data)
        ;; move items from open to waiting
        (doseq [[item-id seller-id] expired]
          (let [sample (->ItemSample item-id seller-id)]
            (.add ^ConcurrentLinkedQueue (:items-waiting state) sample)))))))

;; ---------------------------------------------------------------------------
;; Procedure 14: post-auction (periodic, after check-winning-bid)
;; ---------------------------------------------------------------------------

(defn proc-post-auction
  "Auto-close waiting items that have been waiting long enough."
  [conn ^Random rng state]
  ;; close up to 10 waiting items
  (dotimes [_ 10]
    (when-let [item (.poll ^ConcurrentLinkedQueue (:items-waiting state))]
      (tc/transact conn
        [{:db/id [:item/id (:item-id item)]
          :item/status :closed}
         {:item-purchase/id (next-id (:purchase-counter state))
          :item-purchase/item-id [:item/id (:item-id item)]
          :item-purchase/user-id [:user/id (:seller-id item)]
          :item-purchase/date (now-instant)}])
      (.add ^ConcurrentLinkedQueue (:items-closed state) item))))
