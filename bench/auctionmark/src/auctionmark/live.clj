(ns auctionmark.live
  "AuctionMark live queries and their small reference model."
  (:require [auctionmark.procedures :as proc]
            [auctionmark.schema :as schema]
            [clojure.walk :as walk]
            [xyz.triplox.api :as tc])
  (:import [java.util Random]
           [java.util.concurrent ConcurrentLinkedQueue]
           [java.util.concurrent.atomic AtomicLong]))

;; ---------------------------------------------------------------------------
;; Queries

(def templates
  "Live query templates in subscription order. PARAM is an item id for :item, a user id otherwise."
  [[:item
    '{:find [?id ?name ?description ?price ?bids ?status]
      :where [[?i :item/id PARAM]
              [?i :item/id ?id]
              [?i :item/name ?name]
              [?i :item/description ?description]
              [?i :item/current-price ?price]
              [?i :item/num-bids ?bids]
              [?i :item/status ?status]]}]

   [:watchlist
    '{:find [?id ?name ?price ?status]
      :where [[?u :user/id PARAM]
              [?w :user-watch/user-id ?u]
              [?w :user-watch/item-id ?i]
              [?i :item/id ?id]
              [?i :item/name ?name]
              [?i :item/current-price ?price]
              [?i :item/status ?status]]}]

   [:selling
    '{:find [?id ?name ?price ?bids]
      :where [[?u :user/id PARAM]
              [?i :item/user-id ?u]
              [?i :item/status :open]
              [?i :item/id ?id]
              [?i :item/name ?name]
              [?i :item/current-price ?price]
              [?i :item/num-bids ?bids]]}]

   [:bids
    '{:find [?bid ?id ?amount ?price]
      :where [[?u :user/id PARAM]
              [?b :item-bid/buyer-id ?u]
              [?b :item-bid/id ?bid]
              [?b :item-bid/bid ?amount]
              [?b :item-bid/item-id ?i]
              [?i :item/id ?id]
              [?i :item/status :open]
              [?i :item/current-price ?price]]}]

   [:questions
    '{:find [?cid ?id ?question]
      :where [[?u :user/id PARAM]
              [?c :item-comment/user-id ?u]
              [?c :item-comment/response ""]
              [?c :item-comment/id ?cid]
              [?c :item-comment/question ?question]
              [?c :item-comment/item-id ?i]
              [?i :item/id ?id]]}]])

(defn queries
  "The first n subscriptions, cycling through every template for each user in turn."
  [users items n]
  (when (> n (* (count templates) users))
    (throw (ex-info "Need at least one user per five subscriptions" {:users users :subscriptions n})))
  (->> (for [user (range users)
             [kind template] templates]
         [user kind template])
       (take n)
       (map-indexed (fn [id [user kind template]]
                      (let [param (if (= kind :item) (mod user items) user)]
                        {:id id
                         :kind kind
                         :param param
                         :query (walk/postwalk-replace {'PARAM param} template)})))
       vec))

(defn apply-delta
  "Apply a subscription delta of [row weight] pairs to a row -> multiplicity map."
  [rows delta]
  (reduce (fn [rows [row weight]]
            (let [n (+ (get rows row 0) weight)]
              (when (neg? n)
                (throw (ex-info "Negative result multiplicity" {:row row :weight n})))
              (if (zero? n)
                (dissoc rows row)
                (assoc rows row n))))
          rows delta))

;; ---------------------------------------------------------------------------
;; Reference model: {identity-attr {id doc}}

(def identity-attributes
  [:item/id :user-watch/id :item-bid/id :item-comment/id :item-image/id :item-max-bid/id :user/id])

(defn- tracked-entity
  "The [identity-attr id] a put targets, or nil when the model doesn't track that entity."
  [doc]
  (let [[attr id] (or (some (fn [attr] (when (contains? doc attr) [attr (get doc attr)]))
                            identity-attributes)
                      (when (vector? (:db/id doc)) (:db/id doc)))]
    (when (some #{attr} identity-attributes)
      [attr id])))

(defn apply-tx
  "Apply the workload's puts and entity retractions to the reference model."
  [model tx]
  (reduce (fn [model op]
            (cond
              (map? op)
              (if-let [path (tracked-entity op)]
                (update-in model path merge (dissoc op :db/id))
                model)

              (= :db/retractEntity (first op))
              (let [[_ [attr id]] op]
                (update model attr dissoc id))

              :else
              (throw (ex-info "Unsupported workload transaction" {:op op}))))
          model tx))

(defn result-rows
  "Expected result multiset of a live query, computed from the model."
  [model {:keys [kind param]}]
  (let [user [:user/id param]
        item-by-ref (fn [[_ id]] (get-in model [:item/id id]))
        rows (case kind
               :item
               (when-let [item (get-in model [:item/id param])]
                 [(mapv item [:item/id :item/name :item/description :item/current-price :item/num-bids :item/status])])

               :watchlist
               (for [watch (vals (:user-watch/id model))
                     :when (= user (:user-watch/user-id watch))
                     :let [item (item-by-ref (:user-watch/item-id watch))]
                     :when item]
                 (mapv item [:item/id :item/name :item/current-price :item/status]))

               :selling
               (for [item (vals (:item/id model))
                     :when (= user (:item/user-id item))
                     :when (= :open (:item/status item))]
                 (mapv item [:item/id :item/name :item/current-price :item/num-bids]))

               :bids
               (for [bid (vals (:item-bid/id model))
                     :when (= user (:item-bid/buyer-id bid))
                     :let [item (item-by-ref (:item-bid/item-id bid))]
                     :when (= :open (:item/status item))]
                 [(:item-bid/id bid) (:item/id item) (:item-bid/bid bid) (:item/current-price item)])

               :questions
               (for [comment (vals (:item-comment/id model))
                     :when (= user (:item-comment/user-id comment))
                     :when (= "" (:item-comment/response comment))
                     :let [item (item-by-ref (:item-comment/item-id comment))]
                     :when item]
                 [(:item-comment/id comment) (:item/id item) (:item-comment/question comment)]))]
    (frequencies rows)))

;; ---------------------------------------------------------------------------
;; Fixture

(defn transact! [conn tx]
  (let [result (tc/transact conn tx)]
    (when-not (:committed? result)
      (throw (ex-info "Workload transaction rejected" result)))
    result))

(defn copy-state
  "Copy procedure state so a run can mutate it without touching the fixture's."
  [state]
  (update-vals state (fn [v]
                       (cond
                         (instance? AtomicLong v) (AtomicLong. (.get ^AtomicLong v))
                         (instance? ConcurrentLinkedQueue v) (ConcurrentLinkedQueue. ^java.util.Collection v)
                         (instance? clojure.lang.Atom v) (atom @v)
                         :else v))))

(defn- open-all-items!
  "Give every item a round-robin seller and open it, mirroring that in the procedure queues."
  [conn state users items]
  (transact! conn (mapv (fn [id]
                          {:db/id [:item/id id]
                           :item/user-id [:user/id (mod id users)]
                           :item/status :open})
                        (range items)))
  (doseq [k [:items-open :items-waiting :items-closed]]
    (.clear ^ConcurrentLinkedQueue (get state k)))
  (doseq [id (range items)]
    (.add ^ConcurrentLinkedQueue (:items-open state) (proc/->ItemSample id (mod id users)))))

(defn- read-item-docs [conn]
  (->> (tc/q (tc/db conn)
             '{:find [?id ?seller ?name ?description ?price ?bids ?status]
               :where [[?i :item/id ?id]
                       [?i :item/user-id ?u]
                       [?u :user/id ?seller]
                       [?i :item/name ?name]
                       [?i :item/description ?description]
                       [?i :item/current-price ?price]
                       [?i :item/num-bids ?bids]
                       [?i :item/status ?status]]})
       (mapv (fn [[id seller name description price bids status]]
               {:item/id id
                :item/user-id [:user/id seller]
                :item/name name
                :item/description description
                :item/current-price price
                :item/num-bids bids
                :item/status status}))))

(defn- activity-docs
  "Each user watches up to three items, bids on the item with their id and asks one open question."
  [state users items]
  (let [now (proc/now-instant)
        watches (for [u (range users)
                      i (distinct [u (mod (inc u) items) 0])]
                  {:user-watch/id (proc/next-id (:watch-counter state))
                   :user-watch/user-id [:user/id u]
                   :user-watch/item-id [:item/id i]
                   :user-watch/created now})
        bids (for [u (range users)]
               {:item-bid/id (proc/next-id (:bid-counter state))
                :item-bid/buyer-id [:user/id u]
                :item-bid/user-id [:user/id u]
                :item-bid/item-id [:item/id u]
                :item-bid/bid 1.0
                :item-bid/max-bid 1.0
                :item-bid/created-at now
                :item-bid/updated now})
        comments (for [u (range users)]
                   {:item-comment/id (proc/next-id (:comment-counter state))
                    :item-comment/user-id [:user/id u]
                    :item-comment/buyer-id [:user/id (mod (inc u) users)]
                    :item-comment/item-id [:item/id u]
                    :item-comment/question "Is this available?"
                    :item-comment/response ""
                    :item-comment/created now
                    :item-comment/updated now})]
    (vec (concat watches bids comments))))

(defn load-fixture!
  "Reuse AuctionMark generators, with explicit sizes and populated live views."
  [conn {:keys [users items seed] :or {users 20 items 100 seed 42}}]
  (when-not (and (pos? users) (>= items users))
    (throw (ex-info "Need positive users and at least as many items" {:users users :items items})))
  (when (seq (tc/q (tc/db conn) '{:find [?e] :where [[?e :db/ident :item/id]]}))
    (throw (ex-info "Use a fresh database for the live benchmark" {})))
  (transact! conn schema/schema-tx)
  (let [state (proc/make-state)
        rng (Random. seed)
        categories (mapv #(hash-map :name (str "Category-" %) :weight 1) (range 10))]
    (proc/generate-regions! conn state rng 1)
    (proc/generate-categories! conn state categories)
    (proc/generate-users! conn state rng users)
    (tc/db conn (proc/generate-items! conn state rng items))
    (open-all-items! conn state users items)
    (let [item-docs (read-item-docs conn)
          user-docs (for [u (range users)] {:user/id u :user/balance 0.0})
          activity (activity-docs state users items)
          bid-counts (mapv (fn [u] {:db/id [:item/id u] :item/num-bids 1}) (range users))]
      (transact! conn (into activity bid-counts))
      {:state state
       :model (-> {}
                  (apply-tx (concat item-docs user-docs activity))
                  (apply-tx bid-counts))})))

(defn restore!
  "Transact the database back to the baseline model."
  [conn current baseline]
  (let [deletes (for [attr identity-attributes
                      id (keys (get current attr))
                      :when (not (contains? (get baseline attr) id))]
                  [:db/retractEntity [attr id]])
        puts (for [attr identity-attributes
                   [id doc] (get baseline attr)]
               (cond-> (assoc doc attr id)
                 (contains? (get current attr) id) (assoc :db/id [attr id])))]
    (doseq [batch (partition-all 128 (concat deletes puts))]
      (transact! conn (vec batch)))))

;; ---------------------------------------------------------------------------
;; Workload

(defn- toggle-watch!
  "Drop one of a random user's watches, or add a watch on a random item."
  [conn ^Random rng state model]
  (let [u (proc/pick-random-user rng state)
        watches (filter #(= [:user/id u] (:user-watch/user-id %)) (vals (:user-watch/id model)))]
    (if (and (seq watches) (.nextBoolean rng))
      (proc/transact! conn state [[:db/retractEntity [:user-watch/id (:user-watch/id (first watches))]]])
      (when-let [item (proc/pick-random-item rng state)]
        (proc/transact! conn state [{:user-watch/id (proc/next-id (:watch-counter state))
                                     :user-watch/user-id [:user/id u]
                                     :user-watch/item-id [:item/id (:item-id item)]
                                     :user-watch/created (proc/now-instant)}])))))

(defn- advance-auction!
  "Move the oldest open item to waiting-for-purchase, or the oldest waiting item to closed."
  [conn ^Random rng state]
  (let [waiting ^ConcurrentLinkedQueue (:items-waiting state)
        [from to status] (if (and (seq waiting) (.nextBoolean rng))
                           [:items-waiting :items-closed :closed]
                           [:items-open :items-waiting :waiting-for-purchase])
        from-queue ^ConcurrentLinkedQueue (get state from)]
    (when-let [item (.peek from-queue)]
      (proc/transact! conn state [{:db/id [:item/id (:item-id item)] :item/status status}])
      (.remove from-queue item)
      (.add ^ConcurrentLinkedQueue (get state to) item))))

(defn mutate!
  "The original write procedures plus watch membership and explicit auction transitions."
  [conn ^Random rng state model]
  (let [choice (.nextInt rng 100)]
    (cond
      (< choice 50) (proc/proc-new-bid conn rng state)
      (< choice 65) (proc/proc-new-item conn rng state)
      (< choice 80) (toggle-watch! conn rng state model)
      (< choice 90) (if (.nextBoolean rng)
                      (proc/proc-new-comment conn rng state)
                      (proc/proc-new-comment-response conn rng state))
      :else (advance-auction! conn rng state))))
