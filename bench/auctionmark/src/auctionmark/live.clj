(ns auctionmark.live
  "AuctionMark live queries, their fixture and the write workload."
  (:require [auctionmark.procedures :as proc]
            [auctionmark.schema :as schema]
            [clojure.walk :as walk]
            [xyz.triplox.api :as tc])
  (:import [java.util Random]
           [java.util.concurrent TimeUnit]
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
                         (instance? clojure.lang.Atom v) (atom @v)
                         :else v))))

(def ^:private open-auction-ms (.toMillis TimeUnit/DAYS 30))

(defn- open-all-items!
  "Give every item a round-robin seller and a future end date and open it, mirroring that in the procedure pools."
  [conn state users items]
  (let [end-ms (+ (proc/instant->millis (proc/now-instant)) open-auction-ms)]
    (transact! conn (mapv (fn [id]
                            {:db/id [:item/id id]
                             :item/user-id [:user/id (mod id users)]
                             :item/end-date (proc/millis->instant end-ms)
                             :item/status :open})
                          (range items)))
    (doseq [k [:items-open :items-waiting :items-closed]]
      (reset! (get state k) @(proc/make-pool)))
    (doseq [id (range items)]
      (proc/pool-add! (:items-open state) (proc/->ItemSample id (mod id users) end-ms)))))

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
    (let [activity (activity-docs state users items)
          bid-counts (mapv (fn [u] {:db/id [:item/id u] :item/num-bids 1}) (range users))
          result (transact! conn (into activity bid-counts))]
      {:state state
       :tx-key (select-keys result [:tx-id :system-time])})))

(defn- datoms
  "Every [e a v] of the schema's attributes in db."
  [db]
  (set (for [a (map :db/ident schema/schema-tx)
             [e v] (tc/q db {:find '[?e ?v] :where [['?e a '?v]]})]
         [e a v])))

(defn restore!
  "Transact the database back to its state at tx-key: undo attribute changes on the
  entities that existed then, and retract the entities created since."
  [conn tx-key]
  (let [before (datoms (tc/db conn tx-key))
        now (datoms (tc/db conn))
        existed (set (map first before))
        adds (remove now before)
        ;; Attributes are cardinality-one, so an add already replaces the current value.
        replaced (set (map (fn [[e a]] [e a]) adds))
        retracts (for [[e a v :as d] now
                       :when (and (existed e) (not (before d)) (not (replaced [e a])))]
                   [:db/retract e a v])
        created (for [e (distinct (map first now)) :when (not (existed e))]
                  [:db/retractEntity e])]
    (doseq [batch (partition-all 128 (concat (map #(into [:db/add] %) adds) retracts created))]
      (transact! conn (vec batch)))))

;; ---------------------------------------------------------------------------
;; Workload

(defn- toggle-watch!
  "Drop one of a random user's watches, or add a watch on a random item."
  [conn ^Random rng state]
  (let [u (proc/pick-random-user rng state)
        watches (sort (map first (tc/q (tc/db conn)
                                       {:find '[?id]
                                        :where [['?u :user/id u]
                                                '[?w :user-watch/user-id ?u]
                                                '[?w :user-watch/id ?id]]})))]
    (if (and (seq watches) (.nextBoolean rng))
      (proc/transact! conn state [[:db/retractEntity [:user-watch/id (first watches)]]])
      (when-let [item (proc/pick-random-item rng state)]
        (proc/transact! conn state [{:user-watch/id (proc/next-id (:watch-counter state))
                                     :user-watch/user-id [:user/id u]
                                     :user-watch/item-id [:item/id (:item-id item)]
                                     :user-watch/created (proc/now-instant)}])))))

(defn- advance-auction!
  "Move an open item to waiting-for-purchase, or a waiting item to closed."
  [conn ^Random rng state]
  (let [[from to status] (if (and (pos? (proc/pool-size (:items-waiting state))) (.nextBoolean rng))
                           [:items-waiting :items-closed :closed]
                           [:items-open :items-waiting :waiting-for-purchase])]
    (when-let [item (proc/pool-take! (get state from))]
      (proc/transact! conn state [{:db/id [:item/id (:item-id item)] :item/status status}])
      (proc/pool-add! (get state to) item))))

(defn mutate!
  "The original write procedures plus watch membership and explicit auction transitions."
  [conn ^Random rng state]
  (let [choice (.nextInt rng 100)]
    (cond
      (< choice 50) (proc/proc-new-bid conn rng state)
      (< choice 65) (proc/proc-new-item conn rng state)
      (< choice 80) (toggle-watch! conn rng state)
      (< choice 90) (if (.nextBoolean rng)
                      (proc/proc-new-comment conn rng state)
                      (proc/proc-new-comment-response conn rng state))
      :else (advance-auction! conn rng state))))
