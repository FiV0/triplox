(ns auctionmark.live
  "AuctionMark live queries and their small reference model."
  (:require [auctionmark.procedures :as proc]
            [auctionmark.schema :as schema]
            [clojure.walk :as walk]
            [xyz.triplox.api :as tc])
  (:import [java.util Random]
           [java.util.concurrent ConcurrentLinkedQueue]
           [java.util.concurrent.atomic AtomicLong]))

(def templates
  [[:item '{:find [?id ?name ?description ?price ?bids ?status]
            :where [[?i :item/id PARAM] [?i :item/id ?id] [?i :item/name ?name]
                    [?i :item/description ?description] [?i :item/current-price ?price]
                    [?i :item/num-bids ?bids] [?i :item/status ?status]]}]
   [:watchlist '{:find [?id ?name ?price ?status]
                 :where [[?u :user/id PARAM] [?w :user-watch/user-id ?u]
                         [?w :user-watch/item-id ?i] [?i :item/id ?id]
                         [?i :item/name ?name] [?i :item/current-price ?price]
                         [?i :item/status ?status]]}]
   [:selling '{:find [?id ?name ?price ?bids]
               :where [[?u :user/id PARAM] [?i :item/user-id ?u] [?i :item/status :open]
                       [?i :item/id ?id] [?i :item/name ?name]
                       [?i :item/current-price ?price] [?i :item/num-bids ?bids]]}]
   [:bids '{:find [?bid ?id ?amount ?price]
            :where [[?u :user/id PARAM] [?b :item-bid/buyer-id ?u] [?b :item-bid/id ?bid]
                    [?b :item-bid/bid ?amount] [?b :item-bid/item-id ?i]
                    [?i :item/id ?id] [?i :item/status :open] [?i :item/current-price ?price]]}]
   [:questions '{:find [?cid ?id ?question]
                 :where [[?u :user/id PARAM] [?c :item-comment/user-id ?u]
                         [?c :item-comment/response ""] [?c :item-comment/id ?cid]
                         [?c :item-comment/question ?question] [?c :item-comment/item-id ?i]
                         [?i :item/id ?id]]}]])

(defn queries [users items n]
  (when (> n (* 5 users))
    (throw (ex-info "Need at least one user per five subscriptions" {:users users :subscriptions n})))
  (mapv (fn [index [uid [kind template]]]
          (let [param (if (= kind :item) (mod uid items) uid)]
            {:id index :kind kind :param param
             :query (walk/postwalk-replace {'PARAM param} template)}))
        (range n) (take n (for [u (range users) template templates] [u template]))))

(defn apply-delta [rows delta]
  (reduce (fn [rows [row weight]]
            (let [n (+ (get rows row 0) weight)]
              (when (neg? n)
                (throw (ex-info "Negative result multiplicity" {:row row :weight n})))
              (if (zero? n) (dissoc rows row) (assoc rows row n))))
          rows delta))

(def identity-attributes [:item/id :user-watch/id :item-bid/id :item-comment/id :item-image/id :item-max-bid/id :user/id])

(defn apply-tx
  "Apply the workload's puts and entity retractions to the reference model."
  [model tx]
  (reduce
   (fn [model op]
     (if (map? op)
       (if-let [[attr id] (or (some #(when (contains? op %) [% (get op %)]) identity-attributes)
                             (when (vector? (:db/id op)) (:db/id op)))]
         (if (some #{attr} identity-attributes)
           (update-in model [attr id] merge (dissoc op :db/id))
           model)
         model)
       (let [[verb [attr id]] op]
         (if (= verb :db/retractEntity)
           (update model attr dissoc id)
           (throw (ex-info "Unsupported workload transaction" {:op op}))))))
   model tx))

(defn result-rows [model {:keys [kind param]}]
  (let [items (:item/id model)
        item-columns (fn [item attrs] (mapv item attrs))
        rows
        (case kind
          :item (when-let [item (get items param)]
                  [(item-columns item [:item/id :item/name :item/description :item/current-price :item/num-bids :item/status])])
          :watchlist (for [watch (vals (:user-watch/id model))
                           :when (= [:user/id param] (:user-watch/user-id watch))
                           :let [item (get items (second (:user-watch/item-id watch)))] :when item]
                       (item-columns item [:item/id :item/name :item/current-price :item/status]))
          :selling (for [item (vals items)
                         :when (and (= [:user/id param] (:item/user-id item)) (= :open (:item/status item)))]
                     (item-columns item [:item/id :item/name :item/current-price :item/num-bids]))
          :bids (for [bid (vals (:item-bid/id model))
                      :when (= [:user/id param] (:item-bid/buyer-id bid))
                      :let [item (get items (second (:item-bid/item-id bid)))]
                      :when (= :open (:item/status item))]
                  [(:item-bid/id bid) (:item/id item) (:item-bid/bid bid) (:item/current-price item)])
          :questions (for [comment (vals (:item-comment/id model))
                           :when (and (= [:user/id param] (:item-comment/user-id comment))
                                      (= "" (:item-comment/response comment)))
                           :let [item (get items (second (:item-comment/item-id comment)))] :when item]
                       [(:item-comment/id comment) (:item/id item) (:item-comment/question comment)]))]
    (frequencies rows)))

(defn transact! [conn tx]
  (let [result (tc/transact conn tx)]
    (when-not (:committed? result)
      (throw (ex-info "Workload transaction rejected" result)))
    result))

(defn copy-state [state]
  (into {} (for [[k v] state]
             [k (cond
                  (instance? AtomicLong v) (AtomicLong. (.get ^AtomicLong v))
                  (instance? ConcurrentLinkedQueue v) (ConcurrentLinkedQueue. ^java.util.Collection v)
                  (instance? clojure.lang.Atom v) (atom @v)
                  :else v)])))

(defn load-fixture!
  "Reuse AuctionMark generators, with explicit sizes and populated live views."
  [conn {:keys [users items seed] :or {users 20 items 100 seed 42}}]
  (when-not (and (pos? users) (>= items users))
    (throw (ex-info "Need positive users and at least as many items" {:users users :items items})))
  (when (seq (tc/q (tc/db conn) '{:find [?e] :where [[?e :db/ident :item/id]]}))
    (throw (ex-info "Use a fresh database for the live benchmark" {})))
  (transact! conn schema/schema-tx)
  (let [state (proc/make-state) rng (Random. seed)]
    (proc/generate-regions! conn state rng 1)
    (proc/generate-categories! conn state (mapv #(hash-map :name (str "Category-" %) :weight 1) (range 10)))
    (proc/generate-users! conn state rng users)
    (tc/db conn (proc/generate-items! conn state rng items))
    (transact! conn (mapv (fn [id] {:db/id [:item/id id] :item/user-id [:user/id (mod id users)] :item/status :open}) (range items)))
    (doseq [k [:items-open :items-waiting :items-closed]] (.clear ^ConcurrentLinkedQueue (get state k)))
    (doseq [id (range items)]
      (.add ^ConcurrentLinkedQueue (:items-open state) (proc/->ItemSample id (mod id users))))
    (let [item-docs (mapv (fn [[id seller name description price bids status]]
                            {:item/id id :item/user-id [:user/id seller] :item/name name
                             :item/description description :item/current-price price
                             :item/num-bids bids :item/status status})
                          (tc/q (tc/db conn)
                                '{:find [?id ?seller ?name ?description ?price ?bids ?status]
                                  :where [[?i :item/id ?id] [?i :item/user-id ?u] [?u :user/id ?seller]
                                          [?i :item/name ?name] [?i :item/description ?description]
                                          [?i :item/current-price ?price] [?i :item/num-bids ?bids]
                                          [?i :item/status ?status]]}))
          now (proc/now-instant)
          docs (vec (concat
                     (for [u (range users) i (distinct [u (mod (inc u) items) 0])]
                       {:user-watch/id (proc/next-id (:watch-counter state)) :user-watch/user-id [:user/id u]
                        :user-watch/item-id [:item/id i] :user-watch/created now})
                     (for [u (range users)]
                       {:item-bid/id (proc/next-id (:bid-counter state)) :item-bid/buyer-id [:user/id u]
                        :item-bid/user-id [:user/id u] :item-bid/item-id [:item/id u]
                        :item-bid/bid 1.0 :item-bid/max-bid 1.0 :item-bid/created-at now :item-bid/updated now})
                     (for [u (range users)]
                       {:item-comment/id (proc/next-id (:comment-counter state)) :item-comment/user-id [:user/id u]
                        :item-comment/buyer-id [:user/id (mod (inc u) users)] :item-comment/item-id [:item/id u]
                        :item-comment/question "Is this available?" :item-comment/response ""
                        :item-comment/created now :item-comment/updated now})))
          counts (mapv (fn [u] {:db/id [:item/id u] :item/num-bids 1}) (range users))]
      (transact! conn (into docs counts))
      {:state state
       :model (apply-tx (apply-tx {} (concat item-docs (for [u (range users)] {:user/id u :user/balance 0.0}) docs)) counts)})))

(defn restore! [conn current baseline]
  (let [deletes (for [attr identity-attributes id (keys (get current attr))
                      :when (not (contains? (get baseline attr) id))]
                  [:db/retractEntity [attr id]])
        puts (for [attr identity-attributes [id doc] (get baseline attr)]
               (cond-> (assoc doc attr id)
                 (contains? (get current attr) id) (assoc :db/id [attr id])))]
    (doseq [batch (partition-all 128 (concat deletes puts))] (transact! conn (vec batch)))))

(defn mutate!
  "The original write procedures plus watch membership and explicit auction transitions."
  [conn ^Random rng state model]
  (let [choice (.nextInt rng 100)]
    (cond
      (< choice 50) (proc/proc-new-bid conn rng state)
      (< choice 65) (proc/proc-new-item conn rng state)
      (< choice 80)
      (let [u (proc/pick-random-user rng state)
            watches (filter #(= [:user/id u] (:user-watch/user-id %)) (vals (:user-watch/id model)))]
        (if (and (seq watches) (.nextBoolean rng))
          (proc/*transact* conn [[:db/retractEntity [:user-watch/id (:user-watch/id (first watches))]]])
          (when-let [item (proc/pick-random-item rng state)]
            (proc/*transact* conn [{:user-watch/id (proc/next-id (:watch-counter state))
                                   :user-watch/user-id [:user/id u] :user-watch/item-id [:item/id (:item-id item)]
                                   :user-watch/created (proc/now-instant)}]))))
      (< choice 90) (if (.nextBoolean rng) (proc/proc-new-comment conn rng state)
                       (proc/proc-new-comment-response conn rng state))
      :else
      (let [waiting ^ConcurrentLinkedQueue (:items-waiting state)
            from (if (and (seq waiting) (.nextBoolean rng)) :items-waiting :items-open)
            to (if (= from :items-open) :items-waiting :items-closed)
            status (if (= to :items-waiting) :waiting-for-purchase :closed)]
        (when-let [item (.peek ^ConcurrentLinkedQueue (get state from))]
          (proc/*transact* conn [{:db/id [:item/id (:item-id item)] :item/status status}])
          (.remove ^ConcurrentLinkedQueue (get state from) item)
          (.add ^ConcurrentLinkedQueue (get state to) item))))))
