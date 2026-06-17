(ns auctionmark.procedures-test
  "Ported from xtdb.bench.auctionmark-test."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [xyz.triplox.api :as tc]
            [auctionmark.schema :as schema]
            [auctionmark.procedures :as proc])
  (:import [java.util Random]
           [java.time Instant]
           [java.util.concurrent ConcurrentLinkedQueue]))

;; ---------------------------------------------------------------------------
;; Fixture & helpers
;; ---------------------------------------------------------------------------

(def ^:dynamic *conn* nil)
(def ^:dynamic *state* nil)
(def ^:dynamic *rng* nil)

(defn- connect []
  (let [host (System/getProperty "triplox.host" "localhost")
        port (Integer/parseInt (System/getProperty "triplox.port" "5490"))]
    (tc/connect host port)))

(defn with-node
  "Fresh per-connection dev node per test, with schema installed and a fresh
  benchmark state + deterministic RNG."
  [f]
  (with-open [conn (connect)]
    (tc/transact conn schema/schema-tx)
    (binding [*conn* conn
              *state* (proc/make-state)
              *rng* (Random. 112)]
      (f))))

(use-fixtures :each with-node)

(defn- db
  ([] (tc/db *conn*))
  ([tx-key] (tc/db *conn* tx-key)))

(defn- count-of
  "Number of entities asserting `attr`, optionally as-of a generator's tx-key."
  ([attr] (count-of attr nil))
  ([attr tx-key]
   (ffirst (tc/q (db tx-key) {:find [(list 'count '?e)]
                              :where [['?e attr '?id]]}))))

(defn- single-category
  "Parsed-category data (as `generate-categories!` expects) with `n` entries."
  [n]
  (mapv (fn [i] {:name (str "Category-" i) :weight 1}) (range n)))

;; ---------------------------------------------------------------------------
;; Generators
;; ---------------------------------------------------------------------------

(deftest generate-user-test
  (proc/generate-regions! *conn* *state* *rng* 1)
  (let [tx (proc/generate-users! *conn* *state* *rng* 1)]
    (is (= 1 (count-of :user/id tx)))
    (is (= 0 (proc/pick-random-user *rng* *state*)))))

(deftest generate-categories-test
  (let [tx (proc/generate-categories! *conn* *state* (single-category 1))]
    (is (= 1 (count-of :category/id tx)))
    (is (= [0] @(:category-ids *state*)))))

(deftest generate-region-test
  (let [tx (proc/generate-regions! *conn* *state* *rng* 1)]
    (is (= 1 (count-of :region/id tx)))
    (is (= [0] @(:regions *state*)))))

(deftest generate-global-attribute-group-test
  (proc/generate-categories! *conn* *state* (single-category 1))
  (let [tx (proc/generate-gags! *conn* *state* *rng* 1)]
    (is (= 1 (count-of :gag/id tx)))
    (is (= [0] @(:gag-ids *state*)))))

(deftest generate-global-attribute-value-test
  (proc/generate-categories! *conn* *state* (single-category 1))
  (proc/generate-gags! *conn* *state* *rng* 1)
  (let [tx (proc/generate-gavs! *conn* *state* *rng* 1)]
    (is (= 1 (count-of :gav/id tx)))
    (is (= [0] @(:gav-ids *state*)))))

(deftest generate-user-attributes-test
  (proc/generate-regions! *conn* *state* *rng* 1)
  (proc/generate-users! *conn* *state* *rng* 1)
  (let [tx (proc/generate-user-attributes! *conn* *state* *rng* 1)]
    (is (= 1 (count-of :user-attribute/id tx)))))

(deftest generate-item-test
  (with-redefs [proc/sample-status (constantly :open)]
    (proc/generate-regions! *conn* *state* *rng* 1)
    (proc/generate-users! *conn* *state* *rng* 1)
    (proc/generate-categories! *conn* *state* (single-category 1))
    (let [tx (proc/generate-items! *conn* *state* *rng* 1)]
      (is (= 1 (count-of :item/id tx)))
      (let [item (proc/pick-random-open *rng* *state*)]
        (is (= 0 (:item-id item)))
        (is (= 0 (:seller-id item))))

      (testing "item update"
        (let [desc-q '{:find [?d] :where [[?e :item/description ?d]]}
              old-description (ffirst (tc/q (db tx) desc-q))]
          (proc/proc-update-item *conn* *rng* *state*)
          (let [new-description (ffirst (tc/q (db) desc-q))]
            (is (not= old-description new-description))))))))

;; ---------------------------------------------------------------------------
;; Procedures
;; ---------------------------------------------------------------------------

(deftest proc-get-item-test
  (with-redefs [proc/sample-status (constantly :open)]
    (proc/generate-regions! *conn* *state* *rng* 1)
    (proc/generate-users! *conn* *state* *rng* 1)
    (proc/generate-categories! *conn* *state* (single-category 1))
    (let [tx (proc/generate-items! *conn* *state* *rng* 1)
          _db (db tx)]
      (is (some? (first (proc/proc-get-item *conn* *rng* *state*)))))))

(deftest proc-new-user-test
  (proc/generate-regions! *conn* *state* *rng* 1)
  ;; proc-new-user transacts synchronously, so the new user is already indexed
  (proc/proc-new-user *conn* *rng* *state*)
  (is (= 1 (count-of :user/id)))
  (is (= 0 (proc/pick-random-user *rng* *state*))))

(deftest proc-new-bid-test
  (with-redefs [proc/sample-status (constantly :open)]
    (proc/generate-regions! *conn* *state* *rng* 1)
    (proc/generate-users! *conn* *state* *rng* 2)
    (proc/generate-categories! *conn* *state* (single-category 1))
    (let [tx (proc/generate-items! *conn* *state* *rng* 1)
          _db (db tx)]
      (proc/proc-new-bid *conn* *rng* *state*)

      (testing "a bid exists, referencing item 0"
        (is (= 1 (count-of :item-bid/id)))
        (is (= [[0]] (tc/q (db) '{:find [?item-id]
                                  :where [[?b :item-bid/id 0]
                                          [?b :item-bid/item-id ?i]
                                          [?i :item/id ?item-id]]}))))

      (testing "a max-bid record exists for item 0"
        (is (= 1 (count-of :item-max-bid/id)))
        (is (= [[0]] (tc/q (db) '{:find [?item-id]
                                  :where [[?m :item-max-bid/id 0]
                                          [?m :item-max-bid/item-id ?i]
                                          [?i :item/id ?item-id]]}))))

      ;; NOTE: the XTDB suite also asserts "new bid but does not exceed max" and
      ;; "new exceeds max bid". This Triplox `proc-new-bid` always inserts a fresh
      ;; max-bid record (no read-compare-against-current-max, no `random-price`
      ;; hook), so those scenarios have no equivalent here and are not ported.
      )))

(deftest proc-new-item-test
  (proc/generate-regions! *conn* *state* *rng* 1)
  (proc/generate-users! *conn* *state* *rng* 1)
  (proc/generate-categories! *conn* *state* (single-category 10))
  ;; proc-new-item transacts synchronously; its lookup refs resolve because txs
  ;; are indexed in submission order (the generators were submitted first)
  (proc/proc-new-item *conn* *rng* *state*)

  (testing "new item is owned by user 0"
    (is (= [[0 0]] (tc/q (db) '{:find [?item-id ?seller]
                                :where [[?e :item/id ?item-id]
                                        [?e :item/user-id ?u]
                                        [?u :user/id ?seller]]}))))

  (testing "seller paid the ~1.0 listing fee"
    (let [balance (ffirst (tc/q (db) '{:find [?b]
                                       :where [[?e :user/id 0]
                                               [?e :user/balance ?b]]}))]
      (is (< (Math/abs (- (double balance) -1.0)) 0.0001)))))

(deftest proc-new-comment-and-response-test
  (with-redefs [proc/sample-status (constantly :open)]
    (let [resp-q '{:find [?resp]
                   :where [[?c :item-comment/id 0]
                           [?c :item-comment/response ?resp]]}]
      (proc/generate-regions! *conn* *state* *rng* 1)
      (proc/generate-users! *conn* *state* *rng* 1)
      (proc/generate-categories! *conn* *state* (single-category 1))
      (proc/generate-items! *conn* *state* *rng* 1)

      ;; proc-new-comment transacts synchronously (item resolves by tx ordering)
      (proc/proc-new-comment *conn* *rng* *state*)
      (is (= [[""]] (tc/q (db) resp-q)) "comment created with an empty response")

      (proc/proc-new-comment-response *conn* *rng* *state*)
      (is (= [["Thanks for asking!"]] (tc/q (db) resp-q)) "seller responded"))))

(deftest proc-new-purchase-test
  (with-redefs [proc/sample-status (constantly :waiting-for-purchase)]
    (let [status-q '{:find [?id ?status]
                     :where [[?e :item/id ?id]
                             [?e :item/status ?status]]}]
      (proc/generate-regions! *conn* *state* *rng* 1)
      (proc/generate-users! *conn* *state* *rng* 1)
      (proc/generate-categories! *conn* *state* (single-category 1))
      (let [tx (proc/generate-items! *conn* *state* *rng* 1)]
        (is (= [[0 :waiting-for-purchase]] (tc/q (db tx) status-q)))
        (proc/proc-new-purchase *conn* *rng* *state*)
        (is (= [[0 :closed]] (tc/q (db) status-q)))))))

(deftest proc-new-feedback-test
  (with-redefs [proc/sample-status (constantly :closed)]
    (proc/generate-regions! *conn* *state* *rng* 1)
    (proc/generate-users! *conn* *state* *rng* 1)
    (proc/generate-categories! *conn* *state* (single-category 1))
    (proc/generate-items! *conn* *state* *rng* 1)

    ;; proc-new-feedback transacts synchronously (item resolves by tx ordering)
    (proc/proc-new-feedback *conn* *rng* *state*)
    (is (= [[0]] (tc/q (db) '{:find [?id] :where [[?f :item-feedback/id ?id]]})))))

(deftest proc-check-winning-bid-test
  (with-redefs [proc/sample-status (constantly :open)]
    (let [open-q '{:find [?id] :where [[?e :item/status :open] [?e :item/id ?id]]}
          wait-q '{:find [?id] :where [[?e :item/status :waiting-for-purchase] [?e :item/id ?id]]}]
      (proc/generate-regions! *conn* *state* *rng* 1)
      (proc/generate-users! *conn* *state* *rng* 2)
      (proc/generate-categories! *conn* *state* (single-category 1))
      (let [tx (proc/generate-items! *conn* *state* *rng* 2)]
        (is (= 2 (count (tc/q (db tx) open-q))) "both items start open")

        ;; NOTE: the XTDB `proc-check-winning-bids` both closes won items and moves
        ;; others to waiting. This Triplox `proc-check-winning-bid` only transitions
        ;; *expired* open items to :waiting-for-purchase (closing happens later in
        ;; proc-new-purchase / proc-post-auction). We force expiry by running it
        ;; from far in the future so all open auctions have ended.
        (with-redefs [proc/now-instant (constantly (Instant/parse "2100-01-01T00:00:00Z"))]
          (proc/proc-check-winning-bid *conn* *rng* *state*))

        (is (= 0 (count (tc/q (db) open-q))) "no open items remain")
        (is (= 2 (count (tc/q (db) wait-q))) "both items now waiting-for-purchase")
        (is (= 2 (.size ^ConcurrentLinkedQueue (:items-waiting *state*)))
            "and tracked in the waiting queue")))))

(deftest proc-get-comment-test
  (with-redefs [proc/sample-status (constantly :open)
                ;; only an open item exists; force the (otherwise random) item
                ;; pick so the read is deterministic.
                proc/pick-random-item (fn [rng state] (proc/pick-random-open rng state))]
    (proc/generate-regions! *conn* *state* *rng* 1)
    (proc/generate-users! *conn* *state* *rng* 1)
    (proc/generate-categories! *conn* *state* (single-category 1))
    (proc/generate-items! *conn* *state* *rng* 1)
    ;; proc-new-comment transacts synchronously, so the comment is indexed before
    ;; proc-get-comment reads it
    (proc/proc-new-comment *conn* *rng* *state*)

    (is (= [0] (map first (proc/proc-get-comment *conn* *rng* *state*)))
        "the (single) comment, id 0, is read back")))

(deftest proc-get-user-info-test
  ;; The XTDB `get-user-info` takes seller/buyer/feedback options; this Triplox
  ;; `proc-get-user-info` is the simpler "read the user's profile" variant.
  (proc/generate-regions! *conn* *state* *rng* 1)
  (let [tx (proc/generate-users! *conn* *state* *rng* 1)
        _db (db tx)
        res (proc/proc-get-user-info *conn* *rng* *state*)]
    (is (= 1 (count res)))
    (is (= [0 0.0] (vec (take 2 (first res)))) "rating 0, balance 0.0")))
