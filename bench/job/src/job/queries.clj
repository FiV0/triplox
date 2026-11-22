(ns job.queries
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [job.job-queries :as job-queries]))

;; Datalevin has a more expressive predicate language. For example
;;
;; [(and (< ?a ?b) (< ?c 10))]
;;
;; this namespace translates the more expressive language into queries
;; Triplox supports.

(def queries job-queries/queries)
(def expected (edn/read-string (slurp (io/resource "job/expected.edn"))))

(defn query-order [id]
  (let [[_ number suffix] (re-matches #"(\d+)([a-z])" id)]
    [(Long/parseLong number) suffix]))

(defn selected [selection]
  (let [ids (if (= selection "all") (keys queries) (str/split selection #","))]
    (when (or (empty? ids) (some #(not (contains? queries %)) ids))
      (throw (ex-info "Unknown JOB query selection" {:selection selection})))
    (vec (sort-by query-order (distinct ids)))))

(defn like-regex
  "Translates a SQL LIKE pattern into an anchored, dot-all regex string:
  `%` becomes `.*`, `_` becomes `.`, and regex metacharacters are escaped.
  `\\A`/`\\z` force a whole-value match; LIKE's ESCAPE clause is not supported."
  [pattern]
  (str "(?s)\\A"
       (apply str (map #(case % \% ".*" \_ "."
                             (if (str/includes? "\\.^$|?*+()[]{}" (str %))
                               (str "\\" %) (str %))) pattern))
       "\\z"))

(defn- boolean-chain [op args]
  (if (= op 'and)
    (reduce #(list 'if %2 %1 false) true (reverse args))
    (reduce #(list 'if %2 true %1) false (reverse args))))

(defn expression [form]
  (if-not (seq? form)
    form
    (let [[op & args] form
          args (mapv expression args)]
      (cond
        (#{'and 'or} op) (boolean-chain op args)
        (= op 'in) (boolean-chain 'or (map #(list '= (first args) %) (second args)))
        (#{'like 'not-like} op)
        (let [[value pattern] args
              predicate (list 'regexp_like value (like-regex pattern))]
          (if (= op 'not-like) (list 'not predicate) predicate))
        (and (#{'< '<= '> '>=} op) (> (count args) 2))
        (boolean-chain 'and (map #(apply list op %) (partition 2 1 args)))
        :else (apply list op args)))))

(defn translate-query [query]
  (let [[find-part [_ & clauses]] (split-with #(not= :where %) query)]
    (into (vec (concat find-part [:where]))
          (map (fn [clause]
                 (if (and (vector? clause) (seq? (first clause)))
                   (let [[op _ entity attribute] (first clause)]
                     (if (= op 'missing?)
                       (list 'not [entity attribute '_])
                       [(expression (first clause))]))
                   clause)))
          clauses)))

(defn translate [id]
  (let [query (get queries id)]
    (when-not query (throw (ex-info "Unknown query" {:id id})))
    (translate-query query)))
