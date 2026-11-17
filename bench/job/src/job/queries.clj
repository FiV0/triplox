(ns job.queries
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.walk :as walk])
  (:import [java.io PushbackReader]))

(defn read-queries []
  (with-open [r (PushbackReader. (io/reader (io/resource "job/job-queries.clj")))]
    (loop [queries (sorted-map)]
      (let [form (read {:eof nil} r)]
        (if-not form
          queries
          (let [[_ sym [_ query]] form]
            (when-not (= 'def (first form))
              (throw (ex-info "Unexpected JOB query form" {:form form})))
            (recur (assoc queries (subs (name sym) 2) query))))))))

(def queries (read-queries))
(def expected (edn/read-string (slurp (io/resource "job/expected.edn"))))

(defn query-order [id]
  (let [[_ number suffix] (re-matches #"(\d+)([a-z])" id)]
    [(Long/parseLong number) suffix]))

(defn selected [selection]
  (let [ids (if (= selection "all") (keys queries) (str/split selection #","))]
    (when (or (empty? ids) (some #(not (contains? queries %)) ids))
      (throw (ex-info "Unknown JOB query selection" {:selection selection})))
    (vec (sort-by query-order (distinct ids)))))

(defn like-regex [pattern]
  (str "(?s)\\A"
       (apply str (map #(case % \% ".*" \_ "."
                             (if (str/includes? "\\.^$|?*+()[]{}" (str %))
                               (str "\\" %) (str %))) pattern))
       "\\z"))

(defn- boolean-chain [op args]
  (if (= op 'and)
    (reduce #(list 'if %2 %1 false) true (reverse args))
    (reduce #(list 'if %2 true %1) false (reverse args))))

(declare expression)

(defn expression [form engine]
  (if-not (seq? form)
    form
    (let [[op & args] form
          args (mapv #(expression % engine) args)]
      (cond
        (#{'and 'or} op) (boolean-chain op args)
        (= op 'in) (boolean-chain 'or (map #(list '= (first args) %) (second args)))
        (#{'like 'not-like} op)
        (let [[value pattern] args
              predicate (if (= engine :triplox)
                          (list 'regexp_like value (like-regex pattern))
                          (list 'like value pattern))]
          (if (= op 'not-like) (list 'not predicate) predicate))
        (and (#{'< '<= '> '>=} op) (> (count args) 2))
        (boolean-chain 'and (map #(apply list op %) (partition 2 1 args)))
        :else (apply list op args)))))

(defn- portable-variables [query]
  (let [variables (distinct (filter #(and (symbol? %) (str/starts-with? (str %) "?"))
                                    (tree-seq coll? seq query)))]
    (walk/postwalk-replace (zipmap variables (map #(symbol (str "?jobv" %)) (range))) query)))

(defn translate [id engine]
  (let [query (cond-> (get queries id) (= :triplox engine) portable-variables)
        [find-part [_ & clauses]] (split-with #(not= :where %) query)]
    (when-not query (throw (ex-info "Unknown query" {:id id})))
    (into (vec (concat find-part [:where]))
          (map (fn [clause]
                 (if (and (vector? clause) (seq? (first clause)))
                   (let [[op _ entity attribute] (first clause)]
                     (if (= op 'missing?)
                       (list 'not [entity attribute '_])
                       [(expression (first clause) engine)]))
                   clause)))
          clauses)))
