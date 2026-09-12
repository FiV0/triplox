(ns job.queries
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.walk :as walk])
  (:import [java.io PushbackReader]
           [java.util.regex Pattern]))

(defn read-queries []
  (with-open [r (PushbackReader. (io/reader (io/resource "job/upstream-queries.clj")))]
    (loop [queries (sorted-map)]
      (let [form (read {:eof nil} r)]
        (if-not form
          queries
          (let [[_ sym [_ query]] form]
            (when-not (= 'def (first form))
              (throw (ex-info "Unexpected upstream query form" {:form form})))
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
       (apply str (map #(case % \% ".*" \_ "." (Pattern/quote (str %))) pattern))
       "\\z"))

(def ^:private compiled-like (memoize #(re-pattern (like-regex %))))

(defn like? [value pattern]
  (boolean (re-matches (compiled-like pattern) value)))

(defn- evaluate-expression [form env]
  (cond
    (symbol? form) (get env form form)
    (not (seq? form)) form
    :else
    (let [[op & args] form
          values #(mapv (fn [arg] (evaluate-expression arg env)) args)]
      (case op
        and (every? #(evaluate-expression % env) args)
        or (boolean (some #(evaluate-expression % env) args))
        like (apply like? (values))
        not-like (not (apply like? (values)))
        in (let [[value choices] (values)] (boolean (some #{value} choices)))
        not= (apply not= (values))
        = (apply = (values))
        (< <= > >=)
        (every? (fn [[a b]]
                  (let [comparison (compare a b)]
                    (case op < (neg? comparison) <= (not (pos? comparison))
                          > (pos? comparison) >= (not (neg? comparison)))))
                (partition 2 1 (values)))
        (throw (ex-info "Unsupported JOB expression" {:form form}))))))

(def ^:private parsed-expression (memoize edn/read-string))

(defn predicate [encoded variables & values]
  (boolean (evaluate-expression (parsed-expression encoded)
                               (zipmap (parsed-expression variables) values))))

(defn- datomic-predicate [form]
  (let [variables (vec (distinct (filter #(and (symbol? %) (str/starts-with? (str %) "?"))
                                       (tree-seq coll? seq form))))]
    [(apply list 'job.queries/predicate (pr-str form) (pr-str variables) variables)]))

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
                          (if (= engine :datomic)
                            (list 'job.queries/like? value pattern)
                            (list 'like value pattern)))]
          (if (= op 'not-like) (list 'not predicate) predicate))
        (and (#{'< '<= '> '>=} op) (> (count args) 2))
        (boolean-chain 'and (map #(apply list op %) (partition 2 1 args)))
        :else (apply list op args)))))

(defn- datomic-variables [query]
  (let [variables (distinct (filter #(and (symbol? %) (str/starts-with? (str %) "?"))
                                    (tree-seq coll? seq query)))]
    (walk/postwalk-replace (zipmap variables (map #(symbol (str "?jobv" %)) (range))) query)))

(defn translate [id engine]
  (let [query (cond-> (get queries id) (= engine :datomic) datomic-variables)
        [find-part [_ & clauses]] (split-with #(not= :where %) query)]
    (when-not query (throw (ex-info "Unknown query" {:id id})))
    (into (vec (concat find-part [:where]))
          (map (fn [clause]
                 (if (and (vector? clause) (seq? (first clause)))
                   (let [[op _ entity attribute] (first clause)]
                     (if (= op 'missing?)
                       (list 'not [entity attribute '_])
                       (if (= engine :datomic)
                         (datomic-predicate (first clause))
                         [(expression (first clause) engine)])))
                   clause)))
          clauses)))
