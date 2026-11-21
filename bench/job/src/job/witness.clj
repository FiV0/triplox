(ns job.witness
  "Builds a synthetic dataset with at least one answer for every JOB query.
  Each query gets one witness: an entity per entity variable, with only the
  attributes its patterns ask for and values that satisfy its predicates."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [job.data :as data]
            [job.queries :as queries]))

(def resource-file (io/file "resources/job/witness.edn"))

(defn- variable? [x]
  (and (symbol? x) (str/starts-with? (name x) "?")))

(defn- variables [form]
  (into #{} (filter variable?) (tree-seq coll? seq form)))

(defn- where-clauses [query]
  (rest (drop-while #(not= :where %) query)))

(defn- pattern? [clause]
  (and (vector? clause) (variable? (first clause)) (keyword? (second clause))))

(defn- like? [value pattern]
  (and (string? value) (boolean (re-matches (re-pattern (queries/like-regex pattern)) value))))

(defn- compare-chain [test args]
  (and (apply = (map type args))
       (every? (fn [[a b]] (test (compare a b))) (partition 2 1 args))))

(defn- evaluate [form env]
  (cond
    (variable? form) (get env form)
    (seq? form)
    (let [[op & args] form
          value #(evaluate % env)]
      (case op
        and (every? value args)
        or (boolean (some value args))
        like (like? (value (first args)) (second args))
        not-like (not (like? (value (first args)) (second args)))
        in (contains? (set (second args)) (value (first args)))
        = (apply = (map value args))
        not= (apply not= (map value args))
        < (compare-chain neg? (map value args))
        <= (compare-chain (complement pos?) (map value args))
        > (compare-chain pos? (map value args))
        >= (compare-chain (complement neg?) (map value args))))
    :else form))

;; Strings that satisfy a single like pattern, or the constants a predicate mentions.
(defn- fragments [form]
  (cond
    (and (seq? form) (#{'like 'not-like} (first form)))
    [(-> (nth form 2) (str/replace "%" "") (str/replace "_" "x"))]
    (seq? form) (mapcat fragments (rest form))
    (vector? form) form
    (or (string? form) (number? form)) [form]
    :else []))

(defn- candidates [type default predicates]
  (let [constants (distinct (mapcat fragments predicates))]
    (if (= type :db.type/long)
      (distinct (concat (mapcat (juxt identity inc dec) (filter integer? constants)) [default]))
      (let [strings (filter string? constants)
            joined (for [a strings b strings :when (not= a b)] (str a b))
            tripled (for [a strings b strings c strings :when (distinct? a b c)] (str a b c))
            ;; A proper prefix sorts before a string and an extra digit after it.
            prefixes (keep #(when (< 1 (count %)) (subs % 0 (dec (count %)))) strings)]
        (distinct (concat strings joined tripled (map #(str % "1") strings) prefixes [default]))))))

(defn- solve-group
  "Finds values for `group` that satisfy `predicates`, trying each variable's candidates."
  [env group predicates options]
  (let [assignments (reduce (fn [envs v] (for [e envs c (options v)] (assoc e v c)))
                            [env] group)]
    (or (first (filter (fn [e] (every? #(evaluate % e) predicates)) assignments))
        (throw (ex-info "No witness value satisfies the predicates"
                        {:variables group :predicates predicates})))))

(defn- groups
  "Splits value variables into groups that share a predicate."
  [vars predicates]
  (let [linked (map variables predicates)]
    (loop [remaining (set vars) result []]
      (if-let [v (first remaining)]
        (let [group (loop [group #{v}]
                      (let [grown (into group (comp (filter #(some group %)) cat) linked)]
                        (if (= grown group) group (recur grown))))]
          (recur (reduce disj remaining group) (conj result (filterv remaining group))))
        result))))

(defn- witness
  "Returns `[entity-variable attributes]` pairs for `query`, in order of first use.
  Reference values are entity variables."
  [query]
  (let [clauses (where-clauses query)
        ;; Each `_` placeholder becomes its own variable, so the witness gives it a value.
        patterns (map-indexed (fn [i [e a v]] [e a (if (= '_ v) (symbol (str "?_" i)) v)])
                              (filter pattern? clauses))
        predicates (map first (remove pattern? clauses))
        missing (filter #(= 'missing? (first %)) predicates)
        predicates (remove #(= 'missing? (first %)) predicates)
        entity-vars (distinct (concat (map first patterns)
                                      (keep (fn [[_ a v]] (when (data/ref-attribute? a) v)) patterns)))
        value-attribute (into {} (keep (fn [[_ a v]]
                                         (when (and (variable? v) (not (data/ref-attribute? a))) [v a])))
                              patterns)
        constants (into {} (keep (fn [[e a v]] (when-not (variable? v) [[e a] v]))) patterns)
        ;; A variable on an attribute with a constant takes that constant.
        env (into {} (keep (fn [[e a v]] (when-let [c (and (variable? v) (constants [e a]))] [v c])))
                  patterns)
        free (remove env (keys value-attribute))
        options (fn [v]
                  (let [a (value-attribute v)
                        type (get-in data/schema [a :db/valueType])
                        default (if (= type :db.type/long) 1 (str (name a) " " (subs (name v) 1)))]
                    (candidates type default (filter #(contains? (variables %) v) predicates))))
        env (reduce (fn [env group]
                      (solve-group env group (filter #(every? (set group) (variables %)) predicates)
                                   options))
                    env (groups free predicates))]
    (when-let [unsatisfied (seq (remove #(evaluate % env) predicates))]
      (throw (ex-info "Witness does not satisfy the query" {:predicates unsatisfied})))
    (doseq [[_ _ e a] missing]
      (when (some (fn [[pe pa]] (and (= e pe) (= a pa))) patterns)
        (throw (ex-info "Query asserts an attribute it requires to be missing" {:entity e :attribute a}))))
    (let [attributes (reduce (fn [entities [e a v]]
                               (assoc-in entities [e a] (if (variable? v) (get env v v) v)))
                             (zipmap entity-vars (repeat {}))
                             patterns)]
      (mapv (juxt identity attributes) entity-vars))))

(defn- leaf? [attributes]
  (not-any? data/ref-attribute? (keys attributes)))

(defn- add-witness
  "Gives each entity of a witness a `:job/id`. Leaf entities with the same
  attributes share one id across all queries."
  [state entities]
  (let [{:keys [ids] :as state}
        (reduce (fn [{:keys [next-id leaves] :as state} [e attributes]]
                  (if-let [shared (and (leaf? attributes) (leaves attributes))]
                    (assoc-in state [:ids e] shared)
                    (cond-> (-> state
                                (assoc-in [:ids e] next-id)
                                (update :next-id inc)
                                (update :new conj [next-id attributes]))
                      (leaf? attributes) (assoc-in [:leaves attributes] next-id))))
                (assoc state :ids {} :new []) entities)]
    (-> state
        (update :rows into
                (map (fn [[id attributes]]
                       (into {:job/id id}
                             (map (fn [[a v]] [a (if (data/ref-attribute? a) (ids v) v)]))
                             (sort-by key attributes))))
                (:new state))
        (dissoc :ids :new))))

(defn generate
  "Returns entity maps keyed by `:job/id`, with reference values as `:job/id`s."
  []
  (->> (queries/selected "all")
       (reduce (fn [state id]
                 (try
                   (add-witness state (witness (get queries/queries id)))
                   (catch Exception e
                     (throw (ex-info (str "No witness for JOB " id) {:query id} e)))))
               {:next-id 1 :leaves {} :rows []})
       :rows))

(defn read-resource []
  (read-string (slurp (io/resource "job/witness.edn"))))

(defn -main []
  (let [rows (generate)]
    (with-open [writer (io/writer resource-file)]
      (binding [*out* writer]
        (println ";; Generated by `clojure -M -m job.witness`. One witness per JOB query; do not edit.")
        (println "[")
        (doseq [row rows] (prn row))
        (println "]")))
    (println "Wrote" (count rows) "entities to" (str resource-file))))
