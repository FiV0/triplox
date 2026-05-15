(ns graph.graph-gen
  "Small graph generators for Triplox ingestion benchmarks."
  (:require [clojure.math.combinatorics :as combo]))

(def graph-schema
  [{:db/ident :g/id
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :g/to
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many}])

(def triangle-query
  '{:find [?a ?b ?c]
    :where [[?a :g/to ?b]
            [?b :g/to ?c]
            [?a :g/to ?c]]})

(defn graph->ops
  "Convert an edge seq to Triplox tx ops using lookup refs on :g/id."
  [edges]
  (map (fn [[from to]] [:db/add [:g/id from] :g/to [:g/id to]]) edges))

(defn complete-graph [n]
  (for [i (range n) j (range (inc i) n)]
    [i j]))

(defn complete-bipartite [n]
  (let [n1 (quot n 2)]
    (for [i (range n1) j (range n1 n)]
      [i j])))

(defn star-graph [n]
  (for [i (range 1 n)]
    [0 i]))

(defn star-with-ring [n]
  (concat
   (for [i (range 1 n)]
     [0 i])
   (for [i (range 1 (dec n))]
     [i (inc i)])))

(defn random-graph [n p]
  (filter (fn [_] (<= (rand) p)) (complete-graph n)))

(defn complete-independents [n k]
  (let [k1 (quot n k)
        k2 (inc k1)
        n2 (* k2 (rem n k))
        vertices (concat
                  (partition k2 (take n2 (range n)))
                  (partition k1 (drop n2 (range n))))]
    (->> (combo/combinations vertices 2)
         (mapcat #(apply combo/cartesian-product %))
         (map vec))))

(defn random-independents [n k p]
  (filter (fn [_] (<= (rand) p)) (complete-independents n k)))

(comment
  (complete-graph 5)
  (complete-bipartite 5)
  )

(defn graph->triples [g]
  (map (fn [[from to]] [from :g/to to]) g))

(defn edge-list->adj-list [g]
  (->> (group-by first g)
       (map (fn [[k v]] [k (mapv second v)]))))

(defn- no-repetition [vars]
  (loop [res [] vars vars]
    (if (seq vars)
      (recur (into res (map #(vector (list '!= (first vars) %)) (nthrest vars 2))) (rest vars))
      res)))

(comment
  (no-repetition (range 4)))

(defn k-path-query [k]
  (let [vars (vec (repeatedly (inc k) #(symbol (str "?" (gensym)))))]
    (-> {:find vars}
        (assoc :where (mapv #(vector %1 :g/to %2) vars (rest vars)))
        (update :where (comp vec concat) (no-repetition vars)))))

(comment
  (k-path-query 4))
