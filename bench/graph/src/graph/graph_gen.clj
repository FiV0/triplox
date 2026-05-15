(ns graph.graph-gen
  "Small graph generators for Triplox ingestion benchmarks.")

(defn complete-graph
  "Return undirected complete-graph edges [from to] for vertices 0..n-1."
  [n]
  (for [i (range n)
        j (range (inc i) n)]
    [i j]))

(defn graph->ops
  "Convert an edge seq to Triplox tx ops using lookup refs on :g/id."
  [edges]
  (map (fn [[from to]]
         [:db/add [:g/id (long from)] :g/to [:g/id (long to)]])
       edges))
