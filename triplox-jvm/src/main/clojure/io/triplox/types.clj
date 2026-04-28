(ns io.triplox.types
  "Conversion between Triplox wire types and Clojure types."
  (:import [java.util Map]))

(defn wire->clj
  "Convert a wire protocol value to an idiomatic Clojure value.
   TreeMap<String,Object> → Clojure map with keyword keys.
   Most types pass through unchanged."
  [v]
  (cond
    (instance? Map v)
    (persistent!
     (reduce (fn [m entry]
               (assoc! m (keyword (.getKey ^java.util.Map$Entry entry))
                       (wire->clj (.getValue ^java.util.Map$Entry entry))))
             (transient {})
             (.entrySet ^Map v)))

    (instance? java.util.List v)
    (mapv wire->clj v)

    :else v))
