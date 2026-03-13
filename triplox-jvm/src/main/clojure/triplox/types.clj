(ns triplox.types
  "Conversion between Triplox wire types and Clojure types."
  (:import [java.util Map]
           [io.triplox.client DataTypeCodec$TaggedTuple]))

(defn wire->clj
  "Convert a wire protocol value to an idiomatic Clojure value.
   TreeMap<String,Object> → Clojure map with keyword keys.
   TaggedTuple → vector (same as Vector for Clojure consumers).
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

    (instance? DataTypeCodec$TaggedTuple v)
    (mapv wire->clj (.elements ^DataTypeCodec$TaggedTuple v))

    (instance? java.util.List v)
    (mapv wire->clj v)

    :else v))
