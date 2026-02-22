(ns triplox.types
  "Conversion between Triplox wire types and Clojure types."
  (:import [java.util TreeMap]
           [io.triplox.client DataTypeCodec$TaggedTuple]))

(defn wire->clj
  "Convert a wire protocol value to an idiomatic Clojure value.
   TreeMap<String,Object> → Clojure map with keyword keys.
   TaggedTuple → vector (same as Vector for Clojure consumers).
   Most types pass through unchanged."
  [v]
  (cond
    (instance? TreeMap v)
    (persistent!
     (reduce (fn [m entry]
               (assoc! m (keyword (.getKey ^java.util.Map$Entry entry))
                       (wire->clj (.getValue ^java.util.Map$Entry entry))))
             (transient {})
             (.entrySet ^TreeMap v)))

    (instance? DataTypeCodec$TaggedTuple v)
    (mapv wire->clj (.elements ^DataTypeCodec$TaggedTuple v))

    (instance? java.util.List v)
    (mapv wire->clj v)

    :else v))

(defn clj->wire
  "Convert a Clojure value to a wire protocol value.
   Clojure map with keyword keys → TreeMap<String,Object>.
   Most types pass through unchanged."
  [v]
  (cond
    (map? v)
    (let [tm (TreeMap.)]
      (doseq [[k val] v]
        (.put tm
              (if (keyword? k)
                (subs (str k) 1)
                (str k))
              (clj->wire val)))
      tm)

    (vector? v)
    (java.util.ArrayList. ^java.util.Collection (mapv clj->wire v))

    :else v))
