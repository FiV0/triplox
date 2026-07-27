(ns ^:no-doc xyz.triplox.types
  "Conversion between Triplox wire types and Clojure types."
  (:import [java.util Map]
           [xyz.triplox.client Delta Row]))

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

(defn delta->clj
  [^Delta delta]
  (when delta
    (mapv (fn [^Row row]
            [(mapv wire->clj (.values row)) (.weight row)])
          (.rows delta))))
