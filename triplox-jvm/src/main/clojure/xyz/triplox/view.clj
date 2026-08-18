(ns xyz.triplox.view
  "EXPERIMENTAL: Client-side materialized views backed by incremental query
  subscriptions. This namespace may change or be removed without notice."
  (:require
   [clojure.core.async :as async]
   [xyz.triplox.api :as api])
  (:import
   [java.io Closeable]
   [java.lang AutoCloseable]))

(defn- update-view
  [view-map delta]
  (reduce (fn [view-map [tuple weight]]
            (let [new-weight (+ (get view-map tuple 0) weight)]
              (if (zero? new-weight)
                (dissoc view-map tuple)
                (assoc view-map tuple new-weight))))
          view-map
          delta))

(defn- update-view!
  [view delta]
  (swap! view update-view delta))

(defn- start-worker [sub view]
  (let [stop (async/chan)
        done (async/chan)]
    (async/go-loop []
      (let [[_ channel] (async/alts! [stop (async/timeout 300)])]
        (if (= channel stop)
          (async/close! done)
          (let [delta (api/take! sub 10)]
            (when (not= delta ::api/timeout)
              (update-view! view delta))
            (recur)))))
    {:stop stop :done done}))

(defrecord View [sub view stop-chan done-chan]
  Closeable
  (close [_]
    (async/close! stop-chan)
    (async/<!! done-chan)
    (.close ^AutoCloseable sub)))

(defn ->view
  "Create a client-side materialized view for an incremental query.

  Returns a Closeable View. Close it to stop the worker and unsubscribe."
  [conn query]
  (let [sub (api/subscribe conn query)
        view (atom {})
        {:keys [stop done]} (start-worker sub view)]
    (->View sub view stop done)))

(defn get-view
  "Return the current rows in a materialized view."
  [{:keys [view]}]
  (vec (keys @view)))
