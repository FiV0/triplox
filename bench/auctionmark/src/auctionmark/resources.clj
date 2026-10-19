(ns auctionmark.resources
  "Linux process samples; unavailable fields remain explicit in the report."
  (:import [java.lang ProcessHandle]
           [java.nio.file Files Path]))

(defn path [s] (Path/of s (make-array String 0)))

(defn- open-file-descriptors [pid]
  (try
    (with-open [entries (Files/list (path (str "/proc/" pid "/fd")))]
      (.count entries))
    (catch Exception _ nil)))

(defn process-sample [pid]
  (try
    (let [status (Files/readString (path (str "/proc/" pid "/status")))
          status-number (fn [pattern] (some-> (re-find pattern status) second Long/parseLong))
          handle (.orElse (ProcessHandle/of (long pid)) nil)
          cpu (some-> handle .info .totalCpuDuration (.orElse nil))]
      {:pid pid
       :rss-mib (some-> (status-number #"VmRSS:\s+(\d+)") (/ 1024.0))
       :os-threads (status-number #"Threads:\s+(\d+)")
       :file-descriptors (open-file-descriptors pid)
       :cpu-seconds (some-> cpu .toNanos (/ 1e9))})
    (catch Exception e
      {:pid pid :error (.getMessage e)})))

(defn sample [pids]
  {:at-ns (System/nanoTime)
   :processes (update-vals pids process-sample)})

(defn combined-rss [sample]
  (reduce + 0 (keep :rss-mib (vals (:processes sample)))))

(defn- process-summary
  "Peaks and average CPU of one process over its observations."
  [observations elapsed]
  (let [maximum (fn [k] (when-let [xs (seq (keep k observations))] (apply max xs)))
        cpus (keep :cpu-seconds observations)]
    {:peak-rss-mib (maximum :rss-mib)
     :peak-os-threads (maximum :os-threads)
     :peak-file-descriptors (maximum :file-descriptors)
     :average-cpu-cores (when (and elapsed (pos? elapsed) (> (count cpus) 1))
                          (/ (- (last cpus) (first cpus)) elapsed))
     :errors (vec (distinct (keep :error observations)))}))

(defn summarize [samples]
  (let [elapsed (when (> (count samples) 1)
                  (/ (- (:at-ns (last samples)) (:at-ns (first samples))) 1e9))]
    {:combined-peak-rss-mib (reduce max 0 (map combined-rss samples))
     :processes (into {} (for [role (keys (:processes (first samples)))]
                           [role (process-summary (map #(get-in % [:processes role]) samples) elapsed)]))}))
