(ns graph.gnp
  "Erdős–Rényi G(n, p) random graphs via the Batagelj–Brandes algorithm.

  Instead of flipping a coin for each of the C(n,2) possible edges, the number
  of pairs skipped between consecutive edges is drawn directly from the
  geometric distribution, giving O(n + m) expected time.")

(defn gnp-edges
  "Lazy seq of edges [v w] with n > v > w >= 0, for an Erdős–Rényi G(n, p)
  random graph on vertices 0..n-1. Each of the C(n,2) pairs is present
  independently with probability p. Edges come out in lexicographic order.

  rng is a java.util.Random (defaults to a fresh one); pass a seeded instance
  for reproducible graphs."
  ([n p]
   (gnp-edges n p (java.util.Random.)))
  ([n p ^java.util.Random rng]
   (let [n (long n)
         p (double p)]
     (assert (<= 0.0 p 1.0) "p must be in [0, 1]")
     (if (or (< n 2) (zero? p))
       ()
       ;; log1p(-p) = log(1-p), but accurate for tiny p. Negative, and
       ;; log(r) <= 0 for r in (0,1], so the quotient is >= 0.
       (let [lp   (Math/log1p (- p))
             ;; Skips larger than n^2 can only drain v up to n and end the
             ;; run, so clamping there costs nothing and avoids long overflow
             ;; when p is extremely small (or r underflows).
             cap  (double (* n n))
             step (fn step [v0 w0]
                    (lazy-seq
                     (when (< v0 n)
                       (let [r    (- 1.0 (.nextDouble rng)) ; uniform in (0,1]
                             skip (long (min cap (Math/floor (/ (Math/log r) lp))))]
                         ;; Walk the flat pair index forward into row v.
                         (loop [v v0
                                w (+ w0 1 skip)]
                           (if (and (>= w v) (< v n))
                             (recur (inc v) (- w v))
                             (when (< v n)
                               (cons [v w] (step v w)))))))))]
         (step 1 -1))))))

(defn gnp-adjacency
  "Adjacency map {vertex #{neighbours}} for G(n, p), isolated vertices included."
  ([n p]
   (gnp-adjacency n p (java.util.Random.)))
  ([n p rng]
   (reduce (fn [g [v w]]
             (-> g (update v conj w) (update w conj v)))
           (zipmap (range n) (repeat #{}))
           (gnp-edges n p rng))))

(comment
  ;; Reproducible: same seed, same graph.
  (take 5 (gnp-edges 1000 0.01 (java.util.Random. 42)))
  ;; => ([1 0] [15 6] [17 8] [22 6] [24 12])   ; shape, not exact values

  ;; Edge count concentrates around p * C(n,2) = 1345.5
  (count (gnp-edges 300 0.03))

  ;; Lazy, so this is cheap even though the full graph is huge.
  (first (gnp-edges 10000000 0.5))

  (gnp-adjacency 6 0.4))
