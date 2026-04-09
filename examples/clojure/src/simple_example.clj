(ns simple-example
  "Simple example: connect to a running Triplox server, define schema
  attributes, insert data, and query it back.

  Start the server first:
    cargo run                      (from project root, uses config/triplox.toml)

  Then run this example:
    clj -M -m simple-example       (from examples/clojure/)"
  (:require [io.triplox.api :as tc]))

(defn -main [& _args]
  (let [host "localhost"
        port 5490]
    (println (str "Connecting to " host ":" port "..."))
    (with-open [conn (tc/connect host port)]
      (println "Connected.")

      ;; 1. Define schema attributes
      (let [result (tc/transact conn [{:db/id 200 :db/ident :name
                                       :db/valueType :db.type/string
                                       :db/cardinality :db.cardinality/one}
                                      {:db/id 201 :db/ident :age
                                       :db/valueType :db.type/long
                                       :db/cardinality :db.cardinality/one}])]
        (if (:committed? result)
          (println (str "Schema defined (tx_id=" (:tx-id result) ")."))
          (throw (ex-info (str "Schema transaction aborted: " (:error-message result)) {}))))

      ;; 2. Insert some data
      (let [result (tc/transact conn [{:db/id 100 :name "alice" :age 30}
                                      {:db/id 101 :name "bob" :age 25}])]
        (if (:committed? result)
          (println (str "Data inserted (tx_id=" (:tx-id result) ")."))
          (throw (ex-info (str "Data transaction aborted: " (:error-message result)) {}))))

      ;; 3. Open a DB snapshot and query
      (with-open [db (tc/db conn)]
        (let [rows (tc/q db '{:find [?e ?name ?age]
                              :where [[?e :name ?name]
                                      [?e :age ?age]]})]
          (println (str "Query returned " (count rows) " row(s):"))
          (doseq [row rows]
            (println (str "  " (pr-str row))))))

      (println "Done."))))
