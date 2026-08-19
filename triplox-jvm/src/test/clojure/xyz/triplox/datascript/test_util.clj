(ns xyz.triplox.datascript.test-util
  (:require [clojure.test :as t]))

(defmethod t/assert-expr 'thrown-msg? [msg form]
  (let [[_ match & body] form]
    `(try
       ~@body
       (t/do-report {:type :fail
                     :message ~msg
                     :expected '~form
                     :actual nil})
       (catch Throwable e#
         (let [actual# (.getMessage e#)]
           (if (= ~match actual#)
             (t/do-report {:type :pass
                           :message ~msg
                           :expected '~form
                           :actual e#})
             (t/do-report {:type :fail
                           :message ~msg
                           :expected '~form
                           :actual e#})))
         e#))))
