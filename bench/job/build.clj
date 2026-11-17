(ns build
  (:require [clojure.tools.build.api :as b]))

(def client-dir "../../triplox-jvm/src/main")
(def java-class-dir "classes")
(def class-dir "target/classes")
(def jar-file "target/job.jar")
(def basis (delay (b/create-basis {:project "deps.edn"})))

(defn clean [_]
  (b/delete {:path "target"}))

(defn javac
  "Compiles the CSV reader and the Triplox client's Java sources."
  [_]
  (b/javac {:src-dirs ["java" (str client-dir "/java")]
            :class-dir java-class-dir
            :basis @basis
            :javac-opts ["--release" "21"]}))

(defn uber
  "Builds the benchmark runner uberjar."
  [_]
  (b/delete {:path class-dir})
  (javac nil)
  (b/copy-dir {:src-dirs ["src" "resources" java-class-dir]
               :target-dir class-dir})
  (b/compile-clj {:basis @basis
                  :ns-compile '[job.main]
                  :class-dir class-dir})
  (b/uber {:class-dir class-dir
           :uber-file jar-file
           :basis @basis
           :main 'job.main
           ;; Datalevin's JVM flags; `java -jar` reads them from the manifest.
           :manifest {"Add-Opens" "java.base/java.lang java.base/java.nio java.base/java.util java.base/sun.nio.ch"
                      "Enable-Native-Access" "ALL-UNNAMED"}}))

(comment
  (javac nil)
  (uber nil)
  (clean nil))
