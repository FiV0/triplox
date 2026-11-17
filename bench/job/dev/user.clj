(ns user)

;; Compile the Java sources into classes/, which is on the classpath.
(load-file "build.clj")
(build/javac nil)
