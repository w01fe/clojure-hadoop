(defproject org.clojars.r0man/clojure-hadoop "1.2.0-SNAPSHOT"
  :description "Library to aid writing Hadoop jobs in Clojure."
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [org.apache.hadoop/hadoop-core "0.20.2"]
                 [log4j/log4j "1.2.16"]]
  :dev-dependencies [[swank-clojure "1.2.1"]]
  :aot [clojure-hadoop.config
        clojure-hadoop.defjob
        clojure-hadoop.gen
        clojure-hadoop.imports
        clojure-hadoop.job
        clojure-hadoop.load
        clojure-hadoop.wrap]
  :compile-path "target/classes"
  :source-path "src/main/clojure"
  :test-path "src/test/clojure")
