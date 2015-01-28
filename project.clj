(defproject w01fe/clojure-hadoop "1.4.7-SNAPSHOT"
  :description "Library to aid writing Hadoop jobs in Clojure."
  :url "http://github.com/w01fe/clojure-hadoop"
  :license {:name "Eclipse Public License 1.0"
            :url "http://opensource.org/licenses/eclipse-1.0.php"
            :distribution "repo"
            :comments "Same license as Clojure"}
  :profiles {:provided {:dependencies [[org.clojure/clojure "1.6.0"]
                                       [org.apache.hadoop/hadoop-common "2.6.0"]
                                       [org.apache.hadoop/hadoop-mapreduce-client-core "2.6.0"]
                                       [org.apache.hadoop/hadoop-aws "2.6.0"]]}}
  :dependencies [[log4j/log4j "1.2.16" :exclusions [javax.mail/mail
                                                    javax.jms/jms
                                                    com.sun.jdmk/jmxtools
                                                    com.sun.jmx/jmxri]]]
  :source-paths ["src" "test"]
  ;;:global-vars {*warn-on-reflection* true}
  :aot [clojure-hadoop.config
        clojure-hadoop.defjob
        clojure-hadoop.gen
        clojure-hadoop.imports
        clojure-hadoop.job
        clojure-hadoop.load
        clojure-hadoop.wrap
        clojure-hadoop.flow
        ;; TODO: Remove them? Only needed for the tests.
        clojure-hadoop.examples.wordcount1
        clojure-hadoop.examples.wordcount2])
