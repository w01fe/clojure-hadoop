(defproject clojure-hadoop "1.3.3-SNAPSHOT"
  :description "Library to aid writing Hadoop jobs in Clojure."
  :url "http://github.com/cmiles74/clojure-hadoop"
  :license {:name "Eclipse Public License 1.0"
            :url "http://opensource.org/licenses/eclipse-1.0.php"
            :distribution "repo"
            :comments "Same license as Clojure"}
  :dependencies [[org.apache.hadoop/hadoop-core "0.20.205.0"]
                 [org.clojure/clojure "1.3.0"]
                 [org.codehaus.jackson/jackson-mapper-asl "1.9.2"]
                 [log4j/log4j "1.2.16" :exclusions [javax.mail/mail
                                                    javax.jms/jms
                                                    com.sun.jdmk/jmxtools
                                                    com.sun.jmx/jmxri]]]
  :aot [clojure-hadoop.config
        clojure-hadoop.defjob
        clojure-hadoop.gen
        clojure-hadoop.imports
        clojure-hadoop.job
        clojure-hadoop.load
        clojure-hadoop.wrap
        ;; TODO: Remove them? Only needed for the tests.
        clojure-hadoop.examples.wordcount1
        clojure-hadoop.examples.wordcount2])
