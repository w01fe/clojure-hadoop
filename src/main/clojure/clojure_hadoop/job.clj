(ns clojure-hadoop.job
  (:require [clojure-hadoop.gen :as gen]
            [clojure-hadoop.imports :as imp]
            [clojure-hadoop.wrap :as wrap]
            [clojure-hadoop.config :as config]
            [clojure-hadoop.load :as load])
  (:import (org.apache.hadoop.util Tool)))

(imp/import-conf)
(imp/import-io)
(imp/import-io-compress)
(imp/import-fs)
(imp/import-mapred)

(gen/gen-job-classes)
(gen/gen-main-method)

(def ^JobConf *jobconf* nil)

(def ^{:private true} method-fn-name
     {"map" "mapper-map"
      "reduce" "reducer-reduce"
      "combiner" "combiner-reduce"})

(def ^{:private true} wrapper-fn
     {"map" wrap/wrap-map
      "reduce" wrap/wrap-reduce
      "combiner" wrap/wrap-reduce})

(def ^{:private true} default-reader
     {"map" wrap/clojure-map-reader
      "reduce" wrap/clojure-reduce-reader
      "combiner" wrap/clojure-reduce-reader})

(defn- set-jobconf [jobconf]
  (alter-var-root (var *jobconf*) (fn [_] jobconf)))

(defn- configure-functions
  "Preps the mapper or reducer with a Clojure function read from the
  job configuration.  Called from Mapper.configure and
  Reducer.configure."
  [type ^JobConf jobconf]
  (set-jobconf jobconf)
  (let [function (load/load-name (.get jobconf (str "clojure-hadoop.job." type)))
        reader (if-let [v (.get jobconf (str "clojure-hadoop.job." type ".reader"))]
                 (load/load-name v)
                 (default-reader type))
        writer (if-let [v (.get jobconf (str "clojure-hadoop.job." type ".writer"))]
                 (load/load-name v)
                 wrap/clojure-writer)]
    (assert (fn? function))
    (alter-var-root (ns-resolve (the-ns 'clojure-hadoop.job)
                                (symbol (method-fn-name type)))
                    (fn [_] ((wrapper-fn type) function reader writer)))))

;;; CREATING AND CONFIGURING JOBS

(defn- parse-command-line [jobconf args]
  (try
   (config/parse-command-line-args jobconf args)
   (catch Exception e
     (prn e)
     (config/print-usage)
     (System/exit 1))))

;;; MAPPER METHODS

(defn mapper-configure [this jobconf]
  (configure-functions "map" jobconf))

(defn mapper-map [this wkey wvalue output reporter]
  (throw (Exception. "Mapper function not defined.")))

;;; REDUCER METHODS

(defn reducer-configure [this jobconf]
  (configure-functions "reduce" jobconf))

(defn reducer-reduce [this wkey wvalues output reporter]
  (throw (Exception. "Reducer function not defined.")))

;;; COMBINER METHODS

(gen-class
 :name ~(str the-name "_combiner")
 :extends "org.apache.hadoop.mapred.MapReduceBase"
 :implements ["org.apache.hadoop.mapred.Reducer"]
 :prefix "combiner-"
 :main false)

(defn combiner-configure [this jobconf]
  (configure-functions "combiner" jobconf))

(defn combiner-reduce [this wkey wvalues output reporter]
  (throw (Exception. "Combiner function not defined.")))

(defn- handle-replace-option [^JobConf jobconf]
  (when (= "true" (.get jobconf "clojure-hadoop.job.replace"))
    (let [fs (FileSystem/get jobconf)
          output (FileOutputFormat/getOutputPath jobconf)]
      (.delete fs output true))))

(defn- set-default-config [^JobConf jobconf]
  (doto jobconf
    (.setJobName "clojure_hadoop.job")
    (.setOutputKeyClass Text)
    (.setOutputValueClass Text)
    (.setMapperClass (Class/forName "clojure_hadoop.job_mapper"))
    (.setReducerClass (Class/forName "clojure_hadoop.job_reducer"))
    (.setInputFormat SequenceFileInputFormat)
    (.setOutputFormat SequenceFileOutputFormat)
    (FileOutputFormat/setCompressOutput true)
    (SequenceFileOutputFormat/setOutputCompressionType
     SequenceFile$CompressionType/BLOCK)))

(defn run
  "Runs a Hadoop job given the JobConf object."
  [jobconf]
  (doto jobconf
    (handle-replace-option)
    (JobClient/runJob)))

(defn run-job-fn
  "Runs a Hadoop job given the job-fn."
  [job-fn & [tool]]
  (let [tool (or tool (clojure_hadoop.job.))]
    (doto (JobConf. (.getConf tool) (.getClass tool))      
      (set-default-config)
      (config/conf :job-fn job-fn)
      (run))))

;;; TOOL METHODS

(defn tool-getConf [this]
  (or *jobconf* (Configuration.)))

(defn tool-run [^Tool this args]
  (doto (JobConf. (.getConf this) (.getClass this))
    (set-default-config)
    (parse-command-line args)
    (run))
  0)

(defn tool-setConf [this jobconf]
  (set-jobconf jobconf))
