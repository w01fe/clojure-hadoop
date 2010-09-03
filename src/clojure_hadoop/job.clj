(ns clojure-hadoop.job
  (:require [clojure-hadoop.gen :as gen]
            [clojure-hadoop.imports :as imp]
            [clojure-hadoop.wrap :as wrap]
            [clojure-hadoop.config :as config]
            [clojure-hadoop.load :as load])
  (:import (org.apache.hadoop.util Tool))
  (:use [clojure.contrib.def :only (defvar-)]
        [clojure-hadoop.config :only (configuration)]))

(imp/import-conf)
(imp/import-io)
(imp/import-io-compress)
(imp/import-fs)
(imp/import-mapreduce)
(imp/import-mapreduce-lib)

(gen/gen-job-classes)
(gen/gen-main-method)

(def ^Job *job* nil)

(defvar- method-fn-name
  {"map" "mapper-map"
   "reduce" "reducer-reduce"
   "combine" "combiner-reduce"})

(defvar- wrapper-fn
  {"map" wrap/wrap-map
   "reduce" wrap/wrap-reduce
   "combine" wrap/wrap-reduce})

(defvar- default-reader
  {"map" wrap/clojure-map-reader
   "reduce" wrap/clojure-reduce-reader
   "combine" wrap/clojure-reduce-reader})

(defn set-job [job]
  (alter-var-root (var *job*) (fn [_] job)))

(defn- configure-functions
  "Preps the mapper or reducer with a Clojure function read from the
  job configuration.  Called from Mapper.configure and
  Reducer.configure."
  [type ^Configuration configuration]
  ;; (set-job job)
  (let [function (load/load-name (.get configuration (str "clojure-hadoop.job." type)))
        reader (if-let [v (.get configuration (str "clojure-hadoop.job." type ".reader"))]
                 (load/load-name v)
                 (default-reader type))
        writer (if-let [v (.get configuration (str "clojure-hadoop.job." type ".writer"))]
                 (load/load-name v)
                 wrap/clojure-writer)]
    (assert (fn? function))
    (alter-var-root (ns-resolve (the-ns 'clojure-hadoop.job)
                                (symbol (method-fn-name type)))
                    (fn [_] ((wrapper-fn type) function reader writer)))))

;;; CREATING AND CONFIGURING JOBS

(defn- parse-command-line [job args]
  (try
    (config/parse-command-line-args job args)
    (catch Exception e
      (prn e)
      (config/print-usage)
      (System/exit 1))))

;;; MAPPER METHODS

(defn mapper-cleanup [this context]
  (let [configuration (.getConfiguration context)]
    (if-let [cleanup-fn-name (.get configuration config/map-cleanup)]
      ((load/load-name cleanup-fn-name) context))))

(defn mapper-map [this wkey wvalue context]
  (throw (Exception. "Mapper function not defined.")))

(defn mapper-setup [this context]
  (let [configuration (.getConfiguration context)]
    (configure-functions "map" configuration)
    (if-let [setup-fn-name (.get configuration config/map-setup)]
      ((load/load-name setup-fn-name) context))))

;;; REDUCER METHODS

(defn reducer-cleanup [this context]
  (let [configuration (.getConfiguration context)]
    (if-let [cleanup-fn-name (.get configuration config/reduce-cleanup)]
      ((load/load-name cleanup-fn-name) context))))

(defn reducer-reduce [this wkey wvalues context]
  (throw (Exception. "Reducer function not defined.")))

(defn reducer-setup [this context]
  (let [configuration (.getConfiguration context)]
    (configure-functions "reduce" configuration)
    (if-let [setup-fn-name (.get configuration config/reduce-setup)]
      ((load/load-name setup-fn-name) context))))

;;; COMBINER METHODS

(defn combiner-cleanup [this context]
  (let [configuration (.getConfiguration context)]
    (if-let [cleanup-fn-name (.get configuration config/combine-cleanup)]
      ((load/load-name cleanup-fn-name) context))))

(defn combiner-reduce [this wkey wvalues context]
  (throw (Exception. "Combiner function not defined.")))

(defn combiner-setup [this context]
  (let [configuration (.getConfiguration context)]
    (configure-functions "combine" configuration)
    (if-let [setup-fn-name (.get configuration config/combine-setup)]
      ((load/load-name setup-fn-name) context))))

(defn- handle-replace-option [^Job job]
  (when (= "true" (.get (configuration job) "clojure-hadoop.job.replace"))
    (let [fs (FileSystem/get (configuration job))
          output (FileOutputFormat/getOutputPath job)]
      (.delete fs output true))))

(defn- set-default-config [^Job job]
  (doto job
    (.setJobName "clojure_hadoop.job")
    (.setOutputKeyClass Text)
    (.setOutputValueClass Text)
    (.setMapperClass (Class/forName "clojure_hadoop.job_mapper"))
    (.setReducerClass (Class/forName "clojure_hadoop.job_reducer"))
    (.setInputFormatClass SequenceFileInputFormat)
    (.setOutputFormatClass SequenceFileOutputFormat)
    (FileOutputFormat/setCompressOutput true)
    (SequenceFileOutputFormat/setOutputCompressionType
     SequenceFile$CompressionType/BLOCK)))

(defn run
  "Runs a Hadoop job given the Job object."
  [job]  
  (handle-replace-option job)
  (.waitForCompletion job true))

(defn run-job-fn
  "Runs a Hadoop job given the job-fn."
  ([job-fn]
     (run-job-fn (clojure_hadoop.job.) job-fn))
  ([tool job-fn]
     (doto (Job. (.getConf tool))
       (.setJarByClass (.getClass tool))
       (set-default-config)
       (config/conf :job-fn job-fn)
       (run))))

;;; TOOL METHODS

(gen/gen-conf-methods)

(defn tool-run [^Tool this args]
  (doto (Job. (.getConf this) (.getClass this))
    (set-default-config)
    (parse-command-line args)
    (run))
  0)
