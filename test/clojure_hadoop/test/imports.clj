(ns clojure-hadoop.test.imports
  (:use clojure.test clojure-hadoop.imports))

(deftest test-import-conf
  (import-conf))

(deftest test-import-io
  (import-io))

(deftest test-import-io-compress
  (import-io-compress))

(deftest test-import-fs
  (import-fs))

(deftest test-import-mapreduce
  (import-mapreduce))

(deftest test-import-mapreduce-lib-input
  (import-mapreduce-lib-input))

(deftest test-import-mapreduce-lib-map
  (import-mapreduce-lib-map))

(deftest test-import-mapreduce-lib-output
  (import-mapreduce-lib-output))

(deftest test-import-mapreduce-lib-partition
  (import-mapreduce-lib-partition))

(deftest test-import-mapreduce-lib
  (import-mapreduce-lib))
