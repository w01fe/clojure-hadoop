(ns clojure-hadoop.test.gen
  (:use clojure.test clojure-hadoop.gen))

(deftest test-gen-job-classes
  (gen-job-classes))

(deftest test-gen-main-method
  (gen-main-method))

(deftest test-gen-conf-methods
  (gen-conf-methods))

