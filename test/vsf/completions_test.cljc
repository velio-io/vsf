(ns vsf.completions-test
  (:require
   [clojure.test :refer [deftest is]]
   #?(:cljs [vsf.completions :as completions])))


(deftest completions-list-test
  #?(:cljs (is (vector? completions/streams-completions)))
  #?(:cljs (is (every? #(= #{:label :type :info} (-> % keys set))
                       completions/streams-completions))))