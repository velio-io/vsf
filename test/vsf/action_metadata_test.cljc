(ns vsf.action-metadata-test
  (:require
   [clojure.test :refer [deftest is]]
   #?(:cljs [vsf.action-metadata :as metadata])))


(deftest completions-list-test
  #?(:cljs (is (vector? metadata/completions)))
  #?(:cljs (is (every? #(= #{:label :type :info} (-> % keys set))
                       metadata/completions))))


(deftest compile-controls-test
  #?(:cljs (is (map? metadata/actions-controls)))
  #?(:cljs (is (= :code (get-in metadata/actions-controls ["where" :control-type])))))