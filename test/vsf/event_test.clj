(ns vsf.event-test
  (:require
   [clojure.test :refer :all]
   [vsf.event :as event]))


(deftest expired?-test
  (are [event] (event/expired? 300 event)
    {:time 1}
    {:state "expired"}
    {:time 280 :ttl 10})
  (are [event] (not (event/expired? 300 event))
    {:time 250}
    {:state "ok"}
    {:time 200 :ttl 110}))
