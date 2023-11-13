(ns vsf.action-test
  (:require
   [clojure.test :refer :all]
   [vsf.action :as sut]))


(deftest actions-output-test
  (is (= (sut/where [:and
                     [:= :host "foo"]
                     [:> :metric 10]])
         {:action      :where,
          :description {:message "Filter events based on the provided condition",
                        :params  "[:and [:= :host \"foo\"] [:> :metric 10]]"},
          :params      [[:and [:= :host "foo"] [:> :metric 10]]],
          :children    nil}))

  (is (= (sut/fixed-time-window {:duration 60}
           (sut/coll-where [:and
                            [:= :host "foo"]
                            [:> :metric 10]]))
         {:action      :fixed-time-window
          :children    '({:action      :coll-where
                          :children    nil
                          :description {:message "Filter a list of events based on the provided condition"
                                        :params  "[:and [:= :host \"foo\"] [:> :metric 10]]"}
                          :params      [[:and
                                         [:= :host "foo"]
                                         [:> :metric 10]]]})
          :description {:message "Build 60 seconds fixed time windows"}
          :params      [{:aggr-fn  :fixed-time-window
                         :duration 60}]}))

  (is (= (sut/increment
          (sut/index [:host]))
         {:action      :increment
          :children    '({:action      :index
                          :description {:message "Insert events into the index using the provided fields as keys"
                                        :params  "[:host]"}
                          :params      [[:host]]})
          :description {:message "Increment the :metric field"}}))


  (is (= (sut/decrement
          (sut/index [:host]))
         {:action      :decrement
          :children    '({:action      :index
                          :description {:message "Insert events into the index using the provided fields as keys"
                                        :params  "[:host]"}
                          :params      [[:host]]})
          :description {:message "Decrement the :metric field"}}))


  (is (= (sut/increment
          (sut/debug))
         {:action      :increment
          :children    '({:action      :debug
                          :description {:message "Print the event in the logs as debug"}})
          :description {:message "Increment the :metric field"}}))


  (is (= (sut/increment
          (sut/info))
         {:action      :increment
          :children    '({:action      :info
                          :description {:message "Print the event in the logs as info"}})
          :description {:message "Increment the :metric field"}}))


  (is (= (sut/increment
          (sut/debug))
         {:action      :increment
          :children    '({:action      :debug
                          :description {:message "Print the event in the logs as debug"}})
          :description {:message "Increment the :metric field"}}))


  (is (= (sut/fixed-event-window {:size 5}
           (sut/debug))
         {:action      :fixed-event-window
          :children    '({:action      :debug
                          :description {:message "Print the event in the logs as debug"}})
          :description {:message "Create a fixed event window of size 5"}
          :params      [{:size 5}]}))


  (is (= (sut/fixed-event-window {:size 10}
           (sut/coll-mean
            (sut/debug)))
         {:action      :fixed-event-window
          :children    '({:action      :coll-mean
                          :children    ({:action      :debug
                                         :description {:message "Print the event in the logs as debug"}})
                          :description {:message "Computes the mean of events"}})
          :description {:message "Create a fixed event window of size 10"}
          :params      [{:size 10}]}))


  (is (= (sut/fixed-event-window {:size 10}
           (sut/coll-max
            (sut/debug)))
         {:action      :fixed-event-window
          :children    '({:action      :coll-max
                          :children    ({:action      :debug
                                         :description {:message "Print the event in the logs as debug"}})
                          :description {:message "Get the event with the biggest metric"}})
          :description {:message "Create a fixed event window of size 10"}
          :params      [{:size 10}]}))


  (is (= (sut/fixed-event-window {:size 10}
           (sut/coll-sum
            (sut/debug)))
         {:action      :fixed-event-window
          :children    '({:action      :coll-sum
                          :children    ({:action      :debug
                                         :description {:message "Print the event in the logs as debug"}})
                          :description {:message "Get the event with the biggest metric"}})
          :description {:message "Create a fixed event window of size 10"}
          :params      [{:size 10}]}))


  (is (= (sut/fixed-event-window {:size 10}
           (sut/coll-min
            (sut/debug)))
         {:action      :fixed-event-window
          :children    '({:action      :coll-min
                          :children    ({:action      :debug
                                         :description {:message "Print the event in the logs as debug"}})
                          :description {:message "Get the event with the smallest metric"}})
          :description {:message "Create a fixed event window of size 10"}
          :params      [{:size 10}]}))


  (is (= (sut/fixed-event-window {:size 10}
           (sut/coll-sort :time
             (sut/debug)))
         {:action      :fixed-event-window
          :children    '({:action      :coll-sort
                          :children    ({:action      :debug
                                         :description {:message "Print the event in the logs as debug"}})
                          :description {:message "Sort events based on the field :time"}
                          :params      [:time]})
          :description {:message "Create a fixed event window of size 10"}
          :params      [{:size 10}]}))

  (is (= (sut/sdo
          (sut/increment)
          (sut/decrement))
         {:action      :sdo
          :children    '({:action      :increment
                          :children    nil
                          :description {:message "Increment the :metric field"}}
                         {:action      :decrement
                          :children    nil
                          :description {:message "Decrement the :metric field"}})
          :description {:message "Forward events to children"}}))


  (is (= (sut/expired
          (sut/increment))
         {:action      :expired
          :children    '({:action      :increment
                          :children    nil
                          :description {:message "Increment the :metric field"}})
          :description {:message "Keep expired events"}}))


  (is (= (sut/not-expired
          (sut/increment))
         {:action      :not-expired
          :children    '({:action      :increment
                          :children    nil
                          :description {:message "Increment the :metric field"}})
          :description {:message "Remove expired events"}}))


  (is (= (sut/above-dt {:threshold 100 :duration 10}
           (sut/debug))
         {:action      :above-dt
          :children    '({:action      :debug
                          :description {:message "Print the event in the logs as debug"}})
          :description {:message "Keep events if :metric is greater than 100 during 10 seconds"}
          :params      [[:> :metric 100] 10]}))


  (is (= (sut/below-dt {:threshold 100 :duration 10}
           (sut/debug))
         {:action      :below-dt
          :children    '({:action      :debug
                          :description {:message "Print the event in the logs as debug"}})
          :description {:message "Keep events if :metric is lower than 100 during 10 seconds"}
          :params      [[:< :metric 100] 10]}))


  (is (= (sut/between-dt {:low 50 :high 100 :duration 10}
           (sut/debug))
         {:action      :between-dt
          :children    '({:action      :debug
                          :description {:message "Print the event in the logs as debug"}})
          :description {:message "Keep events if :metric is between 50 and 100 during 10 seconds"}
          :params      [[:and
                         [:> :metric 50]
                         [:< :metric 100]]
                        10]}))


  (is (= (sut/outside-dt {:low 50 :high 100 :duration 10}
           (sut/debug))
         {:action      :outside-dt
          :children    '({:action      :debug
                          :description {:message "Print the event in the logs as debug"}})
          :description {:message "Keep events if :metric is outside 50 and 100 during 10 seconds"}
          :params      [[:or
                         [:< :metric 50]
                         [:> :metric 100]]
                        10]}))


  (is (= (sut/critical-dt {:duration 10}
           (sut/debug))
         {:action      :critical-dt
          :children    '({:action      :debug
                          :description {:message "Print the event in the logs as debug"}})
          :description {:message "Keep events if the state is critical for more than 10 seconds"}
          :params      [[:= :state "critical"]
                        10]}))


  (is (= (sut/critical
          (sut/error))
         {:action      :critical
          :children    '({:action      :error
                          :description {:message "Print the event in the logs as error"}})
          :description {:message "Keep critical events"}})))



;;
;;
;;(warning
;; (warning))
;;
;;
;;(default :state "ok"
;;  (info))
;;
;;
;;(output! :influxdb)
;;
;;
;;(coalesce {:duration 10 :fields [:host :service]}
;;  (debug))
;;
;;
;;(coalesce {:duration 10 :fields [:host [:nested :field]]}
;;  (debug))
;;
;;
;;(with :state "critical"
;;  (debug))
;;
;;
;;(with {:service "foo" :state "critical"}
;;  (debug))
;;
;;
;;(fixed-event-window {:size 3}
;;  (coll-rate
;;    (debug)))
;;
;;
;;(fixed-event-window {:size 5}
;;  (sflatten
;;    (info)))
;;
;;
;;(tag "foo"
;;  (info))
;;
;;
;;(tag ["foo" "bar"] (info))
;;
;;
;;(untag "foo" index)
;;
;;
;;(untag ["foo" "bar"] index)
;;
;;
;;(tagged-all "foo"
;;  (info))
;;
;;
;;(tagged-all ["foo" "bar"] (info))
;;
;;
;;(ddt
;; (info))
;;
;;
;;(scale 1000
;;  (info))
;;
;;
;;(split
;; [:> :metric 10] (debug)
;; [:> :metric 5] (info)
;; (error)
;;
;;
;; (throttle {:count 3 :duration 10}
;;   (error))
;;
;;
;; (moving-event-window {:size 5}
;;   (coll-mean (info))
;;
;;
;; (over 10
;;   (info))
;;
;;
;; (under 10
;;   (info))
;;
;;
;; (changed {:field :state :init "ok"})
;;
;;
;; (changed {:field [:nested :field] :init "ok"})
;;
;;
;; (project [[:= :service "enqueues"]
;;           [:= :service "dequeues"]]
;;   (coll-quotient
;;     (with :service "enqueues per dequeue"
;;       (info))))
;;
;;
;; (index [:host :service])
;;
;;
;; (fixed-time-window {:duration 60}
;;   (coll-count
;;     (debug)))
;;
;;
;; (sdissoc :host (info))
;;
;; (sdissoc [:environment :host] (info))
;;
;; (sdissoc [:environment [:nested :key] (info))
;;
;;
;; (fixed-event-window {:size 10}
;;   (coll-percentiles [0.5 0.75 0.98 0.99]))
;;
;;
;; (by {:fields [:host :service]}
;;     (fixed-time-window {:duration 60}))
;;
;;
;; (by {:fields [:host :service [:a :nested-key]
;;               :gc-interval 3600
;;               :fork-ttl 1800}
;;     (fixed-time-window {:duration 60}))
;;
;;
;; (reinject)
;;
;;
;; (reinject :foo)
;;
;;
;; (async-queue! :my-queue
;;   (info))
;;
;;
;; (where [:= :service "foo"]
;;   (tap :foo)
;;
;;
;; (with :my-field {"foo" "bar"}
;;  (json-fields [:my-field]))
;;
;;
;;(streams
;;  (stream {:name :fobar}
;;    (info))
;;  (stream {:name :foo}
;;    (info)))
;;
;;
;;(custom :my-custom-action ["parameters"]
;;  (info))
;;
;;
;;(reaper 5)
;;
;;
;;(reaper 5 :custom-stream)
;;
;;
;;(sdo
;;  ;; you can pass one field
;;  (to-base64 :host)
;;  ;; or a list of fields
;;  (to-base64 [:host :service]))
;;
;;
;;(sdo
;;  ;; you can pass one field
;;  (from-base64 :host)
;;  ;; or a list of fields
;;  (from-base64 [:host :service]))
;;
;;
;;(sformat "%s-foo-%s" :format-test [:host :service])
;;
;;
;;(publish! :my-channel)
;;
;;
;;(fixed-time-window {:duration 60}
;;  (coll-top 5
;;    (info)))
;;
;;
;;(fixed-time-window {:duration 60}
;;  (coll-bottom 5
;;    (info)))
;;
;;
;;(stable 10 :state
;;  (info))
;;
;;
;;(stable 10 [:nested :field]
;;  (info))
;;
;;
;;(rename-keys {:host :service
;;              :environment :env}
;;
;;
;;(keep-keys [:host :metric :time :environment :description]
;;  (info))
;;
;;
;;(keep-keys [:host :metric :time [:a :nested-key]]
;;  (info))
;;
;;
;;(sum {:duration 10}
;;  (info))
;;
;;
;;(sum {:duration 10 :delay 5}
;;  (info))
;;
;;
;;(top {:duration 10}
;;  (info))
;;
;;
;;(top {:duration 10 :delay 5}
;;  (info))
;;
;;
;;(bottom {:duration 10}
;;  (info))
;;
;;
;;(bottom {:duration 10 :delay 5}
;;  (info))
;;
;;
;;(mean {:duration 10}
;;  (info))
;;
;;
;;(mean {:duration 10 :delay 5}
;;  (info))
;;
;;
;;(fixed-time-window {:duration 60}
;;  (coll-max
;;    (info)))
;;
;;
;;(fixed-time-window {:duration 60 :delay 30}
;;  (coll-max
;;    (info)))
;;
;;
;;(ssort {:duration 10 :field :time}
;;  (info))
;;
;;
;;{:time 1} {:time 10} {:time 4} {:time 9} {:time 13} {:time 31}
;;
;;
;;{:time 1} {:time 4} {:time 9} {:time 10} {:time 13}
;;
;;
;;(ssort {:duration 10 :field :time :delay 10}
;;  (info))
;;
;;
;;[{:time 1 :metric 10} {:time 9 :metric 20} {:time 20 :metric 30}}
;;
;;
;;{:time 20 :metric 20}
;;
;;
;;(smax
;;  (info))
;;
;;
;;(smin
;;  (info))
;;
;;
;;{:time 1 :metric 10} {:time 2 :metric 3} {:time 3 :metric 11}
;;
;;
;;(extract :base-event
;;  (info))
