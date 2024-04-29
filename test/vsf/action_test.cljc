(ns vsf.action-test
  (:require
   [clojure.test :refer [deftest is]]
   [vsf.action :as sut]))


(deftest streams-test
  (is (= {:foo {:actions {:action      :sdo
                          :description {:message "Forward events to children"}
                          :children    [{:action      :increment
                                         :description {:message "Increment the :metric field"}
                                         :children    nil}]}}}
         (sut/streams
          (sut/stream
            {:name :foo}
            (sut/increment)))))
  (is (= {:foo {:actions {:action      :sdo
                          :description {:message "Forward events to children"}
                          :children    [{:action      :increment
                                         :description {:message "Increment the :metric field"}
                                         :children    nil}]}}
          :bar {:actions {:action      :sdo
                          :description {:message "Forward events to children"}
                          :children    [{:action      :decrement
                                         :description {:message "Decrement the :metric field"}
                                         :children    nil}
                                        {:action      :increment
                                         :description {:message "Increment the :metric field"}
                                         :children    nil}]}}}
         (sut/streams
          (sut/stream
            {:name :foo}
            (sut/increment))
          (sut/stream
            {:name :bar}
            (sut/decrement)
            (sut/increment))))))


(deftest custom-test
  (is (= {:action      :foo
          :params      []
          :description {:message "Use the custom action :foo", :params ""}
          :children    [{:action      :decrement
                         :description {:message "Decrement the :metric field"}
                         :children    nil}]}
         (sut/custom :foo nil
           (sut/decrement))))
  (is (= {:action      :foo
          :params      []
          :description {:message "Use the custom action :foo", :params "[]"}
          :children    [{:action      :decrement
                         :description {:message "Decrement the :metric field"}
                         :children    nil}]}
         (sut/custom :foo []
           (sut/decrement))))
  (is (= {:action      :foo
          :params      [:a 1 "a"]
          :description {:message "Use the custom action :foo", :params "[:a 1 \"a\"]"}
          :children    [{:action      :decrement
                         :description {:message "Decrement the :metric field"}
                         :children    nil}]}
         (sut/custom :foo [:a 1 "a"]
           (sut/decrement)))))


(deftest actions-output-test
  (is (= {:action      :where,
          :description {:message "Filter events based on the provided condition",
                        :params  "[:and [:= :host \"foo\"] [:> :metric 10]]"},
          :params      [[:and [:= :host "foo"] [:> :metric 10]]],
          :children    nil}
         (sut/where [:and
                     [:= :host "foo"]
                     [:> :metric 10]])))

  (is (= {:action      :fixed-time-window
          :children    '({:action      :coll-where
                          :children    nil
                          :description {:message "Filter a list of events based on the provided condition"
                                        :params  "[:and [:= :host \"foo\"] [:> :metric 10]]"}
                          :params      [[:and
                                         [:= :host "foo"]
                                         [:> :metric 10]]]})
          :description {:message "Build 60 seconds fixed time windows"}
          :params      [{:aggr-fn  :fixed-time-window
                         :duration 60}]}
         (sut/fixed-time-window {:duration 60}
           (sut/coll-where [:and
                            [:= :host "foo"]
                            [:> :metric 10]]))))

  (is (= {:action      :increment
          :children    '({:action      :index
                          :description {:message "Insert events into the index using the provided fields as keys"
                                        :params  "[:host]"}
                          :params      [[:host]]})
          :description {:message "Increment the :metric field"}}
         (sut/increment
          (sut/index [:host]))))

  (is (= {:action      :decrement
          :children    '({:action      :index
                          :description {:message "Insert events into the index using the provided fields as keys"
                                        :params  "[:host]"}
                          :params      [[:host]]})
          :description {:message "Decrement the :metric field"}}
         (sut/decrement
          (sut/index [:host]))))

  (is (= {:action      :increment
          :children    '({:action      :debug
                          :description {:message "Print the event in the logs as debug"}})
          :description {:message "Increment the :metric field"}}
         (sut/increment
          (sut/debug))))

  (is (= {:action      :increment
          :children    '({:action      :info
                          :description {:message "Print the event in the logs as info"}})
          :description {:message "Increment the :metric field"}}
         (sut/increment
          (sut/info))))

  (is (= {:action      :increment
          :children    '({:action      :debug
                          :description {:message "Print the event in the logs as debug"}})
          :description {:message "Increment the :metric field"}}
         (sut/increment
          (sut/debug))))

  (is (= {:action      :fixed-event-window
          :children    '({:action      :debug
                          :description {:message "Print the event in the logs as debug"}})
          :description {:message "Create a fixed event window of size 5"}
          :params      [{:size 5}]}
         (sut/fixed-event-window {:size 5}
           (sut/debug))))

  (is (= {:action      :fixed-event-window
          :children    '({:action      :coll-mean
                          :children    ({:action      :debug
                                         :description {:message "Print the event in the logs as debug"}})
                          :description {:message "Computes the mean of events"}})
          :description {:message "Create a fixed event window of size 10"}
          :params      [{:size 10}]}
         (sut/fixed-event-window {:size 10}
           (sut/coll-mean
            (sut/debug)))))

  (is (= {:action      :fixed-event-window
          :children    '({:action      :coll-max
                          :children    ({:action      :debug
                                         :description {:message "Print the event in the logs as debug"}})
                          :description {:message "Get the event with the biggest metric"}})
          :description {:message "Create a fixed event window of size 10"}
          :params      [{:size 10}]}
         (sut/fixed-event-window {:size 10}
           (sut/coll-max
            (sut/debug)))))

  (is (= {:action      :fixed-event-window
          :children    '({:action      :coll-sum
                          :children    ({:action      :debug
                                         :description {:message "Print the event in the logs as debug"}})
                          :description {:message "Get the event with the biggest metric"}})
          :description {:message "Create a fixed event window of size 10"}
          :params      [{:size 10}]}
         (sut/fixed-event-window {:size 10}
           (sut/coll-sum
            (sut/debug)))))

  (is (= {:action      :fixed-event-window
          :children    '({:action      :coll-min
                          :children    ({:action      :debug
                                         :description {:message "Print the event in the logs as debug"}})
                          :description {:message "Get the event with the smallest metric"}})
          :description {:message "Create a fixed event window of size 10"}
          :params      [{:size 10}]}
         (sut/fixed-event-window {:size 10}
           (sut/coll-min
            (sut/debug)))))

  (is (= {:action      :fixed-event-window
          :children    '({:action      :coll-sort
                          :children    ({:action      :debug
                                         :description {:message "Print the event in the logs as debug"}})
                          :description {:message "Sort events based on the field :time"}
                          :params      [:time]})
          :description {:message "Create a fixed event window of size 10"}
          :params      [{:size 10}]}
         (sut/fixed-event-window {:size 10}
           (sut/coll-sort :time
             (sut/debug)))))

  (is (= {:action      :sdo
          :children    '({:action      :increment
                          :children    nil
                          :description {:message "Increment the :metric field"}}
                         {:action      :decrement
                          :children    nil
                          :description {:message "Decrement the :metric field"}})
          :description {:message "Forward events to children"}}
         (sut/sdo
          (sut/increment)
          (sut/decrement))))

  (is (= {:action      :expired
          :children    '({:action      :increment
                          :children    nil
                          :description {:message "Increment the :metric field"}})
          :description {:message "Keep expired events"}}
         (sut/expired
          (sut/increment))))

  (is (= {:action      :not-expired
          :children    '({:action      :increment
                          :children    nil
                          :description {:message "Increment the :metric field"}})
          :description {:message "Remove expired events"}}
         (sut/not-expired
          (sut/increment))))

  (is (= {:action      :above-dt
          :children    '({:action      :debug
                          :description {:message "Print the event in the logs as debug"}})
          :description {:message "Keep events if :metric is greater than 100 during 10 seconds"}
          :params      [[:> :metric 100] 10]}
         (sut/above-dt {:threshold 100 :duration 10}
           (sut/debug))))

  (is (= {:action      :below-dt
          :children    '({:action      :debug
                          :description {:message "Print the event in the logs as debug"}})
          :description {:message "Keep events if :metric is lower than 100 during 10 seconds"}
          :params      [[:< :metric 100] 10]}
         (sut/below-dt {:threshold 100 :duration 10}
           (sut/debug))))

  (is (= {:action      :between-dt
          :children    '({:action      :debug
                          :description {:message "Print the event in the logs as debug"}})
          :description {:message "Keep events if :metric is between 50 and 100 during 10 seconds"}
          :params      [[:and
                         [:> :metric 50]
                         [:< :metric 100]]
                        10]}
         (sut/between-dt {:low 50 :high 100 :duration 10}
           (sut/debug))))

  (is (= {:action      :outside-dt
          :children    '({:action      :debug
                          :description {:message "Print the event in the logs as debug"}})
          :description {:message "Keep events if :metric is outside 50 and 100 during 10 seconds"}
          :params      [[:or
                         [:< :metric 50]
                         [:> :metric 100]]
                        10]}
         (sut/outside-dt {:low 50 :high 100 :duration 10}
           (sut/debug))))

  (is (= {:action      :critical-dt
          :children    '({:action      :debug
                          :description {:message "Print the event in the logs as debug"}})
          :description {:message "Keep events if the state is critical for more than 10 seconds"}
          :params      [[:= :state "critical"]
                        10]}
         (sut/critical-dt {:duration 10}
           (sut/debug))))

  (is (= {:action      :critical
          :children    '({:action      :error
                          :description {:message "Print the event in the logs as error"}})
          :description {:message "Keep critical events"}}
         (sut/critical
          (sut/error))))

  (is (= {:action      :warning
          :children    '({:action      :warning
                          :children    nil
                          :description {:message "Keep warning events"}})
          :description {:message "Keep warning events"}}
         (sut/warning
          (sut/warning))))

  (is (= {:action      :default
          :children    '({:action      :info
                          :description {:message "Print the event in the logs as info"}})
          :description {:message "Set (if nil) :state to ok"}
          :params      [:state "ok"]}
         (sut/default :state "ok"
           (sut/info))))

  (is (= {:action      :coalesce
          :children    '({:action      :debug
                          :description {:message "Print the event in the logs as debug"}})
          :description {:message "Returns a list of the latest non-expired events for each fields (10) combinations, every [:host :service] seconds"}
          :params      [{:duration 10
                         :fields   [:host :service]}]}
         (sut/coalesce {:duration 10 :fields [:host :service]}
           (sut/debug))))

  (is (= {:action      :coalesce
          :children    '({:action      :debug
                          :description {:message "Print the event in the logs as debug"}})
          :description {:message "Returns a list of the latest non-expired events for each fields (10) combinations, every [:host [:nested :field]] seconds"}
          :params      [{:duration 10
                         :fields   [:host
                                    [:nested
                                     :field]]}]}
         (sut/coalesce {:duration 10 :fields [:host [:nested :field]]}
           (sut/debug))))

  (is (= {:action      :with
          :children    '({:action      :debug
                          :description {:message "Print the event in the logs as debug"}})
          :description {:message "Set the field :state to critical"}
          :params      [{:state "critical"}]}
         (sut/with :state "critical"
           (sut/debug))))

  (is (= {:action      :with
          :children    '({:action      :debug
                          :description {:message "Print the event in the logs as debug"}})
          :description {:message "Merge the events with the provided fields"
                        :params  "{:service \"foo\", :state \"critical\"}"}
          :params      [{:service "foo"
                         :state   "critical"}]}
         (sut/with {:service "foo" :state "critical"}
           (sut/debug))))

  (is (= {:action      :fixed-event-window
          :children    '({:action      :coll-rate
                          :children    ({:action      :debug
                                         :description {:message "Print the event in the logs as debug"}})
                          :description {:message "Takes a list of events and computes their rates"}})
          :description {:message "Create a fixed event window of size 3"}
          :params      [{:size 3}]}
         (sut/fixed-event-window {:size 3}
           (sut/coll-rate
            (sut/debug)))))

  (is (= {:action      :fixed-event-window
          :children    '({:action      :sflatten
                          :children    ({:action      :info
                                         :description {:message "Print the event in the logs as info"}})
                          :description {:message "Send events from a list downstream one by one"}})
          :description {:message "Create a fixed event window of size 5"}
          :params      [{:size 5}]}
         (sut/fixed-event-window {:size 5}
           (sut/sflatten
            (sut/info)))))

  (is (= {:action      :tag
          :children    '({:action      :info
                          :description {:message "Print the event in the logs as info"}})
          :description {:message "Tag events with %sfoo"}
          :params      ["foo"]}
         (sut/tag "foo"
           (sut/info))))

  (is (= {:action      :tag
          :children    '({:action      :info
                          :description {:message "Print the event in the logs as info"}})
          :description {:message "Tag events with %s[\"foo\" \"bar\"]"}
          :params      [["foo"
                         "bar"]]}
         (sut/tag ["foo" "bar"]
           (sut/info))))

  (is (= {:action      :untag
          :children    nil
          :description {:message "Remove tags foo"}
          :params      ["foo"]}
         (sut/untag "foo")))

  (is (= {:action      :untag
          :children    nil
          :description {:message "Remove tags [\"foo\" \"bar\"]"}
          :params      [["foo"
                         "bar"]]}
         (sut/untag ["foo" "bar"])))

  (is (= {:action      :tagged-all
          :children    '({:action      :info
                          :description {:message "Print the event in the logs as info"}})
          :description {:message "Keep only events with tagged foo"}
          :params      ["foo"]}
         (sut/tagged-all "foo"
           (sut/info))))

  (is (= {:action      :tagged-all
          :children    '({:action      :info
                          :description {:message "Print the event in the logs as info"}})
          :description {:message "Keep only events with tagged [\"foo\" \"bar\"]"}
          :params      [["foo"
                         "bar"]]}
         (sut/tagged-all ["foo" "bar"]
           (sut/info))))

  (is (= {:action      :ddt
          :children    '({:action      :info
                          :description {:message "Print the event in the logs as info"}})
          :description {:message "Differentiate metrics with respect to time"}
          :params      [false]}
         (sut/ddt
          (sut/info))))

  (is (= {:action      :scale
          :children    '({:action      :info
                          :description {:message "Print the event in the logs as info"}})
          :description {:message "Multiples the :metric field by 1000"}
          :params      [1000]}
         (sut/scale 1000
           (sut/info))))

  (is (= {:action      :split
          :children    [{:action      :debug
                         :description {:message "Print the event in the logs as debug"}}
                        {:action      :info
                         :description {:message "Print the event in the logs as info"}}
                        {:action      :error
                         :description {:message "Print the event in the logs as error"}}]
          :description {:message "Split metrics by the clauses provided as parameter"
                        :params  [[:> :metric 10]
                                  [:> :metric 5]
                                  [:always-true]]}
          :params      [[[:> :metric 10]
                         [:> :metric 5]
                         [:always-true]]]}
         (sut/split
          [:> :metric 10] (sut/debug)
          [:> :metric 5] (sut/info)
          (sut/error))))

  (is (= {:action      :throttle
          :children    '({:action      :error
                          :description {:message "Print the event in the logs as error"}})
          :description {:message "Let 3 events pass at most every 10 seconds"}
          :params      [{:count    3
                         :duration 10}]}
         (sut/throttle {:count 3 :duration 10}
           (sut/error))))

  (is (= {:action      :moving-event-window
          :children    '({:action      :coll-mean
                          :children    ({:action      :info
                                         :description {:message "Print the event in the logs as info"}})
                          :description {:message "Computes the mean of events"}})
          :description {:message "Build moving event window of size 5"}
          :params      [{:size 5}]}
         (sut/moving-event-window {:size 5}
           (sut/coll-mean (sut/info)))))

  (is (= {:action      :over
          :children    '({:action      :info
                          :description {:message "Print the event in the logs as info"}})
          :description {:message "Keep events with metrics greater than 10"}
          :params      [10]}
         (sut/over 10
           (sut/info))))

  (is (= {:action      :under
          :children    '({:action      :info
                          :description {:message "Print the event in the logs as info"}})
          :description {:message "Keep events with metrics under than 10"}
          :params      [10]}
         (sut/under 10
           (sut/info))))

  (is (= {:action      :changed
          :children    nil
          :description {:message "Passes on events only if the field :state differs from the previous one (default ok)"}
          :params      [{:field :state
                         :init  "ok"}]}
         (sut/changed {:field :state :init "ok"})))

  (is (= {:action      :changed
          :children    nil
          :description {:message "Passes on events only if the field [:nested :field] differs from the previous one (default ok)"}
          :params      [{:field [:nested
                                 :field]
                         :init  "ok"}]}
         (sut/changed {:field [:nested :field] :init "ok"})))

  (is (= {:action      :project
          :children    '({:action      :coll-quotient
                          :children    ({:action      :with
                                         :children    nil
                                         :description {:message "Set the field :service to enqueues per dequeue"}
                                         :params      [{:service "enqueues per dequeue"}]}
                                        {:action      :info
                                         :description {:message "Print the event in the logs as info"}})
                          :description {:message "Get the event with the biggest metric"}})
          :description {:message "return the most recent events matching the conditions"
                        :params  "[[:= :service \"enqueues\"] [:= :service \"dequeues\"]]"}
          :params      [[[:= :service "enqueues"]
                         [:= :service "dequeues"]]]}
         (sut/project [[:= :service "enqueues"]
                       [:= :service "dequeues"]]
           (sut/coll-quotient
            (sut/with :service "enqueues per dequeue")
            (sut/info)))))

  (is (= {:action      :index
          :description {:message "Insert events into the index using the provided fields as keys"
                        :params  "[:host :service]"}
          :params      [[:host :service]]}
         (sut/index [:host :service])))

  (is (= {:action      :fixed-time-window
          :children    '({:action      :coll-count
                          :children    ({:action      :debug
                                         :description {:message "Print the event in the logs as debug"}})
                          :description {:message "Count the number of events"}})
          :description {:message "Build 60 seconds fixed time windows"}
          :params      [{:aggr-fn  :fixed-time-window
                         :duration 60}]}
         (sut/fixed-time-window {:duration 60}
           (sut/coll-count
            (sut/debug)))))

  (is (= {:action      :sdissoc
          :children    '({:action      :info
                          :description {:message "Print the event in the logs as info"}})
          :description {:message "Remove key(s) :host from events"}
          :params      [[:host]]}
         (sut/sdissoc :host
           (sut/info))))

  (is (= {:action      :sdissoc
          :children    '({:action      :info
                          :description {:message "Print the event in the logs as info"}})
          :description {:message "Remove key(s) [:environment :host] from events"}
          :params      [[:environment :host]]}
         (sut/sdissoc [:environment :host]
           (sut/info))))

  (is (= {:action      :sdissoc
          :children    '({:action      :info
                          :description {:message "Print the event in the logs as info"}})
          :description {:message "Remove key(s) [:environment [:nested :key]] from events"}
          :params      [[:environment
                         [:nested :key]]]}
         (sut/sdissoc [:environment [:nested :key]]
           (sut/info))))

  (is (= {:action      :fixed-event-window
          :children    '({:action      :coll-percentiles
                          :children    nil
                          :description {:message "Computes percentiles for quantiles [0.5 0.75 0.98 0.99]"}
                          :params      [[0.5
                                         0.75
                                         0.98
                                         0.99]]})
          :description {:message "Create a fixed event window of size 10"}
          :params      [{:size 10}]}
         (sut/fixed-event-window {:size 10}
           (sut/coll-percentiles [0.5 0.75 0.98 0.99]))))

  (is (= {:action      :by
          :children    '({:action      :fixed-time-window
                          :children    nil
                          :description {:message "Build 60 seconds fixed time windows"}
                          :params      [{:aggr-fn  :fixed-time-window
                                         :duration 60}]})
          :description {:message "Split streams by field(s) [:host :service]"}
          :params      [{:fields [:host
                                  :service]}]}
         (sut/by {:fields [:host :service]}
           (sut/fixed-time-window {:duration 60}))))

  (is (= {:action      :by
          :children    '({:action      :fixed-time-window
                          :children    nil
                          :description {:message "Build 60 seconds fixed time windows"}
                          :params      [{:aggr-fn  :fixed-time-window
                                         :duration 60}]})
          :description {:message "Split streams by field(s) [:host :service [:a :nested-key]]"}
          :params      [{:fields      [:host
                                       :service
                                       [:a
                                        :nested-key]]
                         :fork-ttl    1800
                         :gc-interval 3600}]}
         (sut/by {:fields      [:host :service [:a :nested-key]]
                  :gc-interval 3600
                  :fork-ttl    1800}
           (sut/fixed-time-window {:duration 60}))))

  (is (= {:action      :reinject!
          :description {:message "Reinject events on the current stream"}
          :params      [nil]}
         (sut/reinject!)))

  (is (= {:action      :reinject!
          :description {:message "Reinject events on stream :foo"}
          :params      [:foo]}
         (sut/reinject! :foo)))

  (is (= {:action      :where
          :children    '({:action      :tap
                          :description {:message "Save events into the tap :foo"}
                          :params      [:foo]})
          :description {:message "Filter events based on the provided condition"
                        :params  "[:= :service \"foo\"]"}
          :params      [[:= :service "foo"]]}
         (sut/where [:= :service "foo"]
           (sut/tap :foo))))

  (is (= {:action      :with
          :children    '({:action      :json-fields
                          :children    nil
                          :description {:message "Parse the provided fields from json to edn"
                                        :params  "[:my-field]"}
                          :params      [[:my-field]]})
          :description {:message "Set the field :my-field to {\"foo\" \"bar\"}"}
          :params      [{:my-field {"foo" "bar"}}]}
         (sut/with :my-field {"foo" "bar"}
           (sut/json-fields [:my-field]))))

  (is (= {:fobar {:actions {:action      :sdo
                            :children    '({:action      :info
                                            :description {:message "Print the event in the logs as info"}})
                            :description {:message "Forward events to children"}}}
          :foo   {:actions {:action      :sdo
                            :children    '({:action      :info
                                            :description {:message "Print the event in the logs as info"}})
                            :description {:message "Forward events to children"}}}}
         (sut/streams
          (sut/stream {:name :fobar}
            (sut/info))
          (sut/stream {:name :foo}
            (sut/info)))))

  (is (= {:action      :my-custom-action
          :children    '({:action      :info
                          :description {:message "Print the event in the logs as info"}})
          :description {:message "Use the custom action :my-custom-action"
                        :params  "[\"parameters\"]"}
          :params      ["parameters"]}
         (sut/custom :my-custom-action ["parameters"]
           (sut/info))))

  (is (= {:action      :reaper
          :children    []
          :description {:message "Expires events every 5 second and reinject them into the current stream"}
          :params      [5 nil]}
         (sut/reaper 5)))

  (is (= {:action      :reaper
          :children    []
          :description {:message "Expires events every 5 second and reinject them into the stream :custom-stream"}
          :params      [5 :custom-stream]}
         (sut/reaper 5 :custom-stream)))

  (is (= {:action      :sdo
          :children    '({:action      :to-base64
                          :children    nil
                          :description {:message "Encodes field(s) :host to base64"}
                          :params      [[:host]]}
                         {:action      :to-base64
                          :children    nil
                          :description {:message "Encodes field(s) [:host :service] to base64"}
                          :params      [[:host :service]]})
          :description {:message "Forward events to children"}}
         (sut/sdo
          ;; you can pass one field
          (sut/to-base64 :host)
          ;; or a list of fields
          (sut/to-base64 [:host :service]))))

  (is (= {:action      :sdo
          :children    '({:action      :from-base64
                          :children    nil
                          :description {:message "Decodes field(s) :host from base64"}
                          :params      [[:host]]}
                         {:action      :from-base64
                          :children    nil
                          :description {:message "Decodes field(s) [:host :service] from base64"}
                          :params      [[:host :service]]})
          :description {:message "Forward events to children"}}
         (sut/sdo
          ;; you can pass one field
          (sut/from-base64 :host)
          ;; or a list of fields
          (sut/from-base64 [:host :service]))))

  (is (= {:action      :sformat
          :children    nil
          :description {:message "Set :format-test to value %s-foo-%s using fields [:host :service]"}
          :params      ["%s-foo-%s" :format-test [:host :service]]}
         (sut/sformat "%s-foo-%s" :format-test [:host :service])))

  (is (= {:action      :publish!
          :children    []
          :description {:message "Publish events into the channel :my-channel"}
          :params      [:my-channel]}
         (sut/publish! :my-channel)))

  (is (= {:action      :fixed-time-window
          :children    '({:action      :coll-top
                          :children    ({:action      :info
                                         :description {:message "Print the event in the logs as info"}})
                          :description {:message "Returns top 5 events with the highest metrics"}
                          :params      [5]})
          :description {:message "Build 60 seconds fixed time windows"}
          :params      [{:aggr-fn  :fixed-time-window
                         :duration 60}]}
         (sut/fixed-time-window {:duration 60}
           (sut/coll-top 5
             (sut/info)))))

  (is (= {:action      :fixed-time-window
          :children    '({:action      :coll-bottom
                          :children    ({:action      :info
                                         :description {:message "Print the event in the logs as info"}})
                          :description {:message "Returns bottom 5 events with the lowest metrics"}
                          :params      [5]})
          :description {:message "Build 60 seconds fixed time windows"}
          :params      [{:aggr-fn  :fixed-time-window
                         :duration 60}]}
         (sut/fixed-time-window {:duration 60}
           (sut/coll-bottom 5
             (sut/info)))))

  (is (= {:action      :stable
          :children    '({:action      :info
                          :description {:message "Print the event in the logs as info"}})
          :description {:message "Returns events where the field :state is stable for more than 10 seconds"}
          :params      [10 :state]}
         (sut/stable 10 :state
           (sut/info))))

  (is (= {:action      :stable
          :children    '({:action      :info
                          :description {:message "Print the event in the logs as info"}})
          :description {:message "Returns events where the field [:nested :field] is stable for more than 10 seconds"}
          :params      [10
                        [:nested :field]]}
         (sut/stable 10 [:nested :field]
           (sut/info))))

  (is (= {:action      :rename-keys
          :children    nil
          :description {:message "Rename events keys"
                        :params  "{:host :service, :environment :env}"}
          :params      [{:environment :env
                         :host        :service}]}
         (sut/rename-keys {:host        :service
                           :environment :env})))

  (is (= {:action      :keep-keys
          :children    '({:action      :info
                          :description {:message "Print the event in the logs as info"}})
          :description {:message "Keep only the specified keys from events"
                        :params  "[:host :metric :time :environment :description]"}
          :params      [[:host :metric :time :environment :description]]}
         (sut/keep-keys [:host :metric :time :environment :description]
           (sut/info))))

  (is (= {:action      :keep-keys
          :children    '({:action      :info
                          :description {:message "Print the event in the logs as info"}})
          :description {:message "Keep only the specified keys from events"
                        :params  "[:host :metric :time [:a :nested-key]]"}
          :params      [[:host :metric :time [:a :nested-key]]]}
         (sut/keep-keys [:host :metric :time [:a :nested-key]]
           (sut/info))))

  (is (= {:action      :sum
          :children    '({:action      :info
                          :description {:message "Print the event in the logs as info"}})
          :description {:message "Sum the events field from the last 10 seconds"}
          :params      [{:aggr-fn  :+
                         :duration 10}]}
         (sut/sum {:duration 10}
           (sut/info))))

  (is (= {:action      :sum
          :children    '({:action      :info
                          :description {:message "Print the event in the logs as info"}})
          :description {:message "Sum the events field from the last 10 seconds"}
          :params      [{:aggr-fn  :+
                         :delay    5
                         :duration 10}]}
         (sut/sum {:duration 10 :delay 5}
           (sut/info))))

  (is (= {:action      :top
          :children    '({:action      :info
                          :description {:message "Print the event in the logs as info"}})
          :description {:message "Get the max event from the last 10 seconds"}
          :params      [{:aggr-fn  :max
                         :duration 10}]}
         (sut/top {:duration 10}
           (sut/info))))

  (is (= {:action      :top
          :children    '({:action      :info
                          :description {:message "Print the event in the logs as info"}})
          :description {:message "Get the max event from the last 10 seconds"}
          :params      [{:aggr-fn  :max
                         :delay    5
                         :duration 10}]}
         (sut/top {:duration 10 :delay 5}
           (sut/info))))

  (is (= {:action      :bottom
          :children    '({:action      :info
                          :description {:message "Print the event in the logs as info"}})
          :description {:message "Get the min event from the last 10 seconds"}
          :params      [{:aggr-fn  :min
                         :duration 10}]}
         (sut/bottom {:duration 10}
           (sut/info))))

  (is (= {:action      :bottom
          :children    '({:action      :info
                          :description {:message "Print the event in the logs as info"}})
          :description {:message "Get the min event from the last 10 seconds"}
          :params      [{:aggr-fn  :min
                         :delay    5
                         :duration 10}]}
         (sut/bottom {:duration 10 :delay 5}
           (sut/info))))

  (is (= {:action      :mean
          :children    '({:action      :info
                          :description {:message "Print the event in the logs as info"}})
          :description {:message "Get the min of events from the last 10 seconds"}
          :params      [{:aggr-fn  :mean
                         :duration 10}]}
         (sut/mean {:duration 10}
           (sut/info))))

  (is (= {:action      :mean
          :children    '({:action      :info
                          :description {:message "Print the event in the logs as info"}})
          :description {:message "Get the min of events from the last 10 seconds"}
          :params      [{:aggr-fn  :mean
                         :delay    5
                         :duration 10}]}
         (sut/mean {:duration 10 :delay 5}
           (sut/info))))

  (is (= {:action      :fixed-time-window
          :children    '({:action      :coll-max
                          :children    ({:action      :info
                                         :description {:message "Print the event in the logs as info"}})
                          :description {:message "Get the event with the biggest metric"}})
          :description {:message "Build 60 seconds fixed time windows"}
          :params      [{:aggr-fn  :fixed-time-window
                         :duration 60}]}
         (sut/fixed-time-window {:duration 60}
           (sut/coll-max
            (sut/info)))))

  (is (= {:action      :fixed-time-window
          :children    '({:action      :coll-max
                          :children    ({:action      :info
                                         :description {:message "Print the event in the logs as info"}})
                          :description {:message "Get the event with the biggest metric"}})
          :description {:message "Build 60 seconds fixed time windows"}
          :params      [{:aggr-fn  :fixed-time-window
                         :delay    30
                         :duration 60}]}
         (sut/fixed-time-window {:duration 60 :delay 30}
           (sut/coll-max
            (sut/info)))))

  (is (= {:action      :ssort
          :children    '({:action      :info
                          :description {:message "Print the event in the logs as info"}})
          :description {:message "Sort events during 10 seconds based on the field :time"}
          :params      [{:aggr-fn  :ssort
                         :duration 10
                         :field    :time
                         :nested?  false}]}
         (sut/ssort {:duration 10 :field :time}
           (sut/info))))

  (is (= {:action      :ssort
          :children    '({:action      :info
                          :description {:message "Print the event in the logs as info"}})
          :description {:message "Sort events during 10 seconds based on the field :time"}
          :params      [{:aggr-fn  :ssort
                         :delay    10
                         :duration 10
                         :field    :time
                         :nested?  false}]}
         (sut/ssort {:duration 10 :field :time :delay 10}
           (sut/info))))

  (is (= {:action      :smax
          :children    '({:action      :info
                          :description {:message "Print the event in the logs as info"}})
          :description {:message "Send downstream the event with the biggest :metric every time it receives an event"}}
         (sut/smax
          (sut/info))))

  (is (= {:action      :smin
          :children    '({:action      :info
                          :description {:message "Print the event in the logs as info"}})
          :description {:message "Send downstream the event with the lowest :metric every time it receives an event"}}
         (sut/smin
          (sut/info))))

  (is (= {:action      :smin
          :children    '({:action      :info
                          :description {:message "Print the event in the logs as info"}})
          :description {:message "Extract the key :base-event from the event and send its value downstream"}
          :params      [:base-event]}
         (sut/extract :base-event
           (sut/info)))))


(deftest format-paths-params-test
  (is (= [:host :service]
         (sut/format-paths-params "host, service")))

  (is (= [:host [:service]]
         (sut/format-paths-params "host, [service]")))

  (is (= [:host [:service :ip] :url]
         (sut/format-paths-params "host, [service, ip], url")))

  (is (= []
         (sut/format-paths-params "")))

  (is (= [:host]
         (sut/format-paths-params "host"))))


(deftest parse-paths-params-test
  (is (= "host,service"
         (sut/parse-paths-params [:host :service])))

  (is (= "host"
         (sut/parse-paths-params ["host"])))

  (is (= ""
         (sut/parse-paths-params [])))

  (is (= "[host,service]"
         (sut/parse-paths-params [[:host :service]])))

  (is (= "host,[service,ip],url"
         (sut/parse-paths-params [:host [:service :ip] :url]))))


(deftest parse-keywords-params-test
  (is (= "foo"
         (sut/parse-keywords-params {:params [[:foo]]})))

  (is (= "bar"
         (sut/parse-keywords-params {:params [[:bar]]})))

  (is (= ""
         (sut/parse-keywords-params {:params [[]]}))))


(deftest format-keywords-params-test
  (is (= [:foo :bar]
         (sut/format-keywords-params "foo, bar")))

  (is (= [:baz :qux]
         (sut/format-keywords-params "baz, qux")))

  (is (= []
         (sut/format-keywords-params ""))))


(deftest parse-map-params-test
  (is (= {:foo "bar"}
         (sut/parse-map-params {:params [{:foo "bar"}]})))

  (is (= {:baz "qux"}
         (sut/parse-map-params {:params [{:baz "qux"}]})))

  (is (= {}
         (sut/parse-map-params {:params [{}]}))))


(deftest parse-string-params-test
  (is (= "foo"
         (sut/parse-string-params {:params ["foo"]})))

  (is (= "bar"
         (sut/parse-string-params {:params ["bar"]})))

  (is (= ""
         (sut/parse-string-params {:params [""]}))))


(deftest format-percentiles-params-test
  (is (= [0.5 0.95]
         (sut/format-percentiles-params "0.5,0.95")))

  (is (= [0.5]
         (sut/format-percentiles-params "0.5")))

  (is (= []
         (sut/format-percentiles-params ""))))
