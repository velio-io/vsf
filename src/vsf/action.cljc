(ns vsf.action
  (:refer-clojure :exclude [format])
  (:require
   [vsf.spec]
   [clojure.string :as string]
   #?(:cljs [goog.string :as gstring])
   #?(:cljs [goog.string.format])
   #?(:cljs [cljs.reader])
   #?(:clj [clojure.tools.logging :as log])
   #?(:clj [exoscale.ex :as ex])))


(def format
  #?(:clj  clojure.core/format
     :cljs gstring/format))


(def read-string
  #?(:clj  clojure.core/read-string
     :cljs cljs.reader/read-string))


(def parse-string-params
  (comp str first :params))


(def parse-keyword-params
  (comp name first :params))


(defn parse-keywords-params [{:keys [params]}]
  (->> (first params)
       (map name)
       (string/join ",")))


(defn format-keywords-params [keywords-string]
  (->> (string/split keywords-string #",")
       (remove string/blank?)
       (mapv (comp keyword string/trim))))


(defn parse-map-params [{:keys [params]}]
  (first params))


(defn format-paths-params [paths]
  ;; split strings only by a top level comma
  (->> (string/split paths #",(?![^\[\]]*])")
       (remove string/blank?)
       (mapv (fn [f]
               (let [path (read-string f)]
                 (if (vector? path)
                   (mapv keyword path)
                   (keyword path)))))))


(defn parse-paths-params [paths]
  (->> paths
       (mapv (fn [path]
               (if (vector? path)
                 (let [path-string (->> (mapv name path)
                                        (string/join ","))]
                   (str "[" path-string "]"))
                 (name path))))
       (string/join ",")))


(defn parse-paths-single-param [{:keys [params]}]
  (let [paths (first params)]
    (parse-paths-params paths)))


(defn format-fields-params [{:keys [fields] :as config}]
  (let [fields (format-paths-params fields)]
    (assoc config :fields fields)))


(defn parse-fields-params [{:keys [params]}]
  (let [config (first params)
        fields (parse-paths-params (:fields config))]
    (assoc config :fields fields)))


(defn format-keyvals-params [kv-map]
  (reduce-kv
   (fn [acc k v]
     (assoc acc (keyword k) v))
   {}
   kv-map))


(defn parse-keyvals-params [{:keys [params]}]
  (reduce-kv
   (fn [acc k v]
     (assoc acc (name k) v))
   {}
   (first params)))


(defn valid-action? [action-name spec value]
  (try
    (vsf.spec/assert-spec-valid spec value)
    (catch #?(:clj Exception :cljs js/Error) e
      (let [data #?(:clj (ex-data e)
                    :cljs (.-cause e))
            message      (format "Invalid call to action '%s' with parameters '%s'. Explanation: %s"
                                 (name action-name)
                                 (pr-str value)
                                 (:explain-data data))]
        #?(:clj  (log/error message)
           :cljs (js/console.error message))
        (throw e)))))


(defn sdo
  "Send events to children. useful when you want to send the same
  events to multiple downstream actions.
  ```clojure
  (sdo
    (increment)
    (decrement))
  ```
  Here, events arriving in sdo will be forwarded to both increment and
  decrement."
  {:control-type :no-args}
  [& children]
  {:action      :sdo
   :description {:message "Forward events to children"}
   :children    children})


(defn stream
  "Creates a new stream. This action takes a map where the `:name` key, which
  will be the name of the stream, is mandatory."
  [config & children]
  (-> (assoc config :actions (apply sdo children))))


(defn streams
  "Entrypoint for all streams.
  ```clojure
  (streams
    (stream {:name :foobar}
      (info))
    (stream {:name :foo}
      (info)))
  ```"
  [& streams]
  (reduce
   (fn [state stream-config]
     (assoc state (:name stream-config) (dissoc stream-config :name)))
   {}
   streams))


(defn where
  "Filter events based on conditions.
   Each condition is a vector composed of the function to apply on the field,
   the field to extract from the event, and the event itself.
   Multiple conditions can be added by using `:or` or `:and`.
   ```clojure
   (where [:= :metric 4])
   ```
   Here, we keep only events where the :metric field is equal to 4.
   ```clojure
   (where [:and [:= :host \"foo\"]
                [:> :metric 10]])
  ```
   Here, we keep only events with :host = foo and with :metric > 10"
  {:control-type   :code
   :control-params {:format 'vsf.action/read-string
                    :parse  'vsf.action/parse-string-params}}
  [conditions & children]
  (valid-action? :where vsf.spec/where [conditions])
  {:action      :where
   :description {:message "Filter events based on the provided condition"
                 :params  (pr-str conditions)}
   :params      [conditions]
   :children    children})


(defn coll-where
  "Like `where` but should receive a list of events.
  ```clojure
  (fixed-time-window {:duration 60}
    (coll-where [:and [:= :host \"foo\"]
                      [:> :metric 10]))
  ```"
  {:control-type   :code
   :control-params {:format 'vsf.action/read-string
                    :parse  'vsf.action/parse-string-params}}
  [conditions & children]
  (valid-action? :coll-where vsf.spec/coll-where [conditions])
  {:action      :coll-where
   :description {:message "Filter a list of events based on the provided condition"
                 :params  (pr-str conditions)}
   :params      [conditions]
   :children    children})


(defn increment
  "Increment the event :metric field.
  ```clojure
  (increment
    (index [:host]))
  ```"
  {:control-type :no-args}
  [& children]
  {:action      :increment
   :description {:message "Increment the :metric field"}
   :children    children})


(defn decrement
  "Decrement the event :metric field.
  ```clojure
  (decrement
    (index [:host]))
  ```"
  {:control-type :no-args}
  [& children]
  {:action      :decrement
   :description {:message "Decrement the :metric field"}
   :children    children})


(defn debug
  "Print the event in the logs using the debug level
  ```clojure
  (increment
    (debug))
  ```"
  {:control-type :no-args
   :leaf-action  true}
  []
  {:action      :debug
   :description {:message "Print the event in the logs as debug"}})


(defn info
  "Print the event in the logs using the info level
  ```clojure
  (increment
    (info))
  ```"
  {:control-type :no-args
   :leaf-action  true}
  []
  {:action      :info
   :description {:message "Print the event in the logs as info"}})


(defn error
  "Print the event in the logs using the error level
  ```clojure
  (increment
    (debug))
  ```"
  {:control-type :no-args
   :leaf-action  true}
  []
  {:action      :error
   :description {:message "Print the event in the logs as error"}})


(defn fixed-event-window
  "Returns a fixed-sized window of events.
  ```clojure
  (fixed-event-window {:size 5}
    (debug))
  ```
  This example will return a vector events partitioned 5 by 5."
  {:control-type   :map
   :control-params {:fields [{:field :size :label "Size" :type :number}]
                    :parse  'vsf.action/parse-map-params}}
  [config & children]
  (valid-action? :fixed-event-window vsf.spec/fixed-event-window [config])
  {:action      :fixed-event-window
   :description {:message (format "Create a fixed event window of size %d"
                                  (:size config))}
   :params      [config]
   :children    children})


(defn coll-mean
  "Computes the events mean (on metric).
  Should receive a list of events from the previous stream.
  The most recent event is used as a base to create the new event
  ```clojure
  (fixed-event-window {:size 10}
    (coll-mean
      (debug)))
  ```
  Computes the mean on windows of 10 events"
  {:control-type :no-args}
  [& children]
  {:action      :coll-mean
   :description {:message "Computes the mean of events"}
   :children    children})


(defn coll-max
  "Returns the event with the biggest metric.
  Should receive a list of events from the previous stream.
  ```clojure
  (fixed-event-window {:size 10}
    (coll-max
      (debug)))
  ```
  Get the event the biggest metric on windows of 10 events.
  Check `top` and `smax` as well."
  {:control-type :no-args}
  [& children]
  {:action      :coll-max
   :description {:message "Get the event with the biggest metric"}
   :children    children})


(defn coll-quotient
  "Divide the first event `:metrìc` field by all subsequent events `:metric`.
  Return a new event containing the new `:metric`.
  Should receive a list of events from the previous stream."
  {:control-type :no-args}
  [& children]
  {:action      :coll-quotient
   :description {:message "Get the event with the biggest metric"}
   :children    children})


(defn coll-sum
  "Sum all the events :metric fields
  Should receive a list of events from the previous stream.
  ```clojure
  (fixed-event-window {:size 10}
    (coll-sum
      (debug)))
  ```
  Sum all :metric fields for windows of 10 events"
  {:control-type :no-args}
  [& children]
  {:action      :coll-sum
   :description {:message "Get the event with the biggest metric"}
   :children    children})


(defn coll-min
  "Returns the event with the smallest metric.
  Should receive a list of events from the previous stream.
  ```clojure
  (fixed-event-window {:size 10}
    (coll-min
      (debug)))
  ```
  Get the event the smallest metric on windows of 10 events.
  Check `bottom` and `smin` as well."
  {:control-type :no-args}
  [& children]
  {:action      :coll-min
   :description {:message "Get the event with the smallest metric"}
   :children    children})


(defn coll-sort
  "Sort events based on the field passed as parameter
  Should receive a list of events from the previous stream.
  ```clojure
  (fixed-event-window {:size 10}
    (coll-sort :time
      (debug)))
  ```"
  {:control-type   :input
   :control-params {:format 'cljs.core/keyword
                    :parse  'vsf.action/parse-keyword-params}}
  [field & children]
  (valid-action? :coll-sort vsf.spec/coll-sort [field])
  {:action      :coll-sort
   :description {:message (format "Sort events based on the field %s" field)}
   :params      [field]
   :children    children})


(defn test-action
  "Bufferize all received events in the state (an atom)
  passed as parameter"
  {:control-type :no-args}
  [state & children]
  {:action   :test-action
   :params   [state]
   :children children})


(defn expired
  "Keep expired events.
  ```clojure
  (expired
    (increment))
  ```
  In this example, all expired events will be forwarded to the `increment`stream."
  {:control-type :no-args}
  [& children]
  {:action      :expired
   :description {:message "Keep expired events"}
   :children    children})


(defn not-expired
  "Keep non-expired events.
  ```clojure
  (not-expired
    (increment))
  ```
  In this example, all non-expired events will be forwarded to the `increment`stream."
  {:control-type :no-args}
  [& children]
  {:action      :not-expired
   :description {:message "Remove expired events"}
   :children    children})


(defn parse-dt-params [{:keys [params]}]
  (let [[conditions duration] params]
    {:threshold (last conditions)
     :duration  duration}))


(defn above-dt
  "Takes a number `threshold` and a time period in seconds `duration`.
  If the condition \"the event metric is > to the threshold\" is valid for all events
  received during at least the period `duration`, valid events received after the `duration`
  period will be passed on until an invalid event arrives.
  `:metric` should not be nil (it will produce exceptions).
  ```clojure
  (above-dt {:threshold 100 :duration 10}
    (debug))
  ```
  In this example, if the events `:metric` field are greater than 100 for more than 10 seconds,
  events are passed downstream."
  {:control-type   :map
   :control-params {:fields [{:field :threshold :label "Threshold" :type :number}
                             {:field :duration :label "Duration" :type :number}]
                    :parse  'vsf.action/parse-dt-params}}
  [config & children]
  (valid-action? :above-dt vsf.spec/above-dt [config])
  {:action      :above-dt
   :description {:message
                 (format "Keep events if :metric is greater than %d during %d seconds"
                         (:threshold config)
                         (:duration config))}
   :params      [[:> :metric (:threshold config)]
                 (:duration config)]
   :children    children})


(defn below-dt
  "Takes a number `threshold` and a time period in seconds `duration`.
  If the condition `the event metric is < to the threshold` is valid for all
  events received during at least the period `duration`, valid events received after
  the `duration` period will be passed on until an invalid event arrives.
  `:metric` should not be nil (it will produce exceptions).
    ```clojure
  (below-dt {:threshold 100 :duration 10}
    (debug))
  ```
  In this example, if the events `:metric` field are lower than 100 for more than 10 seconds,
  events are passed downstream."
  {:control-type   :map
   :control-params {:fields [{:field :threshold :label "Threshold" :type :number}
                             {:field :duration :label "Duration" :type :number}]
                    :parse  'vsf.action/parse-dt-params}}
  [config & children]
  (valid-action? :below-dt vsf.spec/below-dt [config])
  {:action      :below-dt
   :description {:message
                 (format "Keep events if :metric is lower than %d during %d seconds"
                         (:threshold config)
                         (:duration config))}
   :params      [[:< :metric (:threshold config)]
                 (:duration config)]
   :children    children})


(defn parse-dt-delta-params [{:keys [params]}]
  (let [[conditions duration] params
        [low high] (->> conditions rest (map last))]
    {:low      low
     :high     high
     :duration duration}))


(defn between-dt
  "Takes two numbers, `low` and `high`, and a time period in seconds, `duration`.
  If the condition `the event metric is > low and < high` is valid for all events
  received during at least the period `duration`, valid events received after the `duration`
  period will be passed on until an invalid event arrives.
  `:metric` should not be nil (it will produce exceptions).
  ```clojure
  (between-dt {:low 50 :high 100 :duration 10}
    (debug))
  ```
  In this example, if the events `:metric` field are between 50 ans 100 for more than 10 seconds,
  events are passed downstream."
  {:control-type   :map
   :control-params {:fields [{:field :low :label "Low" :type :number}
                             {:field :high :label "High" :type :number}
                             {:field :duration :label "Duration" :type :number}]
                    :parse  'vsf.action/parse-dt-delta-params}}
  [config & children]
  (valid-action? :between-dt vsf.spec/between-dt [config])
  {:action      :between-dt
   :description {:message
                 (format "Keep events if :metric is between %d and %d during %d seconds"
                         (:low config)
                         (:high config)
                         (:duration config))}
   :params      [[:and
                  [:> :metric (:low config)]
                  [:< :metric (:high config)]]
                 (:duration config)]
   :children    children})


(defn outside-dt
  "Takes two numbers, `low` and `high`, and a time period in seconds, `duration`.
  If the condition `the event metric is < low or > high` is valid for all events
  received during at least the period `duration`, valid events received after the `duration`
  period will be passed on until an invalid event arrives.
  `:metric` should not be nil (it will produce exceptions).
  ```clojure
  (outside-dt {:low 50 :high 100 :duration 10}
    (debug))
  ```
  In this example, if the events `:metric` field are outside the 50-100 range for more than 10 seconds,
  events are passed downstream."
  {:control-type   :map
   :control-params {:fields [{:field :low :label "Low" :type :number}
                             {:field :high :label "High" :type :number}
                             {:field :duration :label "Duration" :type :number}]
                    :parse  'vsf.action/parse-dt-delta-params}}
  [config & children]
  (valid-action? :outside-dt vsf.spec/outside-dt [config])
  {:action      :outside-dt
   :description {:message
                 (format "Keep events if :metric is outside %d and %d during %d seconds"
                         (:low config)
                         (:high config)
                         (:duration config))}
   :params      [[:or
                  [:< :metric (:low config)]
                  [:> :metric (:high config)]]
                 (:duration config)]
   :children    children})


(defn parse-critical-dt-params [{:keys [params]}]
  (let [[_ duration] params]
    {:duration duration}))


(defn critical-dt
  "Takes a time period in seconds `duration`.
  If all events received during at least the period `duration` have `:state` critical,
  new critical events received after the `duration` period will be passed on until
  an invalid event arrives.
  ```clojure
  (critical-dt {:duration 10}
    (debug))
  ```
  In this example, if the events `:state` are \"critical\" for more than 10 seconds,
  events are passed downstream."
  {:control-type   :map
   :control-params {:fields [{:field :duration :label "Duration" :type :number}]
                    :parse  'vsf.action/parse-critical-dt-params}}
  [config & children]
  (valid-action? :critical-dt vsf.spec/critical-dt [config])
  {:action      :critical-dt
   :description {:message
                 (format "Keep events if the state is critical for more than %d seconds"
                         (:duration config))}
   :params      [[:= :state "critical"]
                 (:duration config)]
   :children    children})


(defn critical
  "Keep all events in state critical.
  ```clojure
  (critical
    (error))
  ```
  In this example, all events with `:state` \"critical\" will be logged."
  {:control-type :no-args}
  [& children]
  {:action      :critical
   :description {:message "Keep critical events"}
   :children    children})


(defn warning
  "Keep all events in state warning.
  ```clojure
  (warning
    (warning))
  ```
  In this example, all events with `:state` \"warning\" will be logged."
  {:control-type :no-args}
  [& children]
  {:action      :warning
   :description {:message "Keep warning events"}
   :children    children})


(defn parse-default-params [{:keys [params]}]
  (let [[field value] params]
    {field value}))


(defn format-default-params [m]
  (let [[field value] (first m)]
    (list field value)))


(defn default
  "Set a default value for an event
  ```clojure
  (default :state \"ok\"
    (info))
  ```
  In this example, all events where `:state` is not set will be updated with
  `:state` to \"ok\"."
  {:control-type   :key-vals
   :control-params {:pairs-number 1
                    :format       'vsf.action/format-default-params
                    :parse        'vsf.action/parse-default-params}}
  [field value & children]
  (valid-action? :default vsf.spec/default [field value])
  {:action      :default
   :description {:message (format "Set (if nil) %s to %s" field (str value))}
   :params      [field value]
   :children    children})


(defn output!
  "Push events to an external system.
  Outputs are configured into the main configuration file.
  If you create a new output named `:influxdb`
  for example, you can use output! to push all events into this I/O:
  ```clojure
  (output! :influxdb)
  ```
  Outputs are automatically discarded in test mode."
  {:control-type   :input
   :control-params {:format 'cljs.core/keyword
                    :parse  'vsf.action/parse-keyword-params}
   :leaf-action    true}
  [output-name]
  (valid-action? :output! vsf.spec/output! [output-name])
  {:action      :output!
   :description {:message (format "Forward events to the output %s" output-name)}
   :params      [output-name]})


(defn coalesce
  "Returns a list of the latest non-expired events (by `fields`) every dt seconds
  (at best).
  ```clojure
  (coalesce {:duration 10 :fields [:host :service]}
    (debug)
  ```
  In this example, the latest event for each host/service combination will be
  kept and forwarded downstream. The `debug` action will then receive this list
  of events.
  Expired events will be removed from the list.
  coalesce supports nested fields:
  ```clojure
  (coalesce {:duration 10 :fields [:host [:nested :field]]}
    (debug)
  ```"
  {:control-type   :map
   :control-params {:fields [{:field :duration :label "Duration" :type :number}
                             {:field :fields :label "Fields"}]
                    :format 'vsf.action/format-fields-params
                    :parse  'vsf.action/parse-fields-params}}
  [config & children]
  (valid-action? :coalesce vsf.spec/coalesce [config])
  {:action      :coalesce
   :description {:message
                 (format "Returns a list of the latest non-expired events for each fields (%d) combinations, every %s seconds"
                         (:duration config)
                         (pr-str (:fields config)))}
   :children    children
   :params      [config]})


(defn with
  "Set an event field to the given value.
  ```clojure
  (with :state \"critical\"
    (debug))
  ```
  This example set the field `:state` to \"critical\" for events.
  A map can also be provided:
  ```clojure
  (with {:service \"foo\" :state \"critical\"}
    (debug))
  ```
  This example set the field `:service` to \"foo\" and the field `:state`
  to \"critical\" for events."
  {:control-type   :key-vals
   :control-params {:parse  'vsf.action/parse-keyvals-params
                    :format 'vsf.action/format-keyvals-params}}
  [& args]
  (cond
    (map? (first args))
    {:action      :with
     :description {:message "Merge the events with the provided fields"
                   :params  (pr-str (first args))}
     :children    (rest args)
     :params      [(first args)]}

    :else
    (let [[k v & children] args]
      (when (or (not k) (not v))
        (throw #?(:clj  (ex/ex-incorrect (format "Invalid parameters for with: %s %s" k v))
                  :cljs (js/Error (format "Invalid parameters for with: %s %s" k v)))))

      {:action      :with
       :description {:message (format "Set the field %s to %s" k (str v))}
       :children    children
       :params      [{k v}]})))


(defn coll-rate
  "Computes the rate on a list of events.
  Should receive a list of events from the previous stream.
  The latest event is used as a base to build the new event.
  ```clojure
  (fixed-event-window {:size 3}
    (coll-rate
      (debug)))
  ```
  If this example receives the events:
  {:metric 1 :time 1} {:metric 2 :time 2} {:metric 1 :time 3}
  The stream will return {:metric 2 :time 3}
  Indeed, (1+2+1)/2 = 3 (we divide by 2 because we have 2 seconds between the
  min and max events time)."
  {:control-type :no-args}
  [& children]
  {:action      :coll-rate
   :description {:message "Takes a list of events and computes their rates"}
   :children    children})


(defn sflatten
  "Streaming flatten. Calls children with each event in events.
  Events should be a sequence.
  This stream can be used to \"flat\" a sequence of events (emitted
  by a time window stream for example).
  ```clojure
  (fixed-event-window {:size 5}
    (sflatten
      (info)))
  ```"
  {:control-type :no-args}
  [& children]
  {:action      :sflatten
   :description {:message "Send events from a list downstream one by one"}
   :children    children})


(defn format-tag-params [tags]
  (->> (string/split tags #",")
       (remove string/blank?)
       (map string/trim)))


(defn parse-tag-params [{:keys [params]}]
  (let [[tags] params]
    (if (string? tags)
      tags
      (string/join "," tags))))


(defn tag
  "Adds a new tag, or set of tags, to events which flow through.
  ```clojure
  (tag \"foo\"
    (info))
  ```
  This example adds the tag \"foo\" to events.
  ```clojure
  (tag [\"foo\" \"bar\"] (info))
  ```
  This example adds the tag \"foo\" and \"bar\" to events."
  {:control-type   :input
   :control-params {:format 'vsf.action/format-tag-params
                    :parse  'vsf.action/parse-tag-params}}
  [tags & children]
  (valid-action? :tag vsf.spec/tag [tags])
  {:action      :tag
   :description {:message (str "Tag events with %s" tags)}
   :params      [tags]
   :children    children})


;; Copyright Riemann authors (riemann.io), thanks to them!
(defn untag
  "Removes a tag, or set of tags, from events which flow through.
  ```clojure
  (untag \"foo\" index)
  ```
  This example removes the tag \"foo\" from events.
  ```clojure
  (untag [\"foo\" \"bar\"] index)
  ```
  This example removes the tags \"foo\" and \"bar\" from events"
  {:control-type   :input
   :control-params {:format 'vsf.action/format-tag-params
                    :parse  'vsf.action/parse-tag-params}}
  [tags & children]
  (valid-action? :untag vsf.spec/untag [tags])
  {:action      :untag
   :description {:message (str "Remove tags " tags)}
   :params      [tags]
   :children    children})


;; Copyright Riemann authors (riemann.io), thanks to them!
(defn tagged-all
  "Passes on events where all tags are present.
  ```clojure
  (tagged-all \"foo\"
    (info))
  ```
  This example keeps only events tagged \"foo\".
  ```clojure
  (tagged-all [\"foo\" \"bar\"] (info))
  ```
  This example keeps only events tagged \"foo\" and \"bar\"."
  {:control-type   :input
   :control-params {:format 'vsf.action/format-tag-params
                    :parse  'vsf.action/parse-tag-params}}
  [tags & children]
  (valid-action? :tagged-all vsf.spec/tagged-all [tags])
  {:action      :tagged-all
   :description {:message (str "Keep only events with tagged " tags)}
   :params      [tags]
   :children    children})


(defn ddt
  "Differentiate metrics with respect to time.
  Emits an event for each event received, but with metric equal to
  the difference between the current event and the previous one, divided by the
  difference in their times. Skips events without metrics.
  ```clojure
  (ddt
    (info))
  ```
  If ddt receives {:metric 1 :time 1} and {:metric 10 :time 4}, it will produce
  {:metric (/ 9 3) :time 4}."
  {:control-type :no-args}
  [& children]
  {:action      :ddt
   :description {:message "Differentiate metrics with respect to time"}
   :params      [false]
   :children    children})


(defn ddt-pos
  "Like ddt but do not forward events with negative metrics.
  This can be used for counters which may be reseted to zero for example."
  {:control-type :no-args}
  [& children]
  {:action      :ddt-pos
   :description {:message "Differentiate metrics with respect to time"}
   :params      [true]
   :children    children})


(defn format-scale-params [{:keys [factor]}]
  factor)


(defn parse-scale-params [{:keys [params]}]
  {:factor (first params)})


(defn scale
  "Multiplies the event :metric field by the factor passed as parameter.
  ```clojure
  (scale 1000
    (info))
  ```
  This example will multiply the :metric field for all events by 1000."
  {:control-type   :map
   :control-params {:fields [{:field :factor :label "Factor" :type :number}]
                    :format 'vsf.action/format-scale-params
                    :parse  'vsf.action/parse-scale-params}}
  [factor & children]
  (valid-action? :scale vsf.spec/scale [factor])
  {:action      :scale
   :description {:message (str "Multiples the :metric field by " factor)}
   :params      [factor]
   :children    children})


;; @TODO not clear how to use with UI editor controls
(defn split
  "Split by conditions.
  ```clojure
  (split
    [:> :metric 10] (debug)
    [:> :metric 5] (info)
    (error)
  ```
  In this example, all events with :metric > 10 will go into the debug stream,
  all events with :metric > 5 in the info stream, and other events will to the
  default stream which is \"error\".
  The default stream is optional, if not set all events not matching a condition
  will be discarded."
  {:control-type :code}
  [& clauses]
  (let [{:keys [clauses-fn children]}
        (->> (partition-all 2 clauses)
             (reduce (fn [acc [clause children]]
                       (if (some? children)
                         (-> acc
                             (update :children conj children)
                             (update :clauses-fn conj clause))
                         ;; add a default fn to the default close if needed
                         (-> acc
                             (update :children conj clause)
                             (update :clauses-fn conj [:always-true]))))
                     {:clauses-fn [] :children []}))]
    (valid-action? :split vsf.spec/conditions clauses-fn)
    {:action      :split
     :description {:message (format "Split metrics by the clauses provided as parameter")
                   :params  clauses-fn}
     :params      [clauses-fn]
     :children    children}))


(defn throttle
  "Let N event pass at most every duration seconds.
  Can be used for example to avoid sending to limit the number of alerts
  sent to an external system.
  ```clojure
  (throttle {:count 3 :duration 10}
    (error))
  ```
  In this example, throttle will let 3 events pass at most every 10 seconds.
  Other events, or events with no time, are filtered."
  {:control-type   :map
   :control-params {:fields [{:field :count :label "Count" :type :number}
                             {:field :duration :label "Duration" :type :number}]
                    :parse  'vsf.action/parse-map-params}}
  [config & children]
  (valid-action? :throttle vsf.spec/throttle [config])
  {:action      :throttle
   :description {:message (format "Let %d events pass at most every %d seconds"
                                  (:count config)
                                  (:duration config))}
   :params      [config]
   :children    children})


;; Copyright Riemann authors (riemann.io), thanks to them!
(defn moving-event-window
  "A sliding window of the last few events. Every time an event arrives, calls
  children with a vector of the last n events, from oldest to newest. Ignores
  event times. Example:
  ```clojure
  (moving-event-window {:size 5}
    (coll-mean (info))
  ```"
  {:control-type   :map
   :control-params {:fields [{:field :size :label "Size" :type :number}]
                    :parse  'vsf.action/parse-map-params}}
  [config & children]
  (valid-action? :moving-event-window vsf.spec/moving-event-window [config])
  {:action      :moving-event-window
   :description {:message (format "Build moving event window of size %s"
                                  (:size config))}
   :params      [config]
   :children    children})


(defn parse-ration-params [{:keys [params]}]
  {:ratio (first params)})


;; Copyright Riemann authors (riemann.io), thanks to them!
(defn ewma-timeless
  "Exponential weighted moving average. Constant space and time overhead.
  Passes on each event received, but with metric adjusted to the moving
  average. Does not take the time between events into account. R is the ratio
  between successive events: r=1 means always return the most recent metric;
  r=1/2 means the current event counts for half, the previous event for 1/4,
  the previous event for 1/8, and so on."
  {:control-type   :map
   :control-params {:fields [{:field :ratio :label "Ratio" :type :number}]
                    :format :ratio
                    :parse  'vsf.action/parse-ration-params}}
  [ratio & children]
  (valid-action? :ewma-timeless vsf.spec/ewma-timeless [ratio])
  {:action      :ewma-timeless
   :description {:message "Exponential weighted moving average"}
   :params      [ratio]
   :children    children})


(defn parse-grater-than-params [{:keys [params]}]
  {:grater-than (first params)})


;; Copyright Riemann authors (riemann.io), thanks to them!
(defn over
  "Passes on events only when their metric is greater than x.
  ```clojure
  (over 10
    (info))
  ```"
  {:control-type   :map
   :control-params {:fields [{:field :grater-than :label "Greater than" :type :number}]
                    :format :grater-than
                    :parse  'vsf.action/parse-grater-than-params}}
  [grater-than & children]
  (valid-action? :over vsf.spec/over [grater-than])
  {:action      :over
   :description {:message (format "Keep events with metrics greater than %d" grater-than)}
   :params      [grater-than]
   :children    children})


(defn parse-under-params [{:keys [params]}]
  {:under (first params)})


;; Copyright Riemann authors (riemann.io), thanks to them!
(defn under
  "Passes on events only when their metric is under than x.
  ```clojure
  (under 10
    (info))
  ```"
  {:control-type   :map
   :control-params {:fields [{:field :under :label "Under" :type :number}]
                    :format :under
                    :parse  'vsf.action/parse-under-params}}
  [under & children]
  (valid-action? :under vsf.spec/under [under])
  {:action      :under
   :description {:message (format "Keep events with metrics under than %d" under)}
   :params      [under]
   :children    children})


(defn format-changed-params [{:keys [field] :as config}]
  (let [field (format-paths-params field)]
    (assoc config :field field)))


(defn parse-changed-params [{:keys [params]}]
  (let [config (first params)
        field  (parse-paths-params (:field config))]
    (assoc config :field field)))


(defn changed
  "Passes on events only if the `field` passed as parameter differs
  from the previous one.
  The `init` parameter is the default value for the stream.
  ```clojure
  (changed {:field :state :init \"ok\"})
  ```
  For example, this action will let event pass if the :state field vary,
  the initial value being `ok`.
  This stream is useful to get only events making a transition.
  It also supported nested fields:
  ```clojure
  (changed {:field [:nested :field] :init \"ok\"})
  ```"
  {:control-type   :map
   :control-params {:fields [{:field :field :label "Field" :type :string}
                             {:field :init :label "Initial value" :type :string}]
                    :format 'vsf.action/format-changed-params
                    :parse  'vsf.action/parse-changed-params}}
  [config & children]
  (valid-action? :changed vsf.spec/changed [config])
  {:action      :changed
   :description {:message (format "Passes on events only if the field %s differs from the previous one (default %s)"
                                  (:field config)
                                  (:init config))}
   :params      [config]
   :children    children})


(defn project
  "Takes a list of conditions.
  Like coalesce, project will return the most recent events matching
  the conditions.
  ```clojure
  (project [[:= :service \"enqueues\"]
            [:= :service \"dequeues\"]]
    (coll-quotient
      (with :service \"enqueues per dequeue\"
        (info))))
  ```
  We divide here the latest event for the \"enqueues\" :service by the
  latest event from the \"dequeues\" one."
  {:control-type   :code
   :control-params {:format 'vsf.action/read-string
                    :parse  'vsf.action/parse-string-params}}
  [conditions & children]
  (valid-action? :project vsf.spec/project [conditions])
  {:action      :project
   :description {:message "return the most recent events matching the conditions"
                 :params  (pr-str conditions)}
   :params      [conditions]
   :children    children})


(defn index
  "Insert events into the index.
  Events are indexed using the keys passed as parameter.
  ```clojure
  (index [:host :service])
  ```
  This example will index events by host and services."
  {:control-type   :input
   :control-params {:format 'vsf.action/format-keywords-params
                    :parse  'vsf.action/parse-keywords-params}
   :leaf-action    true}
  [labels]
  (valid-action? :index vsf.spec/index [labels])
  {:action      :index
   :description {:message "Insert events into the index using the provided fields as keys"
                 :params  (pr-str labels)}
   :params      [labels]})


(defn coll-count
  "Count the number of events.
  Should receive a list of events from the previous stream.
  The most recent event is used as a base to create the new event, and
  its :metric field is set to the number of events received as input.
  ```clojure
  (fixed-time-window {:duration 60}
    (coll-count
      (debug)))
  ```"
  {:control-type :no-args}
  [& children]
  {:action      :coll-count
   :description {:message "Count the number of events"}
   :children    children})


(defn sdissoc
  "Remove a key (or a list of keys) from the events.
  ```clojure
  (sdissoc :host
    (info))
  (sdissoc [:environment :host]
    (info))
  (sdissoc [:environment [:nested :key]]
    (info))
  ```"
  {:control-type   :input
   :control-params {:format 'vsf.action/format-paths-params
                    :parse  'vsf.action/parse-paths-single-param}}
  [fields & children]
  (valid-action? :sdissoc vsf.spec/sdissoc [fields])
  {:action      :sdissoc
   :description {:message (format "Remove key(s) %s from events" (str fields))}
   :params      [(if (keyword? fields) [fields] fields)]
   :children    children})


(defn format-percentiles-params [percentiles-string]
  (->> (string/split percentiles-string #",")
       (remove string/blank?)
       (mapv (fn [p]
               #?(:clj  (Double/parseDouble p)
                  :cljs (js/parseFloat p))))))


(defn parse-percentiles-params [{:keys [params]}]
  (let [[percentiles] params]
    (if (string? percentiles)
      percentiles
      (string/join "," percentiles))))


;; Copyright Riemann authors (riemann.io), thanks to them!
(defn coll-percentiles
  "Receives a list of events and selects one
  event from that period for each point. If point is 0, takes the lowest metric
  event.  If point is 1, takes the highest metric event. 0.5 is the median
  event, and so forth. Forwards each of these events to children. The event
  has the point appended the `:quantile` key.
  Useful for extracting histograms and percentiles.
  ```clojure
  (fixed-event-window {:size 10}
    (coll-percentiles [0.5 0.75 0.98 0.99]))
  ```"
  {:control-type   :input
   :control-params {:format 'vsf.action/format-percentiles-params
                    :parse  'vsf.action/parse-percentiles-params}}
  [points & children]
  (valid-action? :coll-percentiles vsf.spec/coll-percentiles [points])
  {:action      :coll-percentiles
   :description {:message (format "Computes percentiles for quantiles %s"
                                  (str points))}
   :params      [points]
   :children    children})


(defn by
  "Split stream by field
  Every time an event arrives with a new value of field, this action invokes
  its child forms to return a *new*, distinct set of streams for that
  particular value.
  ```clojure
  (by {:fields [:host :service]}
    (fixed-time-window {:duration 60}))
  ```
  This example generates a moving window for each host/service combination.
  You can also pass the `:gc-interval` and `:fork-ttl` keys to the action.
  This will enable garbage collections of children actions, executed every
  `:gc-interval` (in seconds) and which will remove actions which didn't
  receive events since `:fork-ttl` seconds
  ```clojure
  (by {:fields [:host :service [:a :nested-key]]
       :gc-interval 3600
       :fork-ttl 1800}
    (fixed-time-window {:duration 60}))
  ```"
  {:control-type   :map
   :control-params {:fields [{:field :fields :label "Fields" :type :string}
                             {:field :gc-interval :label "Garbage collection interval" :type :number}
                             {:field :fork-ttl :label "Fork time to live" :type :number}]
                    :format 'vsf.action/format-fields-params
                    :parse  'vsf.action/parse-fields-params}}
  [config & children]
  (valid-action? :by vsf.spec/by [config])
  {:action      :by
   :description {:message (str "Split streams by field(s) " (:fields config))}
   :params      [config]
   :children    children})


(defn reinject!
  "Reinject an event into the streaming system.
  By default, events are reinject into the real time engine. You can reinject
  events to a specific stream by passing the destination stream as parameter.
  ```clojure
  (reinject)
  ```
  This example reinjects events into the real stream engine.
  ```clojure
  (reinject :foo)
  ```
  This example reinjects events into the stream named `:foo`."
  {:control-type   :input
   :control-params {:format 'cljs.core/keyword
                    :parse  'vsf.action/parse-keyword-params}
   :leaf-action    true}
  ([]
   (reinject! nil))
  ([destination-stream]
   (valid-action? :reinject! vsf.spec/reinject [destination-stream])
   {:action      :reinject!
    :description {:message (format "Reinject events on %s"
                                   (if destination-stream
                                     (str "stream " destination-stream)
                                     "the current stream"))}
    :params      [destination-stream]}))


(defn async-queue!
  "Execute children into the specific async queue.
  The async queue should be defined in the I/O configuration file.
  ```clojure
  (async-queue! :my-queue
    (info))
  ```"
  {:control-type   :input
   :control-params {:format 'cljs.core/keyword
                    :parse  'vsf.action/parse-keyword-params}}
  [queue-name & children]
  (valid-action? :async-queue! vsf.spec/async-queue! [queue-name])
  {:action      :async-queue!
   :description {:message (format "Execute the children into the queue %s"
                                  queue-name)}
   :params      [queue-name]
   :children    children})


(defn io
  "Discard all events in test mode. Else, forward to children.
  You can use this stream to avoid side effects in test mode."
  {:control-type :no-args}
  [& children]
  {:action      :io
   :description {:message "Discard all events in test mode"}
   :children    children})


(defn tap
  "Save events into the tap. Noop outside tests.
  ```clojure
  (where [:= :service \"foo\"]
    (tap :foo)
  ```
  In test mode, all events with `:service` \"foo\" will be saved in a tap
  named `:foo`"
  {:control-type   :input
   :control-params {:format 'cljs.core/keyword
                    :parse  'vsf.action/parse-keyword-params}
   :leaf-action    true}
  [tap-name]
  (valid-action? :tap vsf.spec/tap [tap-name])
  {:action      :tap
   :description {:message (format "Save events into the tap %s" tap-name)}
   :params      [tap-name]})


(defn json-fields
  "Takes a field or a list of fields, and converts the values associated to these
  fields from json to edn.
  ```clojure
  (with :my-field \"{\"foo\": \"bar\"}
    (json-fields [:my-field]))
  ```
  In this example, we associate to `:my-field` a json string and then we call
  `json-fields` on it. `:my-field` will now contain an edn map built from the json
  data, with keywords as keys."
  {:control-type   :input
   :control-params {:format 'vsf.action/format-keywords-params
                    :parse  'vsf.action/parse-keywords-params}}
  [fields & children]
  (valid-action? :json-fields vsf.spec/json-fields [fields])
  {:action      :json-fields
   :description {:message "Parse the provided fields from json to edn"
                 :params  (pr-str fields)}
   :params      [(if (keyword? fields) [fields] fields)]
   :children    children})


(defn exception-stream
  "Takes two actions. If an exception is thrown in the first action, an event
  representing this exception is emitted in in the second action.
  ```clojure
  (exception-stream
    (bad-action)
    (error))
  ```
  Here, if `bad-action` throws, an event will be built (using the `exception->event`
  function) and sent to the `error` action (which will log it)."
  {:control-type :no-args}
  [& children]
  (when-not (= 2 (count children))
    #?(:clj  (ex/ex-incorrect! "The exception-stream action should take 2 children")
       :cljs (js/Error "The exception-stream action should take 2 children")))
  {:action      :exception-stream
   :description {:message "Catches exceptions in the first action and reinject errors into the second one"}
   :children    children})


(defn format-custom-params [{:keys [action params]}]
  (list (keyword action) (read-string params)))


(defn parse-custom-params [{:keys [action params]}]
  {:action action
   :params (str params)})


;; TODO how to provide custom actions
(defn custom
  "Executes a custom action.
  Custom actions are defined in the configuration file.
  The action can then be called (by name) using this `custom` action.
  ```clojure
  (custom :my-custom-action [\"parameters\"]
    (info))
  ```"
  {:control-type   :map
   :control-params {:fields [{:field :action :label "Action" :type :string}
                             {:field :params :label "Parameters" :type :string}]
                    :format 'vsf.action/format-custom-params
                    :parse  'vsf.action/parse-custom-params}}
  [action-name params & children]
  {:action      action-name
   :description {:message (str "Use the custom action " action-name)
                 :params  (str params)}
   :params      (or params [])
   :children    children})


(defn format-reaper-params [{:keys [interval destination-stream]}]
  (list interval (keyword destination-stream)))


(defn parse-reaper-params [{:keys [params]}]
  {:interval           (:interval params)
   :destination-stream (-> params :destination-stream name)})


(defn reaper
  "Everytime this action receives an event, it will expires events from the
  index (every dt seconds) and reinject them into a stream
  (default to the current stream if not specified).
  ```clojure
  (reaper 5)
  ```
  ```clojure
  (reaper 5 :custom-stream)
  ```"
  {:control-type   :map
   :control-params {:fields [{:field :interval :label "Interval" :type :number}
                             {:field :destination-stream :label "Destination stream" :type :string}]
                    :format 'vsf.action/format-reaper-params
                    :parse  'vsf.action/parse-reaper-params}
   :leaf-action    true}
  ([interval] (reaper interval nil))
  ([interval destination-stream]
   (valid-action? :reaper vsf.spec/reaper [interval destination-stream])
   {:action      :reaper
    :description {:message (format "Expires events every %d second and reinject them into %s"
                                   interval
                                   (if destination-stream
                                     (str "the stream " destination-stream)
                                     (str "the current stream")))}
    :params      [interval destination-stream]
    :children    []}))


(defn to-base64
  "Convert a field or multiple fields to base64.
  Fields values should be string.
  ```clojure
  (sdo
    ;; you can pass one field
    (to-base64 :host)
    ;; or a list of fields
    (to-base64 [:host :service]))
  ```"
  {:control-type   :input
   :control-params {:format 'vsf.action/format-keywords-params
                    :parse  'vsf.action/parse-keywords-params}}
  [field & children]
  (let [fields (if (keyword? field) [field] field)]
    (valid-action? :to-base64 vsf.spec/to-base64 [fields])
    {:action      :to-base64
     :description {:message (format "Encodes field(s) %s to base64"
                                    field)}
     :params      [fields]
     :children    children}))


(defn from-base64
  "Convert a field or multiple fields from base64 to string.
  Fields values should be string.
  ```clojure
  (sdo
    ;; you can pass one field
    (from-base64 :host)
    ;; or a list of fields
    (from-base64 [:host :service]))
  ```"
  {:control-type   :input
   :control-params {:format 'vsf.action/format-keywords-params
                    :parse  'vsf.action/parse-keywords-params}}
  [field & children]
  (let [fields (if (keyword? field) [field] field)]
    (valid-action? :from-base64 vsf.spec/from-base64 [fields])
    {:action      :from-base64
     :description {:message (format "Decodes field(s) %s from base64"
                                    field)}
     :params      [fields]
     :children    children}))


(defn format-sformat-params [{:keys [template target-field fields]}]
  (list template
        (keyword target-field)
        (->> fields (string/split #",") (remove string/blank?) (map keyword))))


(defn parse-sformat-params [{:keys [params]}]
  (let [[template target-field fields] params]
    {:template     template
     :target-field (name target-field)
     :fields       (->> fields (map name) (string/join ","))}))


(defn sformat
  "Takes the content of multiple event keys, and use them to build a string value
  and assign it to a given key.
  ```clojure
  (sformat \"%s-foo-%s\" :format-test [:host :service])
  ```
  If the event `{:host \"machine\" :service \"bar\"}` is passed to this action
  the event will become
  `{:host \"machine\" :service \"bar\" :format-test \"machine-foo-bar\"}`.
  More information about availables formatters in the Clojure documentation:
  https://clojuredocs.org/clojure.core/format"
  {:control-type   :map
   :control-params {:fields [{:field :template :label "Template" :type :string}
                             {:field :target-field :label "Target field" :type :string}
                             {:field :fields :label "Fields" :type :string}]
                    :format 'vsf.action/format-sformat-params
                    :parse  'vsf.action/parse-sformat-params}}
  [template target-field fields & children]
  (valid-action? :sformat vsf.spec/sformat [template target-field fields])
  {:action      :sformat
   :description {:message (format "Set %s to value %s using fields %s"
                                  target-field
                                  template
                                  fields)}
   :params      [template target-field fields]
   :children    children})


(defn publish!
  "Publish events in the given channel.
  ```clojure
  (publish! :my-channel)
  ```
  Users can then subscribe to channels using the websocket engine."
  {:control-type   :input
   :control-params {:format 'cljs.core/keyword
                    :parse  'vsf.action/parse-keyword-params}
   :leaf-action    true}
  [channel]
  (valid-action? :publish! vsf.spec/publish! [channel])
  {:action      :publish!
   :description {:message (str "Publish events into the channel " channel)}
   :params      [channel]
   :children    []})


(defn coll-top
  "Receives a list of events, returns the top N events with the highest metrics.
  ```clojure
  (fixed-time-window {:duration 60}
    (coll-top 5
      (info)))
  ```"
  {:control-type   :map
   :control-params {:fields [{:field :nb-events :label "Number of events" :type :number}]
                    :format :nb-events
                    :parse  'vsf.action/parse-string-params}}
  [nb-events & children]
  (valid-action? :coll-top vsf.spec/coll-top [nb-events])
  {:action      :coll-top
   :description {:message (format "Returns top %d events with the highest metrics"
                                  nb-events)}
   :params      [nb-events]
   :children    children})


(defn coll-bottom
  "Receives a list of events, returns the bottom N events with the lowest metrics.
  ```clojure
  (fixed-time-window {:duration 60}
    (coll-bottom 5
      (info)))
  ```"
  {:control-type   :map
   :control-params {:fields [{:field :nb-events :label "Number of events" :type :number}]
                    :format :nb-events
                    :parse  'vsf.action/parse-string-params}}
  [nb-events & children]
  (valid-action? :coll-bottom vsf.spec/coll-bottom [nb-events])
  {:action      :coll-bottom
   :description {:message (format "Returns bottom %d events with the lowest metrics"
                                  nb-events)}
   :params      [nb-events]
   :children    children})


(defn format-stable-params [{:keys [dt field]}]
  (list dt (->> field (string/split #",") (remove string/blank?) (map keyword))))


(defn parse-stable-params [{:keys [params]}]
  (let [[dt field] params]
    {:dt    dt
     :field (->> field (map name) (string/join ","))}))


(defn stable
  "Takes a duration (dt) in second and a field name as parameter.
  Returns events where the value of the field specified as second argument
  is equal to the value of the field for the last event, for at least dt seconds.
  Events can be buffered for dt seconds before being forwarded in order to see
  if they are stable or not.
  Events should arrive in order (old events will be dropped).
  You can use this stream to remove flapping states for example.
  ```clojure
  (stable 10 :state
    (info))
  ```
  In this example, events will be forwarded of the value of the `:state` key
  is the same for at least 10 seconds.
  Support nested fields:
  ```clojure
  (stable 10 [:nested :field]
    (info))
  ```"
  {:control-type   :map
   :control-params {:fields [{:field :dt :label "Duration" :type :number}
                             {:field :field :label "Field" :type :string}]
                    :format 'vsf.action/format-stable-params
                    :parse  'vsf.action/parse-stable-params}}
  [dt field & children]
  (valid-action? :stable vsf.spec/stable [dt field])
  {:action      :stable
   :description {:message (format "Returns events where the field %s is stable for more than %s seconds"
                                  field
                                  dt)}
   :params      [dt field]
   :children    children})


(defn format-rename-params [kv-map]
  (-> (format-keyvals-params kv-map)
      (update-vals keyword)))


(defn parse-rename-params [{:keys [params]}]
  (let [[replacement] params]
    (-> (parse-keyvals-params replacement)
        (update-vals name))))


(defn rename-keys
  "Rename events keys.
  ```clojure
  (rename-keys {:host :service
                :environment :env}
  ```
  In this example, the `:host` key will be renamed `:service` and the
  `:environment` key is renamed `:env`.
  Existing values will be overridden."
  {:control-type   :key-vals
   :control-params {:format 'vsf.action/format-rename-params
                    :parse  'vsf.action/parse-rename-params}}
  [replacement & children]
  (valid-action? :rename-keys vsf.spec/rename-keys [replacement])
  {:action      :rename-keys
   :description {:message "Rename events keys"
                 :params  (pr-str replacement)}
   :params      [replacement]
   :children    children})


(defn keep-keys
  "Keep only the specified keys for events.
  ```clojure
  (keep-keys [:host :metric :time :environment :description]
    (info))
  ```
  Also works with nested keys:
  ```clojure
  (keep-keys [:host :metric :time [:a :nested-key]]
    (info))
  ```"
  {:control-type   :input
   :control-params {:format 'vsf.action/format-paths-params
                    :parse  'vsf.action/parse-paths-single-param}}
  [keys-to-keep & children]
  (valid-action? :keep-keys vsf.spec/keep-keys [keys-to-keep])
  {:action      :keep-keys
   :description {:message "Keep only the specified keys from events"
                 :params  (pr-str keys-to-keep)}
   :params      [keys-to-keep]
   :children    children})


(defn sum
  "Sum the events field from the last dt seconds.
  ```clojure
  (sum {:duration 10}
    (info))
  ```
  You can pass a `:delay` key to the configuration in order to tolerate late
  events. In that case, events from previous windows will be flushed after this
  delay:
  ```clojure
  (sum {:duration 10 :delay 5}
    (info))
  ```"
  {:control-type   :map
   :control-params {:fields [{:field :duration :label "Duration" :type :number}
                             {:field :delay :label "Delay" :type :number}]
                    :parse  'vsf.action/parse-map-params}}
  [config & children]
  (valid-action? :sum vsf.spec/sum [config])
  {:action      :sum
   :description {:message (format "Sum the events field from the last %s seconds"
                                  (:duration config))}
   :params      [(assoc config :aggr-fn :+)]
   :children    children})


(defn top
  "Get the max event from the last dt seconds.
  ```clojure
  (top {:duration 10}
    (info))
  ```
  You can pass a `:delay` key to the configuration in order to tolerate late
  events. In that case, events from previous windows will be flushed after this
  delay:
  ```clojure
  (top {:duration 10 :delay 5}
    (info))
  ```"
  {:control-type   :map
   :control-params {:fields [{:field :duration :label "Duration" :type :number}
                             {:field :delay :label "Delay" :type :number}]
                    :parse  'vsf.action/parse-map-params}}
  [config & children]
  (valid-action? :top vsf.spec/top [config])
  {:action      :top
   :description {:message (format "Get the max event from the last %s seconds"
                                  (:duration config))}
   :params      [(assoc config :aggr-fn :max)]
   :children    children})


(defn bottom
  "Get the min event from the last dt seconds.
  ```clojure
  (bottom {:duration 10}
    (info))
  ```
  You can pass a `:delay` key to the configuration in order to tolerate late
  events. In that case, events from previous windows will be flushed after this
  delay:
  ```clojure
  (bottom {:duration 10 :delay 5}
    (info))
  ```"
  {:control-type   :map
   :control-params {:fields [{:field :duration :label "Duration" :type :number}
                             {:field :delay :label "Delay" :type :number}]
                    :parse  'vsf.action/parse-map-params}}
  [config & children]
  (valid-action? :bottom vsf.spec/bottom [config])
  {:action      :bottom
   :description {:message (format "Get the min event from the last %s seconds"
                                  (:duration config))}
   :params      [(assoc config :aggr-fn :min)]
   :children    children})


(defn mean
  "Get the mean of event metrics from the last dt seconds.
  ```clojure
  (mean {:duration 10}
    (info))
  ```
  You can pass a `:delay` key to the configuration in order to tolerate late
  events. In that case, events from previous windows will be flushed after this
  delay:
  ```clojure
  (mean {:duration 10 :delay 5}
    (info))
  ```"
  {:control-type   :map
   :control-params {:fields [{:field :duration :label "Duration" :type :number}
                             {:field :delay :label "Delay" :type :number}]
                    :parse  'vsf.action/parse-map-params}}
  [config & children]
  (valid-action? :mean vsf.spec/mean [config])
  {:action      :mean
   :description {:message (format "Get the min of events from the last %s seconds"
                                  (:duration config))}
   :params      [(assoc config :aggr-fn :mean)]
   :children    children})


(defn fixed-time-window
  "A fixed window over the event stream in time. Emits vectors of events, such
  that each vector has events from a distinct n-second interval. Windows do
  *not* overlap; each event appears at most once in the output stream.
  ```clojure
  (fixed-time-window {:duration 60}
    (coll-max
      (info)))
  ```
  You can pass a `:delay` key to the configuration in order to tolerate late
  events. In that case, previous windows will be flushed after this
  delay:
  ```clojure
  (fixed-time-window {:duration 60 :delay 30}
    (coll-max
      (info)))
  ```"
  {:control-type   :map
   :control-params {:fields [{:field :duration :label "Duration" :type :number}
                             {:field :delay :label "Delay" :type :number}]
                    :parse  'vsf.action/parse-map-params}}
  [config & children]
  (valid-action? :fixed-time-window vsf.spec/fixed-time-window [config])
  {:action      :fixed-time-window
   :description {:message (format "Build %d seconds fixed time windows"
                                  (:duration config))}
   :params      [(assoc config :aggr-fn :fixed-time-window)]
   :children    children})


;; Copyright Riemann authors (riemann.io), thanks to them!
(defn moving-time-window
  "A sliding window of all events with times within the last n seconds. Uses
  the maximum event time as the present-time horizon. Every time a new event
  arrives within the window, emits a vector of events in the window to
  children.
  Events without times accrue in the current window."
  {:control-type   :map
   :control-params {:fields [{:field :duration :label "Duration" :type :number}]
                    :parse  'vsf.action/parse-map-params}}
  [config & children]
  (valid-action? :moving-time-window vsf.spec/moving-time-window [config])
  {:action      :moving-time-window
   :description {:message (format "Build sliding windows of %d seconds"
                                  (:duration config))}
   :params      [config]
   :children    children})


(defn format-ssort-params [{:keys [duration field delay]}]
  (let [field (->> (string/split field #",")
                   (remove string/blank?))]
    {:duration duration
     :delay    delay
     :field    (if (= 1 (count field))
                 (-> field first keyword)
                 (map field keyword))}))


(defn ssort
  "Streaming sort.
  Takes a configuration containing a `:duration` and a `:field` key.
  The action will buffer events during `:duration` seconds and then
  send the events downstream one by one, sorted by `:field`.
  ```clojure
  (ssort {:duration 10 :field :time}
    (info))
  ```
  This example will sort events based on the :time field.
  For example, if it get as input:
  ```clojure
  {:time 1} {:time 10} {:time 4} {:time 9} {:time 13} {:time 31}
  ```
  Info will receive these events:
  ```clojure
  {:time 1} {:time 4} {:time 9} {:time 10} {:time 13}
  ```
  You can add a `:delay` key to the action configuration in order to tolerate
  late events:
  ```clojure
  (ssort {:duration 10 :field :time :delay 10}
    (info))
  ```
  In this example, events from previous windows will be sent with a delay of
  10 seconds.
  `ssort` supports sorting on a nested field (example `:field [:nested :field]`)"
  {:control-type   :map
   :control-params {:fields [{:field :duration :label "Duration" :type :number}
                             {:field :field :label "Field" :type :string}
                             {:field :delay :label "Delay" :type :number}]
                    :format 'vsf.action/format-ssort-params
                    :parse  'vsf.action/parse-map-params}}
  [config & children]
  (valid-action? :ssort vsf.spec/ssort [config])
  {:action      :ssort
   :description {:message (format "Sort events during %d seconds based on the field %s"
                                  (:duration config)
                                  (:field config))}
   :params      [(assoc config :aggr-fn :ssort :nested? (sequential? (:field config)))]
   :children    children})


(defn coll-increase
  "Receives a list of events which should represent an always-increasing counter
  and returns the latest event with its :metric field set to the value of the
  increase between the oldest and the latest event.
  If it receives for example:
  ```clojure
  [{:time 1 :metric 10} {:time 9 :metric 20} {:time 20 :metric 30}}
  ```
  It will produces (30-10 = 20):
  ```clojure
  {:time 20 :metric 20}
  ```
  Events produced with a negative metric (which can happen if the counter is resetted) are not send downstream."
  {:control-type :no-args}
  [& children]
  {:action      :coll-increase
   :description {:message "Takes a list of events and computes the increase of the :metric field"}
   :children    children})


(defn smax
  "Send downstream the event with the biggest :metric every time it receives an event
  ```clojure
  (smax
    (info))
  ```
  If the events `{:time 1 :metric 10} {:time 2 :metric 3} {:time 3 :metric 11}`
  are injected, `info` will receive:
  ```
  {:time 1 :metric 10} {:time 1 :metric 10} {:time 3 :metric 11}
  ```"
  {:control-type :no-args}
  [& children]
  {:action      :smax
   :description {:message "Send downstream the event with the biggest :metric every time it receives an event"}
   :children    children})


(defn smin
  "Send downstream the event with the lowest :metric every time it receives an event
  ```clojure
  (smin
    (info))
  ```
  If the events `{:time 1 :metric 10} {:time 2 :metric 3} {:time 3 :metric 11}`
  are injected, `info` will receive:
  ```clojure
  {:time 1 :metric 10} {:time 2 :metric 3} {:time 2 :metric 3}
  ```"
  {:control-type :no-args}
  [& children]
  {:action      :smin
   :description {:message "Send downstream the event with the lowest :metric every time it receives an event"}
   :children    children})


(defn format-extract-params [{:keys [key]}]
  (keyword key))


(defn parse-extract-params [{:keys [params]}]
  {:key (-> params first name)})


(defn extract
  "Takes a key as parameter and send downstream the content of this key.
  ```clojure
  (extract :base-event
    (info))
  ```
  If `extract` receives in this example
  `{:time 1 :base-event {:time 1 :service \"foo\" :host \"bar\"}`,
  `info` will receive the content of `:base-time`."
  {:control-type   :map
   :control-params {:fields [{:field :key :label "Extract Key" :type :string}]
                    :format 'vsf.action/format-extract-params
                    :parse  'vsf.action/parse-extract-params}}
  [extract-key & children]
  (valid-action? :smin vsf.spec/extract [extract-key])
  {:action      :smin
   :description {:message (format "Extract the key %s from the event and send its value downstream" extract-key)}
   :params      [extract-key]
   :children    children})


(defn rate
  {:control-type   :map
   :control-params {:fields [{:field :duration :label "Duration" :type :number}
                             {:field :delay :label "Delay" :type :number}]
                    :parse  'vsf.action/parse-map-params}}
  [config & children]
  (valid-action? :rate vsf.spec/rate [config])
  {:action      :rate
   :description {:message (format "Computes the rate of received events (by counting them) and emits it every %d seconds" (:duration config))}
   :params      [(assoc config :aggr-fn :rate)]
   :children    children})


(defn format-percentiles-map-params [{:keys [percentiles] :as config}]
  (assoc config :percentiles (format-percentiles-params percentiles)))


(defn parse-percentiles-map-params [{:keys [params]}]
  (let [params-map  (first params)
        percentiles (parse-percentiles-params {:params [(:percentiles params-map)]})]
    (assoc params :percentiles percentiles)))


(defn percentiles
  {:control-type   :map
   :control-params {:fields [{:field :percentiles :label "Percentiles" :type :string}
                             {:field :duration :label "Duration" :type :number}
                             {:field :nb-significant-digits :label "Significant digits" :type :number}
                             {:field :delay :label "Delay" :type :number}
                             {:field :highest-trackable-value :label "Highest Trackable Value" :type :number}
                             {:field :lowest-discernible-value :label "Lowest Discernible Value" :type :number}]
                    :format 'vsf.action/format-percentiles-map-params
                    :parse  'vsf.action/parse-percentiles-map-params}}
  [config & children]
  (valid-action? :percentiles vsf.spec/percentiles [config])
  {:action      :percentiles
   :description {:message (format "Computes the quantiles %s" (:percentiles config))}
   :params      [config]
   :children    children})
