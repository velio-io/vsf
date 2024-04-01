(ns vsf.process
  (:require
   [clojure.set :as set]
   [clojure.string :as string]
   [clojure.tools.logging :as log]
   [exoscale.ex :as ex]
   [cheshire.core :as json]
   [vsf.condition :as condition]
   [vsf.b64 :as b64]
   [vsf.event :as event]
   [vsf.index :as index]
   [vsf.io :as io]
   [vsf.math :as math]
   [vsf.pubsub :as pubsub])
  (:import
   [java.util.concurrent Executor]
   [org.HdrHistogram Histogram Recorder]))


(defn select-keys-nested
  [event keyseq]
  (loop [ret {} keys keyseq]
    (if keys
      (let [key      (first keys)
            seq-key? (sequential? key)
            entry    (if seq-key?
                       (get-in event key ::not-found)
                       (get event key ::not-found))]
        (recur
         (if (not= entry ::not-found)
           (if seq-key?
             (assoc-in ret key entry)
             (assoc ret key entry))
           ret)
         (next keys)))
      ret)))


(defn dissoc-in
  "Dissociates an entry from a nested associative structure returning a new
  nested structure. keys is a sequence of keys. Any empty maps that result
  will not be present in the new structure."
  [m [k & ks :as keys]]
  (if ks
    (if-let [nextmap (get m k)]
      (let [newmap (dissoc-in nextmap ks)]
        (if (seq newmap)
          (assoc m k newmap)
          (dissoc m k)))
      m)
    (dissoc m k)))


(defn call-rescue
  [event children]
  (doseq [child children]
    (child event)))


(defn discard-fn
  [e]
  (some #(= "vsf/discard" %) (:tags e)))


(defn keep-non-discarded-events
  "Takes an event or a list of events. Returns an event (or a list of events
  depending of the input) with all events tagged \"vsf/discard\" filtered.
  Returns nil if all events are filtered."
  [events]
  (if (sequential? events)
    (let [result (remove discard-fn
                         events)]
      (when-not (empty? result)
        result))
    (when-not (discard-fn events)
      events)))


(defn where*
  [_ conditions & children]
  (let [condition-fn (condition/compile-conditions conditions)]
    (fn stream [event]
      (when (condition-fn event)
        (call-rescue event children)))))


(defn coll-where*
  [_ conditions & children]
  (let [condition-fn (condition/compile-conditions conditions)]
    (fn stream [events]
      (call-rescue (filter condition-fn events) children))))


(defn increment*
  [_ & children]
  (fn stream [event]
    (call-rescue (update event :metric inc)
                 children)))


(defn decrement*
  [_ & children]
  (fn stream [event]
    (call-rescue (update event :metric dec)
                 children)))


(defn log-action
  "Generic logger"
  [source-stream level]
  (let [meta {:stream (name source-stream)}]
    (fn stream [event]
      (when-let [event (keep-non-discarded-events event)]
        (condp = level
          :debug (log/debug (json/generate-string event) meta)
          :info (log/info (json/generate-string event) meta)
          :error (log/error (json/generate-string event) meta))))))


(defn debug*
  [ctx]
  (log-action (:source-stream ctx) :debug))


(defn info*
  [ctx]
  (log-action (:source-stream ctx) :info))


(defn error*
  [ctx]
  (log-action (:source-stream ctx) :error))


;; Copyright Riemann authors (riemann.io), thanks to them!
(defn fixed-event-window*
  [_ {:keys [size]} & children]
  (let [window (atom [])]
    (fn stream [event]
      (let [events (swap! window (fn [events]
                                   (let [events (conj events event)]
                                     (if (< size (count events))
                                       [event]
                                       events))))]
        (when (= size (count events))
          (call-rescue events children))))))


(defn coll-mean*
  [_ & children]
  (fn stream [events]
    (call-rescue (math/mean events) children)))


(defn coll-max*
  [_ & children]
  (fn stream [events]
    (call-rescue (math/max-event events) children)))


(defn coll-quotient*
  [_ & children]
  (fn stream [events]
    (call-rescue (math/quotient events) children)))


(defn coll-sum*
  [_ & children]
  (fn stream [events]
    (call-rescue (math/sum-events events) children)))


(defn coll-min*
  [_ & children]
  (fn stream [events]
    (call-rescue (math/min-event events) children)))


(defn coll-sort*
  [_ field & children]
  (fn stream [events]
    (call-rescue (sort-by field events) children)))


(defn test-action*
  [_ state]
  (fn stream [event]
    (swap! state conj event)))


(defn sdo*
  [_ & children]
  (fn stream [event]
    (call-rescue event children)))


(defn expired*
  "Keep expired events."
  [_ & children]
  (let [time-state (atom 0)]
    (fn stream [event]
      (let [current-time (swap! time-state (fn [old-time]
                                             (max old-time (:time event 0))))]
        (when (event/expired? current-time event)
          (call-rescue event children))))))


(defn not-expired*
  "Keep non-expired events."
  [_ & children]
  (let [time-state (atom 0)]
    (fn stream [event]
      (let [current-time (swap! time-state (fn [old-time]
                                             (max old-time (:time event 0))))]
        (when (not (event/expired? current-time event))
          (call-rescue event children))))))


(defn cond-dt*
  "A stream which detects if a condition `(f event)` is true during `dt` seconds.
  Takes `conditions` (like in the where action) and a time period `dt` in seconds.
  If the condition is valid for all events received during at least the period `dt`, valid events received after the `dt` period will be passed on until an invalid event arrives.
  Skips events that are too old or that do not have a timestamp."
  [_ conditions dt & children]
  (let [condition-fn       (condition/compile-conditions conditions)
        last-changed-state (atom {:ok   false
                                  :time nil})]
    (fn stream [event]
      (let [event-time  (:time event)
            valid-event (condition-fn event)]
        (when event-time      ;; filter events with no time
          (let [{ok :ok time :time}
                (swap! last-changed-state
                       (fn [{ok :ok time :time :as state}]
                         (cond
                           ;; event is validating the condition
                           ;; last event is not ok, has no time or is too old
                           ;; => last-changed-state is now ok with a new time
                           (and valid-event (and (not ok)
                                                 (or (not time)
                                                     (> event-time time))))
                           {:ok true :time event-time}
                           ;; event is not validating the condition
                           ;; => last-changed-state is now ko with no time
                           (not valid-event)
                           {:ok false :time nil}
                           ;; default value, return the state
                           :else state)))]
            (when (and ok
                       (> event-time (+ time dt)))
              (call-rescue event children))))))))


(defn critical*
  [_ & children]
  (fn stream [event]
    (when (event/critical? event)
      (call-rescue event children))))


(defn warning*
  [_ & children]
  (fn stream [event]
    (when (event/warning? event)
      (call-rescue event children))))


(defn default*
  [_ field value & children]
  (fn stream [event]
    (if-not (get event field)
      (call-rescue (assoc event field value) children)
      (call-rescue event children))))


(defn output!*
  [context output-name]
  ;; discard io in test mode
  (if (:test-mode? context)
    (fn stream [_] nil)
    (if-let [output-component (get-in context [:outputs output-name :component])]
      (fn stream [event]
        (when-let [events (keep-non-discarded-events event)]
          (io/inject! output-component (event/sequential-events events))))
      (throw (ex/ex-incorrect (format "Output %s not found" output-name))))))


(defn coalesce*
  [_ {:keys [duration fields]} & children]
  (let [state  (atom {:buffer       {}
                      :current-time 0
                      :last-tick    nil
                      :window       nil})
        key-fn #(vals (select-keys-nested % fields))]
    ;; the implementation can probably be optimized ?
    (fn stream [event]
      (let [buffer-update-fn (fn [current-event]
                               (cond
                                 ;; current-event is nil
                                 (not current-event)
                                 event

                                 ;; current event most recent
                                 (event/most-recent? current-event
                                                     event)
                                 current-event
                                 :else
                                 event))

            current-state    (swap! state
                                    (fn [{:keys [current-time last-tick buffer] :as state}]
                                      ;; remove events with no time
                                      (if (nil? (:time event))
                                        (assoc state :window nil)
                                        (let [current-time (max current-time
                                                                (:time event))]
                                          (cond

                                            ;; event expired, don't keep it
                                            (event/expired? current-time event)
                                            (assoc state :window nil)

                                            ;; to last tick, set it to the current time
                                            ;; and keep the event
                                            (nil? last-tick)
                                            (-> (update-in state
                                                           [:buffer (key-fn event)]
                                                           buffer-update-fn)
                                                (assoc :last-tick (:time event)
                                                       :window nil))

                                            ;; we are still in the same window, add the event
                                            ;; to the buffer
                                            (< current-time (+ last-tick duration))
                                            (-> (update-in state
                                                           [:buffer (key-fn event)]
                                                           buffer-update-fn)
                                                (assoc :window nil
                                                       :current-time current-time))

                                            ;; we should emit
                                            :else
                                            (let [tmp-buffer (->> (update buffer
                                                                          (key-fn event)
                                                                          buffer-update-fn)
                                                                  (remove (fn [[_ v]]
                                                                            (event/expired? current-time v))))]
                                              (-> (assoc state
                                                    :last-tick current-time
                                                    :current-time current-time
                                                    :buffer (into {} tmp-buffer)
                                                    :window (map second tmp-buffer)))))))))]
        (when-let [window (:window current-state)]
          (call-rescue window children))))))


(defn with*
  [_ fields & children]
  (fn stream [event]
    (call-rescue (merge event fields) children)))


(defn coll-rate*
  [_ & children]
  (fn stream [events]
    (call-rescue (math/rate events) children)))


(defn sflatten*
  [_ & children]
  (fn stream [events]
    (doseq [e events]
      (call-rescue e children))))


;; Copyright Riemann authors (riemann.io), thanks to them!
(defn tag*
  [_ tags & children]
  (let [tags (flatten [tags])]
    (fn stream [event]
      (call-rescue
       (assoc event :tags (distinct (concat tags (:tags event))))
       children))))


(defn untag*
  [_ tags & children]
  (let [tags      (set (flatten [tags]))
        blacklist #(not (tags %))]
    (fn stream [event]
      (call-rescue (update event :tags #(filter blacklist %))
                   children))))


;; Copyright Riemann authors (riemann.io), thanks to them!
(defn tagged-all*
  [_ tags & children]
  (let [tag-coll (set (flatten [tags]))]
    (fn stream [event]
      (when (event/tagged-all? tag-coll event)
        (call-rescue event children)
        true))))


;; Copyright Riemann authors (riemann.io), thanks to them!
(defn ddt*
  [_ remove-neg? & children]
  (let [prev (atom nil)]
    (fn stream [event]
      (when-let [m (:metric event)]
        (let [prev-event (let [prev-event @prev]
                           (reset! prev event)
                           prev-event)]
          (when prev-event
            (let [dt (- (:time event) (:time prev-event))]
              (when-not (zero? dt)
                (let [diff (/ (- m (:metric prev-event)) dt)]
                  (when-not (and remove-neg? (> 0 diff))
                    (call-rescue (assoc event :metric diff) children)))))))))))


(defn scale*
  [_ factor & children]
  (fn stream [event]
    (call-rescue (update event :metric * factor) children)))


(defn split*
  [_ clauses & children]
  (let [clauses      (for [index (range (count clauses))]
                       [(nth clauses index) (nth children index)])
        comp-clauses (->> clauses
                          (map (fn [clause]
                                 [(condition/compile-conditions (first clause))
                                  (second clause)])))]
    (fn stream [event]
      (when-let [stream (reduce (fn [state clause]
                                  (if ((first clause) event)
                                    (reduced (second clause))
                                    state))
                                nil
                                comp-clauses)]
        (call-rescue event [stream])))))


(defn throttle*
  [_ config & children]
  (let [last-sent (atom [nil nil])]
    (fn stream [event]
      (when (:time event)
        (let [[_ _ event-to-send]
              (swap! last-sent
                     (fn [[last-time-sent counter _]]
                       (cond
                         ;; window is closed
                         ;; we send the event and
                         ;; reset the counter
                         (or (nil? last-time-sent)
                             (>= (:time event)
                                 (+ last-time-sent (:duration config))))
                         [(:time event)
                          1
                          event]

                         ;; we reached the threshold
                         ;; we stop sending
                         (= counter (:count config))
                         [last-time-sent
                          counter
                          nil]

                         ;; counter is smaller, we let the event
                         ;; pass
                         :else [last-time-sent
                                (inc counter)
                                event])))]
          (when event-to-send
            (call-rescue event-to-send children)))))))


;; Copyright Riemann authors (riemann.io), thanks to them!
(defn moving-event-window*
  [_ config & children]
  (let [window (atom (vec []))]
    (fn stream [event]
      (let [w (swap! window (fn swap [w]
                              (vec (take-last (:size config) (conj w event)))))]
        (call-rescue w children)))))


;; Copyright Riemann authors (riemann.io), thanks to them!
(defn ewma-timeless*
  [_ r & children]
  (let [m          (atom 0)
        c-existing (- 1 r)
        c-new      r]
    (fn stream [event]
      ; Compute new ewma
      (let [m (when-let [metric-new (:metric event)]
                (swap! m (comp (partial + (* c-new metric-new))
                               (partial * c-existing))))]
        (call-rescue (assoc event :metric m) children)))))


;; Copyright Riemann authors (riemann.io), thanks to them!
(defn over*
  [_ n & children]
  (fn stream [event]
    (when-let [m (:metric event)]
      (when (< n m)
        (call-rescue event children)))))


;; Copyright Riemann authors (riemann.io), thanks to them!
(defn under*
  [_ n & children]
  (fn stream [event]
    (when-let [m (:metric event)]
      (when (> n m)
        (call-rescue event children)))))


(defn changed*
  [_ {:keys [field init]} & children]
  (let [state   (atom [init nil])
        nested? (sequential? field)
        get-fn  (if nested?
                  get-in
                  get)]
    (fn stream [event]
      (let [[_ event] (swap! state
                             (fn [s]
                               (let [current-val (get-fn event field)]
                                 (if (= (first s)
                                        current-val)
                                   [(first s) nil]
                                   [current-val event]))))]
        (when event
          (call-rescue event children))))))


(defn project*
  [_ conditions & children]
  (let [conditions-fns (map condition/compile-conditions conditions)
        state          (atom {:buffer       (reduce #(assoc %1 %2 nil)
                                                    {}
                                                    (range 0 (count conditions)))
                              :current-time 0})]
    (fn stream [event]
      (let [result (swap! state
                          (fn [{:keys [current-time buffer]}]
                            ;; ffirst compute the current time
                            (let [current-time (cond
                                                 (nil? (:time event))
                                                 current-time

                                                 (> current-time
                                                    (:time event))
                                                 current-time

                                                 :else
                                                 (:time event))]
                              (cond
                                ;; event expired or no time, don't keep it
                                ;; but filter from the current buffer expired events
                                (or (nil? (:time event))
                                    (event/expired? current-time event))
                                {:buffer       (->> (map #(when-not (event/expired? current-time (second %))
                                                            %)
                                                         buffer)
                                                    (into {}))
                                 :current-time current-time}

                                ;; event not expired, check clauses
                                :else
                                {:buffer       (->> (reduce
                                                     ;; reduce on buffer
                                                     ;; use the key as index
                                                     ;; for the condition fn
                                                     (fn [state [k v]]
                                                       (let [condition-fn (nth conditions-fns k)
                                                             match?       (condition-fn event)]
                                                         (if (and match?
                                                                  (or (nil? v)
                                                                      (> (:time event) (:time v))))
                                                           ;; if the event match and if the current
                                                           ;; event in the buffer is nil or less recent,
                                                           ;; keep it.
                                                           (conj state [k event])
                                                           ;; else, keep the old one if not expired
                                                           (conj state [k (when (and v
                                                                                     (not (event/expired? current-time v)))
                                                                            v)]))))
                                                     []
                                                     buffer)
                                                    (into {}))
                                 :current-time current-time}))))
            events (->> (:buffer result)
                        vals
                        (remove nil?))]
        (when-not (zero? (count events))
          (call-rescue events children))))))


(defn index*
  [context labels]
  (let [i               (:index context)
        channel         (index/channel (:source-stream context))
        default-channel (index/channel :default)
        pubsub          (:pubsub context)]
    (fn stream [event]
      (when-let [t (:time event)]
        (index/new-time? i t))
      (when-not (:test-mode? context)
        (pubsub/publish! pubsub channel event)
        (when (:default context)
          (pubsub/publish! pubsub default-channel event)))
      (index/insert i event labels))))


(defn coll-count*
  [_ & children]
  (fn stream [events]
    ;; send empty event if the list is empty
    (call-rescue (or (math/count-events events)
                     {:metric 0})
                 children)))


(defn sdissoc*
  [_ fields & children]
  (let [to-remove (seq fields)]
    (fn stream [event]
      (call-rescue
       (loop [result event key-list to-remove]
         (if key-list
           (let [key      (first key-list)
                 seq-key? (sequential? key)]
             (recur
              (if seq-key?
                (dissoc-in result key)
                (dissoc result key))
              (next key-list)))
           result))
       children))))


(defn coll-percentiles*
  [_ points & children]
  (fn stream [events]
    (doseq [event (math/sorted-sample events points)]
      (call-rescue event
                   children))))


(defn clear-forks
  [state current-time fork-ttl]
  (let [forks (->> (:forks state)
                   (remove #(> (- current-time fork-ttl)
                               (:time (second %))))
                   (into {}))]
    (assoc state :forks forks :last-gc current-time)))


(defn get-fork-and-gc
  [state new-fork fork-name current-time fork-ttl gc-interval]
  (let [current-time (max current-time (:time state current-time))
        state        (if (and gc-interval
                              (or (= (:last-gc state) 0)
                                  (> current-time (+ (:last-gc state) gc-interval))))
                       (clear-forks state current-time fork-ttl)
                       state)]
    (if-let [fork (get-in state [:forks fork-name :fork])]
      ;; return the new fork
      (-> (assoc state :returned-fork fork)
          (assoc-in [:forks fork-name :time] current-time))
      (let [new-fork-instance (new-fork)]
        (-> (assoc-in state [:forks fork-name] {:fork new-fork-instance
                                                :time current-time})
            (assoc :returned-fork new-fork-instance))))))


(defn by-get-field-fn
  [field]
  (if (sequential? field)
    #(get-in % field)
    field))


;; Copyright Riemann authors (riemann.io), thanks to them!
(defn by-fn
  [{:keys [fields gc-interval fork-ttl]} new-fork]
  (let [f-list   (map by-get-field-fn fields)
        f        (apply juxt f-list)
        fork-ttl (or fork-ttl 3600)
        state    (atom {:last-gc 0})]
    (fn stream [event]
      (let [fork-name (f event)
            fork      (:returned-fork (swap! state get-fork-and-gc new-fork fork-name (:time event) fork-ttl gc-interval))]
        (call-rescue event fork)))))


(defn reinject!*
  [context destination-stream]
  (let [reinject-fn        (:reinject context)
        destination-stream (or destination-stream (:source-stream context))]
    (fn stream [event]
      (reinject-fn event destination-stream))))


(defn async-queue!*
  [context queue-name & children]
  (if (:test-mode? context)
    (apply sdo* context children)
    (if-let [^Executor executor (get-in context [:outputs queue-name :component])]
      (fn stream [event]
        (.execute executor
                  (fn []
                    (call-rescue event children))))
      (throw (ex/ex-incorrect (format "Async queue %s not found"
                                      queue-name))))))


(defn io*
  [context & children]
  (if (:test-mode? context)
    (fn stream [_] nil)
    (apply sdo* context children)))


(defn tap*
  [context tape-name]
  (if (:test-mode? context)
    (let [tap (:tap context)]
      (fn stream [event]
        (swap! tap
               (fn [tap]
                 (update tap tape-name (fn [v] (if v (conj v event) [event])))))))
    ;; discard in non-tests
    (fn stream [_] nil)))


(defn json-fields*
  [_ fields & children]
  (fn stream [event]
    (call-rescue
     (reduce (fn [event field]
               (if (get event field)
                 (update event field json/parse-string true)
                 event))
             event
             fields)
     children)))


(defn exception->event
  "Build a new event from an Exception and from the event which caused it."
  [^Exception e base-event]
  {:time        (:time base-event)
   :service     "vsf-exception"
   :state       "error"
   :metric      1
   :tags        ["exception" (.getName (class e))]
   :exception   e
   :base-event  base-event
   :description (str e "\n\n" (string/join "\n" (.getStackTrace e)))})


(defn exception-stream*
  [_ success-child failure-child]
  (fn stream [event]
    (try
      (success-child event)
      (catch Exception e
        (failure-child (exception->event e event))))))


(defn reaper*
  [context interval destination-stream]
  (let [index              (:index context)
        clock              (atom [0 false])
        reinject-fn        (:reinject context)
        destination-stream (or destination-stream (:source-stream context))]
    (fn stream [event]
      (when (:time event)
        (let [[_ expire?] (swap! clock (fn [[previous-tick _ :as s]]
                                         (if (>= (:time event)
                                                 (+ interval previous-tick))
                                           [(:time event) true]
                                           s)))]
          (when expire?
            (doseq [event (index/expire index)]
              (reinject-fn event destination-stream))))))))


(defn to-base64*
  [_ fields & children]
  (fn stream [event]
    (call-rescue (reduce #(update %1 %2 b64/to-base64) event fields) children)))


(defn from-base64*
  [_ fields & children]
  (fn stream [event]
    (call-rescue (reduce #(update %1 %2 b64/from-base64) event fields) children)))


(defn sformat*
  [_ template target-field fields & children]
  (let [value-fn (fn [event] (reduce #(conj %1 (get event %2)) [] fields))]
    (fn stream [event]
      (call-rescue
       (assoc event
         target-field
         (apply format template (value-fn event)))
       children))))


(defn publish!*
  [context channel]
  (let [pubsub (:pubsub context)]
    (fn stream [event]
      (when-not (:test-mode? context)
        (when-let [event (keep-non-discarded-events event)]
          (pubsub/publish! pubsub channel event))))))


(defn coll-top*
  [_ nb-events & children]
  (fn stream [events]
    (call-rescue (math/extremum-n nb-events > events) children)))


(defn coll-bottom*
  [_ nb-events & children]
  (fn stream [events]
    (call-rescue (math/extremum-n nb-events < events) children)))


(defn stable*
  [_ dt field & children]
  (let [state   (atom {:last-state nil
                       :buffer     []
                       :out        nil
                       ;; last flip time
                       :time       nil
                       ;; clock
                       :max-time   0})
        nested? (sequential? field)
        get-fn  (if nested?
                  get-in
                  get)]
    (fn stream [event]
      (let [event-time  (:time event)
            event-state (get-fn event field)]
        (when event-time
          (let [{:keys [out]}
                (swap! state
                       (fn [{:keys [time buffer last-state max-time] :as state}]
                         (if (< event-time max-time)
                           (assoc state :out nil)
                           (cond
                             ;; no time = first event, or else the state
                             ;; was changed
                             (or (not time)
                                 (not= last-state
                                       event-state))
                             {:time       event-time
                              :last-state event-state
                              :buffer     [event]
                              :out        nil
                              :max-time   event-time}

                             ;; state is equal, but the dt period is not completed
                             (<= event-time (+ time dt))
                             (-> (update state :buffer conj event)
                                 (assoc :out nil :max-time event-time))

                             ;; state is equal, dt seconds passed
                             ;; the current buffer + the event should be sent
                             (> event-time (+ time dt))
                             {:time       time
                              :last-state event-state
                              :buffer     []
                              :max-time   event-time
                              :out        (conj buffer event)}))))]
            (doseq [event out]
              (call-rescue event children))))))))


(defn rename-keys*
  [_ replacement & children]
  (fn stream [event]
    (call-rescue (set/rename-keys event replacement) children)))


(defn keep-keys*
  [_ keys-to-keep & children]
  (let [keyseq (seq keys-to-keep)]
    (fn stream [event]
      (call-rescue
       (select-keys-nested event keyseq)
       children))))


(def keyword->aggr-fn
  {:max               (fn [state event]
                        (if state
                          (if (> (:metric state) (:metric event))
                            state
                            event)
                          event))
   :ssort             (fn [state event]
                        (if state
                          (conj state event)
                          [event]))
   :rate              (fn [state event]
                        (if state
                          (-> (update state :count inc)
                              (assoc :event (event/most-recent-event event (:event state))))
                          {:count 1
                           :event event}))
   :min               (fn [state event]
                        (if state
                          (if (< (:metric state) (:metric event))
                            state
                            event)
                          event))
   :mean              (fn [state event]
                        (if state
                          (-> (update state :sum + (:metric event 0))
                              (update :count inc)
                              (assoc :event (event/most-recent-event event (:event state))))
                          {:sum   (:metric event 0)
                           :count 1
                           :event event}))
   :fixed-time-window (fn [state event]
                        (if state
                          (conj state event)
                          [event]))
   :+                 (fn [state event]
                        (if state
                          (if (> (:time event) (:time state))
                            (update event :metric + (:metric state))
                            (update state :metric + (:metric event)))
                          event))})


(def keyword->aggr-finalizer-fn
  {:ssort (fn [config windows]
            (sort-by
             (if (:nested? config)
               (fn [event] (get-in event (:field config)))
               (:field config))
             (flatten windows)))
   :rate  (fn [config windows]
            (map (fn [window]
                   (assoc (:event window) :metric (/ (:count window) (:duration config))))
                 windows))
   :mean  (fn [_ windows]
            (map (fn [window]
                   (assoc (:event window) :metric (/ (:sum window) (:count window))))
                 windows))})


(defn default-aggr-finalizer
  [_ event]
  event)


(defn get-window
  [event start-time duration]
  (let [window (/ (- (:time event) start-time) duration)]
    (if (>= window 0)
      (int window)
      (dec (int window)))))


(defn aggregation*
  [_ {:keys [duration] :as config} & children]
  (let [accepted-delay (:delay config 0)
        finalizer-fn   (get keyword->aggr-finalizer-fn
                            (:aggr-fn config)
                            default-aggr-finalizer)
        aggr-fn        (get keyword->aggr-fn (:aggr-fn config))
        state          (atom {:start-time     nil
                              :current-window nil
                              :current-time   nil
                              :windows        {}
                              :to-send        nil})]
    (when-not aggr-fn
      (ex/ex-fault! (format "Invalid aggregation function %s" aggr-fn)
                    {:aggr-fn aggr-fn}))
    (fn stream [event]
      (let [s (swap! state
                     (fn [{:keys [start-time current-window current-time] :as state}]
                       (cond
                         ;; No start time, initialize everything
                         (nil? start-time)
                         ;; I'm sure the state can be simplified (eg deduct
                         ;; start time from current time/window or stuff like that)
                         ;; but at least this implementation works
                         (assoc state
                           :start-time (:time event)
                           :current-time (:time event)
                           :current-window (get-window event (:time event) duration)
                           :to-send nil
                           :windows {(get-window event (:time event) duration)
                                     (aggr-fn nil event)})

                         ;; before current window
                         (< (get-window event start-time duration)
                            current-window)
                         (if (< (:time event)
                                (- current-time
                                   accepted-delay))
                           ;; too old, just drop it
                           (assoc state :to-send nil)
                           ;; in toleration
                           (-> (update-in state
                                          [:windows (get-window event start-time duration)]
                                          aggr-fn
                                          event)
                               (assoc :to-send nil)))
                         ;; within or above windows
                         :else
                         (let [window          (get-window event start-time duration)
                               windows         (:windows state)
                               current-time    (max (:current-time state)
                                                    (:time event))
                               windows-to-send (->> (keys windows)
                                                    (filter #(>= (- current-time accepted-delay)
                                                                 ;; time of the end of the old window
                                                                 (+ start-time (* (inc %) duration)))))]
                           (-> (update state :windows
                                       #(apply dissoc % windows-to-send))
                               (update-in [:windows window]
                                          aggr-fn
                                          event)
                               (assoc :current-window window
                                      :current-time current-time
                                      :to-send (vals (select-keys windows windows-to-send))))))))]
        (when-let [windows (:to-send s)]
          (doseq [w (finalizer-fn config windows)]
            (call-rescue w
                         children)))))))


(defn moving-time-window*
  [_ {:keys [duration]} & children]
  (let [state (atom {:cutoff 0
                     :buffer []
                     :send?  true})]
    (fn stream [event]
      (let [result (swap!
                    state
                    (fn [{:keys [cutoff buffer]}]
                      ; Compute minimum allowed time
                      (let [cutoff (max cutoff (- (get event :time 0) duration))
                            send?  (or (nil? (:time event))
                                       (< cutoff (:time event)))
                            buffer (if send?
                                     ; This event belongs in the buffer,
                                     ; and our cutoff may have changed.
                                     (vec (filter
                                           (fn [e] (or (nil? (:time e))
                                                       (< cutoff (:time e))))
                                           (conj buffer event)))
                                     buffer)]
                        {:cutoff cutoff
                         :buffer buffer
                         :send?  send?})))]
        (when (:send? result)
          (call-rescue (:buffer result) children))))))


(defn coll-increase*
  [_ & children]
  (fn [[event & events]]
    (when (and event events)
      (let [{:keys [most-recent-event oldest-event]}
            (reduce
             (fn [{:keys [most-recent-event oldest-event] :as state} event]
               (cond

                 (< (:time event) (:time oldest-event))
                 (assoc state :oldest-event event)

                 (> (:time event) (:time most-recent-event))
                 (assoc state :most-recent-event event)

                 :else
                 state))
             {:most-recent-event event
              :oldest-event      event}
             events)
            new-metric (- (:metric most-recent-event)
                          (:metric oldest-event))]
        ;; check if the counter was resetted
        (when (> new-metric 0)
          (call-rescue (assoc most-recent-event :metric new-metric) children))))))


(defn smax*
  [_ & children]
  (let [result (atom {})]
    (fn [event]
      (let [metric (get @result :metric)]
        (if (or (not metric)
                (> (:metric event) metric))
          (do (reset! result event)
              (call-rescue event children))
          (call-rescue @result children))))))


(defn smin*
  [_ & children]
  (let [result (atom {})]
    (fn [event]
      (let [metric (get @result :metric)]
        (if (or (not metric)
                (< (:metric event) metric))
          (do (reset! result event)
              (call-rescue event children))
          (call-rescue @result children))))))


(defn extract*
  [_ k & children]
  (fn [event]
    (when-let [result (get event k)]
      (call-rescue result children))))


(defn percentiles*
  [_ {:keys [duration
             percentiles
             delay
             highest-trackable-value
             nb-significant-digits
             lowest-discernible-value]}
   & children]
  (let [^Recorder recorder
              (cond
                lowest-discernible-value (Recorder. lowest-discernible-value
                                                    highest-trackable-value
                                                    nb-significant-digits)

                highest-trackable-value (Recorder. ^Long highest-trackable-value
                                                   ^Integer nb-significant-digits)

                :else (Recorder. ^Integer nb-significant-digits))
        state (atom {:last-flush 0
                     :emit?      nil})
        delay (or delay 0)]
    (fn stream [event]
      (let [state (swap! state (fn [{:keys [last-flush]}]
                                 (cond
                                   (= 0 last-flush)
                                   {:last-flush (:time event) :emit? false}

                                   (> (:time event)
                                      (+ last-flush duration))
                                   {:last-flush (:time event) :emit? true}

                                   (>= (:time event)
                                       (- last-flush delay))
                                   {:last-flush last-flush :emit? false}

                                   :else
                                   {:last-flush last-flush :emit? false :discard true})))]
        (when-not (:discard state)
          (if (:emit? state)
            (let [^Histogram histogram (.getIntervalHistogram recorder)]
              (.recordValue recorder (:metric event))
              (doseq [percentile percentiles]
                (call-rescue (assoc event
                               :metric (.getValueAtPercentile histogram
                                                              (double (* 100 percentile)))
                               :quantile percentile)
                             children)))
            (.recordValue recorder (:metric event))))))))


(def action->fn
  {:above-dt            cond-dt*
   :sum                 aggregation*
   :async-queue!        async-queue!*
   :below-dt            cond-dt*
   :between-dt          cond-dt*
   :bottom              aggregation*
   :changed             changed*
   :coalesce            coalesce*
   :coll-bottom         coll-bottom*
   :coll-count          coll-count*
   :coll-increase       coll-increase*
   :coll-max            coll-max*
   :coll-mean           coll-mean*
   :coll-min            coll-min*
   :coll-percentiles    coll-percentiles*
   :coll-quotient       coll-quotient*
   :coll-rate           coll-rate*
   :coll-sort           coll-sort*
   :coll-sum            coll-sum*
   :coll-top            coll-top*
   :coll-where          coll-where*
   :critical            critical*
   :critical-dt         cond-dt*
   :debug               debug*
   :default             default*
   :decrement           decrement*
   :ddt                 ddt*
   :ddt-pos             ddt*
   :info                info*
   :error               error*
   :extract             extract*
   :ewma-timeless       ewma-timeless*
   :exception-stream    exception-stream*
   :expired             expired*
   :fixed-event-window  fixed-event-window*
   :fixed-time-window   aggregation*
   :from-base64         from-base64*
   :increment           increment*
   :index               index*
   :io                  io*
   :json-fields         json-fields*
   :keep-keys           keep-keys*
   :mean                aggregation*
   :moving-event-window moving-event-window*
   :moving-time-window  moving-time-window*
   :not-expired         not-expired*
   :outside-dt          cond-dt*
   :over                over*
   :percentiles         percentiles*
   :project             project*
   :publish!            publish!*
   :output!             output!*
   :rate                aggregation*
   :reaper              reaper*
   :reinject!           reinject!*
   :rename-keys         rename-keys*
   :scale               scale*
   :sdissoc             sdissoc*
   :sdo                 sdo*
   :sflatten            sflatten*
   :sformat             sformat*
   :smax                smax*
   :smin                smin*
   :split               split*
   :ssort               aggregation*
   :stable              stable*
   :tag                 tag*
   :tagged-all          tagged-all*
   :tap                 tap*
   :test-action         test-action*
   :throttle            throttle*
   :to-base64           to-base64*
   :top                 aggregation*
   :under               under*
   :untag               untag*
   :warning             warning*
   :where               where*
   :with                with*})


(defmacro compile-completions []
  (->> action->fn
       (mapv (fn [[fn-key]]
               (let [fn-name (name fn-key)
                     fn-var  (requiring-resolve (symbol "vsf.action" fn-name))]
                 {:label fn-name
                  :type  "function"
                  :info  (-> fn-var meta :doc)})))))


(defmacro compile-controls []
  (reduce
   (fn [acc [fn-key]]
     (let [fn-name (name fn-key)
           fn-var  (requiring-resolve (symbol "vsf.action" fn-name))
           {:keys [control-type control-params leaf-action]} (meta fn-var)
           control (cond-> {:id           fn-name
                            :label        fn-name
                            :control-type control-type
                            :leaf-action  leaf-action}
                           (some? control-params)
                           (assoc :control-params control-params))]
       (assoc acc fn-name control)))
   {}
   action->fn))
