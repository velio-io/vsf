;; This code is from the Riemann code base (and slightly adapted)
;; Copyright Riemann authors (riemann.io), thanks to them!
(ns vsf.index
  (:require
   [clojure.tools.logging :as log]
   [vsf.condition :as condition]
   [vsf.time :as time])
  (:import
   [java.util Map$Entry]
   [org.cliffc.high_scale_lib NonBlockingHashMap]))


(defn channel
  "Build the index channel name for ws subscriptions for a given stream."
  [stream-name]
  (-> (format "%s-index" (name stream-name))
      keyword))


(defprotocol IIndex
  (size-index [this]
    "Returns the index size")
  (clear-index [this]
    "Resets the index")
  (current-time [this]
    "Returns the index current time")
  (new-time? [this t]
    "Takes a number representing the time as parameter and set the index current time if necessary")
  (delete [this labels]
    "Deletes an event by labels.")
  (expire [this]
    "Return a seq of expired states from this index, removing each.")
  (search [this condition]
    "Returns a seq of events from the index matching this query.")
  (insert [this event labels]
    "Updates index with event")
  (lookup [this labels]
    "Lookup an indexed event from the index"))


(defrecord Index [^NonBlockingHashMap index current-time]
  IIndex
  (size-index [_]
    (.size index))
  (clear-index [_]
    (.clear index))
  (current-time [_]
    @current-time)
  (new-time? [_ t]
    (swap! current-time (fn [current] (max t current))))
  (delete [_ k]
    (.remove index k))
  (expire [this]
    (reduce
     (fn [state ^Map$Entry map-entry]
       (let [labels (.getKey map-entry)
             event  (.getValue map-entry)]
         (try
           (let [age (- @current-time (:time event))
                 ttl (or (:ttl event) time/default-ttl)]
             (if (< ttl age)
               (do (delete this labels)
                   (conj state (assoc event :state "expired")))
               state))
           (catch Exception e
             (log/error e (format "Caught exception while trying to expire labels %s, event %s"
                                  (pr-str labels)
                                  (pr-str event)))
             (delete this labels)
             state))))
     []
     index))

  (search [_ condition]
    (let [condition-fn (condition/compile-conditions condition)]
      (filter condition-fn (.values index))))

  (insert [this event labels]
    (when (:time event)
      (if (= "expired" (:state event))
        (delete this labels)
        (.put index (select-keys event labels) event))))

  (lookup [_ k]
    (.get index k)))


(defn index
  "Index record constructor function"
  []
  (->Index (NonBlockingHashMap.) (atom 0)))
