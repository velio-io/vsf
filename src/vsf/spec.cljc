(ns vsf.spec
  (:refer-clojure :exclude [format])
  (:require
   [clojure.spec.alpha :as s]
   [clojure.string :as string]
   [vsf.condition :as condition]
   #?(:cljs [goog.string :as gstring])
   #?(:cljs [goog.string.format])
   #?(:clj [exoscale.ex :as ex])))


(def format
  #?(:clj  clojure.core/format
     :cljs gstring/format))


(defn not-null
  [v]
  (not (nil? v)))


(defn ex-invalid-spec
  "Returns an exception when value `x` does not conform to spec `spex`"
  ([spec x]
   (ex-invalid-spec spec x nil))
  ([spec x data]
   (let [message (format "Invalid spec: %s" (s/explain-str spec x))
         data    (assoc data :explain-data (s/explain-data spec x))]
     #?(:clj  (ex/ex-incorrect message data)
        :cljs (js/Error message)))))


(defn assert-spec-valid
  "Asserts that `x` conforms to `spec`, otherwise throws with
   `ex-invalid-spec`"
  ([spex x]
   (assert-spec-valid spex x nil))
  ([spec x data]
   (when-not (s/valid? spec x)
     (throw (ex-invalid-spec spec x data)))
   x))


(s/def ::size pos-int?)
(s/def ::delay nat-int?)
(s/def ::duration pos-int?)
(s/def ::threshold number?)
(s/def ::high number?)
(s/def ::low number?)
(s/def ::init any?)
(s/def ::count pos-int?)
(s/def ::condition condition/valid-condition?)

(s/def ::ne-string
  (s/and string? (complement string/blank?)))

(s/def ::field
  (s/or :keyword keyword?
        :seq (s/coll-of keyword?)))

(s/def ::fields
  (s/coll-of
   (s/or :keyword keyword?
         :seq (s/coll-of keyword?))))

(s/def ::where
  (s/cat :conditions ::condition))

(s/def ::coll-where
  (s/cat :conditions ::condition))

(s/def ::fixed-event-window
  (s/cat :config (s/keys :req-un [::size])))

(s/def ::coll-sort
  (s/cat :field keyword?))

(s/def ::above-dt
  (s/cat :config (s/keys :req-un [::threshold ::duration])))

(s/def ::below-dt
  (s/cat :config (s/keys :req-un [::threshold ::duration])))

(s/def ::between-dt
  (s/cat :config (s/keys :req-un [::high ::low ::duration])))

(s/def ::outside-dt
  (s/cat :config (s/keys :req-un [::low ::high ::duration])))

(s/def ::critical-dt
  (s/cat :config (s/keys :req-un [::duration])))

(s/def ::default
  (s/cat :field not-null :value any?))

(s/def ::output!
  (s/cat :output-name keyword?))

(s/def ::coalesce
  (s/cat :config (s/keys :req-un [::duration ::fields])))

(s/def ::tag
  (s/cat :tags (s/or :single string?
                     :multiple (s/coll-of string?))))

(s/def ::untag
  (s/cat :tags (s/or :single string?
                     :multiple (s/coll-of string?))))

(s/def ::tagged-all
  (s/cat :tags (s/or :single string?
                     :multiple (s/coll-of string?))))

(s/def ::scale
  (s/cat :factor number?))

(s/def ::throttle
  (s/cat :config (s/keys :req-un [::count ::duration])))

(s/def ::moving-event-window
  (s/cat :config (s/keys :req-un [::size])))

(s/def ::ewma-timeless
  (s/cat :r number?))

(s/def ::over
  (s/cat :n number?))

(s/def ::under
  (s/cat :n number?))

(s/def ::changed
  (s/cat :config (s/keys :req-un [::init ::field])))

(s/def ::project
  (s/cat :conditions (s/coll-of ::condition)))

(s/def ::index
  (s/cat :labels (s/coll-of keyword?)))

(s/def ::sdissoc
  (s/cat :sdissoc
         (s/or :single keyword?
               :multiple (s/coll-of
                          (s/or
                           :keyword keyword?
                           :seq (s/coll-of keyword?))))))

(s/def ::coll-percentiles
  (s/cat :points (s/coll-of number?)))

(s/def :by/fields
  (s/coll-of (s/or :single keyword?
                   :multiple (s/coll-of keyword?))))

(s/def :by/gc-interval pos-int?)
(s/def :by/fork-ttl pos-int?)

(s/def ::by
  (s/cat :config (s/keys :req-un [:by/fields]
                         :opt-un [:by/gc-interval
                                  :by/fork-ttl])))

(s/def ::reinject
  (s/cat :destination-stream
         (s/or :keyword keyword?
               :nil nil?)))

(s/def ::async-queue!
  (s/cat :queue-name keyword?))

(s/def ::tap
  (s/cat :tap-name keyword?))

(s/def ::json-fields
  (s/cat :fields
         (s/or :single keyword?
               :multiple (s/coll-of keyword?))))

(s/def ::reaper
  (s/cat :interval pos-int?
         :destination-stream (s/or :keyword keyword?
                                   :nil nil?)))

(s/def ::to-base64
  (s/cat :fields (s/coll-of keyword?)))

(s/def ::from-base64
  (s/cat :fields (s/coll-of keyword?)))

(s/def ::sformat
  (s/cat :template string?
         :target-field keyword?
         :fields (s/coll-of keyword?)))

(s/def ::publish!
  (s/cat :channel keyword?))

(s/def ::coll-top
  (s/cat :nb-events pos-int?))

(s/def ::coll-bottom
  (s/cat :nb-events pos-int?))

(s/def ::stable
  (s/cat :dt pos-int?
         :field (s/or :keyword keyword?
                      :seq (s/coll-of keyword?))))

(s/def ::rename-keys
  (s/cat :replacement (s/map-of keyword? keyword?)))

(s/def ::keep-keys
  (s/cat :keys-to-keep (s/coll-of (s/or :keyword keyword?
                                        :seq (s/coll-of keyword?)))))

(s/def :include/variables
  (s/map-of keyword? any?))

(s/def :include/profile keyword?)

(s/def :include/config
  (s/keys :opt-un [:include/variables :include/profile]))

(s/def ::include
  (s/cat :path ::ne-string
         :config :include/config))

(s/def ::sum
  (s/cat :config (s/keys :req-un [::duration]
                         :opt-un [::delay])))

(s/def ::top
  (s/cat :config (s/keys :req-un [::duration]
                         :opt-un [::delay])))

(s/def ::bottom
  (s/cat :config (s/keys :req-un [::duration]
                         :opt-un [::delay])))

(s/def ::mean
  (s/cat :config (s/keys :req-un [::duration]
                         :opt-un [::delay])))

(s/def ::fixed-time-window
  (s/cat :config
         (s/keys :req-un [::duration]
                 :opt-un [::delay])))

(s/def ::moving-time-window
  (s/cat :config (s/keys :req-un [::duration])))

(s/def ::ssort
  (s/cat :config (s/keys :req-un [::duration ::field]
                         :opt-un [::delay])))

(s/def ::extract
  (s/cat :k keyword?))

(s/def ::rate
  (s/cat :config (s/keys :req-un [::duration]
                         :opt-un [::delay])))

(s/def ::highest-trackable-value pos-int?)
(s/def ::nb-significant-digits pos-int?)
(s/def ::lowest-discernible-value number?)

(s/def :percentiles/percentiles
  (s/coll-of number?))

(s/def ::percentiles
  (s/cat :config
         (s/keys :req-un [::duration
                          :percentiles/percentiles
                          ::nb-significant-digits]
                 :opt-un [::delay
                          ::highest-trackable-value
                          ::lowest-discernible-value])))
