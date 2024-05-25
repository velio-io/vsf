(ns vsf.spec
  (:refer-clojure :exclude [format count delay])
  (:require
   [clojure.string :as string]
   [malli.core :as m]
   [malli.error :as me]
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


(defn float-number? [number]
  #?(:clj  (float? number)
     :cljs (and (float? number) (js/isFinite number))))


(defn ex-invalid-spec
  "Returns an exception when value `x` does not conform to spec `spex`"
  [error data]
  (let [message (format "Invalid spec: %s" error)
        data    (assoc data :explain-data error)]
    #?(:clj  (ex/ex-incorrect message data)
       :cljs (js/Error message #js {:cause data}))))


(defn assert-spec-valid
  "Asserts that `x` conforms to `spec`, otherwise throws with
   `ex-invalid-spec`"
  ([spec x]
   (assert-spec-valid spec x nil))
  ([spec x data]
   (when-some [error (m/explain spec x)]
     (-> (me/humanize error)
         (ex-invalid-spec data)
         (throw)))
   x))


(def size pos-int?)
(def delay nat-int?)
(def duration pos-int?)
(def threshold number?)
(def high number?)
(def low number?)
(def init any?)
(def count pos-int?)

(def condition
  [:fn {:error/message "condition should match the pattern - [:keyword-operator operator-arguments]
   where :keyword-operator is one of :pos? :neg? :zero? :> :>= :< :<= := :always-true :contains
   :absent :regex :nil? :not-nil? :not=. Also there's a two special cases :or and :and for combining multiple conditions.
   operator-arguments could be a fixed value (e.g. number or string) or a keyword pointing to some key in the event (at least one pointer should be present)"}
   condition/valid-condition?])

(def conditions
  [:sequential {:min 1} condition])

(def ne-string
  [:and
   string?
   [:fn {:error/message "string can't be empty"}
    string/blank?]])

(def field
  [:or keyword?
   [:sequential {:min 1}
    [:or keyword?
     [:sequential {:min 1} keyword?]]]])

(def fields
  [:sequential {:min 1} field])

(def where
  [:catn [:conditions condition]])

(def coll-where
  [:catn [:conditions condition]])

(def fixed-event-window
  [:catn [:config
          [:map [:size size]]]])

(def coll-sort
  [:catn [:field keyword?]])

(def above-dt
  [:catn [:config
          [:map
           [:threshold threshold]
           [:duration duration]]]])

(def below-dt
  [:catn [:config
          [:map
           [:threshold threshold]
           [:duration duration]]]])

(def between-dt
  [:catn [:config
          [:map
           [:low low]
           [:high high]
           [:duration duration]]]])

(def outside-dt
  [:catn [:config
          [:map
           [:low low]
           [:high high]
           [:duration duration]]]])

(def critical-dt
  [:catn [:config
          [:map [:duration duration]]]])

(def default
  [:catn
   [:field [:fn not-null]]
   [:value any?]])

(def output!
  [:catn [:output-name keyword?]])

(def coalesce
  [:catn [:config
          [:map
           [:duration duration]
           [:fields fields]]]])

(def tag
  [:catn [:tags
          [:or string?
           [:sequential {:min 1} string?]]]])

(def untag
  [:catn [:tags
          [:or string?
           [:sequential {:min 1} string?]]]])

(def tagged-all
  [:catn [:tags
          [:or string?
           [:sequential {:min 1} string?]]]])

(def scale
  [:catn [:factor number?]])

(def throttle
  [:catn [:config
          [:map
           [:duration duration]
           [:count count]]]])

(def moving-event-window
  [:catn [:config
          [:map [:size size]]]])

(def ewma-timeless
  [:catn [:r number?]])

(def over
  [:catn [:n number?]])

(def under
  [:catn [:n number?]])

(def changed
  [:catn [:config
          [:map
           [:init string?]
           [:field field]]]])

(def project
  [:catn [:conditions
          [:sequential {:min 1} condition]]])

(def index
  [:catn [:labels
          [:sequential {:min 1} keyword?]]])

(def sdissoc
  [:catn [:sdissoc
          [:or keyword?
           [:sequential {:min 1}
            [:or keyword?
             [:sequential {:min 1} keyword?]]]]]])

(def coll-percentiles
  [:catn [:points
          [:sequential {:min 1}
           [:fn {:error/message "each point should be a decimal number"}
            float-number?]]]])

(def by-fields
  [:sequential
   [:or keyword?
    [:sequential {:min 1} keyword?]]])

(def by-gc-interval pos-int?)
(def by-fork-ttl pos-int?)

(def by
  [:catn [:config
          [:map
           [:fields by-fields]
           [:gc-interval {:optional true} by-gc-interval]
           [:fork-ttl {:optional true} by-fork-ttl]]]])

(def reinject
  [:catn [:destination-stream [:or keyword? nil?]]])

(def async-queue!
  [:catn [:queue-name keyword?]])

(def tap
  [:catn [:tap-name keyword?]])

(def json-fields
  [:catn [:fields
          [:or keyword?
           [:sequential {:min 1} keyword?]]]])

(def reaper
  [:catn
   [:interval pos-int?]
   [:destination-stream [:or keyword? nil?]]])

(def to-base64
  [:catn [:fields
          [:sequential {:min 1} keyword?]]])

(def from-base64
  [:catn [:fields
          [:sequential {:min 1} keyword?]]])

(def sformat
  [:catn
   [:template string?]
   [:target-field keyword?]
   [:fields [:sequential {:min 1} keyword?]]])

(def publish!
  [:catn [:channel keyword?]])

(def coll-top
  [:catn [:nb-events pos-int?]])

(def coll-bottom
  [:catn [:nb-events pos-int?]])

(def stable
  [:catn
   [:dt pos-int?]
   [:field [:or keyword?
            [:sequential {:min 1} keyword?]]]])

(def rename-keys
  [:catn [:replacement
          [:map-of keyword? keyword?]]])

(def keep-keys
  [:catn [:keys-to-keep
          [:or keyword?
           [:sequential {:min 1}
            [:or keyword?
             [:sequential {:min 1} keyword?]]]]]])

(def include-variables
  [:map-of keyword? any?])

(def include-profile keyword?)

(def include-config
  [:map
   [:include/variables {:optional true} include-variables]
   [:include/profile {:optional true} include-profile]])

(def include
  [:catn
   [:path ne-string]
   [:config include-config]])

(def sum
  [:catn [:config
          [:map
           [:duration duration]
           [:delay {:optional true} delay]]]])

(def top
  [:catn [:config
          [:map
           [:duration duration]
           [:delay {:optional true} delay]]]])

(def bottom
  [:catn [:config
          [:map
           [:duration duration]
           [:delay {:optional true} delay]]]])

(def mean
  [:catn [:config
          [:map
           [:duration duration]
           [:delay {:optional true} delay]]]])

(def fixed-time-window
  [:catn [:config
          [:map
           [:duration duration]
           [:delay {:optional true} delay]]]])

(def moving-time-window
  [:catn [:config
          [:map [:duration duration]]]])

(def ssort
  [:catn [:config
          [:map
           [:duration duration]
           [:field field]
           [:delay {:optional true} delay]]]])

(def extract
  [:catn [:k keyword?]])

(def rate
  [:catn [:config
          [:map
           [:duration duration]
           [:delay {:optional true} delay]]]])

(def highest-trackable-value pos-int?)
(def nb-significant-digits pos-int?)
(def lowest-discernible-value number?)

(def percentiles-percentiles
  [:sequential {:min 1} number?])

(def percentiles
  [:catn [:config
          [:map
           [:duration duration]
           [:percentiles percentiles-percentiles]
           [:nb-significant-digits nb-significant-digits]
           [:delay {:optional true} delay]
           [:highest-trackable-value {:optional true} highest-trackable-value]
           [:lowest-discernible-value {:optional true} lowest-discernible-value]]]])
