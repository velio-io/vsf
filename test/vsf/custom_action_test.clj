(ns vsf.custom-action-test
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.string :as string]
            [clojure.set :as set]
            [vsf.registry :as registry]
            [vsf.action :as action]
            [vsf.process :as process]))

(defn format-exporter-params
  [exporter-ids]
  (->> exporter-ids
       (map name)
       (string/join ",")))

(defn parse-exporter-params
  [{:keys [params]}]
  (let [[ids] params]
    (if (string? ids)
      (->> (string/split ids #",")
           (remove string/blank?)
           (map string/trim)
           (map keyword))
      ids)))

(defn set-exporters*
  [_ exporter-ids & children]
  (let [exporter-ids (flatten [exporter-ids])]
    (fn stream [event]
      (let [existing (set (get event :exporter_ids []))
            new      (set exporter-ids)]
        (process/call-rescue
         (assoc event :exporter_ids (vec (clojure.set/union existing new)))
         children)))))

(def set-exporters-spec
  [:catn [:exporter-ids
           [:or keyword?
            [:vector keyword?]]]])

(def set-exporters-meta
  {:doc            "Does something really cool"
   :control-type   :map
   :control-params {:fields [{:field :exporter-ids :label "Exporter Ids" :type :string}]
                    :parse  'vsf.custom-action-test/parse-exporter-params
                    :format 'vsf.custom-action-test/format-exporter-params}
   :leaf-action    false})

(defn set-exporters
  "Adds one or more :exporter_ids to the event for routing"
  {:control-type   :input
   :control-params {:format 'vsf.custom-action-test/format-exporter-params
                    :parse  'vsf.custom-action-test/parse-exporter-params}
   :leaf-action    true}
  [exporter-ids & children]
  (action/valid-action? :set-exporters (registry/get-spec :set-exporters) [exporter-ids])
  {:action      :set-exporters
   :description {:message (format "Export event to: %s" exporter-ids)}
   :params      [exporter-ids]
   :children children})

(deftest register-and-retrieve-custom-action
  (testing "Registering :set-exporters populates registry"
    (registry/register-action!
     {:action-key  :set-exporters
      :builder-fn  set-exporters*
      :spec-schema set-exporters-spec
      :meta        set-exporters-meta})
    (is (contains? (set (registry/all-actions)) :set-exporters)
        "registry/all-actions should include :set-exporters")
    (is (= set-exporters*
           (registry/get-builder :set-exporters))
        "get-builder should return the function we registered")
    (is (= set-exporters-spec
           (registry/get-spec :set-exporters))
        "get-spec should return the Malli schema we registered")
    (is (= set-exporters-meta
           (registry/get-meta :set-exporters))
        "get-meta should return the metadata we registered")))

(deftest set-exporters-dsl-constructor-and-validation
  (testing "DSL constructor emits correct action map"
    (let [child (fn [e] e)
          {:keys [action params children]} (apply set-exporters [:baz child])]
      (is (= :set-exporters action))
      (is (= [:baz] params))
      (is (= [child] children)))

    (testing "DSL constructor rejects bad params"
      (is (thrown? Exception (set-exporters 123))
          "non-keyword/string-or-vector should fail spec validation"))))