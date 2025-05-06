(ns vsf.metadata
  (:require
    [vsf.registry :as registry]))

(defn compile-completions []
  (->> (registry/all-actions)
       (mapv (fn [k]
               {:label (name k)
                :type  "function"
                :info  (get-in (registry/get-meta k) [:doc])}))))

(defn compile-controls []
  (->> (registry/all-actions)
       (reduce (fn [acc k]
                 (let [{:keys [doc control-type control-params leaf-action]}
                       (registry/get-meta k)
                       base  {:id           (name k)
                              :label        (name k)
                              :doc          doc
                              :control-type control-type
                              :leaf-action  leaf-action}
                       entry (if control-params
                               (assoc base :control-params control-params)
                               base)]
                   (assoc acc (name k) entry)))
               {})))