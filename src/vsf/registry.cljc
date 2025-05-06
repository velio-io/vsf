(ns vsf.registry)

(defonce builders (atom {}))
(defonce specs    (atom {}))
(defonce metas    (atom {}))

(defn register-spec! [k schema]
  (swap! specs assoc k schema))

(defn register-action!
  "Register (or overwrite) an action.  keys:
     :action-key   keyword
     :builder-fn   fn
     :spec-schema  Malli schema
     :meta         {:doc ..., :control-type ..., :control-params ..., :leaf-action ...}"
  [{:keys [action-key builder-fn spec-schema meta]}]
  (swap! builders assoc action-key builder-fn)
  (swap! specs    assoc action-key spec-schema)
  (swap! metas    assoc action-key meta))

(defn all-actions []
  (keys @builders))

(defn get-builder [k]
  (get @builders k))

(defn get-spec [k]
  (get @specs k))

(defn get-meta [k]
  (get @metas k))