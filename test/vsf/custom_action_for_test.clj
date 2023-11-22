(ns vsf.custom-action-for-test
  (:require
   [vsf.process :refer [call-rescue]]))


(defn calc-sum [_ctx {:keys [of-keys as]} & children]
  (fn [event]
    (let [sum (->> (select-keys event of-keys)
                   (map val)
                   (apply +))]
      (call-rescue (assoc event as sum) children))))
