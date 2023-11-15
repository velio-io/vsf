(ns vsf.pubsub)


(defprotocol IPubSub
  (add [this channel handler])
  (rm [this channel id])
  (publish! [this channel event]))


(defrecord PubSub [subscriptions]
  IPubSub
  (add [_ channel handler]
    (let [id (random-uuid)]
      (swap! subscriptions assoc-in
             [(keyword channel)
              id]
             handler)
      id))
  (rm [_ channel id]
    (swap! subscriptions update (keyword channel) dissoc id))
  (publish! [_ channel event]
    (doseq [[_ handler] (get @subscriptions channel)]
      (handler event))))


(defn pubsub
  "PubSub record constructor function"
  []
  (->PubSub (atom {})))
