(ns vsf.core
  (:require
   [clojure.set :as set]
   [clojure.string :as string]
   [clojure.tools.logging :as log]
   [exoscale.ex :as ex]
   [vsf.process :as process]
   [vsf.index :as index]))


(defn compile!
  [context stream]
  (cond
    (nil? stream)
    nil

    (sequential? stream)
    (->> stream (map (partial compile! context)) doall)

    :else
    (let [action (:action stream)
          func   (if (symbol? action)
                   (requiring-resolve action)
                   (get process/action->fn action))
          params (:params stream)]
      ;; verify if the fn is found or if we are in the special
      ;; case of the by stream
      (if (or (= :by action) func)
        (if (= :by action)
          ;; pass a fn compiling children to by-fn
          ;; in order to generate one child per fork
          (process/by-fn (first params)
                         #(compile! context (:children stream)))

          (let [children (compile! context (:children stream))]
            (try
              (if (seq params)
                (apply func context (concat params children))
                (apply func context children))
              (catch Exception e
                (log/error (format "Your EDN configuration is incorrect. Error in action '%s' with parameters '%s'"
                                   (name action)
                                   (pr-str params)))
                (throw e)))))
        (let [error (format "Your EDN configuration is incorrect. Action %s not found." action)]
          (log/error error)
          (ex/ex-incorrect! error))))))


(defn compile-stream!
  "Compile a stream to functions and associate to it its entrypoint."
  [context stream]
  (assoc stream
    :context context
    :entrypoint (compile! context (:actions stream))))


(defn compile-output!
  "Config an output configuration.
  Adds the :component key to the output"
  [_output-name output-config]
  (let [t (:type output-config)]
    (cond
      ;; it's a custom output
      ;; need to resolve the fn from the config
      (= :custom t)
      (assoc output-config :component
                           ((requiring-resolve (:builder output-config)) (:config output-config)))

      :else
      (throw (ex/ex-incorrect (format "Invalid Output: %s" t) output-config)))))


(defn stream!
  [stream event]
  ((:entrypoint stream) event))


(defn config-keys
  "Returns, from a configuration, the keys as a set"
  [config]
  (->> config
       keys
       (map keyword)
       set))


(defn new-config
  [old-config new-config]
  (let [old-names  (config-keys old-config)
        new-names  (config-keys new-config)
        to-remove  (set/difference old-names new-names)
        to-add     (set/difference new-names old-names)
        to-compare (set/intersection old-names new-names)
        to-reload  (set (remove
                         (fn [n]
                           (= (get old-config n)
                              (get new-config n)))
                         to-compare))]
    {:to-remove to-remove
     :to-add    to-add
     :to-reload to-reload}))


(defprotocol IStreamHandler
  (init [this] "Prepare the required state for stream handler")
  (reload [this streams-configurations] "Add the new configuration")
  (get-stream [this stream-name] "Get a  stream")
  (list-streams [this] "List streams")
  (add-stream [this stream-name stream-configuration] "Add a new stream")
  (remove-stream [this stream-name] "Remove a stream by name")
  (push! [this event streams] "Inject an event into a list of streams"))


(deftype StreamHandler
  [context state outputs-configurations streams-configurations test-mode? pubsub]
  IStreamHandler
  (init [this]
    (let [outputs (->> outputs-configurations
                       (map (fn [[k v]]
                              [k (compile-output! k v)]))
                       (into {}))
          ctx     {:outputs    outputs
                   :test-mode? test-mode?
                   :pubsub     pubsub
                   :reinject   #(push! this %1 %2)}
          streams (->> streams-configurations
                       (mapv (fn [[k v]]
                               [k (compile-stream!
                                   (assoc ctx
                                     :source-stream k
                                     :index (index/index)
                                     :default (boolean (:default v)))
                                   (update v :default boolean))]))
                       (into {}))]
      (reset! context ctx)
      (reset! state {:streams                streams
                     :streams-configurations streams-configurations})))

  (reload [_ new-streams-configurations]
    (log/info "Reloading streams")

    (let [{:keys [streams streams-configurations]} @state
          {:keys [to-remove to-add to-reload]}
          (new-config streams-configurations new-streams-configurations)
          ;; new or streams to reload should be added to the current config
          ;; should be compiled first
          ctx                        @context
          streams-configs-to-compile (select-keys new-streams-configurations (set/union to-add to-reload))
          new-compiled-streams       (->> streams-configs-to-compile
                                          (mapv (fn [[k v]]
                                                  [k (compile-stream!
                                                      (assoc ctx
                                                        :source-stream k
                                                        :index (index/index)
                                                        :default (boolean (:default v)))
                                                      (update v :default boolean))]))
                                          (into {})
                                          (merge (apply dissoc streams to-remove)))]
      (when (seq to-remove)
        (log/infof "Removing streams %s" (string/join #", " to-remove)))

      (when (seq to-reload)
        (log/infof "Reloading streams %s" (string/join #", " to-reload)))

      (when (seq to-add)
        (log/infof "Adding new streams %s" (string/join #", " to-add)))

      (swap! state assoc
             :streams new-compiled-streams
             :streams-configurations new-streams-configurations)))

  (add-stream [_ stream-name stream-configuration]
    (log/infof "Adding stream %s" stream-name)

    (let [ctx                        @context
          {:keys [streams streams-configurations]} @state
          compiled-stream            (compile-stream!
                                      (assoc ctx
                                        :source-stream stream-name
                                        :index (index/index))
                                      (update stream-configuration :default boolean))
          new-compiled-streams       (assoc streams stream-name compiled-stream)
          new-streams-configurations (assoc streams-configurations stream-name stream-configuration)]
      (swap! state assoc
             :streams new-compiled-streams
             :streams-configurations new-streams-configurations)))

  (remove-stream [_ stream-name]
    (log/infof "Removing stream %s" stream-name)

    (let [{:keys [streams streams-configurations]} @state
          new-compiled-streams       (dissoc streams stream-name)
          new-streams-configurations (dissoc streams-configurations stream-name)]
      (swap! state assoc
             :streams new-compiled-streams
             :streams-configurations new-streams-configurations)))

  (list-streams [_]
    (let [{:keys [streams]} @state]
      (or (keys streams) [])))

  (get-stream [_ stream-name]
    (let [{:keys [streams]} @state]
      (if-let [stream (get streams stream-name)]
        stream
        (ex/ex-not-found! (format "stream %s not found" stream-name)))))

  (push! [_ event stream]
    (let [{:keys [streams]} @state]
      (if (= :default stream)
        (doseq [[_ s] streams]
          (when (:default s)
            (stream! s event)))

        (if-let [s (get streams stream)]
          (stream! s event)
          (ex/ex-not-found! (format "Stream %s not found" stream)))))))


(defn stream-handler
  [{:keys [streams-configurations outputs-configurations test-mode? pubsub]
    :or   {streams-configurations {}
           outputs-configurations {}
           test-mode?             false}}]
  (doto (->StreamHandler (atom {}) (atom {}) outputs-configurations streams-configurations test-mode? pubsub)
    (init)))
