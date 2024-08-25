# VSF (Vespir Stream Processing)

Aim of this library is to provide the convenient DSL to describe the event-stream processing logic.
This DSL is defined in terms of Clojure EDN syntax. 

This project has taken the DSL from Mirabelle (https://github.com/appclacks/mirabelle) and extracted it out as a separate re-usable library. The DSL has also been extended to support additional actions.

The basic structure of DSL:

```clojure
{:foo {:actions {...}}
 :bar {:actions {...}}}
```
Every top level key defines a separate stream of events, containing its own processing actions.
Average action could look like a map with action name (`:action`),
action parameters (`:params`) and a list of child actions (`:children`)

```clojure
{:action      :where,
 :description {:message "Filter events based on the provided condition",
               :params  "[:and [:= :host \"foo\"] [:> :metric 10]]"},
 :params      [[:and [:= :host "foo"] [:> :metric 10]]],
 :children    nil}
```

### Basic usage

When you have an EDN streams configuration you can compile it to Clojure function and push events through.
For every action type (`:action`) this library provides a respective function, which will be invoked when you pass an event to the stream.

```clojure
(require '[vsf.core :refer [stream-handler] :as vsf])

(def streams
  {:foo {:actions {:action   :fixed-event-window
                   :params   [{:size 100}]
                   :children []}}
   :bar {:actions {:action   :above-dt
                   :params   [[:> :metric 100] 200]
                   :children []}}})

(def handler
  (stream-handler {:streams-configurations streams}))

;; broadcast event to all streams
(vsf/push! handler {:host "f" :metric 12 :time 1})

;; send event only to a specified stream
(vsf/push! handler {:host "f" :metric 12 :time 1} :foo)
```
You can provide additional configuration for stream handler, 
such as `:outputs-configurations`, `:test-mode?` and `:pubsub`.

`:outputs-configurations` 
EDN map which defines one or more output destinations for processed events.
Keys are outputs names and values are maps with 
`:builder` (fully qualified function name available in the classpath) and 
`:config` (whatever required for builder function) options

```clojure
;; outputs.file namespace
(require [clojure.java.io :as io])

(defn append-to-file [config]
  (let [file (io/file (:file-name config))]
    (io/make-parents file)
    (fn [event]
      (spit file event :append true))))


;; handler namespace
(def handler
  (stream-handler
   {:streams-configurations
    {:foo {:actions {:action :output!
                     :params [:file-out]}}}

    :outputs-configurations
    {:file-out {:type    :custom
                :builder 'outputs.file/append-to-file
                :config  {:file-name "tmp/events.txt"}}}}))

(vsf/push! handler {:host "f" :metric 12 :time 1})
```

`:pubsub` instance of PubSub record defined in  the `vsf.pubsub` namespace.
Can be used to broadcast events to integrate another modules or systems which sits outside defined streams

`:test-mode?` true or false

In case if streams configuration is dynamic and changes over time 
you can reload your handler by providing a new streams configuration.
New streams will be added, old ones (not specified in the new config) will be removed

```clojure
(vsf/reload handler new-streams-configurations)
```

Or you can manage individual streams

```clojure
(vsf/list-streams handler)

(vsf/get-stream handler :foo)

(vsf/remove-stream handler :foo)

(vsf/add-stream handler :baz baz-stream-configuration)
```


### Define streams

`vsf.action` namespace provides a variety of functions which helps to build the EDN DSL data structure.
For example, this code:

```clojure
(require '[vsf.action :as a])

(a/streams
 (a/stream {:name :foo}
   (a/where [:and
             [:= :host "foo"]
             [:> :metric 10]]
    (a/increment))))
```
will produce such EDN:

```clojure
{:foo {:actions {:action      :sdo,
                 :description {:message "Forward events to children"},
                 :children    ({:action      :where,
                                :description {:message "Filter events based on the provided condition",
                                              :params  "[:and [:= :host \"foo\"] [:> :metric 10]]"},
                                :params      [[:and [:= :host "foo"] [:> :metric 10]]],
                                :children    ({:action      :increment,
                                               :description {:message "Increment the :metric field"},
                                               :children    nil})})}}}

```

The resulting EDN could be stored in a file or sent via the wire to another system. 
This namespace is in `.cljc` format so could be used on for frontend and backend. 

Check the `vsf.action` namespace for the full list of available actions and their descriptions.

### Run tests

in clj environment

```shell
lein test
```

in browser (cljs)

```shell
shadow-cljs watch test
```
after build is done, navigate to http://localhost:8021/
