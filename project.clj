(defproject io.velio/vsf "0.1.0-SNAPSHOT"
  :description "A DSL for stream processing"

  :url "https://github.com/velio-io/vsf"

  :license {:name "EPL-1.0" :url "https://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.clojure/clojure "1.11.1"]
                 [org.clojure/tools.logging "1.2.4"]
                 [cheshire "5.12.0"]
                 [exoscale/ex "0.4.1"]
                 [com.boundary/high-scale-lib "1.0.6"]
                 [org.hdrhistogram/HdrHistogram "2.1.12"]
                 [metosin/malli "0.15.0"]]

  :repl-options {:init-ns vsf.core}

  :profiles {:dev  {:dependencies [[vvvvalvalval/scope-capture "0.3.3"]]}
             :cljs {:dependencies [[thheller/shadow-cljs "2.26.1"]]}})
