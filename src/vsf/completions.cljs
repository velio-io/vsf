(ns vsf.completions
  (:require-macros
   [vsf.process :refer [compile-completions]]))


(def streams-completions
  (compile-completions))
