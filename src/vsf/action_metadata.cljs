(ns vsf.action-metadata
  (:require [vsf.action])
  (:require-macros
   [vsf.process :refer [compile-completions compile-controls]]))


(def completions
  (compile-completions))


(def actions-controls
  (compile-controls))

