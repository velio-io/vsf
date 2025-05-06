(ns vsf.action-metadata
  (:require [vsf.metadata :refer [compile-completions compile-controls]]))

(def completions
  (compile-completions))

(def actions-controls
  (compile-controls))