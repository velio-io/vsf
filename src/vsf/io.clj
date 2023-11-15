(ns vsf.io)


(defprotocol Output
  (inject! [this event] "Inject an event in this output component"))
