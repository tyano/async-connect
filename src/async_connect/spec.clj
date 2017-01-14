(ns async-connect.spec
  (:import [clojure.core.async.impl.channels ManyToManyChannel]))

(defn async-channel? [ch] (when ch (instance? ManyToManyChannel ch)))
