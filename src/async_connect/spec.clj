(ns async-connect.spec
  (:require [clojure.spec :as s])
  (:import [clojure.core.async.impl.channels ManyToManyChannel]
           [clojure.core.async.impl.protocols ReadPort WritePort]))

(s/def ::async-channel #(instance? ManyToManyChannel %))
(s/def ::read-channel  #(instance? ReadPort %))
(s/def ::write-channel  #(instance? WritePort %))
(s/def ::atom #(instance? clojure.lang.Atom %))

