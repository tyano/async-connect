(ns async-connect.spec
  (:require [clojure.spec.alpha :as s]
            [async-connect.spec.generator :as gen]
            [clojure.core.async :refer [chan]])
  (:import [clojure.core.async.impl.channels ManyToManyChannel]
           [clojure.core.async.impl.protocols ReadPort WritePort]))

(s/def ::async-channel (s/with-gen #(instance? ManyToManyChannel %) #(gen/create (chan))))
(s/def ::read-channel  (s/with-gen #(instance? ReadPort %) #(gen/create (chan))))
(s/def ::write-channel  (s/with-gen #(instance? WritePort %) #(gen/create (chan))))
(s/def ::atom (s/with-gen #(instance? clojure.lang.Atom %) #(gen/create (atom {}))))

