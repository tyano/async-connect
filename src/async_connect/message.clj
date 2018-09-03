(ns async-connect.message
  (:require [clojure.spec.alpha :as s]
            [async-connect.netty.spec :as netty]))


(s/def ::flush? boolean?)
(s/def ::close? boolean?)
(s/def ::data any?)
(s/def ::promise ::netty/channel-promise)

(s/def :async-connect/message
  (s/keys
   :req [::data]
   :opt [::flush?
         ::close?
         ::promise]))


