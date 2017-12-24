(ns async-connect.client.spec
  (:require [clojure.spec.alpha :as s]
            [async-connect.netty.spec :as netty]))


(s/def ::flush? boolean?)
(s/def ::close? boolean?)
(s/def ::message any?)
(s/def ::promise ::netty/channel-promise)

(s/def ::writedata
  (s/keys
    :req-un [::message]
    :opt-un [::flush?
             ::close?
             ::promise]))
