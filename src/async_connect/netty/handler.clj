(ns async-connect.netty.handler
  (:require [clojure.spec :as s])
  (:import [clojure.core.async.impl.channels ManyToManyChannel]
           [io.netty.bootstrap ServerBootstrap]
           [io.netty.util ReferenceCountUtil]
           [io.netty.channel
              ChannelHandlerContext
              ChannelInboundHandler
              ChannelInboundHandlerAdapter]))

(defn- throwable? [th] (when th (instance? Throwable th)))
(defn- channel-handler-context? [o] (when o (instance? ChannelHandlerContext o)))
(defn- channel-inbound-handler? [h] (when h (instance? ChannelInboundHandler h)))

(s/def :inbound/channel-active        (s/fspec :args (s/cat :ctx channel-handler-context?)))
(s/def :inbound/channel-inactive      (s/fspec :args (s/cat :ctx channel-handler-context?)))
(s/def :inbound/channel-read          (s/fspec :args (s/cat :ctx channel-handler-context? :obj any?)))
(s/def :inbound/channel-read-complete (s/fspec :args (s/cat :ctx channel-handler-context?)))
(s/def :inbound/channel-registered    (s/fspec :args (s/cat :ctx channel-handler-context?)))
(s/def :inbound/channel-unregistered  (s/fspec :args (s/cat :ctx channel-handler-context?)))
(s/def :inbound/channel-writability-changed
                                      (s/fspec :args (s/cat :ctx channel-handler-context?)))
(s/def :inbound/exception-caught      (s/fspec :args (s/cat :ctx channel-handler-context? :throwable throwable?)))
(s/def :inbound/user-event-triggered  (s/fspec :args (s/cat :ctx channel-handler-context? :event any?)))

(s/def :inbound/handler-map
  (s/keys
    :opt [:inbound/channel-active
          :inbound/channel-inactive
          :inbound/channel-read
          :inbound/channel-read-complete
          :inbound/channel-registered
          :inbound/channel-unregistered
          :inbound/channel-writability-changed
          :inbound/exception-caught
          :inbound/user-event-triggered]))

(s/fdef make-inbound-handler
  :args (s/cat :handlers :inbound/handler-map)
  :ret  channel-inbound-handler?)


(defn make-inbound-handler
  [handlers]
  (proxy [ChannelInboundHandlerAdapter] []
    (channelActive
      [^ChannelHandlerContext ctx]
      (if-let [h (:inbound/channel-active handlers)]
        (h ctx)
        (proxy-super channelActive ctx)))

    (channelInactive
      [^ChannelHandlerContext ctx]
      (if-let [h (:inbound/channel-inactive handlers)]
        (h ctx)
        (proxy-super channelInactive ctx)))

    (channelRead
      [^ChannelHandlerContext ctx, ^Object msg]
      (if-let [h (:inbound/channel-read handlers)]
        (h ctx msg)
        (proxy-super channelRead ctx msg)))

    (channelReadComplete
      [^ChannelHandlerContext ctx]
      (if-let [h (:inbound/channel-read-complete handlers)]
        (h ctx)
        (proxy-super channelReadComplete ctx)))

    (channelRegistered
      [^ChannelHandlerContext ctx]
      (if-let [h (:inbound/channel-registered handlers)]
        (h ctx)
        (proxy-super channelRegistered ctx)))

    (channelUnregistered
      [^ChannelHandlerContext ctx]
      (if-let [h (:inbound/channel-unregistered handlers)]
        (h ctx)
        (proxy-super channelUnregistered ctx)))

    (channelWritabilityChanged
      [^ChannelHandlerContext ctx]
      (if-let [h (:inbound/channel-writability-changed handlers)]
        (h ctx)
        (proxy-super channelWritabilityChanged ctx)))

    (exceptionCaught
      [^ChannelHandlerContext ctx, ^Throwable cause]
      (if-let [h (:inbound/exception-caught handlers)]
        (h ctx cause)
        (proxy-super exceptionCaught ctx cause)))

    (userEventTriggered
      [^ChannelHandlerContext ctx, ^Object evt]
      (if-let [h (:inbound/user-event-triggered handlers)]
        (h ctx evt)
        (proxy-super userEventTriggered ctx evt)))))

