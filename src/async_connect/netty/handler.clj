(ns async-connect.netty.handler
  (:require [clojure.spec :as s])
  (:import [clojure.core.async.impl.channels ManyToManyChannel]
           [io.netty.bootstrap ServerBootstrap]
           [io.netty.util ReferenceCountUtil]
           [io.netty.channel
              ChannelHandlerContext
              ChannelInboundHandler
              ChannelInboundHandlerAdapter
              ChannelOutboundHandler
              ChannelOutboundHandlerAdapter
              ChannelPromise]
           [java.net
              SocketAddress]))

(defn- throwable? [th] (when th (instance? Throwable th)))
(defn- channel-handler-context? [o] (when o (instance? ChannelHandlerContext o)))
(defn- channel-inbound-handler? [h] (when h (instance? ChannelInboundHandler h)))
(defn- channel-outbound-handler? [h] (when h (instance? ChannelOutboundHandler h)))
(defn- channel-promise? [p] (when p (instance? ChannelPromise p)))
(defn- socket-address? [addr] (when addr (instance? SocketAddress addr)))

(s/def :handler/handler-added         (s/fspec :args (s/cat :ctx channel-handler-context?)))
(s/def :handler/handler-removed       (s/fspec :args (s/cat :ctx channel-handler-context?)))

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
    :opt [:handler/handler-added
          :handler/handler-removed
          :inbound/channel-active
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
    (handlerAdded
      [^ChannelHandlerContext ctx]
      (if-let [h (:handler/handler-added handlers)]
        (h ctx)
        (proxy-super handlerAdded ctx)))

    (handlerRemoved
      [^ChannelHandlerContext ctx]
      (if-let [h (:handler/handler-removed handlers)]
        (h ctx)
        (proxy-super handlerRemoved ctx)))

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



(s/def :outbound/bind    (s/fspec :args (s/cat :ctx channel-handler-context?
                                               :local-address socket-address?
                                               :promise channel-promise?)))

(s/def :outbound/close   (s/fspec :args (s/cat :ctx channel-handler-context?
                                               :promise channel-promise?)))

(s/def :outbound/connect (s/fspec :args (s/cat :ctx channel-handler-context?
                                               :remote-addr socket-address?
                                               :local-addr socket-address?
                                               :promise channel-promise?)))

(s/def :outbound/deregister (s/fspec :args (s/cat :ctx channel-handler-context?, :promise channel-promise?)))
(s/def :outbound/disconnect (s/fspec :args (s/cat :ctx channel-handler-context?, :promise channel-promise?)))
(s/def :outbound/flush      (s/fspec :args (s/cat :ctx channel-handler-context?)))
(s/def :outbound/read       (s/fspec :args (s/cat :ctx channel-handler-context?)))
(s/def :outbound/write      (s/fspec :args (s/cat :ctx channel-handler-context?, :msg any?, :promise channel-promise?)))

(s/def :outbound/handler-map
  (s/keys
    :opt [:handler/handler-added
          :handler/handler-removed
          :outbound/bind
          :outbound/close
          :outbound/connect
          :outbound/deregister
          :outbound/disconnect
          :outbound/flush
          :outbound/read
          :outbound/write]))


(s/fdef make-outbound-handler
  :args (s/cat :handlers :outbound/handler-map)
  :ret  channel-inbound-handler?)


(defn make-outbound-handler
  [handlers]
  (proxy [ChannelOutboundHandlerAdapter] []
    (handlerAdded
      [^ChannelHandlerContext ctx]
      (if-let [h (:handler/handler-added handlers)]
        (h ctx)
        (proxy-super handlerAdded ctx)))

    (handlerRemoved
      [^ChannelHandlerContext ctx]
      (if-let [h (:handler/handler-removed handlers)]
        (h ctx)
        (proxy-super handlerRemoved ctx)))

    (bind
      [^ChannelHandlerContext ctx, ^SocketAddress local-address, ^ChannelPromise promise]
      (if-let [h (:outbound/bind handlers)]
        (h ctx local-address promise)
        (proxy-super bind ctx local-address promise)))

    (close
      [^ChannelHandlerContext ctx, ^ChannelPromise promise]
      (if-let [h (:outbound/close handlers)]
        (h ctx promise)
        (proxy-super close ctx promise)))

    (connect
      [^ChannelHandlerContext ctx, ^SocketAddress remote-address, ^SocketAddress local-address, ^ChannelPromise promise]
      (if-let [h (:outbound/connect handlers)]
        (h ctx remote-address local-address promise)
        (proxy-super connect ctx remote-address local-address promise)))

    (deregister
      [^ChannelHandlerContext ctx, ^ChannelPromise promise]
      (if-let [h (:outbound/disconnect handlers)]
        (h ctx promise)
        (proxy-super deregister ctx promise)))

    (disconnect
      [^ChannelHandlerContext ctx, ^ChannelPromise promise]
      (if-let [h (:outbound/disconnect handlers)]
        (h ctx promise)
        (proxy-super disconnect ctx promise)))

    (flush
      [^ChannelHandlerContext ctx]
      (if-let [h (:outbound/flush handlers)]
        (h ctx)
        (proxy-super flush ctx)))

    (read
      [^ChannelHandlerContext ctx]
      (if-let [h (:outbound/read handlers)]
        (h ctx)
        (proxy-super read ctx)))

    (write
      [^ChannelHandlerContext ctx, ^Object msg, ^ChannelPromise promise]
      (if-let [h (:outbound/write handlers)]
        (h ctx msg promise)
        (proxy-super write ctx msg promise)))))




