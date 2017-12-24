(ns async-connect.netty.handler
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [async-connect.netty.spec :as netty]
            [async-connect.spec.generator :as agen]
            [clojure.tools.logging :as log])
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

(declare make-inbound-handler make-outbound-handler)

(s/def ::channel-inbound-handler (s/with-gen #(instance? ChannelInboundHandler %) #(agen/create (make-inbound-handler {}))))
(s/def ::channel-outbound-handler (s/with-gen #(instance? ChannelOutboundHandler %) #(agen/create (make-outbound-handler {}))))

(s/def ::handler-added         (s/fspec :args (s/cat :ctx :netty/context)))
(s/def ::handler-removed       (s/fspec :args (s/cat :ctx :netty/context)))

(s/def ::channel-active        (s/fspec :args (s/cat :ctx ::netty/context)))
(s/def ::channel-inactive      (s/fspec :args (s/cat :ctx ::netty/context)))
(s/def ::channel-read          (s/fspec :args (s/cat :ctx ::netty/context :obj any?)))
(s/def ::channel-read-complete (s/fspec :args (s/cat :ctx ::netty/context)))
(s/def ::channel-registered    (s/fspec :args (s/cat :ctx ::netty/context)))
(s/def ::channel-unregistered  (s/fspec :args (s/cat :ctx ::netty/context)))
(s/def ::channel-writability-changed
                                       (s/fspec :args (s/cat :ctx ::netty/context)))
(s/def ::exception-caught      (s/fspec :args (s/cat :ctx ::netty/context :throwable ::throwable)))
(s/def ::user-event-triggered  (s/fspec :args (s/cat :ctx ::netty/context :event any?)))

(s/def ::inbound-handler-map
  (s/with-gen
    (s/keys
      :opt [::handler-added
            ::handler-removed
            ::channel-active
            ::channel-inactive
            ::channel-read
            ::channel-read-complete
            ::channel-registered
            ::channel-unregistered
            ::channel-writability-changed
            ::exception-caught
            ::user-event-triggered])
    #(gen/one-of {})))

(s/fdef make-inbound-handler
  :args (s/cat :handlers ::inbound-handler-map)
  :ret  ::channel-inbound-handler)

(defn make-inbound-handler
  ([context handlers]
    ;; `context` must be an atom.
    (proxy [ChannelInboundHandlerAdapter] []
      (handlerAdded
        [^ChannelHandlerContext ctx]
        (when context (reset! context ctx))
        (if-let [h (::handler-added handlers)]
          (h ctx)
          (proxy-super handlerAdded ctx)))

      (handlerRemoved
        [^ChannelHandlerContext ctx]
        (when context (reset! context ctx))
        (if-let [h (::handler-removed handlers)]
          (h ctx)
          (proxy-super handlerRemoved ctx)))

      (channelActive
        [^ChannelHandlerContext ctx]
        (when context (reset! context ctx))
        (if-let [h (::channel-active handlers)]
          (h ctx)
          (proxy-super channelActive ctx)))

      (channelInactive
        [^ChannelHandlerContext ctx]
        (when context (reset! context ctx))
        (if-let [h (::channel-inactive handlers)]
          (h ctx)
          (proxy-super channelInactive ctx)))

      (channelRead
        [^ChannelHandlerContext ctx, ^Object msg]
        (when context (reset! context ctx))
        (if-let [h (::channel-read handlers)]
          (h ctx msg)
          (proxy-super channelRead ctx msg)))

      (channelReadComplete
        [^ChannelHandlerContext ctx]
        (when context (reset! context ctx))
        (if-let [h (::channel-read-complete handlers)]
          (h ctx)
          (proxy-super channelReadComplete ctx)))

      (channelRegistered
        [^ChannelHandlerContext ctx]
        (when context (reset! context ctx))
        (if-let [h (::channel-registered handlers)]
          (h ctx)
          (proxy-super channelRegistered ctx)))

      (channelUnregistered
        [^ChannelHandlerContext ctx]
        (when context (reset! context ctx))
        (if-let [h (::channel-unregistered handlers)]
          (h ctx)
          (proxy-super channelUnregistered ctx)))

      (channelWritabilityChanged
        [^ChannelHandlerContext ctx]
        (when context (reset! context ctx))
        (if-let [h (::channel-writability-changed handlers)]
          (h ctx)
          (proxy-super channelWritabilityChanged ctx)))

      (exceptionCaught
        [^ChannelHandlerContext ctx, ^Throwable cause]
        (when context (reset! context ctx))
        (if-let [h (::exception-caught handlers)]
          (h ctx cause)
          (proxy-super exceptionCaught ctx cause)))

      (userEventTriggered
        [^ChannelHandlerContext ctx, ^Object evt]
        (when context (reset! context ctx))
        (if-let [h (::user-event-triggered handlers)]
          (h ctx evt)
          (proxy-super userEventTriggered ctx evt)))))

  ([handlers]
   (make-inbound-handler nil handlers)))


(s/def ::bind    (s/fspec :args (s/cat :ctx ::netty/context
                                                :local-address ::netty/socket-address
                                                :promise ::netty/channel-promise)))

(s/def ::close   (s/fspec :args (s/cat :ctx ::netty/context
                                                :promise ::netty/channel-promise)))

(s/def ::connect (s/fspec :args (s/cat :ctx ::netty/context
                                                :remote-addr ::netty/socket-address
                                                :local-addr ::netty/socket-address
                                                :promise :netty/channel-promise)))

(s/def ::deregister (s/fspec :args (s/cat :ctx ::netty/context, :promise ::netty/channel-promise)))
(s/def ::disconnect (s/fspec :args (s/cat :ctx ::netty/context, :promise ::netty/channel-promise)))
(s/def ::flush      (s/fspec :args (s/cat :ctx ::netty/context)))
(s/def ::read       (s/fspec :args (s/cat :ctx ::netty/context)))
(s/def ::write      (s/fspec :args (s/cat :ctx ::netty/context, :msg any?, :promise ::netty/channel-promise)))

(s/def ::outbound-handler-map
  (s/with-gen
    (s/keys
      :opt [::handler-added
            ::handler-removed
            ::bind
            ::close
            ::connect
            ::deregister
            ::disconnect
            ::flush
            ::read
            ::write])
    #(gen/one-of
        {}
        {::read (fn [ctx] nil)})))


(s/fdef make-outbound-handler
  :args (s/cat :handlers ::outbound-handler-map)
  :ret  ::channel-inbound-handler)


(defn make-outbound-handler
  ([context handlers]
    (proxy [ChannelOutboundHandlerAdapter] []
      (handlerAdded
        [^ChannelHandlerContext ctx]
        (when context (reset! context ctx))
        (if-let [h (::handler-added handlers)]
          (h ctx)
          (proxy-super handlerAdded ctx)))

      (handlerRemoved
        [^ChannelHandlerContext ctx]
        (when context (reset! context ctx))
        (if-let [h (::handler-removed handlers)]
          (h ctx)
          (proxy-super handlerRemoved ctx)))

      (bind
        [^ChannelHandlerContext ctx, ^SocketAddress local-address, ^ChannelPromise promise]
        (when context (reset! context ctx))
        (if-let [h (::bind handlers)]
          (h ctx local-address promise)
          (proxy-super bind ctx local-address promise)))

      (close
        [^ChannelHandlerContext ctx, ^ChannelPromise promise]
        (when context (reset! context ctx))
        (if-let [h (::close handlers)]
          (h ctx promise)
          (proxy-super close ctx promise)))

      (connect
        [^ChannelHandlerContext ctx, ^SocketAddress remote-address, ^SocketAddress local-address, ^ChannelPromise promise]
        (when context (reset! context ctx))
        (if-let [h (::connect handlers)]
          (h ctx remote-address local-address promise)
          (proxy-super connect ctx remote-address local-address promise)))

      (deregister
        [^ChannelHandlerContext ctx, ^ChannelPromise promise]
        (when context (reset! context ctx))
        (if-let [h (::disconnect handlers)]
          (h ctx promise)
          (proxy-super deregister ctx promise)))

      (disconnect
        [^ChannelHandlerContext ctx, ^ChannelPromise promise]
        (when context (reset! context ctx))
        (if-let [h (::disconnect handlers)]
          (h ctx promise)
          (proxy-super disconnect ctx promise)))

      (flush
        [^ChannelHandlerContext ctx]
        (when context (reset! context ctx))
        (if-let [h (::flush handlers)]
          (h ctx)
          (proxy-super flush ctx)))

      (read
        [^ChannelHandlerContext ctx]
        (when context (reset! context ctx))
        (if-let [h (::read handlers)]
          (h ctx)
          (proxy-super read ctx)))

      (write
        [^ChannelHandlerContext ctx, ^Object msg, ^ChannelPromise promise]
        (when context (reset! context ctx))
        (if-let [h (::write handlers)]
          (h ctx msg promise)
          (proxy-super write ctx msg promise)))))

  ([handlers]
   (make-outbound-handler nil handlers)))



