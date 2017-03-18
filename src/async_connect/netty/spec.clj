(ns async-connect.netty.spec
  (:require [clojure.spec :as s]
            [clojure.spec.gen :as gen]
            [async-connect.spec.generator :as agen])
  (:import [clojure.core.async.impl.channels ManyToManyChannel]
           [java.net
              SocketAddress]
           [io.netty.bootstrap
              Bootstrap
              ServerBootstrap]
           [io.netty.buffer
              ByteBufAllocator]
           [io.netty.channel
              ChannelHandlerContext
              ChannelPromise
              ChannelFuture
              Channel
              EventLoopGroup
              DefaultChannelPromise
              ChannelInboundHandlerAdapter
              DefaultChannelProgressivePromise]
           [io.netty.channel.local
              LocalChannel]
           [io.netty.channel.nio
              NioEventLoopGroup]
           [io.netty.channel.socket
              SocketChannel]
           [io.netty.channel.socket.nio
              NioSocketChannel]
           [io.netty.util
              AttributeKey
              Attribute]
           [io.netty.util.concurrent
              GlobalEventExecutor]))

(def ^:private dummy-handler (ChannelInboundHandlerAdapter.))

(defn- ^Attribute empty-attribute
  [k]
  (reify
    Attribute
    (compareAndSet [this o n] false)
    (get [this] nil)
    (getAndRemove [this] nil)
    (getAndSet [this v] nil)
    (key [this] k)
    (remove [this] nil)
    (set [this v] nil)
    (setIfAbsent [this v] nil)))

(defn- make-dummy-context
  []
  (reify
    ChannelHandlerContext
    (alloc [_] ByteBufAllocator/DEFAULT)
    (attr [_ k] (empty-attribute k))
    (channel [_] (LocalChannel.))
    (executor [_] GlobalEventExecutor/INSTANCE)
    (fireChannelActive [this] this)
    (fireChannelInactive [this] this)
    (fireChannelRead [this obj] this)
    (fireChannelReadComplete [this] this)
    (fireChannelRegistered [this] this)
    (fireChannelUnregistered [this] this)
    (fireChannelWritabilityChanged [this] this)
    (fireExceptionCaught [this th] this)
    (fireUserEventTriggered [this evt] this)
    (flush [this] this)
    (handler [this] dummy-handler)
    (hasAttr [_ key] false)
    (isRemoved [_] false)
    (name [_] "dummy-context")
    (pipeline [_] nil)
    (read [this] this)

    (bind [this addr] (DefaultChannelPromise. (LocalChannel.)))
    (bind [this addr promise] (DefaultChannelPromise. (LocalChannel.)))
    (close [this] (DefaultChannelPromise. (LocalChannel.)))
    (close [this promise] (DefaultChannelPromise. (LocalChannel.)))
    (^ChannelFuture connect [^ChannelHandlerContext this ^SocketAddress addr] (DefaultChannelPromise. (LocalChannel.)))
    (^ChannelFuture connect [^ChannelHandlerContext this ^SocketAddress addr ^ChannelPromise promise] (DefaultChannelPromise. (LocalChannel.)))
    (^ChannelFuture connect [^ChannelHandlerContext this ^SocketAddress remote ^SocketAddress local] (DefaultChannelPromise. (LocalChannel.)))
    (deregister [this] (DefaultChannelPromise. (LocalChannel.)))
    (deregister [this promise] (DefaultChannelPromise. (LocalChannel.)))
    (disconnect [this] (DefaultChannelPromise. (LocalChannel.)))
    (disconnect [this promise] (DefaultChannelPromise. (LocalChannel.)))
    (newFailedFuture [this cause] (DefaultChannelPromise. (LocalChannel.)))
    (newProgressivePromise [this] (DefaultChannelProgressivePromise. (LocalChannel.)))
    (newPromise [this] (DefaultChannelPromise. (LocalChannel.)))
    (newSucceededFuture [this] (-> (DefaultChannelPromise. (LocalChannel.)) (.setSuccess)))
    (voidPromise [this] (DefaultChannelPromise. (LocalChannel.)))
    (write [this msg] (DefaultChannelPromise. (LocalChannel.)))
    (write [this msg promise] (DefaultChannelPromise. (LocalChannel.)))
    (writeAndFlush [this msg] (DefaultChannelPromise. (LocalChannel.)))
    (writeAndFlush [this msg promise] (DefaultChannelPromise. (LocalChannel.)))))


(s/def :netty/context (s/with-gen #(instance? ChannelHandlerContext %) #(agen/create (make-dummy-context))))
(s/def :netty/message any?)
(s/def :netty/channel-promise  (s/with-gen #(instance? ChannelPromise %) #(agen/create (DefaultChannelPromise. (LocalChannel.)))))
(s/def :netty/channel          (s/with-gen #(instance? Channel %) #(agen/create (LocalChannel.))))
(s/def :netty/bootstrap        (s/with-gen #(instance? Bootstrap %) #(agen/create (Bootstrap.))))
(s/def :netty/server-bootstrap (s/with-gen #(instance? ServerBootstrap %) #(agen/create (ServerBootstrap.))))
(s/def :netty/socket-channel   (s/with-gen #(instance? SocketChannel %) #(agen/create (NioSocketChannel.))))
(s/def :netty/event-loop-group (s/with-gen #(instance? EventLoopGroup %) #(agen/create (NioEventLoopGroup.))))

(s/def :netty/flush? boolean?)
(s/def :netty/close? boolean?)
