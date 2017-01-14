(ns async-connect.netty.server
  (:require [clojure.spec :as s]
            [clojure.core.async :refer [>!! <!! go-loop thread chan sub unsub pub close!]]
            [clojure.tools.logging :as log])
  (:import [clojure.core.async.impl.channels ManyToManyChannel]
           [io.netty.bootstrap ServerBootstrap]
           [io.netty.util ReferenceCountUtil]
           [io.netty.channel
              Channel
              ChannelFuture
              ChannelInitializer
              ChannelOption
              EventLoopGroup
              ChannelHandlerContext
              ChannelInboundHandler
              ChannelInboundHandlerAdapter
              ChannelPromise
              ChannelHandler]
           [io.netty.channel.nio
              NioEventLoopGroup]
           [io.netty.channel.socket
              SocketChannel]
           [io.netty.channel.socket.nio
              NioServerSocketChannel]))

(defn- socket-channel? [ch] (when ch (instance? SocketChannel ch)))
(defn- channel-handler-context? [o] (when o (instance? ChannelHandlerContext o)))
(defn- async-channel? [ch] (when ch (instance? ManyToManyChannel ch)))
(defn- throwable? [th] (when th (instance? Throwable th)))
(defn- channel-inbound-handler? [h] (when h (instance? ChannelInboundHandler h)))

(s/def :server.config/port pos-int?)
(s/def :server.config/channel-initializer
  (s/fspec :args (s/cat :netty-channel socket-channel?
                        :read-channel async-channel?
                        :write-channel async-channel?
                        :config ::config)
           :ret socket-channel?))

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


(s/def ::config
  (s/keys
    :opt [:server.config/port
          :server.config/channel-initializer]))

(s/def :netty/context #(instance? ChannelHandlerContext %))
(s/def :netty/message any?)
(s/def :netty/channel-promise #(instance? ChannelPromise %))

(s/def :server.writedata/flush? boolean?)
(s/def :server.writedata/close? boolean?)

(s/def ::writedata
  (s/keys
    :req-un [:netty/context
             :netty/message]
    :opt-un [:server.writedata/flush?
             :server.writedata/close?
             :netty/channel-promise]))

(defn write-if-possible
  [^ChannelHandlerContext ctx, flush?, data, ^ChannelPromise promise]
  (let [netty-ch ^Channel (.channel ctx)]
    (loop [writable? (.isWritable netty-ch)]
      (if writable?
        (if flush?
          (.writeAndFlush ctx data promise)
          (.write ctx data promise))
        (do
          (Thread/sleep 200)
          (recur (.isWritable netty-ch)))))))

(defn default-channel-active
  [^ChannelHandlerContext ctx, publication, sub-ch]
  (log/debug "channel active: " (.name ctx))
  (sub publication (.name ctx) sub-ch)
  (thread
    (loop []
      (when-some [{:keys [^ChannelHandlerContext context message flush? close? ^ChannelPromise promise]
                   :or {flush? false
                        close? false
                        promise ^ChannelPromise (.voidPromise context)}
                   :as data}
                  (<!! sub-ch)]
        (s/assert ::writedata data)
        (write-if-possible context (or flush? close?) message promise)
        (when close?
          (.close context))
        (recur)))))

(defn default-channel-inactive
  [^ChannelHandlerContext ctx, publication, sub-ch]
  (log/debug "channel inactive: " (.name ctx))
  (unsub publication (.name ctx) sub-ch)
  (close! sub-ch))

(defn default-channel-read
  [^ChannelHandlerContext ctx, ^Object msg, read-ch]
  (log/debug "channel read: " (.name ctx))
  (try
    (>!! read-ch {:context ctx, :message msg})
    (finally
      (ReferenceCountUtil/releaseLater ^ByteBuf msg))))

(defn make-default-handler
  [read-ch write-publication]
  (let [sub-ch-ref (atom nil)]
    (make-inbound-handler
      {:inbound/channel-read
          (fn [ctx msg]
            (default-channel-read ctx msg read-ch))

       :inbound/channel-active
          (fn [ctx]
            (let [sub-ch (chan)]
              (reset! sub-ch-ref sub-ch)
              (default-channel-active ctx write-publication sub-ch)))

       :inbound/channel-inactive
          (fn [ctx]
            (default-channel-inactive ctx write-publication @sub-ch-ref))

       :inbound/exception-caught
          (fn [^ChannelHandlerContext ctx, ^Throwable th]
            (.printStackTrace th)
            (.close ctx))})))

(defn make-write-publication
  [write-ch]
  (pub write-ch #(.name (:context %))))

(defn make-channel-initializer
  [write-ch]
  (let [publication (make-write-publication write-ch)]
    (fn [^SocketChannel netty-ch read-ch write-ch config]
      (.. netty-ch
        (pipeline)
        (addLast (into-array ChannelHandler [(make-default-handler read-ch publication)]))))))

(s/fdef run-server
  :args (s/cat :read-channel async-channel?, :write-channel async-channel?, :config ::config)
  :ret  any?)

(defn run-server
  [read-channel,
   write-channel,
   {:keys [:server.config/port
           :server.config/channel-initializer]
      :or {port 8080
           channel-initializer (make-channel-initializer write-channel)}
      :as config}]

  (assert read-channel "read-channel must not be nil.")
  (assert write-channel "write-channel must not be nil.")

  (let [boss-group ^EventLoopGroup (NioEventLoopGroup.)
        worker-group ^EventLoopGroup (NioEventLoopGroup.)]
    (try
      (let [bootstrap ^ServerBootstrap (ServerBootstrap.)]
        (.. bootstrap
          (group boss-group worker-group)
          (channel NioServerSocketChannel)
          (childHandler
            (proxy [ChannelInitializer] []
              (initChannel
                [^SocketChannel ch]
                (when channel-initializer
                  (channel-initializer ch read-channel write-channel config)))))
          (option ChannelOption/SO_BACKLOG (int 128))
          (childOption ChannelOption/SO_KEEPALIVE true))
        (let [f ^ChannelFuture (.. bootstrap (bind (int port)) (sync))]
          (.. f
            (channel)
            (closeFuture)
            (sync))))
      (finally
        (.shutdownGracefully worker-group)
        (.shutdownGracefully boss-group)))))

