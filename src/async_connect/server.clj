(ns async-connect.server
  (:require [clojure.spec :as s]
            [clojure.core.async :refer [>!! <!! go-loop thread chan sub unsub pub close!]]
            [clojure.tools.logging :as log]
            [async-connect.netty.handler :refer [make-inbound-handler]])
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
              ChannelPromise
              ChannelHandler]
           [io.netty.channel.nio
              NioEventLoopGroup]
           [io.netty.channel.socket
              SocketChannel]
           [io.netty.channel.socket.nio
              NioServerSocketChannel]))

(defn- socket-channel? [ch] (when ch (instance? SocketChannel ch)))
(defn- async-channel? [ch] (when ch (instance? ManyToManyChannel ch)))

(s/def :server.config/port pos-int?)
(s/def :server.config/channel-initializer
  (s/fspec :args (s/cat :netty-channel socket-channel?
                        :read-channel async-channel?
                        :write-channel async-channel?
                        :config ::config)
           :ret socket-channel?))

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

(defn make-default-handler-map
  [read-ch write-publication]
  (let [sub-ch-ref (atom nil)]
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
          (.close ctx))}))

(defn make-write-publication
  [write-ch]
  (pub write-ch #(.name (:context %))))

(defn make-channel-initializer
  [write-ch]
  (let [publication (make-write-publication write-ch)]
    (fn [^SocketChannel netty-ch read-ch write-ch config]
      (.. netty-ch
        (pipeline)
        (addLast (into-array ChannelHandler [(make-inbound-handler (make-default-handler-map read-ch publication))]))))))

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
