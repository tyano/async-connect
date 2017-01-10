(ns async-connect.netty.server
  (:require [clojure.spec :as s]
            [clojure.core.async :refer [>! >!! <! go-loop thread]])
  (:import [clojure.core.async.impl.channels ManyToManyChannel]
           [io.netty.bootstrap ServerBootstrap]
           [io.netty.util ReferenceCountUtil]
           [io.netty.channel
              ChannelFuture
              ChannelInitializer
              ChannelOption
              EventLoopGroup
              ChannelHandlerContext
              ChannelInboundHandlerAdapter
              ChannelPromise
              ChannelHandler]
           [io.netty.channel.nio
              NioEventLoopGroup]
           [io.netty.channel.socket
              SocketChannel]
           [io.netty.channel.socket.nio
              NioServerSocketChannel]))

(defn- socket-channel? [ch] (when ch (instance? SocketChannel)))

(defn- async-channel? [ch] (instance? ManyToManyChannel ch))

(s/def :server.config/port pos-int?)
(s/def :server.config/channel-initializer
  (s/fspec :args (s/cat :channel socket-channel?) :ret socket-channel?))

(s/def ::config
  (s/keys
    :opt [:server.config/port
          :server.config/channel-initializer]))

(s/def :netty/context #(instance? ChannelHandlerContext %))
(s/def :netty/message any?)

(s/def :server.writedata/flush? boolean?)
(s/def :server.writedata/close? boolean?)

(s/def ::writedata
  (s/keys
    :req-un [:netty/context
             :netty/message]
    :opt-un [:server.writedata/flush?
             :server.writedata/close?]))


(defn default-channel-read
  [^ChannelHandlerContext ctx, ^Object msg, channel]
  (try
    (>!! channel {:context ctx, :message msg})
    (finally
      (ReferenceCountUtil/releaseLater ^ByteBuf msg))))

(defn default-handler
  [channel]
  (proxy [ChannelInboundHandlerAdapter] []
    (channelRead
      [^ChannelHandlerContext ctx, ^Object msg]
      (default-channel-read ctx msg channel))

    (exceptionCaught
      [^ChannelHandlerContext ctx, ^Throwable th]
      (.printStackTrace th)
      (.close ctx))))

(defn default-channel-initializer
  [^SocketChannel netty-ch read-ch write-ch config]
  (.. netty-ch
    (pipeline)
    (addLast (into-array ChannelHandler [(default-handler read-ch)]))))

(s/fdef run-server
  :args (s/cat :read-channel async-channel?, :write-channel async-channel?, :config ::config)
  :ret  any?)

(defn run-server
  [read-channel,
   write-channel,
   {:keys [:server.config/port
           :server.config/channel-initializer]
      :or {port 8080
           channel-initializer default-channel-initializer}
      :as config}]

  (assert read-channel "read-channel must not be nil.")
  (assert write-channel "write-channel must not be nil.")

  (go-loop []
    (if-some [{:keys [^ChannelHandlerContext context message flush? close?] :or {flush? false, close? false} :as data} (<! write-channel)]
      (do
        (s/assert ::writedata data)
        (thread
          (if (or flush? close?)
            (.writeAndFlush context message)
            (.write context message))
          (when close?
            (.close context)))
        (recur))
      (throw (IllegalStateException. "The global channel is closed. You should not close the global channel."))))

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

