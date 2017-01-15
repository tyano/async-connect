(ns async-connect.server
  (:require [clojure.spec :as s]
            [clojure.core.async :refer [>!! <!! go-loop thread chan sub unsub pub close!]]
            [clojure.tools.logging :as log]
            [async-connect.netty :refer [write-if-possible]]
            [async-connect.netty.handler :refer [make-inbound-handler]]
            [async-connect.spec :as spec])
  (:import [io.netty.bootstrap ServerBootstrap]
           [io.netty.buffer
              PooledByteBufAllocator]
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

(s/def :server.config/port pos-int?)
(s/def :server.config/channel-initializer
  (s/fspec :args (s/cat :netty-channel :netty/socket-channel
                        :read-channel  ::spec/read-channel
                        :write-channel ::spec/write-channel
                        :config ::config)
           :ret :netty/socket-channel))

(s/def :server.config/bootstrap-initializer
  (s/fspec :args (s/cat :bootstrap :netty/server-bootstrap)
           :ret  :netty/server-bootstrap))

(s/def ::config
  (s/keys
    :opt [:server.config/port
          :server.config/channel-initializer
          :server.config/bootstrap-initializer]))

(s/def ::writedata
  (s/keys
    :req-un [:netty/context
             :netty/message]
    :opt-un [:netty/flush
             :netty/close
             :netty/channel-promise]))

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
  (>!! read-ch {:context ctx, :message msg}))

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
  (pub write-ch #(.name ^ChannelHandlerContext (:context %))))

(defn make-channel-initializer
  [write-ch]
  (let [publication (make-write-publication write-ch)]
    (fn [^SocketChannel netty-ch read-ch write-ch]
      (.. netty-ch
        (pipeline)
        (addLast "default" ^ChannelHandler (make-inbound-handler (make-default-handler-map read-ch publication)))))))


(defn- init-bootstrap
  [bootstrap initializer]
  (if initializer
    (initializer bootstrap)
    bootstrap))

(s/fdef run-server
  :args (s/cat :read-channel ::spec/read-channel, :write-channel ::spec/write-channel, :config ::config)
  :ret  any?)

(defn run-server
  [read-channel,
   write-channel,
   {:keys [:server.config/port
           :server.config/channel-initializer
           :server.config/bootstrap-initializer]
      :or {port 8080}
      :as config}]

  (assert read-channel "read-channel must not be nil.")
  (assert write-channel "write-channel must not be nil.")

  (let [boss-group ^EventLoopGroup (NioEventLoopGroup.)
        worker-group ^EventLoopGroup (NioEventLoopGroup.)]
    (try
      (let [bootstrap ^ServerBootstrap (.. ^ServerBootstrap (ServerBootstrap.)
                                          (childHandler
                                            (proxy [ChannelInitializer] []
                                              (initChannel
                                                [^SocketChannel ch]
                                                (let [initializer (make-channel-initializer write-channel)]
                                                  (initializer ch read-channel write-channel))
                                                (when channel-initializer
                                                  (channel-initializer ch read-channel write-channel config)))))
                                          (childOption ChannelOption/SO_KEEPALIVE true)
                                          (group boss-group worker-group)
                                          (channel NioServerSocketChannel)
                                          (option ChannelOption/SO_BACKLOG (int 128))
                                          (option ChannelOption/WRITE_BUFFER_HIGH_WATER_MARK (int (* 32 1024)))
                                          (option ChannelOption/WRITE_BUFFER_LOW_WATER_MARK (int (* 8 1024)))
                                          (option ChannelOption/ALLOCATOR PooledByteBufAllocator/DEFAULT))]

        (init-bootstrap bootstrap bootstrap-initializer)

        (let [f ^ChannelFuture (.. bootstrap (bind (int port)) (sync))]
          (.. f
            (channel)
            (closeFuture)
            (sync))))
      (finally
        (close! read-channel)
        (close! write-channel)
        (.shutdownGracefully worker-group)
        (.shutdownGracefully boss-group)))))

