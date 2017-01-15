(ns async-connect.server
  (:require [clojure.spec :as s]
            [clojure.core.async :refer [>!! <!! >! <! go go-loop thread chan close!]]
            [clojure.tools.logging :as log]
            [async-connect.netty :refer [write-if-possible]]
            [async-connect.netty.handler :refer [make-inbound-handler]]
            [async-connect.spec :as spec]
            [async-connect.box :refer [boxed] :as box])
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
                        :config ::config)
           :ret :netty/socket-channel))

(s/def :server.config/bootstrap-initializer
  (s/fspec :args (s/cat :bootstrap :netty/server-bootstrap)
           :ret  :netty/server-bootstrap))

(s/def :server.config/read-channel-builder
  (s/fspec :args [] :ret ::spec/read-channel))

(s/def :server.config/write-channel-builder
  (s/fspec :args [] :ret ::spec/write-channel))

(s/def :server.config/server-handler
  (s/fspec :args (s/cat :read-ch ::spec/read-channel, :write-ch ::spec/write-channel)
           :ret  any?))

(s/def ::config
  (s/keys
    :req [:server.config/server-handler]
    :opt [:server.config/read-channel-builder
          :server.config/write-channel-builder
          :server.config/port
          :server.config/channel-initializer
          :server.config/bootstrap-initializer]))

(s/def ::writedata
  (s/keys
    :req-un [:netty/message]
    :opt-un [:netty/flush
             :netty/close
             :netty/channel-promise]))

(defn default-channel-active
  [^ChannelHandlerContext ctx, write-ch]
  (log/debug "channel active: " (.name ctx))
  (thread
    (loop []
      (when-some [{:keys [message flush? close? ^ChannelPromise promise]
                   :or {flush? false
                        close? false
                        promise ^ChannelPromise (.voidPromise ctx)}
                   :as data}
                  (<!! write-ch)]
        (s/assert ::writedata data)
        (write-if-possible ctx (or flush? close?) message promise)
        (when close?
          (.close ctx))
        (recur)))))

(defn default-channel-inactive
  [^ChannelHandlerContext ctx, read-ch, write-ch]
  (log/debug "channel inactive: " (.name ctx))
  (close! read-ch)
  (close! write-ch))

(defn default-channel-read
  [^ChannelHandlerContext ctx, ^Object msg, read-ch]
  (log/debug "channel read: " (.name ctx))
  (>!! read-ch (boxed msg)))

(defn make-default-handler-map
  [read-ch write-ch]
  {:inbound/channel-read
    (fn [ctx msg]
      (default-channel-read ctx msg read-ch))

   :inbound/channel-active
    (fn [ctx]
      (default-channel-active ctx write-ch))

   :inbound/channel-inactive
    (fn [ctx]
      (default-channel-inactive ctx read-ch write-ch))

   :inbound/exception-caught
    (fn [^ChannelHandlerContext ctx, ^Throwable th]
      (go (>! read-ch (boxed th)))
      (.close ctx))})

(defn append-server-handler
  [^SocketChannel netty-ch read-ch write-ch]
  (.. netty-ch
    (pipeline)
    (addLast "async-connect-server" ^ChannelHandler (make-inbound-handler (make-default-handler-map read-ch write-ch)))))

(defn- init-bootstrap
  [bootstrap initializer]
  (if initializer
    (initializer bootstrap)
    bootstrap))

(s/fdef run-server
  :args (s/cat :read-channel ::spec/read-channel, :write-channel ::spec/write-channel, :config ::config)
  :ret  any?)

(defn run-server
  [{:keys [:server.config/server-handler
           :server.config/port
           :server.config/channel-initializer
           :server.config/bootstrap-initializer
           :server.config/read-channel-builder
           :server.config/write-channel-builder]
      :or {port 8080
           read-channel-builder #(chan)
           write-channel-builder #(chan)}
      :as config}]

  {:pre [config (:server.config/server-handler config)]}

  (let [boss-group ^EventLoopGroup (NioEventLoopGroup.)
        worker-group ^EventLoopGroup (NioEventLoopGroup.)]
    (try
      (let [bootstrap ^ServerBootstrap (.. ^ServerBootstrap (ServerBootstrap.)
                                          (childHandler
                                            (proxy [ChannelInitializer] []
                                              (initChannel
                                                [^SocketChannel netty-ch]
                                                (when channel-initializer
                                                  (channel-initializer netty-ch config))
                                                (let [read-ch  (read-channel-builder)
                                                      write-ch (write-channel-builder)]
                                                  (append-server-handler netty-ch read-ch write-ch)
                                                  (server-handler read-ch write-ch)))))
                                          (childOption ChannelOption/SO_KEEPALIVE true)
                                          (group boss-group worker-group)
                                          (channel NioServerSocketChannel)
                                          (option ChannelOption/SO_BACKLOG (int 128))
                                          (option ChannelOption/WRITE_BUFFER_HIGH_WATER_MARK (int (* 32 1024)))
                                          (option ChannelOption/WRITE_BUFFER_LOW_WATER_MARK (int (* 8 1024)))
                                          (option ChannelOption/ALLOCATOR PooledByteBufAllocator/DEFAULT))]

        (init-bootstrap bootstrap bootstrap-initializer)

        (let [f ^ChannelFuture (.. bootstrap (bind (int port)) (sync))]
          (log/info "SERVER STARTS AT" port)
          (.. f
            (channel)
            (closeFuture)
            (sync))))

      (finally
        (.shutdownGracefully worker-group)
        (.shutdownGracefully boss-group)))))

