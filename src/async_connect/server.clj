(ns async-connect.server
  (:require [clojure.spec :as s]
            [clojure.spec.gen :as gen]
            [clojure.core.async :refer [>!! <!! >! <! go go-loop thread chan close!]]
            [clojure.tools.logging :as log]
            [async-connect.netty :refer [write-if-possible
                                         channel-handler-context-start
                                         default-channel-inactive
                                         default-channel-read
                                         default-exception-caught]]
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
              NioServerSocketChannel]
           [io.netty.util
              ReferenceCountUtil]))

(s/def :server.config/address (s/nilable string?))
(s/def :server.config/port (s/or :zero zero? :pos pos-int?))

#_(s/def :server.config/channel-initializer
  (s/fspec :args (s/cat :netty-channel :netty/channel
                        :config ::config-no-initializer)
           :ret :netty/channel))

#_(s/def :server.config/bootstrap-initializer
  (s/fspec :args (s/cat :bootstrap :netty/server-bootstrap)
           :ret  :netty/server-bootstrap))

#_(s/def :server.config/read-channel-builder
  (s/fspec :args empty? :ret ::spec/read-channel))

#_(s/def :server.config/write-channel-builder
  (s/fspec :args empty? :ret ::spec/write-channel))

#_(s/def :server.config/server-handler
  (s/fspec :args (s/cat :read-ch ::spec/read-channel, :write-ch ::spec/write-channel)
           :ret  any?))

#_(s/def :server.config/server-handler-factory
  (s/fspec :args (s/cat :address string? :port int?)
           :ret  :server.config/server-handler))

#_(s/def :server.config/close-handler
  (s/fspec :args empty? :ret any?))

(s/def :server.config/boss-group :netty/event-loop-group)
(s/def :server.config/worker-group :netty/event-loop-group)

(s/def ::config-no-initializer
  (s/with-gen
    (s/keys
      :req [:server.config/server-handler]
      :opt [:server.config/read-channel-builder
            :server.config/write-channel-builder
            :server.config/port
            :server.config/bootstrap-initializer
            :server.config/boss-group
            :server.config/worker-group])
    #(gen/return
        {:server.config/server-handler (fn [read-ch write-ch] nil)})))

(s/def ::config
  (s/with-gen
    (s/merge
      (s/keys :opt [:server.config/channel-initializer])
      ::config-no-initializer)
    #(gen/return
        {:server.config/server-handler (fn [read-ch write-ch] nil)})))


(defn make-default-handler-map
  [read-ch write-ch]
  {:inbound/channel-read
    (fn [ctx msg] (default-channel-read ctx msg read-ch))

   :inbound/channel-active
    (fn [ctx] (channel-handler-context-start ctx write-ch))

   :inbound/channel-inactive
    (fn [ctx] (default-channel-inactive ctx read-ch write-ch))

   :inbound/exception-caught
    (fn [ctx, th] (default-exception-caught ctx th read-ch))})

(defn append-server-handler
  [^SocketChannel netty-ch read-ch write-ch]
  (.. netty-ch
    (pipeline)
    (addLast
        "async-connect-server"
        ^ChannelHandler (with-instrument-disabled (make-inbound-handler (make-default-handler-map read-ch write-ch))))))

(defn- init-bootstrap
  [bootstrap initializer]
  (if initializer
    (initializer bootstrap)
    bootstrap))

(defprotocol IServer
  (close-wait [this close-handler]))

(s/fdef run-server
  :args (s/cat :config ::config)
  :ret  #(instance? IServer %))

(defn run-server
  [{:keys [:server.config/server-handler
           :server.config/port
           :server.config/channel-initializer
           :server.config/bootstrap-initializer
           :server.config/read-channel-builder
           :server.config/write-channel-builder
           :server.config/boss-group
           :server.config/worker-group]
      :or {port 8080
           read-channel-builder #(chan)
           write-channel-builder #(chan)
           boss-group (NioEventLoopGroup.)
           worker-group (NioEventLoopGroup.)}
      :as config}]

  {:pre [config (:server.config/server-handler config)]}

  (let [bootstrap ^ServerBootstrap (.. ^ServerBootstrap (ServerBootstrap.)
                                          (childHandler
                                            (proxy [ChannelInitializer] []
                                              (initChannel
                                                [^Channel netty-ch]
                                                (when channel-initializer
                                                  (channel-initializer netty-ch config))
                                                (let [read-ch  (read-channel-builder)
                                                      write-ch (write-channel-builder)]
                                                  (append-server-handler netty-ch read-ch write-ch)
                                                  (server-handler read-ch write-ch)))))
                                          (childOption ChannelOption/SO_KEEPALIVE true)
                                          (group
                                            ^EventLoopGroup boss-group
                                            ^EventLoopGroup worker-group)
                                          (channel NioServerSocketChannel)
                                          (option ChannelOption/SO_BACKLOG (int 128))
                                          (option ChannelOption/WRITE_BUFFER_HIGH_WATER_MARK (int (* 32 1024)))
                                          (option ChannelOption/WRITE_BUFFER_LOW_WATER_MARK (int (* 8 1024)))
                                          (option ChannelOption/ALLOCATOR PooledByteBufAllocator/DEFAULT))]

    (init-bootstrap bootstrap bootstrap-initializer)

    (let [f ^ChannelFuture (.. bootstrap (bind (int port)) (sync))]
      (reify
        IServer
        (close-wait [_ close-handler]
          (try
            (.. f
              (channel)
              (closeFuture)
              (sync))
            (finally
              (when close-handler (close-handler))
              (.shutdownGracefully ^EventLoopGroup worker-group)
              (.shutdownGracefully ^EventLoopGroup boss-group))))))))


