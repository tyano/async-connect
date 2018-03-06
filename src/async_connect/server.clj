(ns async-connect.server
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [clojure.core.async :refer [>!! <!! >! <! go go-loop thread chan close!]]
            [clojure.tools.logging :as log]
            [async-connect.netty :refer [write-if-possible
                                         channel-handler-context-start
                                         default-channel-inactive
                                         default-channel-read
                                         default-exception-caught]]
            [async-connect.netty.handler :refer [make-inbound-handler] :as handler]
            [async-connect.netty.spec :as netty-spec]
            [async-connect.spec :as spec]
            [async-connect.box :refer [boxed] :as box])
  (:import [java.net
              InetAddress
              Inet4Address
              Inet6Address
              NetworkInterface]
           [io.netty.bootstrap ServerBootstrap]
           [io.netty.buffer
              PooledByteBufAllocator]
           [io.netty.channel
              Channel
              ChannelFuture
              ChannelFutureListener
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
              ReferenceCountUtil]
           [io.netty.util.concurrent
              GenericFutureListener]))

(s/def ::address (s/nilable string?))
(s/def ::port (s/or :zero zero? :pos pos-int?))

(s/def ::channel-initializer
  (s/fspec :args (s/cat :netty-channel ::netty-spec/channel
                        :config ::config-no-initializer)
           :ret ::netty-spec/channel))

(s/def ::bootstrap-initializer
  (s/fspec :args (s/cat :bootstrap ::netty-spec/server-bootstrap)
           :ret  ::netty-spec/server-bootstrap))

(s/def ::read-channel-builder
  (s/fspec :args (s/cat :channel ::netty-spec/channel) :ret ::spec/read-channel))

(s/def ::write-channel-builder
  (s/fspec :args (s/cat :channel ::netty-spec/channel) :ret ::spec/write-channel))

(s/def ::server-handler
  (s/fspec :args (s/cat :context :spec/atom, :read-ch ::spec/read-channel, :write-ch ::spec/write-channel)
           :ret  any?))

(s/def ::server-handler-factory
  (s/fspec :args (s/cat :address string? :port int?)
           :ret  ::server-handler))

(s/def ::close-handler
  (s/fspec :args empty? :ret any?))

(s/def ::boss-group ::netty-spec/event-loop-group)
(s/def ::worker-group ::netty-spec/event-loop-group)

(s/def ::shutdown-hook (s/fspec
                        :args (s/cat
                               :server-info (s/keys
                                             :req-un [:server-info/host
                                                      :server-info/port]))
                        :ret any?))

(s/def ::config-no-initializer
  (s/with-gen
    (s/keys
      :req [::server-handler-factory]
      :opt [::read-channel-builder
            ::write-channel-builder
            ::port
            ::bootstrap-initializer
            ::boss-group
            ::worker-group
            ::shutdown-hook])
    #(gen/return
        {::server-handler-factory
          (fn [host port]
            (fn [ctx read-ch write-ch] nil))})))

(s/def ::config
  (s/with-gen
    (s/merge
      (s/keys :opt [::channel-initializer])
      ::config-no-initializer)
    #(gen/return
        {::server-handler (fn [ctx read-ch write-ch] nil)})))


(defn make-default-handler-map
  [read-ch write-ch]
  {::handler/channel-read
    (fn [ctx msg] (default-channel-read ctx msg read-ch))

   ::handler/channel-active
    (fn [ctx] (channel-handler-context-start ctx write-ch))

   ::handler/channel-inactive
    (fn [ctx] (default-channel-inactive ctx read-ch write-ch))

   ::handler/exception-caught
    (fn [ctx, th] (default-exception-caught ctx th read-ch))})

(defn append-preprocess-handler
  [^SocketChannel netty-ch context read-ch write-ch]
  (.. netty-ch
    (pipeline)
    (addLast
        "async-connect-server"
        ^ChannelHandler (make-inbound-handler context (make-default-handler-map read-ch write-ch)))))

(defn- init-bootstrap
  [bootstrap initializer]
  (if initializer
    (initializer bootstrap)
    bootstrap))

(defprotocol IServer
  (close-wait [this close-handler])
  (port [this])
  (shutdown [this]))

(s/fdef run-server
  :args (s/cat :config ::config)
  :ret  #(instance? IServer %))

(defn run-server
  [{:keys [::server-handler-factory
           ::address
           ::port
           ::channel-initializer
           ::bootstrap-initializer
           ::read-channel-builder
           ::write-channel-builder
           ::boss-group
           ::worker-group
           ::shutdown-hook]
      :or {port 0
           read-channel-builder #(chan)
           write-channel-builder #(chan)
           boss-group (NioEventLoopGroup.)
           worker-group (NioEventLoopGroup.)}
      :as config}]

  {:pre [config (::server-handler-factory config)]}

  (let [server-address (when address (InetAddress/getByName address))
        handler-promise (promise)
        bootstrap ^ServerBootstrap (.. ^ServerBootstrap (ServerBootstrap.)
                                          (childHandler
                                            (proxy [ChannelInitializer] []
                                              (initChannel
                                                [^Channel netty-ch]
                                                (when channel-initializer
                                                  (channel-initializer netty-ch config))
                                                (let [server-handler @handler-promise
                                                      context  (atom nil)
                                                      read-ch  (read-channel-builder netty-ch)
                                                      write-ch (write-channel-builder netty-ch)]
                                                  (append-preprocess-handler netty-ch context read-ch write-ch)
                                                  (server-handler context read-ch write-ch)))))
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

    (let [f ^ChannelFuture (.. bootstrap (bind ^InetAddress server-address (int port)))
          server-promise   (promise)]

      (.addListener
        f
        (reify
          ChannelFutureListener
          (operationComplete
            [this fut]
            (when (.cause fut)
              (log/error (.cause fut) "Can not bind.")
              (throw (.cause fut)))

            (let [{:keys [server-host server-port]}
                    (let [local-address (-> fut (.channel) (.localAddress))]
                      {:server-host (when server-address (.getHostAddress server-address))
                       :server-port (.getPort local-address)})]

              (deliver handler-promise (server-handler-factory server-host server-port))
              (log/info "Listening address : " server-host ", port: " server-port)
              (deliver server-promise {:host server-host, :port server-port})))))

      (reify
        IServer
        (close-wait [_ close-handler]
          (try
            (-> f
              (.channel)
              (.closeFuture)
              (.sync))
            (finally
              (when close-handler (close-handler))
              (.shutdownGracefully ^EventLoopGroup (.group bootstrap)))))

        (port [this] (:port @server-promise))

        (shutdown [this]
          (when shutdown-hook
            (shutdown-hook @server-promise))
          (-> (.shutdownGracefully ^EventLoopGroup (.group bootstrap))
              (.addListener
               (reify GenericFutureListener
                 (operationComplete [this future]
                   (log/info "Server stopped."))))))))))

