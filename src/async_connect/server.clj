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
            [async-connect.netty.handler :refer [make-inbound-handler]]
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
      :req [:server.config/server-handler-factory]
      :opt [:server.config/read-channel-builder
            :server.config/write-channel-builder
            :server.config/port
            :server.config/bootstrap-initializer
            :server.config/boss-group
            :server.config/worker-group])
    #(gen/return
        {:server.config/server-handler-factory
          (fn [host port]
            (fn [read-ch write-ch] nil))})))

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

(defn append-preprocess-handler
  [^SocketChannel netty-ch read-ch write-ch]
  (.. netty-ch
    (pipeline)
    (addLast
        "async-connect-server"
        ^ChannelHandler (make-inbound-handler (make-default-handler-map read-ch write-ch)))))

(defn- init-bootstrap
  [bootstrap initializer]
  (if initializer
    (initializer bootstrap)
    bootstrap))

(defprotocol IServer
  (close-wait [this close-handler])
  (port [this]))


(defn sort-addresses
  [addresses]
  (sort-by #(cond (instance? Inet4Address %) 0
                  (instance? Inet6Address %) 1
                  :else 2)
    addresses))

(defn find-one-public-address
  []
  (or
    (->> (enumeration-seq (NetworkInterface/getNetworkInterfaces))
         (filter #(not (.isLoopback %)))
         (mapcat #(enumeration-seq (.getInetAddresses %)))
         (sort-addresses)
         (first))
    (->> (enumeration-seq (NetworkInterface/getNetworkInterfaces))
         (filter #(.isLoopback %))
         (first))))

(s/fdef run-server
  :args (s/cat :config ::config)
  :ret  #(instance? IServer %))

(defn run-server
  [{:keys [:server.config/server-handler-factory
           :server.config/address
           :server.config/port
           :server.config/channel-initializer
           :server.config/bootstrap-initializer
           :server.config/read-channel-builder
           :server.config/write-channel-builder
           :server.config/boss-group
           :server.config/worker-group]
      :or {port 0
           read-channel-builder #(chan)
           write-channel-builder #(chan)
           boss-group (NioEventLoopGroup.)
           worker-group (NioEventLoopGroup.)}
      :as config}]

  {:pre [config (:server.config/server-handler-factory config)]}

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
                                                      read-ch  (read-channel-builder)
                                                      write-ch (write-channel-builder)]
                                                  (append-preprocess-handler netty-ch read-ch write-ch)
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
                      {:server-host (if server-address
                                      (.getHostAddress server-address)
                                      (.getHostAddress (find-one-public-address)))
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
              (.shutdownGracefully ^EventLoopGroup worker-group)
              (.shutdownGracefully ^EventLoopGroup boss-group))))

        (port [this] (:port @server-promise))))))



