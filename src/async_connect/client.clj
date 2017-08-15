(ns async-connect.client
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [clojure.spec.test.alpha :refer [with-instrument-disabled]]
            [clojure.tools.logging :as log]
            [clojure.core.async :refer [>!! <!! >! <! thread close! chan go go-loop]]
            [async-connect.spec :as spec]
            [async-connect.netty :refer [write-if-possible
                                         bytebuf->string
                                         string->bytebuf
                                         channel-handler-context-start
                                         default-channel-inactive
                                         default-channel-read
                                         default-exception-caught]]
            [async-connect.netty.handler :refer [make-inbound-handler make-outbound-handler]]
            [async-connect.box :refer [boxed]])
  (:import [io.netty.bootstrap
              Bootstrap]
           [io.netty.buffer
              PooledByteBufAllocator]
           [io.netty.channel
              EventLoopGroup
              ChannelOption
              ChannelInitializer
              ChannelHandler
              ChannelHandlerContext
              ChannelFuture
              ChannelFutureListener
              ChannelPromise
              ChannelPipeline]
           [io.netty.channel.nio
              NioEventLoopGroup]
           [io.netty.channel.socket
              SocketChannel]
           [io.netty.channel.socket.nio
              NioSocketChannel]))


#_(s/def :client.config/channel-initializer
   (s/fspec :args (s/cat :netty-channel :netty/socket-channel
                         :config ::config)
            :ret :netty/socket-channel))

#_(s/def :client.config/bootstrap-initializer
   (s/fspec :args (s/cat :bootstrap :netty/bootstrap)
            :ret  :netty/bootstrap))

(s/def ::config
  (s/with-gen
    (s/keys
      :opt [:client.config/bootstrap-initializer
            :client.config/channel-initializer])
    #(gen/one-of
        {}
        {:client.config/bootstrap-initializer (fn [bootstrap] bootstrap)}
        {:client.config/channel-initializer   (fn [channel config] channel)}
        {:client.config/bootstrap-initializer (fn [bootstrap] bootstrap)
         :client.config/channel-initializer   (fn [channel config] channel)})))

(defn add-future-listener
  [^ChannelPromise prms read-ch]
  (.. prms
    (addListener
      (reify ChannelFutureListener
        (operationComplete
          [this f]
          (when-let [cause (.cause ^ChannelFuture f)]
            (>!! read-ch (boxed cause))))))))

(defn make-default-promise
  [^ChannelHandlerContext ctx, read-ch]
  (-> ctx
    (.newPromise)
    (add-future-listener read-ch)))

(s/fdef make-client-inbound-handler-map
  :args (s/cat :read-ch ::spec/read-channel, :write-ch ::spec/write-channel)
  :ret  :inbound/handler-map)

(defn make-client-inbound-handler-map
  [read-ch write-ch]
  {:inbound/channel-read
    (fn [ctx msg] (default-channel-read ctx msg read-ch))

   :handler/handler-added
    (fn [ctx] (channel-handler-context-start ctx write-ch))

   :inbound/channel-inactive
    (fn [ctx] (default-channel-inactive ctx read-ch write-ch))

   :inbound/exception-caught
    (fn [ctx, th] (default-exception-caught ctx th read-ch))})


(defn add-client-handler
  [^SocketChannel netty-channel context read-ch write-ch]
  (when netty-channel
    (log/trace "add-client-handler: " netty-channel)
    (let [inbound-handler-name "async-connect-client-inbound"
          outbound-handler-name "async-connect-client-outbound"
          pipeline     ^ChannelPipeline (.pipeline netty-channel)]
      (when (.context pipeline inbound-handler-name)
        (.remove pipeline inbound-handler-name))
      (when (.context pipeline outbound-handler-name)
        (.remove pipeline outbound-handler-name))

      (.addLast pipeline
        inbound-handler-name
        ^ChannelHandler (make-inbound-handler context (make-client-inbound-handler-map read-ch write-ch)))
      (.addLast pipeline
        outbound-handler-name
        ^ChannelHandler (make-outbound-handler context {})))))


(defn- init-bootstrap
  [bootstrap initializer]
  (if initializer
    (initializer bootstrap)
    bootstrap))

(s/fdef make-bootstrap
  :args (s/cat :config ::config)
  :ret  :netty/bootstrap)

(defn make-bootstrap
  ([{:keys [:client.config/bootstrap-initializer
            :client.config/channel-initializer]
      :as config}]
   (let [worker-group ^EventLoopGroup (NioEventLoopGroup.)]
     (let [bootstrap (.. (Bootstrap.)
                       (group worker-group)
                       (channel NioSocketChannel)
                       (option ChannelOption/WRITE_BUFFER_HIGH_WATER_MARK (int (* 32 1024)))
                       (option ChannelOption/WRITE_BUFFER_LOW_WATER_MARK (int (* 8 1024)))
                       (option ChannelOption/SO_SNDBUF (int (* 1024 1024)))
                       (option ChannelOption/SO_RCVBUF (int (* 1024 1024)))
                       (option ChannelOption/TCP_NODELAY true)
                       (option ChannelOption/SO_KEEPALIVE true)
                       (option ChannelOption/SO_REUSEADDR true)
                       (option ChannelOption/ALLOCATOR PooledByteBufAllocator/DEFAULT)
                       (handler
                         (proxy [ChannelInitializer] []
                           (initChannel
                             [^SocketChannel ch]
                             (when channel-initializer
                               (channel-initializer ch config))
                             nil))))]
       (init-bootstrap bootstrap bootstrap-initializer))))
  ([]
   (make-bootstrap {})))


(s/def :client/channel  (s/nilable :netty/channel))
(s/def :client/context  ::spec/atom)
(s/def :client/read-ch  ::spec/read-channel)
(s/def :client/write-ch ::spec/write-channel)
(s/def :client/connection
  (s/keys
    :req [:client/channel
          :client/context
          :client/read-ch
          :client/write-ch]))

(s/fdef close
  :args (s/cat :connection :client/connection :close? (s/? boolean?))
  :ret  :client/connection)

(defprotocol IConnection
  (close [this] [this force?]
    "Close this connection. In simple implementation, a netty connection held by this connection will be closed.
    If this connection uses a kind of connection pools, calling `close` will not close a read connection, but
    return the connection to a pool.
    if `force?` is true, the connection must be really closed instead of returning it into a pool."))

(defn close-connection
  [{:keys [:client/channel :client/read-ch :client/write-ch] :as connection}]
  (when channel
    (.. ^SocketChannel channel
        (close)
        (addListener
          (reify ChannelFutureListener
            (operationComplete
              [this f]
              (log/debug "connection closed: " channel))))))

  (when read-ch (close! read-ch))
  (when write-ch (close! write-ch))
  (assoc connection :client/channel nil))


(defrecord NettyConnection []
  IConnection
  (close
    [connection force?]
    (close-connection connection))

  (close [this]
    (close this false)))


(defprotocol IConnectionFactory
  (create-connection [this host port read-ch write-ch]
    "Connect to a `port` of a `host` using `bootstrap`, and return a IConnection object.
     If read-ch and write-ch are supplied, all data written and read are transfered to the supplied channels,
     If read-ch and write-ch aren't supplied, channels made by `(chan)` are used."))


(s/def ::connection-factory #(satisfies? IConnectionFactory %))

(defn- connect*
  [^Bootstrap bootstrap ^String host port read-ch write-ch]
  (let [read-chan  (or read-ch (chan))
        write-chan (or write-ch (chan))
        context (atom nil)
        channel (.. bootstrap (connect host (int port)) (sync) (channel))]

    (log/debug "connected:" (str "host: " host ", port: " port))
    (add-client-handler channel context read-chan write-chan)

    (map->NettyConnection {:client/channel  channel
                           :client/context  context
                           :client/read-ch  read-chan
                           :client/write-ch write-chan})))

(defrecord NettyConnectionFactory
  [bootstrap]
  IConnectionFactory
  (create-connection
    [this host port read-ch write-ch]
    (connect* (:bootstrap this) host port read-ch write-ch)))

(defn connection-factory
  ([bootstrap]
   (->NettyConnectionFactory bootstrap))
  ([]
   (connection-factory (make-bootstrap {}))))

(s/fdef connect
  :args (s/cat :factory   ::connection-factory
               :host      string?
               :port      pos-int?
               :read-ch   (s/? (s/nilable :client/read-ch))
               :write-ch  (s/? (s/nilable :client/write-ch)))
  :ret  :client/connection)

(defn connect
  ([factory host port read-ch write-ch]
   (log/trace "async: connect:" factory)
   (create-connection factory host port read-ch write-ch))

  ([factory host port]
   (log/trace "async: connect:" factory)
   (create-connection factory host port nil nil)))


(s/fdef closed?
  :args (s/cat :connection :client/connection)
  :ret  boolean?)

(defn closed?
  [{:keys [:client/channel]}]
  (nil? channel))

(defn sample-connect
  []
  (let [factory  (connection-factory)
        read-ch  (chan 1 bytebuf->string)
        write-ch (chan 1 string->bytebuf)
        conn     (connect factory "localhost" 8080 read-ch write-ch)]
    (go-loop []
      (println "result: " @(<! read-ch))
      (recur))
    conn))
