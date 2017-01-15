(ns async-connect.client
  (:require [clojure.spec :as s]
            [clojure.tools.logging :as log]
            [clojure.core.async :refer [>!! <!! >! <! thread close! chan go go-loop]]
            [async-connect.spec :as spec]
            [async-connect.netty :refer [write-if-possible bytebuf->string string->bytebuf]]
            [async-connect.netty.handler :refer [make-inbound-handler]]
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


(s/def :client.config/port pos-int?)
(s/def :client.config/channel-initializer
  (s/fspec :args (s/cat :netty-channel :netty/socket-channel
                        :config ::config)
           :ret :netty/socket-channel))

(s/def :client.config/bootstrap-initializer
  (s/fspec :args (s/cat :bootstrap :netty/bootstrap)
           :ret  :netty/bootstrap))

(s/def ::config
  (s/keys
    :req [:client.config/port]
    :opt [:client.config/bootstrap-initializer
          :client.config/channel-initializer]))

(s/def ::writedata
  (s/keys
    :req-un [:netty/message]
    :opt-un [:netty/flush
             :netty/close
             :netty/channel-promise]))

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
  {:handler/handler-added
    (fn [^ChannelHandlerContext ctx]
      (log/debug "handler-added")
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
            (if close?
              (.close ctx)
              (recur))))
        (log/info "A writer-thread stops.")))

   :inbound/channel-read
    (fn [^ChannelHandlerContext ctx, ^Object msg]
      (log/trace "channel-read")
      (>!! read-ch (boxed msg)))

   :inbound/channel-inactive
    (fn [^ChannelHandlerContext ctx]
      (log/debug "channel-inactive")
      (close! read-ch)
      (close! write-ch))

   :inbound/exception-caught
    (fn [^ChannelHandlerContext ctx, ^Throwable th]
      (log/debug "exception-caught")
      (go (>! read-ch (boxed th)))
      (.close ctx))})


(defn add-client-handler
  [^SocketChannel netty-channel read-ch write-ch]
  (when netty-channel
    (log/debug "add-client-handler: " netty-channel)
    (let [handler-name "async-connect-client"
          pipeline     ^ChannelPipeline (.pipeline netty-channel)]
      (when (.context pipeline handler-name)
        (.remove pipeline handler-name))
      (.addLast pipeline
        handler-name
        (make-inbound-handler (make-client-inbound-handler-map read-ch write-ch))))))


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
                                (channel-initializer ch))
                              nil))))]
        (init-bootstrap bootstrap bootstrap-initializer))))
  ([]
    (make-bootstrap {})))


(s/def :client/channel  (s/nilable :netty/channel))
(s/def :client/read-ch  ::spec/read-channel)
(s/def :client/write-ch ::spec/write-channel)
(s/def :client/connection
  (s/keys
    :req [:client/channel
          :client/read-ch
          :client/write-ch]))

(s/fdef connect
  :args (s/cat :bootstrap :netty/bootstrap,
               :host      string?
               :port      pos-int?
               :read-ch   (s/? (s/nilable :client/read-ch))
               :write-ch  (s/? (s/nilable :client/write-ch)))
  :ret  :client/connection)

(defn connect
  ([^Bootstrap bootstrap ^String host port read-ch write-ch]
    (let [read-chan  (or read-ch (chan))
          write-chan (or write-ch (chan))
          channel (.. bootstrap (connect host (int port)) (sync) (channel))]

      (add-client-handler channel read-chan write-chan)

      {:client/channel  channel
       :client/read-ch  read-chan
       :client/write-ch write-chan}))

  ([^Bootstrap bootstrap host port]
    (connect bootstrap host port nil nil)))

(s/fdef close
  :args (s/cat :connection :client/connection)
  :ret  :client/connection)

(defn close
  [{:keys [:client/channel :client/read-ch :client/write-ch] :as connection}]
  (when channel
    (.. ^SocketChannel channel (close) (sync))
    (log/debug "connection closed: " channel)
    (close! read-ch)
    (close! write-ch)
    (assoc connection :client/channel nil)))

(s/fdef closed?
  :args (s/cat :connection :client/connection)
  :ret  boolean?)

(defn closed?
  [{:keys [:client/channel]}]
  (nil? channel))

(defn sample-connect
  []
  (let [bootstrap (make-bootstrap {})
        read-ch  (chan 1 bytebuf->string)
        write-ch (chan 1 string->bytebuf)
        conn     (connect bootstrap "localhost" 8080 read-ch write-ch)]
    (go-loop []
      (println "result: " @(<! read-ch))
      (recur))
    conn))
