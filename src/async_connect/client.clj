(ns async-connect.client
  (:require [clojure.spec :as s]
            [clojure.tools.logging :as log]
            [async-connect.netty :refer [make-channel-initializer]]
            [async-connect.spec :refer [async-channel?]]
            [async-connect.netty.handler :refer [make-inbound-handler]])
  (:import [io.netty.bootstrap
              Bootstrap]
           [io.netty.channel
              EventLoopGroup
              ChannelOption
              ChannelInitializer
              ChannelHandler
              ChannelHandlerContext]
           [io.netty.channel.nio
              NioEventLoopGroup]
           [io.netty.channel.socket
              SocketChannel]
           [io.netty.channel.socket.nio
              NioSocketChannel]))



(s/def :client.config/port pos-int?)
(s/def ::config
  (s/key
    :req [:client.config/port]))


(defn default-channel-read
  [^ChannelHandlerContext ctx, ^Object msg, read-ch]
  (log/debug "channel read: " (.name ctx))
  (try
    (>!! read-ch msg)
    (finally
      (ReferenceCountUtil/releaseLater ^ByteBuf msg))))

(defn make-default-inbound-handler-map
  [read-ch]
  {:inbound/channel-read
      (fn [^ChannelHandlerContext ctx, ^Object msg]
        (default-channel-read ctx msg read-ch))

   :inbound/exception-caught
      (fn [^ChannelHandlerContext ctx, ^Throwable th]
        (.printStackTrace th)
        (.close ctx))})


(defn make-default-outbound-handler-map
  [write-ch]
  {:outbound/bind
      (fn [ctx local-addr promise])
   :outbound/close
      (fn [ctx promise])
   :outbound/connect
      (fn [ctx remote-addr local-addr promise])})


(defn default-channel-initializer
  [read-ch write-ch]
  (fn [^SocketChannel netty-ch]
    (.. netty-ch
      (pipeline)
      (addLast (into-array ChannelHandler [(make-inbound-handler (make-default-inbound-handler-map read-ch))])))))


(defn make-bootstrap
  [{:keys [:client.config/port] :as config}]
  (let [worker-group ^EventLoopGroup (NioEventLoopGroup.)]
    (let [bootstrap (Bootstrap.)]
      (.. bootstrap
        (group worker-group)
        (channel NioSocketChannel)
        (option ChannelOption/SO_KEEPALIVE true)
        (handler
          (proxy [ChannelInitializer] []
            (initChannel
              [^SocketChannel ch]
              )))))))
