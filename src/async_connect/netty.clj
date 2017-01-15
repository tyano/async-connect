(ns async-connect.netty
  (:require [clojure.spec :as s]
            [clojure.tools.logging :as log]
            [async-connect.box :refer [boxed] :as box])
  (:import [clojure.core.async.impl.channels ManyToManyChannel]
           [io.netty.bootstrap
              Bootstrap
              ServerBootstrap]
           [io.netty.channel
              ChannelHandlerContext
              ChannelPromise
              Channel]
           [io.netty.channel.nio
              NioEventLoopGroup]
           [io.netty.channel.socket
              SocketChannel]
           [io.netty.channel.socket.nio
              NioServerSocketChannel]
           [io.netty.buffer
              ByteBuf
              Unpooled]
           [io.netty.util
              ReferenceCountUtil]))

(s/def :netty/context #(instance? ChannelHandlerContext %))
(s/def :netty/message any?)
(s/def :netty/channel-promise #(instance? ChannelPromise %))
(s/def :netty/channel #(instance? Channel %))
(s/def :netty/bootstrap #(instance? Bootstrap %))
(s/def :netty/server-bootstrap #(instance? ServerBootstrap %))
(s/def :netty/socket-channel #(instance? SocketChannel %))

(s/def :netty/flush boolean?)
(s/def :netty/close boolean?)

(s/def :netty/writedata
  (s/keys
    :req-un [:netty/context
             :netty/message]
    :opt-un [:netty/flush
             :netty/close
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

(defn bytebuf->bytes
  [data]
  (box/update data
    (fn [^ByteBuf buf]
      (let [bytes (byte-array (.readableBytes buf))]
        (.readBytes buf bytes)
        (ReferenceCountUtil/safeRelease buf)
        bytes))))

(defn bytes->string
  [data]
  (box/update data #(String. ^bytes %)))

(defn string->bytes
  [data]
  (update data :message #(.getBytes ^String % "UTF-8")))

(defn- bytes->bytebuf
  [data]
  (update data :message #(Unpooled/wrappedBuffer ^bytes %)))

(def bytebuf->string (comp (map bytebuf->bytes) (map bytes->string)))
(def string->bytebuf (comp (map string->bytes) (map bytes->bytebuf)))

