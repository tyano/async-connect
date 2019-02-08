(ns async-connect.netty
  (:require [clojure.spec.alpha :as s]
            [async-connect.netty.spec :as netty]
            [async-connect.message :as message]
            [clojure.tools.logging :as log]
            [databox.core :as box]
            [clojure.core.async :refer [>!! <!! >! <! thread close! go go-loop put!]])
  (:import [io.netty.buffer
              ByteBuf
              Unpooled]
           [io.netty.channel
              Channel
              ChannelHandlerContext
              ChannelPromise]
           [io.netty.util
              ReferenceCountUtil]))

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

(defn channel-handler-context-start
  [^ChannelHandlerContext ctx, write-ch]
  (log/debug "context-start: " ctx)
  (go-loop []
    (if-some [{::message/keys [data flush? close? ^ChannelPromise promise]
                 :or {flush? false
                      close? false
                      promise ^ChannelPromise (.voidPromise ctx)}
                 :as msg}
              (<! write-ch)]
      (do
        (s/assert :async-connect/message msg)
        (<! (thread
              (write-if-possible ctx (or flush? close?) data promise)
              (when close?
                (.close ctx))))
        (recur))
      (do
        (log/debug "A writer-thread stops: " ctx)
        (.close ctx)))))

(defn default-channel-inactive
  [^ChannelHandlerContext ctx, read-ch, write-ch]
  (log/trace "channel inactive: " ctx)
  (close! read-ch)
  (close! write-ch))

(defn default-channel-read
  [^ChannelHandlerContext ctx, ^Object msg, read-ch]
  (log/trace "channel read: " ctx)
  (when-not (put! read-ch (box/value msg))
    ;; the port is already closed. close a current context.
    (.close ctx)))

(defn default-exception-caught
  [^ChannelHandlerContext ctx, ^Throwable th, read-ch]
  (log/trace "exception-caught: " ctx)
  (put! read-ch (box/value th))
  (.close ctx))

(defn bytebuf->bytes
  [data]
  (box/map data
    (fn [^ByteBuf buf]
      (let [bytes (byte-array (.readableBytes buf))]
        (.readBytes buf bytes)
        (ReferenceCountUtil/safeRelease buf)
        bytes))))

(defn bytes->string
  [data]
  (box/map data #(String. ^bytes %)))

(defn string->bytes
  [msg]
  (update msg ::message/data #(.getBytes ^String % "UTF-8")))

(defn- bytes->bytebuf
  [msg]
  (update msg ::message/data #(Unpooled/wrappedBuffer ^bytes %)))

(def bytebuf->string (comp (map bytebuf->bytes) (map bytes->string)))
(def string->bytebuf (comp (map string->bytes) (map bytes->bytebuf)))

