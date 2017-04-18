(ns async-connect.netty
  (:require [clojure.spec :as s]
            [clojure.spec.gen :as gen]
            [async-connect.spec.generator :as agen]
            [async-connect.netty.spec]
            [clojure.tools.logging :as log]
            [async-connect.box :refer [boxed] :as box]
            [clojure.core.async :refer [>!! <!! >! <! thread close! go go-loop]])
  (:import [io.netty.buffer
              ByteBuf
              Unpooled]
           [io.netty.channel
              ChannelHandlerContext
              ChannelPromise
              Channel]
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


(s/def :writedata/promise :netty/channel-promise)

(s/def ::writedata
  (s/keys
    :req-un [:netty/message]
    :opt-un [:netty/flush?
             :netty/close?
             :writedata/promise]))

(defn channel-handler-context-start
  [^ChannelHandlerContext ctx, write-ch]
  (log/debug "context-start: " ctx)
  (go-loop []
    (if-some [{:keys [message flush? close? ^ChannelPromise promise]
                 :or {flush? false
                      close? false
                      promise ^ChannelPromise (.voidPromise ctx)}
                 :as data}
              (<! write-ch)]
      (do
        (s/assert ::writedata data)
        (thread
          (write-if-possible ctx (or flush? close?) message promise)
          (when close?
            (.close ctx)))
        (recur))
      (log/debug "A writer-thread stops: " ctx))))

(defn default-channel-inactive
  [^ChannelHandlerContext ctx, read-ch, write-ch]
  (log/trace "channel inactive: " ctx)
  (close! read-ch)
  (close! write-ch))

(defn default-channel-read
  [^ChannelHandlerContext ctx, ^Object msg, read-ch]
  (log/trace "channel read: " ctx)
  (>!! read-ch (boxed msg)))

(defn default-exception-caught
  [^ChannelHandlerContext ctx, ^Throwable th, read-ch]
  (log/trace "exception-caught: " ctx)
  (go (>! read-ch (boxed th)))
  (.close ctx))

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

