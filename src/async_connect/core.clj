(ns async-connect.core
  (:require [async-connect.netty.server :refer [run-server] :as server]
            [clojure.spec :as s]
            [clojure.tools.logging :as log]
            [clojure.core.async :refer [chan go-loop <! >!]])
  (:import [io.netty.buffer ByteBuf Unpooled]
           [io.netty.channel
              ChannelHandlerContext
              ChannelInboundHandlerAdapter
              ChannelHandler])
  (:gen-class))


(defn- bytebuf->bytes
  [data]
  (update data :message
    (fn [buf]
      (assert #(instance? ByteBuf buf))
      (let [bytes (byte-array (.readableBytes buf))]
        (.readBytes buf bytes)
        bytes))))

(defn- bytes->string
  [data]
  (update data :message #(String. %)))

(defn- string->bytes
  [data]
  (update data :message #(.getBytes % "UTF-8")))

(defn- bytes->bytebuf
  [{:keys [context message] :as data}]
  (update data :message #(Unpooled/wrappedBuffer %)))

(def bytebuf->string (comp (map bytebuf->bytes) (map bytes->string)))
(def string->bytebuf (comp (map string->bytes) (map bytes->bytebuf)))

(defn -main
  [& args]
  (let [config   {:server.config/port 8080}
        read-ch  (chan 1 bytebuf->string)
        write-ch (chan 1 string->bytebuf)]
    (s/assert ::server/config config)

    (go-loop []
      (if-some [{:keys [message] :as data} (<! read-ch)]
        (do
          (log/info "received data" message)
          (>! write-ch (assoc data :message "OK\n", :flush? true, :close? false))
          (recur))
        (log/info "channel is close.")))


    (run-server read-ch write-ch config)))
