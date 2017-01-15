(ns async-connect.core
  (:require [async-connect.server :refer [run-server close-wait] :as server]
            [clojure.spec :as s]
            [clojure.tools.logging :as log]
            [clojure.core.async :refer [chan go-loop <! >!]]
            [clojure.spec.test :as stest]
            [async-connect.spec :as spec]
            [async-connect.netty :refer [bytebuf->string string->bytebuf]])
  (:import [io.netty.buffer ByteBuf Unpooled]
           [io.netty.channel
              ChannelHandlerContext
              ChannelInboundHandlerAdapter
              ChannelHandler])
  (:gen-class))

(s/def ::run-server-spec
  (s/fspec
    :args (s/cat :read-channel ::spec/read-channel, :write-channel ::spec/write-channel, :config any?)
    :ret  any?))


(defn- server-handler
  [read-ch write-ch]
  {:pre [read-ch write-ch]}
  (go-loop []
    (if-some [msg (<! read-ch)]
      (do
        (log/info "received data" @msg)
        (>! write-ch {:message "OK\n", :flush? true, :close? false})
        (recur))
      (log/info "channel is close."))))

(defn -main
  [& args]
  (let [config   {:server.config/port 8080
                  :server.config/read-channel-builder #(chan 1 bytebuf->string)
                  :server.config/write-channel-builder #(chan 1 string->bytebuf)
                  :server.config/server-handler server-handler}]
    (s/assert ::server/config config)

    (stest/instrument
      {:spec {'async-connect.server/run-server ::run-server-spec}})

    (-> (run-server config)
      (close-wait))))
