(ns async-connect.netty.spec
  (:require [clojure.spec.alpha :as s])
  (:import [java.net
              SocketAddress]
           [io.netty.bootstrap
              Bootstrap
              ServerBootstrap]
           [io.netty.channel
              ChannelHandlerContext
              ChannelPromise
              Channel
              EventLoopGroup]
           [io.netty.channel.socket
              SocketChannel]))

(s/def ::context          #(instance? ChannelHandlerContext %))
(s/def ::channel-promise  #(instance? ChannelPromise %))
(s/def ::channel          #(instance? Channel %))
(s/def ::bootstrap        #(instance? Bootstrap %))
(s/def ::server-bootstrap #(instance? ServerBootstrap %))
(s/def ::socket-channel   #(instance? SocketChannel %))
(s/def ::event-loop-group #(instance? EventLoopGroup %))
(s/def ::throwable        #(instance? Throwable %))
(s/def ::socket-address   #(instance? SocketAddress %))
