(ns async-connect.pool
  (:require [async-connect.client :refer [IConnection IConnectionFactory] :as client]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [clojure.core.async :refer [chan <! >! go-loop]]
            [async-connect.netty :refer [bytebuf->string
                                         string->bytebuf]])
  (:import [io.netty.channel.socket
              SocketChannel]
           [io.netty.channel
              Channel
              ChannelFutureListener
              ChannelHandlerContext
              ChannelDuplexHandler
              ChannelPipeline
              ChannelHandler]
           [io.netty.bootstrap
              Bootstrap]
           [io.netty.handler.timeout
              IdleStateHandler
              IdleStateEvent
              IdleState]))

(s/def :pool/host string?)
(s/def :pool/port pos-int?)
(s/def :pool/key
  (s/keys :req [:pool/host :pool/port]))

(defn- make-pool-key
  [host port]
  {:pool/host host, :pool/port port})

(defn remove-from-pool
  [pooled-connections {:keys [:pool/host :pool/port] :as conn}]
  (let [pool-key (make-pool-key host port)]
    (locking pooled-connections
      (log/trace "removing a connection:" conn)
      (swap! pooled-connections update pool-key #(when % (vec (filter (fn [c] (not= c conn)) %)))))
    nil))

(defn- make-idle-state-handler
  [timeout-sec]
  (IdleStateHandler. (int 0) (int 0) (int (or timeout-sec 0))))

(defn- pooled?
  [pooled-connections pool-key torn-conn]
  (locking pooled-connections
    (boolean
      (if-let [conns (get @pooled-connections pool-key)]
        (some #(= % torn-conn) conns)))))

(defn- close-physical-connection
  [{:keys [:pool/host :pool/port] :as conn} force-close?]
  (when conn
    (let [pool-key (make-pool-key host port)
          pooled-connections (:pooled-connections conn)
          must-remove? (locking pooled-connections
                          (let [torn-conn   (dissoc conn :pooled-connections)
                                must-remove? (pooled? pooled-connections pool-key torn-conn)]

                            ;; remove this connection from connection-pool.
                            (when must-remove?
                              (log/trace "remove a connection:" (pr-str torn-conn))
                              (remove-from-pool pooled-connections torn-conn))

                            must-remove?))]

      ;; and close it only if the connection is in connection-pool.
      ;; it might not be in pool because ALL_IDLE event might occurred when the connection is out of pool
      ;; ex) when waiting a long-running request.
      ;; But always close it if force-close? is true.
      (when (or force-close? must-remove?)
        (client/close-connection conn)))))


(defn- make-idle-event-handler
  [conn]
  (proxy [ChannelDuplexHandler] []
    (userEventTriggered
      [^ChannelHandlerContext ctx, ^Object evt]
      (when (and (instance? IdleStateEvent evt)
                 (= (.state ^IdleStateEvent evt) IdleState/ALL_IDLE))
        (log/debug "connection idle timeout. closed : " (pr-str (dissoc conn :pooled-connections)))
        (close-physical-connection conn false)))))

(defrecord PooledConnection
  [pooled-connections])

(extend-type PooledConnection
  IConnection
  (close
    ([{:keys [:pool/host :pool/port] :as conn} force?]
     (if force?
       (close-physical-connection conn true)
       (let [pool-key (make-pool-key host port)
             pooled-connections (:pooled-connections conn)]
         (locking pooled-connections
           (let [torn-conn (dissoc conn :pooled-connections)]
             (log/trace "returning a connection:" torn-conn)
             (swap! pooled-connections update pool-key #(if % (vec (cons torn-conn %)) [torn-conn]))))
         nil)))

    ([this]
     (client/close this false))))

(defn- connect*
  "Connect to a `port` of a `host` using `factory`, and return a IConnection object, but before making
   a new real connection, this fn checks a pool containing already connected connections and if the pool
   have a connection with same address and port, this fn don't make a new connection but return the found
   connection.
   If read-ch and write-ch are supplied, all data written and read are transfered to the supplied channels,
   If read-ch and write-ch aren't supplied, channels made by `(chan)` are used."
  [factory pooled-connections idle-timeout-sec ^String host port read-ch write-ch]
  (let [pool-key (make-pool-key host port)
        found    (locking pooled-connections
                   (let [conns (get @pooled-connections pool-key)
                         found (first conns)]
                     (swap! pooled-connections update pool-key (fn [_] (vec (rest conns))))
                     found))]

    (if found
      (do
        (log/trace (str "a pooled connection is found for: " pool-key ", found: " found))
        ;; returned connection don't have :pooled-connections key,
        ;; so we need to reassign it and create a new PooledConnection from the reassigned map.
        (map->PooledConnection (assoc found :pooled-connections pooled-connections)))
      (do
        (log/trace "no pooled connection is found. create a new one.")
        (let [{:keys [:client/channel] :as new-conn}
              (merge (->PooledConnection pooled-connections)
                    (client/connect factory host port read-ch write-ch)
                    {:pool/host host
                     :pool/port port})]

          ;; add an IdleStateHandler to a pipeline of this netty channel.
          (let [pipeline ^ChannelPipeline (.pipeline ^SocketChannel channel)]
            (.. pipeline
              (addFirst "idleEventHandler" ^ChannelHandler (make-idle-event-handler new-conn))
              (addFirst "idleStateHandler" ^ChannelHandler (make-idle-state-handler idle-timeout-sec))))

          ;; remove this new-conn from our connection-pool when this channel is closed.
          (.. ^SocketChannel channel
            (closeFuture)
            (addListener
              (reify ChannelFutureListener
                (operationComplete [this f]
                  (remove-from-pool pooled-connections new-conn)))))

          new-conn)))))

(defrecord PooledNettyConnectionFactory
  [factory pooled-connections idle-timeout-sec])

(extend-type PooledNettyConnectionFactory
  IConnectionFactory
  (create-connection
    [this host port read-ch write-ch]
    (connect* (:factory this) (:pooled-connections this) (:idle-timeout-sec this) host port read-ch write-ch)))

(defn create-default-pool
  []
  (atom {}))

(defn pooled-connection-factory
  ([factory pool idle-timeout-sec]
   (->PooledNettyConnectionFactory factory pool idle-timeout-sec))
  ([factory idle-timeout-sec]
   (pooled-connection-factory factory (create-default-pool) idle-timeout-sec))
  ([idle-timeout-sec]
   (pooled-connection-factory (client/connection-factory) idle-timeout-sec)))

(defn sample-connect
  [factory]
  (let [read-ch  (chan 1 bytebuf->string)
        write-ch (chan 1 string->bytebuf)
        conn     (client/connect factory "localhost" 8080 read-ch write-ch)]
    (go-loop []
      (println "result: " @(<! read-ch))
      (recur))
    conn))
