(ns async-connect.pool
  (:require [async-connect.client :refer [IConnection IConnectionFactory] :as client]
            [clojure.spec :as s]
            [clojure.tools.logging :as log]
            [clojure.core.async :refer [thread chan <! >! go-loop]]
            [async-connect.netty :refer [bytebuf->string
                                         string->bytebuf]])
  (:import [io.netty.channel.socket
              SocketChannel]
           [io.netty.channel
              ChannelFutureListener]
           [io.netty.bootstrap
              Bootstrap]))

(s/def :pool/host string?)
(s/def :pool/port pos-int?)
(s/def :pool/key
  (s/keys :req [:pool/host :pool/port]))

(defn remove-from-pool
  [pooled-connections {:keys [:pool/host :pool/port] :as conn}]
  (let [pool-key {:pool/host host, :pool/port port}]
    (locking pooled-connections
      (log/trace "removing a connection:" conn)
      (vswap! pooled-connections update pool-key #(when % (vec (filter (fn [c] (not= c conn)) %)))))
    nil))

(defrecord PooledConnection
  [pooled-connections])

(extend-type PooledConnection
  IConnection
  (close
    [{:keys [:pool/host :pool/port] :as conn}]
    (let [pool-key {:pool/host host, :pool/port port}
          pooled-connections (:pooled-connections conn)]
      (locking pooled-connections
        (let [torn-conn (dissoc conn :pooled-connections)]
          (log/trace "returning a connection:" torn-conn)
          (vswap! pooled-connections update pool-key #(if % [torn-conn] (conj % torn-conn)))))
      nil)))

(defn- connect*
  "Connect to a `port` of a `host` using `factory`, and return a IConnection object, but before making
   a new real connection, this fn checks a pool containing already connected connections and if the pool
   have a connection with same address and port, this fn don't make a new connection but return the found
   connection.
   If read-ch and write-ch are supplied, all data written and read are transfered to the supplied channels,
   If read-ch and write-ch aren't supplied, channels made by `(chan)` are used."
  [factory pooled-connections ^String host port read-ch write-ch]
  (let [pool-key {:pool/host host, :pool/port port}]
    (locking pooled-connections
      (let [conns (get @pooled-connections pool-key)
            found (first conns)]
        (vswap! pooled-connections update pool-key (fn [_] (vec (rest conns))))
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

              ;; remove this new-conn from our connection-pool when this channel is closed.
              (thread
                (.. ^SocketChannel channel
                  (closeFuture)
                  (addListener
                    (reify ChannelFutureListener
                      (operationComplete [this f]
                        (remove-from-pool pooled-connections new-conn))))
                  (sync)))

              new-conn)))))))

(defrecord PooledNettyConnectionFactory
  [factory pooled-connections])

(extend-type PooledNettyConnectionFactory
  IConnectionFactory
  (create-connection
    [this host port read-ch write-ch]
    (connect* (:factory this) (:pooled-connections this) host port read-ch write-ch)))

(defn create-default-pool
  []
  (volatile! {}))

(defn pooled-connection-factory
  ([factory pool]
    (->PooledNettyConnectionFactory factory pool))
  ([factory]
    (pooled-connection-factory factory (create-default-pool)))
  ([]
    (pooled-connection-factory (client/connection-factory))))

(defn sample-connect
  [factory]
  (let [read-ch  (chan 1 bytebuf->string)
        write-ch (chan 1 string->bytebuf)
        conn     (client/connect factory "localhost" 8080 read-ch write-ch)]
    (go-loop []
      (println "result: " @(<! read-ch))
      (recur))
    conn))
