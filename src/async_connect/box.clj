(ns async-connect.box
  (:refer-clojure :exclude [update]))

(defprotocol IWrapper
  (update [this f])
  (reset [this v])
  (error? [this]))

(declare boxed)

(deftype Box
  [value]

  clojure.lang.IDeref
  (deref
    [this]
    (if (instance? Throwable value)
      (throw value)
      value))

  IWrapper
  (update
    [this f]
    (if (instance? Throwable value)
      this
      (boxed (f value))))

  (reset
    [this v]
    (if (instance? Throwable value)
      this
      (boxed v)))

  (error?
    [this]
    (instance? Throwable value)))

(defn boxed
  [value]
  (Box. value))
