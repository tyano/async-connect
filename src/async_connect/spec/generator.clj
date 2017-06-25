(ns async-connect.spec.generator
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]))

(defn create-fn
  [f]
  (gen/fmap (fn [_] (f)) (s/gen int?)))

(defmacro create
  [& body]
  `(create-fn (fn [] ~@body)))
