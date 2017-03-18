(ns async-connect.spec.generator
  (:require [clojure.spec :as s]
            [clojure.spec.gen :as gen]))

(defn create-fn
  [f]
  (gen/fmap (fn [_] (f)) (s/gen int?)))

(defmacro create
  [& body]
  `(create-fn (fn [] ~@body)))
