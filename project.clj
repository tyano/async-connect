(defproject async-connect "0.2.0-SNAPSHOT"
  :description "A tcp/ip server/client implementations for Clojure with core.async"
  :url "https://github.com/tyano/async-connect"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [io.netty/netty-all "4.1.21.Final"]
                 [org.clojure/tools.logging "0.4.0"]
                 [org.clojure/core.async "0.4.474"]]

  :profiles {:dev
              {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]
                              [org.clojure/tools.namespace "0.2.11"]
                              [org.clojure/test.check "0.9.0"]]
               :resource-paths ["resources-dev"]}
             :release
              {:resource-paths ["resources-release"]}}

  :main async-connect.core
  :aot [async-connect.core]
  :javac-options ["-source" "1.8" "-target" "1.8"])
