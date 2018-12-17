(defproject async-connect "0.2.2-SNAPSHOT"
  :description "A tcp/ip server/client implementations for Clojure with core.async"
  :url "https://github.com/tyano/async-connect"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [io.netty/netty-all "4.1.22.Final"]
                 [org.clojure/tools.logging "0.4.0"]
                 [org.clojure/core.async "0.4.474"]
                 [box "0.1.0"]]

  :profiles {:dev
             {:dependencies [[org.clojure/tools.namespace "0.2.11"]
                             [org.clojure/test.check "0.9.0"]]
              :resource-paths ["resources-dev"]}
             :logging
             {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]]
              :resource-paths ["resources-logging"]}
             :release
              {:resource-paths ["resources-release"]}}

  :main async-connect.core
  :aot [async-connect.core]
  :javac-options ["-source" "1.8" "-target" "1.8"])
