(defproject async-connect "0.1.0-SNAPSHOT"
  :description "A tcp/ip server/client implementations for Clojure with core.async"
  :url "https://github.com/tyano/async-connect"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0-alpha15"]
                 [io.netty/netty-all "4.1.6.Final"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.clojure/core.async "0.3.442"]]

  :profiles {:dev
              {:dependencies [[ch.qos.logback/logback-classic "1.2.2"]
                              [org.clojure/tools.namespace "0.2.11"]
                              [org.clojure/test.check "0.9.0"]]
               :resource-paths ["resources-dev"]}
             :release
              {:resource-paths ["resources-release"]}}

  :main async-connect.core
  :aot [async-connect.core])
