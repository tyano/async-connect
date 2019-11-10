(defproject async-connect "0.2.4-SNAPSHOT"
  :description "A tcp/ip server/client implementations for Clojure with core.async"
  :url "https://github.com/tyano/async-connect"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [io.netty/netty-all "4.1.43.Final" :upgrade false]
                 [org.clojure/tools.logging "0.5.0"]
                 [org.clojure/core.async "0.4.500"]
                 [databox "0.1.4"]]

  :profiles {:dev
             {:dependencies [[org.clojure/tools.namespace "0.3.1"]
                             [org.clojure/test.check "0.10.0"]]
              :resource-paths ["resources-dev"]}
             :logging
             {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]]
              :resource-paths ["resources-logging"]}
             :release
              {:resource-paths ["resources-release"]}}

  :main async-connect.core
  :aot [async-connect.core])
