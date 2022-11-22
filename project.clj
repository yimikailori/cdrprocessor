(defproject medcdrprocessor "0.1.0-SNAPSHOT"
  :description "Airtel Nigeria: CDR Mediated Processor."
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/data.zip "0.1.2"]
                 [org.clojure/math.numeric-tower "0.0.4"]
                 ;; folder watcher.
                 ;[juxt/dirwatch "0.2.5"]
                 [clojure-watch "0.1.9"]
                 [org.clojure/core.async "1.1.587"]
                 [org.javassist/javassist "3.27.0-GA"]
                 ;; Data.
                 [org.clojure/data.csv "1.0.0"]
                 ;; Date/time.
                 ;[joda-time/joda-time "2.9.9"]
                 [clj-time "0.15.1"]
                 [org.apache.commons/commons-lang3 "3.5"]
                 [ch.qos.logback/logback-classic "1.3.0-alpha5"]
                 [org.clojure/tools.logging "1.0.0"]
                 [log4j/log4j "1.2.17" :exclusions [javax.mail/mail
                                                    javax.jms/jms
                                                    com.sun.jdmk/jmxtools
                                                    com.sun.jmx/jmxri]]

                 ;;database
                 [org.postgresql/postgresql "42.2.12.jre7"]
                 [org.clojure/java.jdbc "0.7.11"]
                 [hikari-cp "2.11.0"]]
  :main ^:skip-aot medcdrprocessor.core
  :target-path "target/%s"
  :omit-source true
  ;:jvm-opts ["-XX:-OmitStackTraceInFastThrow"]
  ;;clojure.core.async.pool-size -- to set number of threads in go async (default is 8)
  :jvm-opts  ["-Dmedconfig=/Users/yimika/Documents/IdeaProjects/medcdrprocessor/medcdr.conf",
  "-Dlogback.configurationFile=/Users/yimika/Documents/IdeaProjects/medcdrprocessor/logback.xml"]
  :profiles {:uberjar {:aot :all
                       :uberjar-name "medcdrprocessor.jar"
                       :source-paths ["src"]}})
