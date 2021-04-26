(defproject ai.verbo/sokka-core (-> "../version" slurp .trim)
  :description "Task management and async utilities for Clojure"
  :url "https://github.com/verbo-ai/sokka"

  :license {:name "Apache License 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}

  :dependencies [[org.clojure/clojure "1.10.1"]
                 [prismatic/schema "1.1.12"]
                 [clj-time "0.15.2"]
                 [clojure.java-time "0.3.2"]
                 [com.brunobonacci/mulog "0.7.1"
                  :exclude [metosin/jsonista]]
                 [com.brunobonacci/safely  "0.7.0-alpha1"]
                 [org.clojure/core.async "1.3.610"
                  :exclusions [org.clojure/tools.reader]]
                 [com.taoensso/nippy "3.0.0"
                  :exclusions [com.taoensso/encore]]
                 [com.taoensso/timbre "5.1.0"]
                 [pandect "0.6.1"]

                 ;;logging madness
                 [org.clojure/tools.logging "1.1.0"]
                 [com.fzakaria/slf4j-timbre "0.3.20"]
                 [org.slf4j/log4j-over-slf4j "1.7.30"]
                 [org.slf4j/jul-to-slf4j "1.7.30"]
                 [org.slf4j/jcl-over-slf4j "1.7.30"]
                 [amazonica "0.3.152"
                  :exclusions [com.amazonaws/aws-java-sdk
                               com.taoensso/nippy]]]
  :profiles {:dev
             {:plugins [[lein-midje "3.1.1"]]
              :dynamodb-local {:port 7000 :in-memory? true}
              :resource-paths ["resources" "dev" "test"]
              :source-paths ["src" "dev" "test"]
              :dependencies [[midje "1.9.9"]]}

             :test
             {:plugins
              [[lein-midje "3.1.1"]
               [clj-dynamodb-local "0.1.2"]]
              :dynamodb-local {:port 7000 :in-memory? true}
              :dependencies [[midje "1.9.9"]]}})
