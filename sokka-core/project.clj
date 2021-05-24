(defproject net.clojars.sathyavijayan/sokka (-> "../version" slurp .trim)
  :description "Task management and async utilities for Clojure"
  :url "https://github.com/verbo-ai/sokka"

  :license {:name "Apache License 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}

  :scm {:name "git" :url "https://github.com/verbo-ai/sokka/tree/initial"}

  :dependencies [[org.clojure/clojure "1.10.1"]
                 [prismatic/schema "1.1.12"]
                 [clj-time "0.15.2"]
                 [clojure.java-time "0.3.2"]
                 [com.brunobonacci/mulog "0.7.1"]
                 [com.brunobonacci/safely  "0.7.0-alpha1"]
                 [org.clojure/core.async "1.3.610"
                  :exclusions [org.clojure/tools.reader]]
                 [com.taoensso/nippy "3.0.0"]
                 [pandect "0.6.1"]

                 [com.cognitect.aws/api "0.8.505"]
                 [com.cognitect.aws/endpoints "1.1.11.1001"]
                 [com.cognitect.aws/dynamodb "810.2.801.0"]]

  :global-vars {*warn-on-reflection* true}

  ;; generating AOT jar alongside Clojure JAR
  :classifiers {:aot :aot-jar}

  :profiles {:aot-jar {:aot :all}
             :dev
             {:plugins [[lein-midje "3.1.1"]]
              :dynamodb-local {:port 7000 :in-memory? true}
              :resource-paths ["resources" "dev" "test"]
              :source-paths ["src" "dev" "test"]
              :dependencies [[midje "1.9.9"]
                             [com.taoensso/timbre "5.1.0"]
                             [org.clojure/tools.logging "1.1.0"]
                             [com.fzakaria/slf4j-timbre "0.3.20"]
                             [org.slf4j/log4j-over-slf4j "1.7.30"]
                             [org.slf4j/jul-to-slf4j "1.7.30"]
                             [org.slf4j/jcl-over-slf4j "1.7.30"]]}

             :test
             {:plugins
              [[lein-midje "3.1.1"]
               [clj-dynamodb-local "0.1.2"]]
              :dynamodb-local {:port 7000 :in-memory? true}
              :dependencies [[midje "1.9.9"]]}})
