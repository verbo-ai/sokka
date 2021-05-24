(defproject net.clojars.sathyavijayan/sokka (-> "version" slurp .trim)
  :description "Task management and async utilities for Clojure"
  :url "https://github.com/verbo-ai/sokka"

  :license {:name "Apache License 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}

  :scm {:name "git"
        :url "https://github.com/verbo-ai/sokka"
        :branch "initial"}

  :source-paths ["sokka-core/src" ]

  :global-vars {*warn-on-reflection* true})
