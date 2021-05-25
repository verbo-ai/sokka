(defn sokka-core-deps []
  (->> "./sokka-core/project.clj"
    slurp
    read-string
    (drop 3)
    (apply hash-map)
    :dependencies))

(defproject ai.verbo/sokka (-> "version" slurp .trim)
  :description "Task management and async utilities for Clojure"
  :url "https://github.com/verbo-ai/sokka"

  :license {:name "Apache License 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}

  :dependencies #=(sokka-core-deps)

  :scm {:name "git"
        :url "https://github.com/verbo-ai/sokka.git"}

  :source-paths ["sokka-core/src"])
