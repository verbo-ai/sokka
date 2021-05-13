(ns verbo.sokka.core
  (:require [verbo.sokka.utils :refer [defalias]]
            [verbo.sokka.task :as task]))


(defn create-task!
  "Create and returns a new task."
  [taskq task]
  (task/create-task! taskq task))

(defn task
  "Get task by `task-id`. Returns nil when no task is found."
  [taskq task-id])

(defn tasks
  "Get tasks for a given `task-group-id`. Returns nil when no tasks are
  found for the supplied input."
  [this task-group-id])

(defn list-tasks
  "List all tasks"
  [this topic {:keys [from to sub-topic] :as filters} {:keys [limit] :as cursor}])


(defmethod new-taskq
  [])
