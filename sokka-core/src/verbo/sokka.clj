(ns verbo.sokka
  "Background task management library for Clojure."
  (:require [verbo.sokka.utils :refer [defalias]]
            [verbo.sokka.worker :as wrk]
            [verbo.sokka.task :as task]
            [verbo.sokka.impl.dynamodb-task :as dyn]
            [clj-time.core :as t]
            [clj-time.coerce :as tc]))

(defn create-task!
  "Create and returns a new task."
  [taskq task]
  ;; TODO: validate the task before creation.
  (assert (:topic task) "Topic must be set.")
  (task/create-task! taskq task))

(defn task
  "Get task by `task-id`. Returns nil when no task is found."
  [taskq task-id]
  (task/task taskq task-id))

(defn tasks
  "Get tasks for a given `task-group-id`. Returns nil when no tasks are
  found for the supplied input."
  [taskq task-group-id]
  (task/tasks taskq task-group-id))

(defn list-tasks
  "List all tasks for a given `topic`."
  [taskq {:keys [from to topic sub-topic] :as filters} {:keys [limit] :as cursor}]
  (task/list-tasks taskq filters cursor))

(defalias worker wrk/worker)

(defalias ok wrk/ok)

(defalias failed wrk/failed)

(defalias snoozed wrk/snoozed)

(defmulti taskq (fn [{:keys [type]}] type))

(defmethod taskq :dynamodb
  [cfg]
  (dyn/dyn-taskq cfg))
