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

(defn worker
  "Polls the task service on the given `topic` for a task and when
  available, obtains a lease for the task, and calls processor
  function `pfn` in a separate thread, passing the task as an
  argument. It also spins up a sidecar thread (keepalive) to extend
  the lease of the task periodically, while pfn is being executed.

  `processor-fn` - should accept task as an argument, perform thecleanup-leased-tasks
  operation corresponding to the task and return a tuple containing
  [event-name opts]. valid event-names and args are:
  [:sokka/completed nil], [:sokka/failed, {:keys [error-message]}]
  [:sokka/snoozed, {:keys [snooze-time]}].

  `keepalive-ms`  - keepalive timeout, will be lease-time * 0.7

  `timeout-ms` - Time in ms after which the task will be interrupted
  and marked as failed. This value can be overridden at the task
  level, by setting `timeout-ms` in the task itself. If the task times
  out, the status of the task is set to :failed.

  The worker polls for tasks using an exponentially increasing
  sleeper function to prevent the worker from flooding the queue with
  requests during inactivity."
  [{:keys [taskq topic pid pfn lease-time-ms keepalive-ms timeout-ms max-poll-interval-ms] :as opts}]
  (wrk/worker
    (-> opts
      (assoc :executor-fn (wrk/default-executor pfn))
      (dissoc :pfn))))

(defalias ok wrk/ok)

(defalias failed wrk/failed)

(defalias snoozed wrk/snoozed)

(defmulti taskq (fn [{:keys [type]}] type))

(defmethod taskq :dynamodb
  [cfg]
  (dyn/dyn-taskq cfg))
