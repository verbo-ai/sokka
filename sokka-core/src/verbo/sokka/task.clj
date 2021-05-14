(ns ^{:author "Sathya Vittal (@sathyavijayan)"
      :doc "Protocols and constants to implement a task queue."}
    verbo.sokka.task)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                          ---==| T A S K S |==----                          ;;
;;                                                                            ;;
 ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def ^:const task-allowed-status-transitions
  "List of possible task statuses mapped to statuses they can transition to"
  {:starting #{:running  :failed :terminated}
   :running  #{:failed  :snoozed  :terminated :starting}
   :snoozed  #{:starting :failed :terminated}
   :failed   nil
   :terminated nil})

(def ^:const task-statuses
  "List of possible statuses for a task"
  (->> task-allowed-status-transitions
    keys
    (into #{})))

(defprotocol TaskStore
  "Protocol for a task store with functions to manage the life-cycle of
  a `task`."

  (create-task! [this task]
    "Create and returns a new task.")

  (task [this task-id]
    "Get task by `task-id`. Returns nil when no task is found.")

  (tasks [this task-group-id]
    "Get tasks for a given `task-group-id`. Returns nil when no tasks
    are found for the supplied input.")

  (list-tasks [this {:keys [from to sub-topic] :as filters} {:keys [limit] :as cursor}]
    "List all tasks")

  (reserve-task! [this topic pid]
    "Attempts to reserve the next available task for execution by
    obtaining a lease for configured amount of time (implementation
    specific). Returns nil when there is no task is available for
    reservation.")

  (extend-lease! [this task-id pid]
    "Attempts to extend lease of the specified task. Throws the
    following exceptions:
     :no-task-found - Task not found.
     :already-done  - Task terminated.
     :wrong-owner   - Attempted to extend lease of a task owned by
     another process.
     :lease-expired - Attempted to extend lease of a task whose lease
     already expired.")

  (terminate! [this task-id pid]
    "Updates the status to `:terminated`. This means the task has been
    executed successfully." )

  (snooze! [this task-id pid snooze-time]
    "Temporarily pause the task for `snooze-time`
    milliseconds. Updates the status to `:snoozed`.")

  (revoke-lease! [this task-id record-ver]
    "Forcefully revoke the lease held for the specified `task-id`. The
    `record-ver` from the last time the task was read must be
    supplied. This is used to implement MVCC to ensure that the task
    wasn't updated by another process concurrently.")

  (fail! [this task-id pid error]
    "Updates the status of the task to `:failed`. An optional
    error-message can be passed."))


(defprotocol LeaseSupervision
  "Protocol for backends (usually stores that dont support locks) that
  require special supervison of leased tasks to prevent time skew
  issues."
  (list-leased-tasks [this topic cursor]
    "List all running / snoozed tasks"))
