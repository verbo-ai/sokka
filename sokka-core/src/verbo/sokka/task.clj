(ns verbo.sokka.task)
;; ** Objectives:
;; *** Must have:
;; - [X] Must support all DurableQueue features (create, reserve, extend-lease, query, acknowledge/complete tasks)
;; - [X] Must be possible to group tasks and query by tasks for a given group.
;; - [X] Must be possible to snooze a task for a given amount of time.
;; - [ ] Must be possible to distinguish completed with error and completed successfully states.
;; - [ ] Tasks must be distributed.
;; *** Nice to have:
;; - [ ] Support querying by additional tags (eg., store-id). [The task-group can be extended to support this. For eg., the task-group-id can be a hierarchial value like: store-id:group-id. We can use BEGINS_WITH to then allow querying by just store-id or group-id. Need to ensure that this is a scalable solution.]
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                          ---==| T A S K S |==----                          ;;
;;                                                                            ;;
 ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def ^:const task-allowed-status-transitions
  {:starting #{:running  :failed :terminated}
   :running  #{:failed  :snoozed  :terminated :starting}
   :snoozed  #{:starting :failed :terminated}
   :failed   nil
   :terminated nil})

(def ^:const task-statuses
  (->> task-allowed-status-transitions
    keys
    (into #{})))

(defprotocol TaskService

  (create-task! [this task]
    "Creates a new task.")

  (task [this task-id]
    "Returns task for the specified task-id combination. Returns nil
     if task-id not found.")

  (tasks [this task-group-id]
    "Returns tasks for the specified task-group-id")

  (list-tasks [this topic {:keys [from to sub-topic] :as filters} {:keys [limit] :as cursor}]
    "List all tasks")

  (reserve-task! [this topic pid]
    "Attempts to reserve the next available task. Tasks that are not
     owned by any process and whose lease expired are available to be
     reserved. Throws exceptions in the following scenarios:
     :no-task-found - No task available to be picked up.")

  (extend-lease! [this task-id pid]
    "Attempts to extend lease of the specified task.
     :no-task-found - Task not found.
     :already-done  - Task terminated.
     :wrong-owner   - Attempted to extend lease of a task owned by
     another process.
     :lease-expired - Attempted to extend lease of a task whose lease
     already expired.")

  (terminate! [this task-id pid])

  (snooze! [this task-id pid snooze-time])

  (revoke-lease! [this task-id record-ver])

  (fail! [this task-id pid error]))


(defprotocol LeaseSupervision
  (list-leased-tasks [this topic cursor]
    "List all running / snoozed tasks"))
