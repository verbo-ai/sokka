(ns ^:no-doc verbo.sokka.supervisor
  "Monitors running and snoozed tasks and revokes the lease of tasks
  whose lease has expired. Locking implementations in distributed
  systems are prone to issues caused by clock skew. A process with
  skewed time can cause the active and valid lease held by other
  processes to be revoked. This can cause thrashing of tasks and
  results in the same task being attempted to run multiple times
  wasting system resources and delaying successful task execution. This
  issue can be resolved by maintaining an in memory list of tasks with
  active lease and a projected expiry time based on the current time
  of the process/instance. If the task still has the same record
  version after the expiry, it is considered stale and its lease is
  forcefully revoked freeing it up to be reserved again. The projected
  expiry time calculated based on the current time (for running tasks,
  it is current time + lease time, and for snoozed tasks it is current
  time + snooze time). The `cleanup-leased-tasks!` function implements
  a mark-sweep pattern, where the state of leased tasks are updated in
  an agent during the 'mark' stage and the expired tasks are revoked
  in the 'sweep' state."
  (:require [verbo.sokka.utils :as u]
            [verbo.sokka.task :as task]
            [safely.core :refer [safely]]
            [clojure.tools.logging :as log]))

(defn ^:no-doc mark-task!
  "'marks' the last seen record version and expiry time for the given
  task. `expiry` is computed by adding `lease-time-ms` to the current
  time for running tasks and `snooze-time` for snoozed tasks. If there
  is not entry for the given task-id, a new entry is added to `m`. If
  an entry already exists, and if the record version in the entry is
  not the same as the task the entry is updated, otherwise, the entry
  is left as-is."
  [m taskq {:keys [lease-time-ms] :as opts} {:keys [task-id record-ver status snooze-time] :as task}]
  (if (= record-ver (:record-ver (get m task-id)))
    m
    (assoc m task-id
      {:record-ver record-ver
       :status status
       :expiry (if (= status :snoozed)
                 (+ (u/now) snooze-time)
                 (+ (u/now) lease-time-ms))})))

(defn ^:no-doc sweep-task!
  "Revoke lease of a given task, so it will be available for
  reservation. The `record-ver` of the task is passed to the update
  function to ensure that tasks which were updated concurrently
  else-where are not overwritten by mistake. Any exceptions thrown
  during the update except `throttling-exception` are ignored and the
  record for the task is removed from the map `m`."
  [a taskq task-id record-ver]
  (safely
    (safely
      (task/revoke-lease! taskq task-id record-ver)
      :on-error
      :max-retries 3
      :log-level :debug
      :tracking :enabled
      :log-stacktrace true
      ;; TODO: seems to retry for concurrent modification exception as well - fix this.
      :retryable-error? (fn [e] (some-> e ex-data :type (= :throttling-exception)))
      :retry-delay [:random-exp-backoff :base 500 :+/- 0.50])
    :on-error
    :tracking :enabled
    :log-level :debug
    :default nil)
  (dissoc a task-id))

(defn cleanup-leased-tasks!
  [monitored-tasks taskq topic {:keys [lease-time-ms] :as opts}]
  (try
    (doseq [task (u/scroll (partial task/list-leased-tasks taskq topic)
                   {:limit 100})]
      (send monitored-tasks mark-task! taskq opts task))
    ;; sweep
    (doseq [[task-id {:keys [record-ver status expiry]}] @monitored-tasks]
      (when (> (u/now) expiry)
        (send-off monitored-tasks sweep-task! taskq task-id record-ver)))
    (catch Throwable t
      (log/warnf t "worker[%s]: error cleaning up leased tasks! " topic))
    (finally
      (when (agent-error monitored-tasks)
        (restart-agent monitored-tasks {})))))
