(ns verbo.sokka.supervisor
  (:require [verbo.sokka.utils :as u]
            [verbo.sokka.task :as task]
            [safely.core :refer [safely]]
            [clojure.tools.logging :as log]))

(defn mark-task!
  [m taskq {:keys [lease-time-ms] :as opts} {:keys [task-id record-ver status snooze-time] :as task}]
  (if (= record-ver (:record-ver (get m task-id)))
    m
    (assoc m task-id
      {:record-ver record-ver
       :status status
       :expiry (if (= status :snoozed)
                 (+ (u/now) snooze-time)
                 (+ (u/now) lease-time-ms))})))

(defn sweep-task!
  [a taskq task-id record-ver]
  (safely
      (safely
          (task/revoke-lease! taskq task-id record-ver)
        :on-error
        :max-retries 3
        :log-level :debug
        :tracking :disabled
        :log-stacktrace true
        :retryable-error? (fn [e] (some-> e ex-data :type (= :throttling-exception)))
        :retry-delay [:random-exp-backoff :base 500 :+/- 0.50])
    :on-error
    :tracking :disabled
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
