(ns verbo.sokka.supervisor
  (:require [verbo.sokka.utils :as u]
            [verbo.sokka.task :as task]
            [safely.core :refer [safely]]
            [clojure.tools.logging :as log]))

(defn mark-task!
  [m {:keys [lease-time] :as task-service} {:keys [task-id record-ver status snooze-time]}]
  (if (= record-ver (:record-ver (get m task-id)))
    m
    (assoc m task-id
      {:record-ver record-ver
       :status status
       :expiry (if (= status :snoozed)
                 (+ (u/now) snooze-time)
                 (+ (u/now) lease-time))})))

(defn sweep-task!
  [a {:keys [lease-time] :as task-service} task-id record-ver]
  (safely
      (task/revoke-lease! task-service task-id record-ver)
    :on-error
    :max-retries 3
    :log-level :debug
    :tracking :disabled
    :log-stacktrace false
    :default nil
    :retry-delay [:random-exp-backoff :base 500 :+/- 0.50])
  (dissoc a task-id))

(defn cleanup-leased-tasks!
  [monitored-tasks task-service topic]
  (try
    ;; mark everything
    (doseq [task (u/scroll (partial task/list-leased-tasks task-service topic)
                   {:limit 100})]
      (send monitored-tasks mark-task! task-service task))
    ;; sweep
    (doseq [[task-id {:keys [record-ver expiry]}] (taoensso.timbre/spy :info
                                                    @monitored-tasks)]
      (when (> (u/now) expiry)
        (send-off monitored-tasks sweep-task! task-service task-id record-ver)))

    (catch Throwable t
      (log/warnf t "worker[%s]: error cleaning up leased tasks! " topic))

    (finally
      (when (agent-error monitored-tasks)
        (restart-agent monitored-tasks {})))))
