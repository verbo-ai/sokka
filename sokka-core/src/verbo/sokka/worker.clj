(ns verbo.sokka.worker
  (:require [verbo.sokka.task :as task]
            [verbo.sokka.ctrl :refer :all]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [go]]
            [safely.core :refer [sleeper safely]]
            [verbo.sokka.supervisor :as supervisor]
            [verbo.sokka.utils :as u]
            [verbo.sokka.ctrl :as ctrl]))

(def ^:const DEFAULT-TASK-KEEPALIVE-TIME (* 3 60 1000))

;; ;; worker - polls a topic for tasks, and executes them by spinning up
;; ;; a processor.

;; ;; processor - a thread that executes a given task, usually
;; ;; accompanied by a side car thread to keep the lease alive.

;; ;; keepalive - a side car thread that extends lease of a task in
;; ;; regular intervals.

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                     ----==| P R O C E S S O R |==----                      ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- raise!
  [taskq pid task-id error-message]
  (safely (task/fail! taskq task-id pid error-message)
    :on-error
    :max-retries 3
    :log-level :debug
    :tracking :disabled
    :log-stacktrace false
    :retry-delay [:random-exp-backoff :base 500 :+/- 0.50]))

(defn- complete!
  [taskq pid task-id]
  (safely (task/terminate! taskq task-id pid)
    :on-error
    :max-retries 3
    :log-level :debug
    :tracking :disabled
    :log-stacktrace false
    :retry-delay [:random-exp-backoff :base 500 :+/- 0.50]))

(defn- snooze!
  [taskq pid task-id snooze-time]
  (safely (task/snooze! taskq task-id pid snooze-time)
    :on-error
    :max-retries 3
    :log-level :debug
    :tracking :disabled
    :log-stacktrace false
    :retry-delay [:random-exp-backoff :base 500 :+/- 0.50]))

(defn- wrap-ex
  "ring style wrapper that accepts `processor-fn` as an argument and
  returns a function with the same signature as `processor-fn`,
  catches any exceptions thrown and returns a `:sokkka/failed` event."
  [pfn]
  (fn [task]
    (try
      (let [[event-name _ :as ret] (pfn task)]
        (if (#{:sokka/completed :sokka/snoozed :sokka/failed} event-name)
          ret
          (do
            (log/errorf "processing function returned invalid response: %s, %s"
              (:task-id task)
              ret)
            [:sokka/failed
             {:error-message "processing function returned invalid response"}])))
      (catch Throwable t
        (log/errorf t "exception processing task with id %s" (:task-id task))
        [:sokka/failed {:error-message (ex-message t)}]))))

(defn- ->processor-fn
  "`->processor-fn` - a ring style wrapper that accepts `processor-fn`
  as an argument and returns a function with the same signature as
  `processor-fn`, calls the `processor-fn` and attempts to update the
  status of the task based on the event-name in the return value."
  [taskq pid pfn]
  (fn [task]
    (let [[event-name {:keys [error-message snooze-time]}]
          (pfn task)]
      (condp = event-name
        :sokka/completed
        (complete! taskq pid (:task-id task))

        :sokka/snoozed
        (snooze! taskq pid (:task-id task) snooze-time)

        (raise! taskq pid (:task-id task)
          (or error-message "unknown error"))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                    ----==| K E E P   A L I V E |==----                     ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn keepalive!*
  "Spawns a thread that monitors the async pipeline, does regular
  keepalive pings every `keepalive-ms` by calling the `keepalive-fn`,
  signals termination by delivering value to a promise and does
  housekeeping when things go wrong. Returns a record that implements
  `TaskCtrl`."
  [{:keys [abort-chan close-chan] :as ctrl} keepalive-ms keepalive-fn]
  (let [p (promise)
        timeout-ms (or keepalive-ms DEFAULT-TASK-KEEPALIVE-TIME)]
    (async/thread
      (try
        (loop [keepalive-chan (async/timeout timeout-ms)]
          (let [[_ c] (async/alts!! [keepalive-chan abort-chan close-chan])]
            (condp = c
              close-chan   (deliver p :closed)
              abort-chan   (deliver p :aborted)
              keepalive-chan (do
                               (keepalive-fn)
                               (recur (async/timeout timeout-ms))))))

        (catch Throwable e
          (abort! ctrl)
          (deliver p :failed)
          (throw e))))

    p))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                      ----==| E X E C U T O R |==----                       ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn execute!*
  [{:keys [abort-chan close-chan] :as ctrl} pfn]
  (let [out-chan (async/chan 1)
        ftr  (future
               (try
                 (pfn)
                 (close! ctrl)
                 (catch Throwable t
                   (abort! ctrl)
                   (throw t))))]
    (async/go
      (try
        (let [_ (async/alts! [abort-chan close-chan])]
          (.cancel ftr true))
        (catch Throwable e
          (abort! ctrl)
          (throw e))))

    ftr))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                        ----==| W O R K E R |==----                         ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn reserve!
  "Reserve next message from the tasks service, with exponential backoff
  retries. This function will return nil if reservation was not
  successful after the set number of retries."
  [taskq topic pid]
  (safely (task/reserve-task! taskq topic pid)
    :on-error
    :tracking :disabled
    :log-errors false
    :log-level :debug
    :log-stacktrace false
    :default nil))

(defn keepalive-fn!
  "Extends lease of task with the given `id` and `pid` with exponential
  backoff retry."
  [taskq id pid]
  (safely
      (task/extend-lease! taskq id pid)
    :on-error
    ;;hardcoding the retry settings for sake of
    ;;simplicity. something to do later. Intentionally setting
    ;;max-retry to a finite number. After the retries are
    ;;exhausted, the exception will be thrown,triggering an
    ;;abort.
    :max-retries 3
    :retry-delay [:random-exp-backoff :base 300 :+/- 0.50]
    :log-level :warn
    :tracking :disabled
    ;;do not retry when the exception is one of [:lease-expired
    ;;:wrong-owner :already-acknowledged :no-messages-found].
    ;;this will throw the exception back triggering an abort.
    :retryable-error?
    (fn [e]
      (if-let [{:keys [error]} (ex-data e)]
        (not (some #(= error %)
               [:lease-expired
                :wrong-owner
                :invalid-status
                :task-not-found]))
        true))))

(defn execute!
  [{:keys [taskq topic pid pfn keepalive-ms timeout-ms]} ctrl task]
  (ctrl/monitor! ctrl)

  (keepalive!* ctrl keepalive-ms
    #(keepalive-fn! taskq (:task-id task) pid))

  (execute!*
    ctrl
    (partial (->> pfn
         wrap-ex
         (->processor-fn taskq pid))
      task)))


(defn- reserve-and-execute!
  [{:keys [taskq topic pid pfn keepalive-ms timeout-ms max-poll-interval-ms] :as opts}]
  (when-let [{:keys [task-id timeout-ms] :as task}
             (reserve! taskq topic pid)]
    (let [{:keys [timeout-ms] :as opts}
          (cond-> opts
            timeout-ms (assoc opts :timeout-ms timeout-ms))
          ctrl (new-control timeout-ms)]
      (log/infof "worker[%s]: reserved task %s:" topic
        (pr-str task-id))
      [ctrl (execute! opts ctrl task)])))

(defn- cleanup-leased-tasks!
  [{:keys [taskq monitored-tasks topic pid max-poll-interval-ms] :as opts} last-cleanup-time-ms]
  (if (satisfies? task/LeaseSupervision taskq)
    (let [now (u/now)]
      (if (> now (+ last-cleanup-time-ms max-poll-interval-ms))
        (do
          (supervisor/cleanup-leased-tasks! monitored-tasks taskq topic)
          now)
        last-cleanup-time-ms))
    (u/now)))

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

  ; keepalive-ms  - keepalive timeout, will be lease-time * 0.7
  ; task-timeout - how long do we expect the task to run. this can be
  overridden at the task level, but set at the worker level

  The worker will wait until `worker-timeout-ms` has passed
  for the task to complete, if the task isn't complete by then, it
  closes the task, acknowledges the task with status = :failed and
  carries on. If the task completes successfully, it acknowledges the
  task with status = :ok and carries on.

  The worker polls for tasks using an exponentially increasing
  sleeper function to prevent the worker from flooding the queue with
  requests during inactivity."
  ;;TODO: may be make all times a factor of lease.
  [{:keys [taskq topic pid pfn keepalive-ms timeout-ms max-poll-interval-ms] :as opts}]
  (let [opts (merge opts {:monitored-tasks (agent {})
                          :keepalive-ms
                          (-> taskq
                            :lease-time
                            (* 0.7)
                            int)
                          :max-poll-interval-ms 30000
                          :timeout-ms
                          (-> taskq
                            :lease-time
                            (* 2))})
        close-chan  (async/chan 1)
        p           (promise)
        proc        (future
                      (try
                        (loop [sleeper-fn nil
                               last-cleanup-time-ms 0]

                          (when sleeper-fn
                            (sleeper-fn))

                          (when-not (.closed? close-chan)
                            (let [[ctrl ftr :as reserved] (reserve-and-execute! opts)
                                  ;; this is the best opportunity to
                                  ;; run cleanup (if
                                  ;; max-poll-interval-ms has passed
                                  ;; since last cleanup of-course),
                                  ;; just before blocking on
                                  ;; completion of the task that may
                                  ;; have been reserved
                                  last-cleanup-time-ms'
                                  (cleanup-leased-tasks! opts last-cleanup-time-ms)]
                              (if reserved
                                (do
                                  (try
                                    (deref ftr (+ (:timeout-ms opts) 300)
                                      :timed-out)
                                    (catch Throwable t
                                      (log/warnf t "worker[%s]: error waiting for task to complete"
                                        topic)
                                      (abort! ctrl))
                                    (finally
                                      (cleanup! ctrl)))

                                  (when-not (.closed? close-chan)
                                    (recur nil last-cleanup-time-ms')))

                                (when-not (.closed? close-chan)
                                  (recur
                                    (or sleeper-fn
                                      (sleeper
                                        :random-exp-backoff
                                        :base 300
                                        :+/- 0.5
                                        :max (:max-poll-interval-ms opts)))
                                    last-cleanup-time-ms))))))

                        (log/infof "Exiting worker :%s " pid)

                        (catch Throwable t
                          (log/warnf t "Error in worker %s" pid)
                          (throw t))

                        (finally
                          (deliver p true))))]

    (fn []
      (async/close! close-chan)
      (.cancel proc true)
      p)))
