(ns verbo.sokka.supervisor-test
  (:require [verbo.sokka.supervisor :as sut]
            [midje.sweet :refer :all]
            [verbo.sokka.impl.dynamodb-task :as dyn-task]
            [verbo.sokka.utils :as u]
            [clojure.tools.logging :as log]
            [verbo.sokka.task :as task]
            [amazonica.aws.dynamodbv2 :as dyn])
  (:import com.amazonaws.services.dynamodbv2.model.ResourceInUseException))


(defn new-task
  ([] (new-task (u/rand-id) (u/rand-id)))

  ([topic] (new-task topic (u/rand-id)))

  ([topic task-group-id]
   {:task-group-id task-group-id
    :topic         topic
    :data
    {:op   :do-something
     :with :something}}))

(defn test-cleanup-leased-tasks!
  [taskq]
  (fact "cleanup-leased-tasks! changes the status of a task whose lease has expired to :starting"
    (let [monitored-tasks (agent {})
          pid (u/rand-id)
          topic (u/rand-id)
          task (task/create-task! taskq (new-task topic))]
      (:task-id (task/reserve-task! taskq topic pid)) => (:task-id task)
      ;; mark
      (sut/cleanup-leased-tasks! monitored-tasks taskq topic)
      (deref (promise) 500 :timeout)

      (let [ctime (u/now)]
        (with-redefs [u/now (constantly (+ ctime (:lease-time taskq) 1))]
          ;; sweep
          (sut/cleanup-leased-tasks! monitored-tasks taskq topic)
          (deref (promise) 500 :timeout)
          (task/task taskq (:task-id task)) => (every-checker
                                                 (contains {:status :starting})
                                                 #(nil? (:pid %)))))))

  (fact "cleanup-leased-tasks! does not change status of a task that has been updated between mark and sweep"
    (let [monitored-tasks (agent {})
          pid (u/rand-id)
          topic (u/rand-id)
          task (task/create-task! taskq (new-task topic))]
      (:task-id (task/reserve-task! taskq topic pid)) => (:task-id task)
      ;; mark
      (sut/cleanup-leased-tasks! monitored-tasks taskq topic)
      (deref (promise) 500 :timeout)

      (let [ctime (u/now)]
        (with-redefs [u/now (constantly (+ ctime (:lease-time taskq) 1))]
          ;; sweep
          (sut/cleanup-leased-tasks! monitored-tasks taskq topic)
          (deref (promise) 500 :timeout)
          (task/task taskq (:task-id task)) => (every-checker
                                                 (contains {:status :starting})
                                                 #(nil? (:pid %)))))))

  (fact "cleanup-leased-tasks! changes the status of a snoozing task that is ready to wake to :starting"
    (let [monitored-tasks (agent {})
          pid (u/rand-id)
          topic (u/rand-id)
          task (task/create-task! taskq (new-task topic))]
      (:task-id (task/reserve-task! taskq topic pid)) => (:task-id task)
      (task/snooze! taskq (:task-id task) pid (* 5 60 1000))
      ;; mark
      (sut/cleanup-leased-tasks! monitored-tasks taskq topic)
      (deref (promise) 500 :timeout)

      (let [ctime (u/now)]
        (with-redefs [u/now (constantly (+ ctime (* 5 60 1000) 1))]
          ;; sweep
          (sut/cleanup-leased-tasks! monitored-tasks taskq topic)
          (deref (promise) 500 :timeout)
          (:status (task/task taskq (:task-id task))) => :starting)))))

(defn run-all-tests
  [taskq]
  (test-cleanup-leased-tasks! taskq))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                         ----==| T E S T S |==----                          ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defonce test-config
  (delay
    {:creds {:endpoint "http://localhost:7000"}
     :tasks-table (str "sc-tasks-v2-" (u/rand-id))}))

(defn ensure-test-table
  [config]
  (try
    (dyn-task/create-table config)
    (catch ResourceInUseException e
      (log/info "test table already exists"))))

(with-state-changes [(before :facts (ensure-test-table @test-config))]
  (let [dyn-taskq (dyn-task/dyn-taskq
                    @test-config)]
    (facts "dynamodb task service"
      (run-all-tests dyn-taskq))))
