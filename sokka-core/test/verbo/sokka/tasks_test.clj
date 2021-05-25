(ns verbo.sokka.tasks-test
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clj-time.periodic :as tp]
            [clojure.tools.logging :as log]
            [midje.sweet :refer :all]
            [verbo.sokka.test-helpers :as h]
            [verbo.sokka.utils :as u]
            [verbo.sokka.impl.dynamodb-task :as dyn-task]
            [verbo.sokka.task :as task]))

(h/start-mulog-publisher {:type :console})

(defn new-task
  ([] (new-task (u/rand-id) (u/rand-id)))

  ([topic] (new-task topic (u/rand-id)))

  ([topic task-group-id]
   {:task-group-id task-group-id
    :topic         topic
    :data
    {:op   :do-something
     :with :something}}))

(def ^:const TASK_KEYS
  #{:task-id :task-group-id :status :topic})

(defn test-create-task
  [{:keys [lease-time]} taskq]
  (facts "create-task!"
    (let [task-def   (new-task)
          task       (task/create-task! taskq task-def)]
      (fact "injects a task-id"
        (:task-id task) => truthy)
      (fact "sets default status to :starting"
        (:status task) => :starting)
      (fact "only returns keys defined in t.h.data-stores/Task"
        (keys task) => (contains TASK_KEYS :in-any-order :gaps-ok)))))

(defn test-get-task-by-id
  [{:keys [lease-time]} taskq]
  (facts "get-task by task-id"
    (fact "returns nil when task not found"
      (task/task taskq (u/rand-id)) => nil)

    (let [{:keys [task-id]} (task/create-task! taskq (new-task))
          task (task/task taskq task-id)]
      (fact "returns only the keys defined in t.h.data-stores/Task"
        (keys task) => (contains TASK_KEYS :in-any-order :gaps-ok))
      (fact "returns status as a keyword"
        (:status task) => keyword?))))

(defn test-get-tasks-by-task-group
  [{:keys [lease-time]} taskq]
  (facts "get-tasks by taskq"
    (let [topic (u/rand-id)
          task-group-id (u/rand-id)
          n         5
          task-defs (repeatedly n #(new-task topic task-group-id))]
      (doseq [task-def task-defs]
        (task/create-task! taskq task-def))
      (fact "returns [] when task group id not found"
        (task/tasks taskq (u/rand-id)) => [])
      (let [tasks (task/tasks taskq task-group-id)]
        (fact "returns only the keys defined in t.h.data-stores/Task"
          (doseq [t tasks]
            (keys t) => (contains TASK_KEYS :in-any-order :gaps-ok)))
        (fact "returns the correct amount of tasks for the route-id"
          (count tasks) => n)
        (fact "returns status as a keyword"
          (:status (first tasks)) => keyword?)))))

(defn test-reserve-task!
  [{:keys [lease-time]} taskq]
  (let [pid (u/rand-id)
        topic (u/rand-id)]
    (facts "reserve-task!"
      (fact "returns nil when no tasks available to reserve"
        (task/reserve-task! taskq topic pid) => nil)

      (let [task-def (new-task topic)
            task (task/create-task! taskq task-def)
            ctime    (u/now)
            reserved (with-redefs [u/now (constantly ctime)]
                       (task/reserve-task! taskq topic pid))]
        (fact "sets status of reserved task to running"
          (:status reserved) => :running)
        (fact "sets pid of reserved task to pid supplied"
          (:pid reserved) => pid)
        (fact "sets lease to current-time + lease-time"
          (:lease reserved) => (+ ctime lease-time)))

      (let [task-def (new-task topic)
            {:keys [task-id]} (task/create-task! taskq task-def)]
        (fact "does not reserve tasks that are terminated"
          (task/reserve-task! taskq topic pid)
          (task/terminate! taskq task-id pid)
          (task/reserve-task! taskq topic pid) => nil))

      (let [task-def (new-task topic)
            {:keys [task-id]} (task/create-task! taskq task-def)]
        (fact "does not reserve tasks that are terminated"
          ;;reserve and update status
          (task/reserve-task! taskq topic pid)
          (task/terminate! taskq task-id pid)
          (let [ctime (u/now)]
            (with-redefs [u/now (constantly (+ ctime (* 1000 60)))]
              (task/reserve-task! taskq topic pid))) => nil)))

    (let [task-def (new-task topic)
          {:keys [task-id]} (task/create-task! taskq task-def)]
      (fact "does not reserve tasks that have failed"
        ;;reserve and update status
        (task/reserve-task! taskq topic pid)
        (task/fail! taskq task-id pid "fail!")
        (let [ctime (u/now)]
          (with-redefs [u/now (constantly (+ ctime (* 1000 60)))]
            (task/reserve-task! taskq topic pid))) => nil))

    (let [task-def (new-task topic)
          {:keys [task-id]} (task/create-task! taskq task-def)]
      (fact "does not reserve tasks that are snoozed"
        ;;reserve and update status
        (task/reserve-task! taskq topic pid)
        (task/snooze! taskq task-id pid (* 5 60 1000))
        (let [ctime (u/now)]
          (with-redefs [u/now (constantly (+ ctime (* 1000 60)))]
            (task/reserve-task! taskq topic pid))) => nil))))

(defn test-extend-lease!
  [{:keys [lease-time]} taskq]
  (let [topic (u/rand-id)
        task-group-id (u/rand-id)
        pid (u/rand-id)]
    (facts "extend-lease!"
      (fact "throws :task-not-found when task-id supplied does not exist"
        (task/extend-lease! taskq (u/rand-id) pid)
        => (throws (h/error= :forbidden :task-not-found)))
      (fact "throws :invalid-status when task status is not :running"
        (let [{:keys [task-id]} (task/create-task! taskq
                                  (new-task topic (u/rand-id)))]
          (task/extend-lease! taskq task-id pid) => (throws (h/error= :forbidden :invalid-status))
          ;;reserve and update status
          (task/reserve-task! taskq topic pid)
          (task/snooze! taskq task-id pid (* 5 60 1000))
          (task/extend-lease! taskq task-id pid) => (throws (h/error= :forbidden :invalid-status))
          (task/terminate! taskq task-id pid)
          (task/extend-lease! taskq task-id pid) => (throws (h/error= :forbidden :invalid-status))))
      (fact "throws :wrong-owner when invalid pid is supplied"
        (let [_ (task/create-task! taskq (new-task topic task-group-id))
              {:keys [task-id]} (task/reserve-task! taskq topic pid)]
          (task/extend-lease! taskq task-id (u/rand-id)) =>
          (throws #(= :wrong-owner (-> % ex-data :error)))))
      (fact "throws :lease-expired when lease of the task is already expired"
        (let [_          (task/create-task! taskq (new-task topic))
              {:keys [task-id]} (task/reserve-task! taskq topic pid)
              lease-time lease-time
              later (+ (u/now) (* 2 lease-time))]
          (with-redefs [u/now (constantly later)]
            (task/extend-lease! taskq task-id pid)) =>
          (throws (h/error= :forbidden :lease-expired))))
      (fact "extends lease to current-time + lease_time"
        (let [_          (task/create-task! taskq (new-task topic))
              {:keys [task-id lease]} (task/reserve-task! taskq topic pid)
              ctime (u/now)]
          (with-redefs [u/now (constantly ctime)]
            (task/extend-lease! taskq task-id pid)) => :ok
          (:lease (task/task taskq task-id)) => (+ ctime lease-time )))
      (fact "returns :ok when successful"
        (let [_          (task/create-task! taskq (new-task topic))
              {:keys [task-id]} (task/reserve-task! taskq topic pid)]
          (task/extend-lease! taskq task-id pid) => :ok)))))

(defn test-update-status!
  [{:keys [lease-time]} taskq]
  (let [topic (u/rand-id)
        {:keys [task-id]} (task/create-task! taskq (new-task topic))
        pid (u/rand-id)]
    (facts "update-status!"
      (fact "returns updated task with only the keys defined in t.h.data-stores/Task when successful"
        (task/reserve-task! taskq topic pid)
        (let [utask (task/snooze! taskq task-id pid (* 5 60 1000))]
          (:status utask) => :snoozed
          (keys utask) => (contains TASK_KEYS :in-any-order :gaps-ok))))))

(defn test-snoozing!
  [{:keys [lease-time]} taskq]
  (let [topic (u/rand-id)
        pid (u/rand-id)]
    (facts "snooze!"
      (let [{:keys [task-id]} (task/create-task! taskq (new-task topic))]
        (fact "snoozed tasks cannot be reserved until the snooze time is elapsed"
          (task/reserve-task! taskq topic pid)

          (task/snooze! taskq task-id pid 30000)
          (:status (task/task taskq task-id)) => :snoozed
          (task/reserve-task! taskq topic pid) => nil)

        (fact "snoozed task can be reserved after the lease is revoked"
          (let [task (task/task taskq task-id)]
            (task/revoke-lease! taskq task-id (:record-ver task))
            (:task-id (task/reserve-task! taskq topic (u/rand-id))) => task-id))))))


(defn test-list-tasks
  [{:keys [lease-time]} taskq]
  ;;TODO:
  )

(defn run-all-tests
  [config taskq]
  (test-create-task config taskq)
  (test-get-task-by-id config taskq)
  (test-get-tasks-by-task-group config taskq)
  (test-reserve-task! config taskq)
  (test-extend-lease! config taskq)
  (test-update-status! config taskq)
  (test-snoozing! config taskq)
  (test-list-tasks config taskq))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                         ----==| T E S T S |==----                          ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defonce test-config
  (delay
    {:cognitect-aws/client
     {:endpoint-override
      {:all {:port 7000
             :region "us-east-1"
             :hostname "localhost"
             :protocol :http}}}
     :lease-time (* 2 60 1000)
     :tasks-table (str "sokka-tasks-" (u/rand-id))}))

(defn ensure-test-table
  [config]
  (try
    (dyn-task/create-table config)
    (catch Exception e
      (when (some->> e
              ex-data
              :__type
              (re-matches #"com.amazonaws.dynamodb.*?ResourceInUseException"))
        (log/info "test table already exists")))))

(with-state-changes [(before :facts (ensure-test-table @test-config))]
  (let [dyn-taskq (dyn-task/dyn-taskq @test-config)]
    (facts "dynamodb task service"
      (run-all-tests @test-config dyn-taskq))))
