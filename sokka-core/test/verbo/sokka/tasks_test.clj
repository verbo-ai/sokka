(ns verbo.sokka.tasks-test
  (:require [amazonica.aws.dynamodbv2 :as dyn]
            [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clj-time.periodic :as tp]
            [clojure.tools.logging :as log]
            [midje.sweet :refer :all]
            [verbo.sokka.test-helpers :as h]
            [verbo.sokka.utils :as u]
            [verbo.sokka.impl.dynamodb-task :as dyn-task]
            [verbo.sokka.task :as task])
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

(def ^:const TASK_KEYS
  #{:task-id :task-group-id :status :topic})

(defn test-create-task
  [task-service]
  (facts "create-task!"
    (let [task-def   (new-task)
          task       (task/create-task! task-service task-def)]
      (fact "injects a task-id"
        (:task-id task) => truthy)
      (fact "sets default status to :starting"
        (:status task) => :starting)
      (fact "only returns keys defined in t.h.data-stores/Task"
        (keys task) => (contains TASK_KEYS :in-any-order :gaps-ok)))
    (fact "injects parent tasks when parent shards are supplied"
      (let [td (-> (new-task)
                 (assoc :parent-tasks [(u/rand-id) (u/rand-id)]))
            {:keys [parent-tasks]} (task/create-task! task-service td)]
        (count parent-tasks) => 2))))

(defn test-get-task-by-id
  [task-service]
  (facts "get-task by task-id"
    (fact "returns nil when task not found"
      (task/task task-service (u/rand-id)) => nil)

    (let [{:keys [task-id]} (task/create-task! task-service (new-task))
          task (task/task task-service task-id)]
      (fact "returns only the keys defined in t.h.data-stores/Task"
        (keys task) => (contains TASK_KEYS :in-any-order :gaps-ok))
      (fact "returns status as a keyword"
        (:status task) => keyword?))))

(defn test-get-tasks-by-task-group
  [task-service]
  (facts "get-tasks by task-service"
    (let [topic (u/rand-id)
          task-group-id (u/rand-id)
          n         5
          task-defs (repeatedly n #(new-task topic task-group-id))]
      (doseq [task-def task-defs]
        (task/create-task! task-service task-def))
      (fact "returns [] when task group id not found"
        (task/tasks task-service (u/rand-id)) => [])
      (let [tasks (task/tasks task-service task-group-id)]
        (fact "returns only the keys defined in t.h.data-stores/Task"
          (doseq [t tasks]
            (keys t) => (contains TASK_KEYS :in-any-order :gaps-ok)))
        (fact "returns the correct amount of tasks for the route-id"
          (count tasks) => n)
        (fact "returns status as a keyword"
          (:status (first tasks)) => keyword?)))))

(defn test-reserve-task!
  [task-service]
  (let [pid (u/rand-id)
        topic (u/rand-id)]
    (facts "reserve-task!"
      (fact "returns nil when no tasks available to reserve"
        (task/reserve-task! task-service topic pid) => nil)

      (let [task-def (new-task topic)
            task (task/create-task! task-service task-def)
            ctime    (u/now)
            reserved (with-redefs [u/now (constantly ctime)]
                       (task/reserve-task! task-service topic pid))]
        (fact "sets status of reserved task to running"
          (:status reserved) => :running)
        (fact "sets pid of reserved task to pid supplied"
          (:pid reserved) => pid)
        (fact "sets lease to current-time + lease-time"
          (:lease reserved) => (+ ctime (:lease-time task-service))))

      (let [task-def (new-task topic)
            {:keys [task-id]} (task/create-task! task-service task-def)]
        (fact "does not reserve tasks that are terminated"
          (task/reserve-task! task-service topic pid)
          (task/terminate! task-service task-id pid)
          (task/reserve-task! task-service topic pid) => nil))

      (let [task-def (new-task topic)
            {:keys [task-id]} (task/create-task! task-service task-def)]
        (fact "does not reserve tasks that are terminated"
          ;;reserve and update status
          (task/reserve-task! task-service topic pid)
          (task/terminate! task-service task-id pid)
          (let [ctime (u/now)]
            (with-redefs [u/now (constantly (+ ctime (* 1000 60)))]
              (task/reserve-task! task-service topic pid))) => nil)))

    (let [task-def (new-task topic)
          {:keys [task-id]} (task/create-task! task-service task-def)]
      (fact "does not reserve tasks that have failed"
        ;;reserve and update status
        (task/reserve-task! task-service topic pid)
        (task/fail! task-service task-id pid "fail!")
        (let [ctime (u/now)]
          (with-redefs [u/now (constantly (+ ctime (* 1000 60)))]
            (task/reserve-task! task-service topic pid))) => nil))

    (let [task-def (new-task topic)
          {:keys [task-id]} (task/create-task! task-service task-def)]
      (fact "does not reserve tasks that are snoozed"
        ;;reserve and update status
        (task/reserve-task! task-service topic pid)
        (task/snooze! task-service task-id pid (* 5 60 1000))
        (let [ctime (u/now)]
          (with-redefs [u/now (constantly (+ ctime (* 1000 60)))]
            (task/reserve-task! task-service topic pid))) => nil))))

(defn test-extend-lease!
  [task-service]
  (let [topic (u/rand-id)
        task-group-id (u/rand-id)
        pid (u/rand-id)]
    (facts "extend-lease!"
      (fact "throws :task-not-found when task-id supplied does not exist"
        (task/extend-lease! task-service (u/rand-id) pid)
        => (throws (h/error= :forbidden :task-not-found)))
      (fact "throws :invalid-status when task status is not :running"
        (let [{:keys [task-id]} (task/create-task! task-service
                                  (new-task topic (u/rand-id)))]
          (task/extend-lease! task-service task-id pid) => (throws (h/error= :forbidden :invalid-status))
          ;;reserve and update status
          (task/reserve-task! task-service topic pid)
          (task/snooze! task-service task-id pid (* 5 60 1000))
          (task/extend-lease! task-service task-id pid) => (throws (h/error= :forbidden :invalid-status))
          (task/terminate! task-service task-id pid)
          (task/extend-lease! task-service task-id pid) => (throws (h/error= :forbidden :invalid-status))))
      (fact "throws :wrong-owner when invalid pid is supplied"
        (let [_ (task/create-task! task-service (new-task topic task-group-id))
              {:keys [task-id]} (task/reserve-task! task-service topic pid)]
          (task/extend-lease! task-service task-id (u/rand-id)) =>
          (throws #(= :wrong-owner (-> % ex-data :error)))))
      (fact "throws :lease-expired when lease of the task is already expired"
        (let [_          (task/create-task! task-service (new-task topic))
              {:keys [task-id]} (task/reserve-task! task-service topic pid)
              lease-time (:lease-time task-service)
              later (+ (u/now) (* 2 lease-time))]
          (with-redefs [u/now (constantly later)]
            (task/extend-lease! task-service task-id pid)) =>
          (throws (h/error= :forbidden :lease-expired))))
      (fact "extends lease to current-time + lease_time"
        (let [_          (task/create-task! task-service (new-task topic))
              {:keys [task-id lease]} (task/reserve-task! task-service topic pid)
              ctime (u/now)]
          (with-redefs [u/now (constantly ctime)]
            (task/extend-lease! task-service task-id pid)) => :ok
          (:lease (task/task task-service task-id)) => (+ ctime (:lease-time task-service))))
      (fact "returns :ok when successful"
        (let [_          (task/create-task! task-service (new-task topic))
              {:keys [task-id]} (task/reserve-task! task-service topic pid)]
          (task/extend-lease! task-service task-id pid) => :ok)))))

(defn test-update-status!
  [task-service]
  (let [topic (u/rand-id)
        {:keys [task-id]} (task/create-task! task-service (new-task topic))
        pid (u/rand-id)]
    (facts "update-status!"
      (fact "returns updated task with only the keys defined in t.h.data-stores/Task when successful"
        (task/reserve-task! task-service topic pid)
        (let [utask (task/snooze! task-service task-id pid (* 5 60 1000))]
          (:status utask) => :snoozed
          (keys utask) => (contains TASK_KEYS :in-any-order :gaps-ok))))))

;; TODO:: fix these tests....
(defn test-snoozing!
  [task-service]
  (let [topic (u/rand-id)
        pid (u/rand-id)]
    (facts "snooze!"
      (let [{:keys [task-id]} (task/create-task! task-service (new-task topic))]
        (fact "snoozed tasks cannot be reserved until the snooze time is elapsed"
          (task/reserve-task! task-service topic pid)

          (task/snooze! task-service task-id pid 30000)
          (:status (task/task task-service task-id)) => :snoozed
          (task/reserve-task! task-service topic pid) => nil)

        #_(fact "snoozed task can be reserved after the snooze time is elapsed"
            (let [ctime (+ (u/now) 30001)]
              (with-redefs [u/now (constantly ctime)]
                (:task-id (task/reserve-task! task-service topic pid)) => task-id)))

        #_(fact "snoozed task can be reserved by other pids after soonze time is elapsed"
            (task/snooze! task-service task-id pid 30000)
            (let [ctime (+ (u/now) 30001)]
              (with-redefs [u/now (constantly ctime)]
                (:task-id (task/reserve-task! task-service topic (u/rand-id))) => task-id)))))))


(defn test-list-tasks
  [task-service]
  ;;TODO:
  )

(defn run-all-tests
  [task-service]
  (test-create-task task-service)
  (test-get-task-by-id task-service)
  (test-get-tasks-by-task-group task-service)
  (test-reserve-task! task-service)
  (test-extend-lease! task-service)
  (test-update-status! task-service)
  (test-snoozing! task-service)
  (test-list-tasks task-service))

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
  (let [dyn-task-service (dyn-task/dyn-task-service
                           @test-config)]
    (facts "dynamodb task service"
      (run-all-tests dyn-task-service))))



(comment

  (def config
    {:creds {:endpoint "http://localhost:7000"}
     :tasks-table (str "sc-tasks-v2-test1" )})

  (dyn/delete-table (:creds config) :table-name (:tasks-table config))

  (dyn-task/create-table config)

  (def tasks (repeat 50 (new-task "topic-a")))

  (def task-service (dyn-task/dyn-task-service config))

  (doseq [task tasks]
    (task/create-task! task-service
      (assoc task
        :sub-topic (rand-nth ["acme" "rand-enterprise" "kalimark"]))))

  (doseq [_ (range 0 5)]
    (taoensso.timbre/spy :info
      (task/reserve-task! task-service "topic-a" "001")))


  (u/scroll
    #(#'dyn-task/list-tasks-by-topic-scan-hkey
       task-service
       {:from 0
        :to   9999999999999
        :sub-topic "kalimark"
        :status :starting}
       (#'dyn-task/->topic-scan-hkey "topic-a")
       %)
    {:limit 1})



  (let [topic "something"
        from (-> 3 t/weeks t/ago)
        to   (t/now)
        weeks     (map #(#'dyn-task/->topic-scan-hkey* % topic)
                    (tp/periodic-seq from to (t/weeks 1)))]

    weeks)

  (count (u/scroll
           #(dyn-task/list-tasks
              task-service
              {:from (-> 1 t/weeks t/ago tc/to-long)
               :to   (-> 1 t/weeks t/from-now tc/to-long)
               :sub-topic "acme"
               :topic "topic-a"
               :status :starting}
              %)
           {:limit 100}))


  (#'dyn-task/list-tasks-by-topic-scan-hkey
    task-service
    {:from (-> 1 t/weeks t/ago tc/to-long)
     :to   (-> (t/now) tc/to-long)
     :sub-topic "kalimark"
     :topic "topic-a"
     :status :starting}
    "topic-a:2021:16"
    {})

  ;;
  )
