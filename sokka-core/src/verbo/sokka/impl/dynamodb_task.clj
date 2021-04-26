(ns verbo.sokka.impl.dynamodb-task
  (:require [amazonica.aws.dynamodbv2 :as dyn]
            [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clj-time.periodic :as tp]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [pandect.algo.sha256 :refer [sha256]]
            [verbo.sokka.task :refer :all]
            [verbo.sokka.utils :as u :refer [now]])
  (:import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                 ---==| D E F A U L T   C O N F I G |==----                 ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:const DEFAULT_LEASE_TIME (* 5 60 1000))


(def ^:const DEFAULT-CONFIG
  {;; the aws region's endpoint to contact
   :creds {:endpoint    :eu-west-1}

   ;; the name of the table
   :tasks-table         "sc-tasks-v2"

   ;; the initial read and write capacity
   ;; for the table and indices
   :read-capacity-units      10
   :write-capacity-units     10

   ;; The name of the index used for the reservation
   ;; queries.
   :reservation-index        "reservation-index"

   ;; The name of the index used for the task-group queries
   :task-scan-index         "task-scan-index"

   ;; The default reservation lease time in millis.
   ;; After this time the reserved item, if not acknowledged,
   ;; will be available to grant reservation to other processes.
   :lease-time               DEFAULT_LEASE_TIME

   :snooze-time              DEFAULT_LEASE_TIME})


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                  ---==| C R E A T E   T A B L E S |==----                  ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn create-table
  "Creates the tables for the DurableQueue and all necessary indexes"
  [config]
  (let [{:keys [creds tasks-table reservation-index task-group-index task-scan-index
                read-capacity-units write-capacity-units]}
        (merge DEFAULT-CONFIG config)]
    (dyn/create-table
      creds
      {:table-name tasks-table
       :key-schema
       [{:attribute-name "task-group-id"
         :key-type "HASH"}
        {:attribute-name "task-id"
         :key-type "RANGE"}]
       :attribute-definitions
       [{:attribute-name "task-id"
         :attribute-type "S"}
        {:attribute-name "__topic-key"
         :attribute-type "S"}
        {:attribute-name "__topic-scan-hkey"
         :attribute-type "S"}
        {:attribute-name "__topic-scan-rkey"
         :attribute-type "S"}
        {:attribute-name "task-group-id"
         :attribute-type "S"}
        {:attribute-name "__reserv-key"
         :attribute-type "S"}]

       :provisioned-throughput
       {:read-capacity-units read-capacity-units
        :write-capacity-units write-capacity-units}

       ;; indexes
       :global-secondary-indexes
       ;; reservation-index
       [{:index-name reservation-index
         :key-schema
         [{:attribute-name "__topic-key"
           :key-type "HASH"}
          {:attribute-name "__reserv-key"
           :key-type "RANGE"}]
         :projection {:projection-type "ALL"}
         :provisioned-throughput
         {:read-capacity-units read-capacity-units
          :write-capacity-units write-capacity-units}}

        {:index-name task-scan-index
         :key-schema
         [{:attribute-name "__topic-scan-hkey"
           :key-type "HASH"}
          {:attribute-name "__topic-scan-rkey"
           :key-type "RANGE"}]
         :projection {:projection-type "ALL"}
         :provisioned-throughput
         {:read-capacity-units read-capacity-units
          :write-capacity-units write-capacity-units}}]})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;             ---==| I N T E R N A L   F U N C T I O N S |==----             ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- parse-task*
  [task]
  (some-> task
    (update :status keyword)
    (update :data u/deserialize)))

(defn- ->returnable-item
  [task]
  (dissoc  task :__topic-key
    :__topic-scan-hkey
    :__topic-scan-rkey
    :__ver))

(def status-flags*
  {:failed 400 :terminated 300 :running 200 :snoozed 200 :starting 100})

(defn- status-flags
  [{:keys [status pid] :as task}]
  (cond
    (= status :failed)     (status-flags* :failed)
    (= status :terminated) (status-flags* :terminated)
    pid                    (status-flags* :running)
    (= status :snoozed)    (status-flags* :snoozed)
    :else                  (status-flags* :starting)))

(defn- ->topic-key
  [topic status]
  (format "%s%020d"
    topic
    (if (#{:terminated :failed} status)
      (u/now)
      0)))

(defn- ->topic-scan-hkey*
  [now topic]
  (format "%s:%s:%02d"
    topic
    (t/year now)
    (t/week-number-of-year now)))

(defn- ->topic-scan-hkey
  [topic]
  (->topic-scan-hkey* (tc/from-long (u/now))
    topic))

(defn- ->topic-scan-rkey*
  ([sub-topic status-flag]
   (format "%s:%03d"
     (or sub-topic "default")
     status-flag))

  ([now sub-topic status-flag]
   (format "%s:%03d:%020d"
     (or sub-topic "default")
     status-flag
     now)))

(defn- ->topic-scan-rkey
  [sub-topic status-flag]
  (->topic-scan-rkey* (u/now)
    sub-topic
    status-flag))

(defn- inject-derived-attributes
  "Ensure that synthetic attributes are always present with the correct value"
  [{:keys [status sub-topic topic lease pid] :or {lease 0} :as task}]
  (as-> task $
    ;; ver is used for Multiversion Concurrency Control and optimistic lock
    (update $ :__ver (fnil inc 0))

    ;; ensure that lease is always populated
    (update $ :lease (fnil identity 0))

    ;; ensure that status is populated
    (update $ :status (fnil identity :starting))

    (update $ :data u/serialize)

    (assoc $ :__topic-key (->topic-key topic status))

    (assoc $ :__topic-scan-hkey (->topic-scan-hkey topic))

    (assoc $ :__topic-scan-rkey (->topic-scan-rkey sub-topic (status-flags $)))

    ;; __reserv_key is used for adding item which can be reserved to
    ;; the appropriate index (reservation-index).
    (assoc $ :__reserv-key (format "%03d%020d" (status-flags $) lease))))

(defn- safe-put-item
  "This function uses MVCC to ensure that updates are not conflicting
   with other concurrent updates, causing data to be overwritten."
  {:style/indent 1}
  [{:keys [creds tasks-table] :as aws-config} {:keys [__ver] :as item}]
  (let [updated-item (inject-derived-attributes item)]
    ;; checking whether it is a first-time insert or update
    (if-not __ver
      ;; if it is a first time insert then no item with the same id should
      ;; already exists
      (dyn/put-item
        creds
        :table-name tasks-table
        :item updated-item
        :condition-expression "attribute_not_exists(#taskid)"
        :expression-attribute-names {"#taskid" "task-id"})
      ;; if it is performing an update then the condition is that
      ;; __ver must be unchanged in the db (no concurrent updates)
      (dyn/put-item
        creds
        :table-name tasks-table
        :item updated-item
        :condition-expression "#curver = :oldver"
        :expression-attribute-names {"#curver" "__ver"}
        :expression-attribute-values {":oldver" __ver}))
    updated-item))

(defn- find-reservable-tasks
  "Queries the reservation index to find tasks that can be
  reserved. This includes new tasks and tasks whose lease has already
  expired."
  [{:keys [creds tasks-table reservation-index] :as config} topic timestamp limit]
  (let [reserve-key (format "%03d%020d" 200 timestamp)]
    (->> (dyn/query creds
           {:table-name tasks-table
            :index-name reservation-index
            :select "ALL_ATTRIBUTES"
            :key-condition-expression
            "#topic = :topic AND #reserv < :reserved"
            :expression-attribute-values
            {":topic" topic,
             ":reserved" reserve-key}
            :expression-attribute-names
            {"#reserv" "__reserv-key"
             "#topic" "__topic-key"}
            :limit limit})
      :items
      (map parse-task*))))

(defn- get-task-by-id
  [{:keys [creds tasks-table]} task-id]
  (let [[task-group-id __task-id] (str/split task-id #"--")]
    (when (every? not-empty [task-group-id __task-id])
      (->> (dyn/get-item creds
             {:table-name tasks-table
              :key {:task-group-id
                    {:s task-group-id}
                    :task-id
                    {:s task-id}}})
        :item
        parse-task*))))

(defn- get-tasks-by-task-group-id
  ;; TODO:
  [{:keys [creds tasks-table task-group-index]} task-group-id]
  (->> (dyn/query creds
         {:table-name tasks-table
          :select "ALL_ATTRIBUTES"
          :key-condition-expression
          "#tgid = :tgid"
          :expression-attribute-values
          {":tgid" task-group-id}
          :expression-attribute-names
          {"#tgid" "task-group-id"}
          :limit 200})
    :items
    (map parse-task*)))

(defn- list-tasks-by-topic-scan-hkey
  "see: ->topic-scan-hkey*"
  [{:keys [creds tasks-table task-scan-index]}
   {:keys [sub-topic status from to] :as filters}
   hk
   {:keys [limit last-evaluated-key] :as cursor}]
  (let [from-range-key  (->topic-scan-rkey* from
                          (or sub-topic "default")
                          (if status (status-flags* status) 100))
        to-range-key   (->topic-scan-rkey* to
                         (or sub-topic "default")
                         (if status (status-flags* status) 999))]
    (->> (dyn/query creds
           (cond-> {:table-name tasks-table
                    :index-name task-scan-index
                    :key-condition-expression "#h = :h AND #r between :fr AND :tr"
                    :expression-attribute-names
                    {"#h" "__topic-scan-hkey"
                     "#r" "__topic-scan-rkey"}
                    :expression-attribute-values
                    {":h"  hk
                     ":fr" from-range-key
                     ":tr" to-range-key}}
             limit (assoc :limit limit)
             last-evaluated-key (assoc :exclusive-start-key last-evaluated-key)))
      (u/query-results->paginated-response parse-task*))))


(defn list-tasks*
  [{:keys [creds tasks-table task-scan-index] :as cfg}
   {:keys [topic sub-topic status from to] :as filters}
   {:keys [inner-cursor limit next-period] :as cursor}]
  (when-not (= next-period -1)
    (let [hkeys (map #(->topic-scan-hkey* % topic)
                  (tp/periodic-seq
                    (-> from tc/from-long)
                    (-> to   tc/from-long (t/plus (t/weeks 1)))
                    (t/weeks 1)))

          {next-cursor :cursor :as res}
          (list-tasks-by-topic-scan-hkey cfg filters
            (nth hkeys (or next-period 0))
            (or inner-cursor {:limit limit}))

          out {:data (:data res)
               :cursor (assoc cursor :inner-cursor
                         next-cursor)}]
      (cond
        ;; this means there is more data to be returned for the
        ;; current period
        next-cursor
        (update-in out [:cursor :next-period] (fnil identity 0))

        ;; all data for current period has been returned.
        ;; but there are more periods to fetch
        (and (nil? next-cursor) (> (count hkeys) (inc (or next-period 0))))
        (update-in out [:cursor :next-period] (fnil inc 0))

        ;; all data for current period has been returned.
        ;; but there are more periods to fetch
        :else
        (update out :cursor assoc :next-period -1)))))

(defn update-status!
  [this task-id target-status pid {:keys [snooze-time error] :as opts}]
  (when-not (target-status task-statuses)
    (throw (ex-info "Invalid status."
             {:error :invalid-status
              :type :forbidden
              :status-requested target-status
              :allowed task-statuses})))

  (let [{:keys [status] :as task} (get-task-by-id this task-id)
        allowed-transitions (task-allowed-status-transitions status)]
    (when-not task
      (throw (ex-info "Task not found."
               {:error :task-not-found :task-id task-id})))

    (when-not (target-status allowed-transitions)
      (throw (ex-info "Invalid status transition requested."
               {:error :invalid-status-transition
                :type :forbidden
                :status-requested target-status
                :current-status status
                :allowed allowed-transitions})))

    (when-not (= status :snoozed)
      ;; Perform pid check and check for lease validity for all
      ;; transitions other than the ones that start from :paused or
      ;; :snoozed
      (when (not= pid (:pid task))
        (throw (ex-info "Cannot update status for a task that you don't own."
                 {:error :wrong-owner
                  :type :forbidden
                  :your-pid pid
                  :task-pid (:pid task)
                  :task task})))

      (when (and (= pid (:pid task)) (> (now) (:lease task)))
        (throw (ex-info "Lease expired."
                 {:error :lease-expired
                  :type :forbidden
                  :your-pid pid
                  :task-pid (:pid task)
                  :task task}))))


    (cond-> (assoc task :status target-status)
      (or  (= target-status :paused) (= target-status :terminated))
      (-> (assoc :lease 0) (dissoc :pid))

      (= target-status :failed)
      (assoc :error (or error ""))

      (= target-status :snoozed)
      (-> (assoc :lease (+ (now) (or snooze-time (:snooze-time this))))
        (dissoc :pid))

      :default
      (->> (safe-put-item this)
        ->returnable-item))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;           ---==| D Y A N M O D B   T A S K S   S T O R E |==----           ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defrecord DynamoTaskService
    [creds tasks-table reservation-index lease-time snooze-time]

  TaskService

  (create-task! [this {:keys [topic sub-topic task-group-id data] :as task-def}]
    (assert #(nil? (:task-id task-def)) "can't set task-id")
    (try
      (let [rnd-id (u/rand-id)
            task-group-id  (or task-group-id
                             (-> rnd-id
                               sha256
                               (subs 0 7)))
            task-id       (str/join "--" [task-group-id rnd-id])
            task-def (-> task-def
                       (assoc :task-id task-id
                              :sub-topic (or sub-topic "default")
                              :task-group-id task-group-id
                              :status :starting))]
        (safe-put-item this task-def)
        task-def)
      (catch ConditionalCheckFailedException e
        (log/debug e "create-task! failed - task already exists")
        (throw (ex-info "Task already exists"
                 {:type :forbidden
                  :error :task-already-exists})))))

  (task [this task-id]
    (some-> (get-task-by-id this task-id)
      ->returnable-item))

  (tasks [this task-group-id]
    (->> task-group-id
      str
      (get-tasks-by-task-group-id this)
      (map ->returnable-item)))

  (reserve-task! [this topic pid]
    (let [topic-key (->topic-key topic :starting)
          task (first (find-reservable-tasks this topic-key (now) 1))
          lease-time (or lease-time DEFAULT_LEASE_TIME)]

      (when task
        (-> (safe-put-item this
              (assoc task
                :status :running
                :pid  pid
                :lease (+ (now) lease-time)))
          parse-task*))))

  (extend-lease! [this task-id pid]
    (let [task (get-task-by-id this task-id)
          lease-time (or lease-time DEFAULT_LEASE_TIME)]
      (log/info "Trying to extend lease" task-id)

      (when-not task
        (throw (ex-info "Task not found."
                 {:error :task-not-found
                  :type :forbidden
                  :task-id task-id})))

      (when-not (= (:status task) :running)
        (throw (ex-info "Task status NOT :running"
                 {:error :invalid-status
                  :type :forbidden
                  :task task})))

      (when (not= pid (:pid task))
        (throw (ex-info "Cannot extend the lease for a task that you don't own."
                 {:error :wrong-owner
                  :type :forbidden
                  :your-pid pid
                  :task-pid (:pid task)
                  :task task})))

      (when (and (= pid (:pid task)) (> (now) (:lease task)))
        (throw (ex-info "Lease expired."
                 {:error :lease-expired
                  :type :forbidden
                  :your-pid pid
                  :task-pid (:pid task)
                  :task task})))

      ;; extending the lease expiration time
      (safe-put-item this
        (update task :lease (fn [ol] (max ol (+ (now) lease-time)))))
      :ok))

  (list-tasks [this topic {:keys [from to sub-topic] :as filters} {:keys [limit] :as cursor}]
    (list-tasks* this filters cursor))

  (terminate! [this task-id pid]
    (update-status! this task-id :terminated pid {}))

  (snooze! [this task-id pid snooze-time]
    (update-status! this task-id :snoozed pid {:snooze-time snooze-time}))

  (fail! [this task-id pid error]
    (update-status! this task-id :failed pid {:error (str error)})))

(defn dyn-task-service
  [config]
  (map->DynamoTaskService (merge DEFAULT-CONFIG config)))
