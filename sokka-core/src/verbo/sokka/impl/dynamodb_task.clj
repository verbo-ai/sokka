(ns verbo.sokka.impl.dynamodb-task
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clj-time.periodic :as tp]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [pandect.algo.sha256 :refer [sha256]]
            [verbo.sokka.task :refer :all]
            [verbo.sokka.utils :as u :refer [now]]
            [safely.core :refer [safely]]
            [verbo.sokka.aws :as aws])
  (:import [com.amazonaws.services.dynamodbv2.model
            ConditionalCheckFailedException
            ProvisionedThroughputExceededException]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                 ---==| D E F A U L T   C O N F I G |==----                 ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:const DEFAULT_LEASE_TIME (* 5 60 1000))


(def ^:const DEFAULT-CONFIG
  {;; the aws region's endpoint to contact
   :cognitect-aws/client {:region "eu-west-1"}
   ;; the name of the table
   :tasks-table "sc-tasks-v2"

   ;; the initial read and write capacity
   ;; for the table and indices
   :throughput-provisioned? true
   :read-capacity-units      10
   :write-capacity-units     10

   ;; The name of the index used for the reservation
   ;; queries.
   :reservation-index "reservation-index"
   ;; The name of the index used for the task-group queries
   :task-scan-index "task-scan-index"
   ;; index name used to query tasks by task-group-id
   :task-group-index "task-group-index"

   ;; The default reservation lease time in millis.
   ;; After this time the reserved item, if not acknowledged,
   ;; will be available to grant reservation to other processes.
   :lease-time DEFAULT_LEASE_TIME
   :snooze-time DEFAULT_LEASE_TIME})

(defn ddb-rec->m
  "converts form a dynamo record representation to a clojure map"
  [r]
  (->> r
    (map (fn [[k [& [[t v]]]]] [k (case t :N (Long/parseLong v) v)]))
    (into {})))

(defn m->ddb-rec
  "converts form a Clojure map to a dynamo record representation"
  [r]
  (->> r
    (map (fn [[k v]]
           [k (cond
                (number? v) {:N v}
                :else {:S (name v)})]))
    (into {})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                  ---==| C R E A T E   T A B L E S |==----                  ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn create-table
  "Creates the tables for the DurableQueue and all necessary indexes"
  [config]
  (let [{:keys [cognitect-aws/client tasks-table reservation-index
                task-group-index task-scan-index throughput-provisioned?
                read-capacity-units write-capacity-units]}
        (u/deep-merge DEFAULT-CONFIG config)
        ddb (aws/make-client client :dynamodb)]
    (cond->> {:TableName tasks-table
              :KeySchema
              [{:AttributeName "task-id" :KeyType "HASH"}]

              :AttributeDefinitions
              [{:AttributeName "task-id" :AttributeType "S"}
               {:AttributeName "__topic-key" :AttributeType "S"}
               {:AttributeName "__topic-scan-hkey" :AttributeType "S"}
               {:AttributeName "__topic-scan-rkey" :AttributeType "S"}
               {:AttributeName "task-group-id" :AttributeType "S"}
               {:AttributeName "__reserv-key" :AttributeType "S"}]
              ;; billing mode
              :BillingMode "PAY_PER_REQUEST"
              ;; indexes
              :GlobalSecondaryIndexes
              [(cond-> {:IndexName task-group-index
                        :KeySchema
                        [{:AttributeName "task-group-id"
                          :KeyType "HASH"}
                         {:AttributeName "task-id"
                          :KeyType "RANGE"}]
                        :Projection {:ProjectionType "ALL"}}
                 throughput-provisioned?
                 (assoc :ProvisionedThroughPut
                   {:ReadCapacityUnits read-capacity-units
                    :WriteCapacityUnits write-capacity-units}))

               ;; reservation-index
               (cond-> {:IndexName reservation-index
                        :KeySchema
                        [{:AttributeName "__topic-key"
                          :KeyType "HASH"}
                         {:AttributeName "__reserv-key"
                          :KeyType "RANGE"}]
                        :Projection {:ProjectionType "ALL"}}
                 throughput-provisioned?
                 (assoc :ProvisionedThroughPut
                   {:ReadCapacityUnits read-capacity-units
                    :WriteCapacityUnits write-capacity-units}))

               (cond-> {:IndexName task-scan-index
                        :KeySchema
                        [{:AttributeName "__topic-scan-hkey"
                          :KeyType "HASH"}
                         {:AttributeName "__topic-scan-rkey"
                          :KeyType "RANGE"}]
                        :Projection {:ProjectionType "ALL"}}
                 throughput-provisioned?
                 (assoc :ProvisionedThroughPut
                   {:ReadCapacityUnits read-capacity-units
                    :WriteCapacityUnits write-capacity-units}))]}

      throughput-provisioned?
      (merge {:BillingMode "PROVISIONED"
              :ProvisionedThroughPut
              {:ReadCapacityUnits read-capacity-units
               :WriteCapacityUnits write-capacity-units}})

      :default
      (aws/invoke! ddb :CreateTable))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;             ---==| I N T E R N A L   F U N C T I O N S |==----             ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- parse-task*
  [task]
  (some-> task
    ddb-rec->m
    (update :status keyword)
    (update :data (comp u/deserialize u/base64->bytes))))

(defn- ->returnable-item
  [task]
  (dissoc task
    :__topic-key
    :__topic-scan-hkey
    :__topic-scan-rkey
    :__reserv-key
    :record-ver))

(def status-flags*
  {:failed 400 :terminated 300 :running 200 :snoozed 150 :starting 100})

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
    (update $ :record-ver (fnil inc 0))
    ;; ensure that lease is always populated
    (update $ :lease (fnil identity 0))
    ;; ensure that status is populated
    (update $ :status (fnil identity :starting))
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
  [{:keys [ddb tasks-table] :as aws-config} {:keys [record-ver] :as item}]
  (let [item' (inject-derived-attributes item)
        serialized  (update item' :data (comp u/bytes->base64 u/serialize))]
    ;; checking whether it is a first-time insert or update
    (if-not record-ver
      ;; if it is a first time insert then no item with the same id should
      ;; already exists
      (aws/invoke! ddb
        :PutItem
        {:TableName tasks-table
         :Item (m->ddb-rec serialized)
         :ConditionExpression
         "attribute_not_exists(#taskid)"
         :ExpressionAttributeNames
         {"#taskid" "task-id"}})
      ;; if it is performing an update then the condition is that
      ;; record-ver must be unchanged in the db (no concurrent updates)
      (aws/invoke! ddb
        :PutItem
        {:TableName tasks-table
         :Item (m->ddb-rec serialized)
         :ConditionExpression "#curver = :oldver"
         :ExpressionAttributeNames
         {"#curver" "record-ver"}
         :ExpressionAttributeValues
         {":oldver" {:N record-ver}}}))
    item'))

(defn- find-reservable-tasks
  "Queries the reservation index to find tasks that can be
  reserved."
  [{:keys [ddb tasks-table reservation-index] :as config} topic timestamp limit]
  (let [reserve-key (format "%03d%s" 101 0)]
    (->> (aws/invoke! ddb
           :Query
           {:TableName tasks-table
            :IndexName reservation-index
            :Select "ALL_ATTRIBUTES"
            :KeyConditionExpression
            "#topic = :topic AND #reserv < :reserved"
            :ExpressionAttributeNames
            {"#reserv" "__reserv-key"
             "#topic" "__topic-key"}
            :ExpressionAttributeValues
            {":topic" {:S (->topic-key topic :starting)}
             ":reserved" {:S reserve-key}}
            :Limit limit})
      :Items
      (map parse-task*))))

(defn- find-running-and-snoozed-tasks
  "Queries the reservation index to find tasks that are running/snoozed."
  [{:keys [ddb tasks-table reservation-index] :as config} topic
   {:keys [limit last-evaluated-key] :as cursor}]
  (->> (cond-> {:TableName tasks-table
                :IndexName reservation-index
                :Select "ALL_ATTRIBUTES"
                :KeyConditionExpression
                "#topic = :topic  AND #reserv between :from AND :to"
                :ExpressionAttributeValues
                {":topic" {:S (->topic-key topic :running-or-snoozed)} ;;FIXME: WTF dude!
                 ":from" {:S (format "%03d%020d" 150 0)}
                 ":to"   {:S (format "%03d%020d" 201 0)}}
                :ExpressionAttributeNames
                {"#reserv" "__reserv-key"
                 "#topic" "__topic-key"}
                :Limit limit}
         last-evaluated-key (assoc :ExclusiveStartKey last-evaluated-key))
    (aws/invoke! ddb :Query)
    (u/query-results->paginated-response
        parse-task*)))

(defn- get-task-by-id
  [{:keys [ddb tasks-table]} task-id]
  (->> (aws/invoke! ddb
         :GetItem
         {:TableName tasks-table
          :Key {"task-id" {:S task-id}}})
    :Item
    parse-task*))

(defn- get-tasks-by-task-group-id
  ;; TODO: paginate
  [{:keys [ddb tasks-table task-group-index]} task-group-id]
  (->> (aws/invoke! ddb
         :Query
         {:TableName tasks-table
          :IndexName task-group-index
          :Select "ALL_ATTRIBUTES"
          :KeyConditionExpression "#tgid = :tgid"
          :ExpressionAttributeNames
          {"#tgid" "task-group-id"}
          :ExpressionAttributeValues
          {":tgid" {:S task-group-id}}
          :Limit 200})
    :Items
    (map parse-task*)))

(defn- list-tasks-by-topic-scan-hkey
  "see: ->topic-scan-hkey*"
  [{:keys [ddb tasks-table task-scan-index]}
   {:keys [sub-topic status from to] :as filters}
   hk
   {:keys [limit last-evaluated-key] :as cursor}]
  (let [from-range-key  (->topic-scan-rkey* from
                          (or sub-topic "default")
                          (if status (status-flags* status) 100))
        to-range-key   (->topic-scan-rkey* to
                         (or sub-topic "default")
                         (if status (status-flags* status) 999))]
    (->> (aws/invoke! ddb
           :Query
           (cond-> {:TableName tasks-table
                    :IndexName task-scan-index
                    :KeyConditionExpression "#h = :h AND #r between :fr AND :tr"
                    :ExpressionAttributeNames
                    {"#h" "__topic-scan-hkey"
                     "#r" "__topic-scan-rkey"}
                    :ExpressionAttributeValues
                    {":h" {:S hk}
                     ":fr" {:S from-range-key}
                     ":tr" {:S to-range-key}}}
             limit (assoc :Limit limit)
             last-evaluated-key (assoc :ExclusiveStartKey last-evaluated-key)))
      (u/query-results->paginated-response (comp ->returnable-item parse-task*)))))


(defn list-tasks*
  [{:keys [ddb tasks-table task-scan-index] :as cfg}
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

(defn- update-status!
  [this task-id target-status pid {:keys [snooze-time error record-ver] :as opts}]
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

    (when (and record-ver (not= record-ver (:record-ver task)))
      (throw (ex-info "Task updated elsewhere."
               {:error :task-concurrently-updated
                :task-id task-id
                :task (select-keys task [:task-id :status :record-ver])
                :record-ver record-ver})))

    (when-not (target-status allowed-transitions)
      (throw (ex-info "Invalid status transition requested."
               {:error :invalid-status-transition
                :type :forbidden
                :status-requested target-status
                :current-status status
                :allowed allowed-transitions})))

    (when-not (or (= status :snoozed) (= target-status :starting))
      ;; Perform pid check and check for lease validity for all
      ;; transitions other than the ones that start from :snoozed or
      ;; transition to :starting
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
      (or  (= target-status :starting) (= target-status :terminated))
      (-> (assoc :lease 0) (dissoc :pid))

      (= target-status :failed)
      (assoc :error (or error ""))

      (= target-status :snoozed)
      (-> (assoc :snooze-time (or snooze-time (:snooze-time this))
                 :lease (+ (now) (or snooze-time (:snooze-time this))))
        (dissoc :pid))

      :default
      (->> (safe-put-item this)
        ->returnable-item))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;           ---==| D Y A N M O D B   T A S K S   S T O R E |==----           ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defrecord DynamoTaskService [tasks-table reservation-index lease-time snooze-time]
  TaskStore

  (create-task! [this {:keys [topic sub-topic task-group-id data] :as task-def}]
    (try
      (let [task-id (or (:task-id task-def) (u/rand-id))
            task-def (-> task-def
                       (update :task-id (fnil identity task-id))
                       (update :task-group-id (fnil identity task-id))
                       (assoc :sub-topic (or sub-topic "default")
                              :status :starting))]
        (safe-put-item this task-def)
        task-def)
      (catch Exception e
        (if (some->> e
              ex-data
              :__type
              (re-matches #"com.amazonaws.dynamodb.*?ConditionalCheckFailedException"))
          (do (log/debug e "create-task! failed - task already exists")
              (throw (ex-info "Task already exists"
                       {:type :forbidden
                        :error :task-already-exists})))
          (throw e)))))

  (task [this task-id]
    (some-> (get-task-by-id this task-id)
      ->returnable-item))

  (tasks [this task-group-id]
    (->> task-group-id
      str
      (get-tasks-by-task-group-id this)
      (map ->returnable-item)))

  (reserve-task! [this topic pid]
    (let [task (first (find-reservable-tasks this topic (now) 1))
          lease-time (or lease-time DEFAULT_LEASE_TIME)]

      (when task
        (-> (safe-put-item this
              (assoc task
                :status :running
                :pid  pid
                :lease (+ (now) lease-time)))))))

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

  (revoke-lease! [this task-id record-ver]
    (update-status! this task-id :starting nil {:record-ver record-ver}))

  (list-tasks [this {:keys [topic from to sub-topic] :as filters} {:keys [limit] :as cursor}]
    (list-tasks* this filters  cursor))

  (terminate! [this task-id pid]
    (update-status! this task-id :terminated pid {}))

  (snooze! [this task-id pid snooze-time]
    (update-status! this task-id :snoozed pid {:snooze-time snooze-time}))

  (fail! [this task-id pid error]
    (update-status! this task-id :failed pid {:error (str error)}))

  LeaseSupervision

  (list-leased-tasks [this topic cursor]
    (find-running-and-snoozed-tasks this topic cursor)))

(defn- with-default-errors
  [f & args]
  (try
    (apply f args)
    (catch ProvisionedThroughputExceededException e
      (throw (ex-info "dynamo throughput exceeded"
               {:type :throttling-exception} e)))
    (catch ConditionalCheckFailedException e
      nil)))

(defrecord DynamoTaskServiceWrapper [dyn-taskq]
  TaskStore
  (create-task! [_ task]
    (with-default-errors create-task! dyn-taskq task))

  (task [_ task-id]
    (with-default-errors task dyn-taskq task-id))

  (tasks [_ task-group-id]
    (with-default-errors tasks dyn-taskq task-group-id))

  (list-tasks [_ filters cursor]
    (with-default-errors list-tasks dyn-taskq filters cursor))

  (reserve-task! [_ topic pid]
    (with-default-errors reserve-task! dyn-taskq topic pid))

  (extend-lease! [_ task-id pid]
    (with-default-errors extend-lease! dyn-taskq task-id pid))

  (terminate! [_ task-id pid]
    (with-default-errors terminate! dyn-taskq task-id pid))

  (snooze! [_ task-id pid snooze-time]
    (with-default-errors snooze! dyn-taskq task-id pid snooze-time))

  (revoke-lease! [_ task-id record-ver]
    (with-default-errors revoke-lease! dyn-taskq task-id record-ver))

  (fail! [_ task-id pid error]
    (with-default-errors fail! dyn-taskq task-id pid error))

  LeaseSupervision
  (list-leased-tasks [this topic cursor]
    (with-default-errors list-leased-tasks dyn-taskq topic cursor)))


(defn dyn-taskq
  [config]
  (let [config'    (u/deep-merge DEFAULT-CONFIG config)
        dyn-client (aws/make-client (:cognitect-aws/client config') :dynamodb)]
    (->> (assoc config' :ddb dyn-client)
      (map->DynamoTaskService)
      (DynamoTaskServiceWrapper.))))


(comment

  (def ddb  (aws/create-client
              (:client
               (u/deep-merge DEFAULT-CONFIG
                 {:client
                  {:api :dynamodb
                   :region  :us-east-1
                   :endpoint-override {:port 7000,
                                       :hostname "localhost",
                                       :path "/"
                                       :protocol :http}}}))))


  (aws/help (:ddb taskq) :CreateTable)

  (aws/invoke! ddb :CreateTable {})

  (dyn/list-tables {:endpoint "http://localhost:7000"})


  )
