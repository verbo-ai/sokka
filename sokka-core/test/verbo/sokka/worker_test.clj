(ns verbo.sokka.worker-test
  (:require [verbo.sokka.worker :as sut]
            [verbo.sokka.ctrl :as ctrl]
            [verbo.sokka.utils :as u]
            [verbo.sokka.task :as task]
            [verbo.sokka.test-helpers :as h]
            [verbo.sokka.impl.dynamodb-task :as dyn-task]
            [midje.sweet :refer :all]
            [clojure.core.async :as async]
            [clojure.tools.logging :as log])
  (:import [java.util.concurrent ExecutionException CancellationException]))

(h/start-mulog-publisher {:type :console})

(facts "about monitor!"
  (fact "handles close properly"
    (let [ctrl (ctrl/default-control (* 1 60 1000))]
      (try
        (ctrl/close! ctrl)
        (deref ctrl 300 :didnt-complete) => {:status :closed}
        (finally
          (ctrl/cleanup! ctrl)))))

  (fact "handles abort properly"
    (let [ctrl (ctrl/default-control (* 1 60 1000))]
      (try
        (ctrl/abort! ctrl)
        (deref ctrl 300 :didnt-complete) => (contains {:status :aborted})
        (finally
          (ctrl/cleanup! ctrl)))))

  (fact "aborts ctrl on timeout"
    (let [ctrl (ctrl/default-control 1)]
      (try
        (deref ctrl 100 :didnt-complete) =>  {:status :aborted :err-data {:reason :timed-out}}
        (finally
          (ctrl/cleanup! ctrl))))))

(def keepalive!* #'sut/keepalive!*)

(facts "about keepalive!*"
  (facts "calls keepalive-fn every keepalive-ms times"
    (let [heartbeats (atom 0)
          ctrl  (ctrl/default-control (* 1 60 1000))
          p    (keepalive!* ctrl 100 #(swap! heartbeats inc))]
      (async/go
        (async/<! (async/timeout 310))
        (ctrl/close! ctrl))
      (try
        (fact "completes gracefully"
          (deref p 600 :didnt-complete) => (contains {:status :closed}))
        (fact "heartbeats are received"
          @heartbeats => 3)
        (finally
          (ctrl/cleanup! ctrl)))))

  (fact "shuts down cleanly when aborted"
    (let [ctrl (ctrl/default-control (* 1 60 1000))
          p    (keepalive!* ctrl 100 (constantly :no-op))]
      (async/go (ctrl/abort! ctrl))
      (try
        (fact "completes gracefully"
          (deref p 600 :didnt-complete) => (contains {:status :aborted}))
        (finally
          (ctrl/cleanup! ctrl)))))

  (fact "closes ctrl when keepalive fails"
    (let [ctrl (ctrl/default-control (* 1 60 1000))
          p    (keepalive!* ctrl 100 #(throw (ex-info "kaboom!" {})))]
      (try
        (fact "completes gracefully"
          (deref p 600 :didnt-complete) => :failed)
        (fact "aborts ctrl"
          @ctrl => (contains {:status :closed}))
        (finally
          (ctrl/cleanup! ctrl))))))

(def execute!* #'sut/execute!*)

(facts "about execute!*"
  (facts "executes pfn in a separate thread and stops gracefully"
    (let [out  (atom nil)
          ctrl (ctrl/default-control (* 1 60 1000))
          ftr (execute!* ctrl #(reset! out "Hello!"))]
      (try
        (fact "completes gracefully"
          (deref ftr 600 :didnt-complete) => :closed)
        (fact "executes pfn"
          @out => "Hello!")
        (finally
          (ctrl/cleanup! ctrl)))))

  (facts "when pfn fails, the ctrl is aborted"
    (let [out  (atom nil)
          ctrl (ctrl/default-control (* 1 60 1000))
          ftr (execute!* ctrl  #(throw (ex-info "kaboom!" {})))]
      (try
        (fact "future is interrupted"
          (deref ftr 600 :didnt-complete) => (throws Exception))
        (fact "ctrl is aborted"
          @ctrl => (contains {:status :aborted}))
        (finally
          (ctrl/cleanup! ctrl)))))

  (facts "when ctrl is aborted, future is cancelled"
    (let [out  (atom nil)
          ctrl (ctrl/default-control (* 1 60 1000))
          ftr (execute!* ctrl #(Thread/sleep (* 1 60 1000)))]
      (ctrl/abort! ctrl)
      (try
        (fact "future is interrupted"
          (deref ftr 600 :didnt-complete) => (throws Exception))
        (finally
          (ctrl/cleanup! ctrl)))))

  (facts "when ctrl is closed, future is cancelled"
    (let [out  (atom nil)
          ctrl (ctrl/default-control (* 1 60 1000))
          ftr (execute!* ctrl #(Thread/sleep (* 1 60 1000)))]
      (ctrl/close! ctrl)
      (try
        (fact "future is interrupted"
          (deref ftr 600 :didnt-complete) => (throws Exception))
        (finally
          (ctrl/cleanup! ctrl))))))

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

(defonce test-config
  (delay
    {:cognitect-aws/client
     {:endpoint-override
      {:all {:port 7000
             :region "us-east-1"
             :hostname "localhost"
             :protocol :http}}}
     :lease-time 1000
     :tasks-table (str "sokka-tasks" (u/rand-id))}))

(with-state-changes [(before :facts (ensure-test-table @test-config))]
  (let [dyn-taskq (dyn-task/dyn-taskq
                    @test-config)]
    (facts "about worker"
      (fact "it is possible to start stop the worker gracefully"
        (let [topic (u/rand-id)
              pid (u/rand-id)
              stop-fn (sut/worker {:taskq dyn-taskq
                                   :lease-time-ms (:lease-time @test-config)
                                   :topic topic
                                   :pid pid
                                   :pfn (constantly [:sokka/completed])})]
          (deref (stop-fn) 600 :didnt-complete) => true))

      (facts "aborts current running task when closed"
        (let [test-ctrl (ctrl/default-control (* 1 60 1000))
              topic (u/rand-id)
              pid (u/rand-id)
              _ (task/create-task! dyn-taskq
                  {:topic topic
                   :data :noop})]
          (with-redefs [ctrl/default-control (constantly test-ctrl)]
            (let [stop-fn (sut/worker {:taskq dyn-taskq
                                       :lease-time-ms (:lease-time @test-config)
                                       :topic topic
                                       :pid pid
                                       :executor-fn (sut/default-executor
                                                      (fn [_]
                                                        (Thread/sleep (* 6 1000))))})]
              (try
                ;; wait for task to be picked up
                ;; TODO: more non-deterministic tests, cant think of a
                ;; better option at this point, something to fix later
                (deref (promise) 1000 :timeout)

                (deref (stop-fn) 600 :didnt-complete)

                (fact "running task is aborted"
                  @test-ctrl)

                (finally
                  (stop-fn)
                  (ctrl/cleanup! test-ctrl))))))))))

(with-state-changes [(before :facts (ensure-test-table @test-config))]
  (let [dyn-taskq (dyn-task/dyn-taskq
                    @test-config)
        topic (u/rand-id)
        pid (u/rand-id)
        db (atom {})
        test-task-handler
        (fn [task]
          (swap! db update (-> task :data :op) (fnil inc 0))
          [:sokka/completed])

        foo (task/create-task! dyn-taskq
              {:topic topic
               :data {:op :foo}})
        bar (task/create-task! dyn-taskq
              {:topic topic
               :data {:op :bar}})
        baz (task/create-task! dyn-taskq
              {:topic topic
               :data {:op :baz}})]

    (facts "worker schedules all tasks and updates the status"
      (let [stop-fn (sut/worker
                      {:taskq dyn-taskq
                       :lease-time-ms (:lease-time @test-config)
                       :topic topic
                       :pid pid
                       :executor-fn (sut/default-executor test-task-handler)})]
        ;; wait for task to be picked up
        ;; TODO: more non-deterministic tests, cant think of a
        ;; better option at this point, something to fix later
        (deref (promise) 3000 :timeout)

        (deref (stop-fn) 600 :didnt-complete)

        (fact "tasks complete successfully"
          @db => {:foo 1 :bar 1 :baz 1})

        (fact "task statuses are updated correctly"
          (:status (task/task dyn-taskq (:task-id foo))) => :terminated
          (:status (task/task dyn-taskq (:task-id bar))) => :terminated
          (:status (task/task dyn-taskq (:task-id baz))) => :terminated)))))


(with-state-changes [(before :facts (ensure-test-table @test-config))]
  (let [dyn-taskq (dyn-task/dyn-taskq
                    @test-config)
        topic (u/rand-id)
        pid (u/rand-id)
        db (atom {})
        test-task-handler
        (fn [{{op :op} :data :as task}]
          (swap! db update (-> task :data :op) (fnil inc 0))
          (cond
            (= op :foo) [:sokka/completed]
            (= op :bar) [:sokka/failed {:error-message "fail!"}]
            (and (= op :baz) (= (:baz @db) 1))  [:sokka/snoozed {:snooze-time 100}]
            :else
            [:sokka/completed]))

        foo (task/create-task! dyn-taskq
              {:topic topic
               :data {:op :foo}})
        bar (task/create-task! dyn-taskq
              {:topic topic
               :data {:op :bar}})
        baz (task/create-task! dyn-taskq
              {:topic topic
               :data {:op :baz}})]

    (facts "worker schedules all tasks and updates the status"
      (let [stop-fn (sut/worker {:taskq dyn-taskq
                                 :lease-time-ms (:lease-time @test-config)
                                 :topic topic
                                 :pid pid
                                 :executor-fn (sut/default-executor test-task-handler)})]

        ;; wait for task to be picked up
        ;; TODO: more non-deterministic tests, cant think of a
        ;; better option at this point, something to fix later
        (deref (promise) 6000 :timeout)

        (deref (stop-fn) 600 :didnt-complete)

        (fact "tasks complete successfully"
          @db => {:foo 1 :bar 1 :baz 2})

        (fact "task statuses are updated correctly"
          (:status (task/task dyn-taskq (:task-id foo))) => :terminated
          (:status (task/task dyn-taskq (:task-id bar))) => :failed
          (:status (task/task dyn-taskq (:task-id baz))) => :terminated)))))
