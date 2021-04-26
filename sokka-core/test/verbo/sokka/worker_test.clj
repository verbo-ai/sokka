(ns verbo.sokka.worker-test
  (:require [verbo.sokka.worker :as sut]
            [verbo.sokka.utils :as u]
            [verbo.sokka.task :as task]
            [verbo.sokka.impl.dynamodb-task :as dyn-task]
            [midje.sweet :refer :all]
            [clojure.core.async :as async]
            [clojure.tools.logging :as log])
  (:import com.amazonaws.services.dynamodbv2.model.ResourceInUseException))

(facts "about monitor!"
  (fact "handles close properly"
    (let [ctrl (sut/new-control)]
      (try
        (sut/monitor! ctrl)
        (sut/close! ctrl)
        (deref ctrl 300 :didnt-complete) => :closed
        (finally
          (sut/cleanup! ctrl)))))

  (fact "handles abort properly"
    (let [ctrl (sut/new-control)]
      (try
        (sut/monitor! ctrl)
        (sut/abort! ctrl)
        (deref ctrl 300 :didnt-complete) => :aborted
        (finally
          (sut/cleanup! ctrl)))))

  (fact "aborts ctrl on timeout"
    (let [ctrl (sut/new-control 1)]
      (try
        (sut/monitor! ctrl)
        (deref ctrl 100 :didnt-complete) => :timed-out
        (.closed? (:abort-chan ctrl)) => true
        (finally
          (sut/cleanup! ctrl))))))

(facts "about keepalive!*"
  (facts "calls keepalive-fn every keepalive-ms times"
    (let [heartbeats (atom 0)
          ctrl (sut/new-control)
          p    (sut/keepalive!* ctrl 100 #(swap! heartbeats inc))]
      (async/go
        (async/<! (async/timeout 310))
        (sut/close! ctrl))
      (try
        (fact "completes gracefully"
          (deref p 600 :didnt-complete) => :closed)
        (fact "heartbeats are received"
          @heartbeats => 3)
        (finally
          (sut/cleanup! ctrl)))))

  (fact "shuts down cleanly when aborted"
    (let [ctrl (sut/new-control)
          p    (sut/keepalive!* ctrl 100 (constantly :no-op))]
      (async/go (sut/abort! ctrl))
      (try
        (fact "completes gracefully"
          (deref p 600 :didnt-complete) => :aborted)
        (finally
          (sut/cleanup! ctrl)))))

  (fact "aborts ctrl when there is an exception"
    (let [ctrl (sut/new-control)
          p    (sut/keepalive!* ctrl 100 #(throw (ex-info "kaboom!" {})))]
      (try
        (fact "completes gracefully"
          (deref p 600 :didnt-complete) => :failed)
        (fact "aborts ctrl"
          (.closed? (:abort-chan ctrl)) => true)
        (finally
          (sut/cleanup! ctrl))))))

(facts "about execute!"
  (facts "executes pfn in a separate thread and stops gracefully"
    (let [out  (atom nil)
          ctrl (sut/new-control)
          ftr (sut/execute!* ctrl #(reset! out "Hello!"))]
      (try
        (fact "completes gracefully"
          (deref ftr 600 :didnt-complete) => :closed)
        (fact "executes pfn"
          @out => "Hello!")
        (finally
          (sut/cleanup! ctrl)))))

  (facts "when pfn fails, the ctrl is aborted"
    (let [out  (atom nil)
          ctrl (sut/new-control)
          ftr (sut/execute!* ctrl #(throw (ex-info "kaboom!" {})))]
      (try
        (fact "future is interrupted"
          (deref ftr 600 :didnt-complete) => (throws java.util.concurrent.ExecutionException))
        (fact "ctrl is aborted"
          (.closed? (:abort-chan ctrl)) => true)
        (finally
          (sut/cleanup! ctrl)))))

  (facts "when ctrl is aborted, future is cancelled"
    (let [out  (atom nil)
          ctrl (sut/new-control)
          ftr (sut/execute!* ctrl #(Thread/sleep (* 1 60 1000)))]
      (sut/abort! ctrl)
      (try
        (fact "future is interrupted"
          (deref ftr 600 :didnt-complete) => (throws java.util.concurrent.CancellationException))
        (finally
          (sut/cleanup! ctrl)))))

  (facts "when ctrl is closed, future is cancelled"
    (let [out  (atom nil)
          ctrl (sut/new-control)
          ftr (sut/execute!* ctrl #(Thread/sleep (* 1 60 1000)))]
      (sut/close! ctrl)
      (try
        (fact "future is interrupted"
          (deref ftr 600 :didnt-complete) => (throws java.util.concurrent.CancellationException))
        (finally
          (sut/cleanup! ctrl))))))

(defn ensure-test-table
  [config]
  (try
    (dyn-task/create-table config)
    (catch ResourceInUseException e
      (log/info "test table already exists"))))

(defonce test-config
  (delay
    {:creds {:endpoint "http://localhost:7000"}
     :tasks-table (str "sc-tasks-v2-" (u/rand-id))}))

(with-state-changes [(before :facts (ensure-test-table @test-config))]
  (let [dyn-task-service (dyn-task/dyn-task-service
                           @test-config)]
    (facts "about worker"
      (fact "it is possible to start stop the worker gracefully"
        (let [topic (u/rand-id)
              pid (u/rand-id)
              stop-fn (sut/worker {:task-service dyn-task-service
                                   :topic topic
                                   :pid pid
                                   :pfn (constantly [:sokka/completed])})]
          (deref (stop-fn) 600 :didnt-complete) => true))

      (facts "aborts current running task when closed"
        (let [test-ctrl (sut/new-control)
              topic (u/rand-id)
              pid (u/rand-id)
              _ (task/create-task! dyn-task-service
                  {:topic topic
                   :data :noop})]
          (with-redefs [sut/new-control (constantly test-ctrl)]
            (let [stop-fn (sut/worker {:task-service dyn-task-service
                                       :topic topic
                                       :pid pid
                                       :pfn (fn [_]
                                              (Thread/sleep (* 6 1000)))})]
              (try
                ;; wait for task to be picked up

                ;; TODO: more non-deterministic tests, cant think of a
                ;; better option at this point, something to fix later
                (deref (promise) 1000 :timeout)

                (deref (stop-fn) 600 :didnt-complete)

                (fact "running task is aborted"
                  (.closed? (:abort-chan test-ctrl)) => true)

                (finally
                  (stop-fn)
                  (sut/cleanup! test-ctrl))))))))))

(with-state-changes [(before :facts (ensure-test-table @test-config))]
  (let [dyn-task-service (dyn-task/dyn-task-service
                           @test-config)
        topic (u/rand-id)
        pid (u/rand-id)
        db (atom {})
        test-task-handler
        (fn [task]
          (swap! db update (-> task :data :op) (fnil inc 0))
          [:sokka/completed])

        foo (task/create-task! dyn-task-service
              {:topic topic
               :data {:op :foo}})
        bar (task/create-task! dyn-task-service
              {:topic topic
               :data {:op :bar}})
        baz (task/create-task! dyn-task-service
              {:topic topic
               :data {:op :baz}})]

    (facts "worker schedules all tasks and updates the status"
      (let [stop-fn (sut/worker {:task-service dyn-task-service
                                 :topic topic
                                 :pid pid
                                 :pfn test-task-handler})]

        ;; wait for task to be picked up
        ;; TODO: more non-deterministic tests, cant think of a
        ;; better option at this point, something to fix later
        (deref (promise) 2000 :timeout)

        (deref (stop-fn) 600 :didnt-complete)

        (fact "tasks complete successfully"
          @db => {:foo 1 :bar 1 :baz 1})

        (fact "task statuses are updated correctly"
          (:status (task/task dyn-task-service (:task-id foo))) => :terminated
          (:status (task/task dyn-task-service (:task-id bar))) => :terminated
          (:status (task/task dyn-task-service (:task-id baz))) => :terminated)))))


(with-state-changes [(before :facts (ensure-test-table @test-config))]
  (let [dyn-task-service (dyn-task/dyn-task-service
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

        foo (task/create-task! dyn-task-service
              {:topic topic
               :data {:op :foo}})
        bar (task/create-task! dyn-task-service
              {:topic topic
               :data {:op :bar}})
        baz (task/create-task! dyn-task-service
              {:topic topic
               :data {:op :baz}})]

    (facts "worker schedules all tasks and updates the status"
      (let [stop-fn (sut/worker {:task-service dyn-task-service
                                 :topic topic
                                 :pid pid
                                 :pfn test-task-handler})]

        ;; wait for task to be picked up
        ;; TODO: more non-deterministic tests, cant think of a
        ;; better option at this point, something to fix later
        (deref (promise) 2000 :timeout)

        (deref (stop-fn) 600 :didnt-complete)

        (fact "tasks complete successfully"
          @db => {:foo 1 :bar 1 :baz 2})

        (fact "task statuses are updated correctly"
          (:status (task/task dyn-task-service (:task-id foo))) => :terminated
          (:status (task/task dyn-task-service (:task-id bar))) => :failed
          (:status (task/task dyn-task-service (:task-id baz))) => :terminated)))))
