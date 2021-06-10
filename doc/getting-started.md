# Getting Started

This quick start guide assumes that you are using the out of the box
DynamoDB implementation and that you are using [dynamodb-local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html) to run DynamoDB locally.


- Add sokka to your project.
- Create DynamoDB table required for sokka.
- Create a task handler function (called `pfn` in sokka), which pretty
  prints the value of key `:s` in the task definition.
- Start a worker that will dequeue and execute tasks from topic "foo".
- Submit a new task to topic "foo" and verify that the task executes
  as expected.

### Installation
Add sokka to your `project.clj`:

``` clojure
[ai.verbo/sokka "0.1.0-alpha1"]
```

Add the necessary requires:
``` clojure
(ns user
  (:require [verbo.sokka :as sokka]
            [verbo.sokka.impl.dynamodb-task :as dyn]
            [verbo.sokka.utils :as u]))
```


### Create the DynamoDB TaskQ table
``` clojure
(dyn/create-table
    {:cognitect-aws/client
     {:endpoint-override
      {:all
       {:region "us-east-1"
        :hostname "localhost"
        :port 7000
        :protocol :http}}}
     :tasks-table "sokka-tasks"})
```


### Function to handle the task
``` clojure
(defn foo
 [task]
 ;; perform the task
 (println
   (format "%s\n%s\n%s"
     (apply str (repeat 60 "="))
     (:s (:data task))
     (apply str (repeat 60 "="))))
 ;; return a valid response to ensure
 ;; the status of the task is updated
 ;; correctly.
 ;; to signal success:
 ;; (sokka/ok)
 ;; to signal task failure:
 ;; (sokka/failed {:error-message "error message"})
 ;; to temporarily pause the task for 10s:
 ;; (sokka/snoozed {:snooze-time 10000})
 (sokka/ok))
```

### Start the worker
``` clojure
(def foo-worker
    (sokka/worker
      {:taskq taskq
       ;; the topic to dequeue / reserve tasks from.
       :topic "foo"
       :pid "a1235" ;; the pid must be unique per worker
       :lease-time-ms 2000
       ;; function to handle the task.
       :pfn foo}))

;; to stop the worker, and wait for completion:
;; @(foo-worker)
```

### Create a new task
``` clojure
(def task
  (sokka/create-task! taskq
    {:task-id (u/rand-id)
     :task-description "say 'Hello Foo'"
     :topic "foo"
     :data {:s "Hello Foo"}}))

;; you will notice that Hello Foo is printed with a banner
;; in the REPL.
```

### Check status of the task

``` clojure
(sokka/task taskq (:task-id task))
```

### List all tasks
``` clojure
;; list tasks since 1 day ago.
(u/scroll
  (partial sokka/list-tasks taskq
    {:from (-> 1 t/days t/ago tc/to-long)
     :to (u/now)
     :topic  "foo"
     :status :terminated
      :sub-topic "default"})
  {:limit 100})
```

For a list of all available functions to query tasks, see:
(d/ai.verbo/sokka/0.0.1-alpha1/api/verbo.sokka.task#TaskStore)[TaskStore]
