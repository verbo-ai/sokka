# Getting Started

``` clojure
[verbo-ai/sokka "0.1.0-alpha1"]
```

``` clojure
(ns user
  (:require [verbo.sokka :as sokka]
            [verbo.sokka.impl.dynamodb-task :as dyn]
            [verbo.sokka.utils :as u]))
```


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


``` clojure
(defn foo
 [sokka-quotes task]
 (println
   (format "%s\n%s\n%s"
     (apply str (repeat 60 "="))
     (:s (:data task))
     (apply str (repeat 60 "="))))
 (sokka/ok))
```

``` clojure
(def foo-worker
    (sokka/worker
      {:taskq taskq
       :topic "foo"
       :pid "a1235"
       :lease-time-ms 2000
       :pfn (partial foo sokka-quotes)}))

;; to stop the worker, and wait for completion do:
;; @(foo-worker)
```

``` clojure
(sokka/create-task! taskq
    {:task-id (u/rand-id)
     :task-description "say 'Hello Foo'"
     :topic "foo"
     :data {:s "Hello Foo"}})
```


``` clojure
;; list tasks
(u/scroll
  (partial sokka/list-tasks taskq
    {:from (-> 10 t/days t/ago tc/to-long)
     :to (u/now)
     :topic  "foo"
     :status :terminated
     :sub-topic "default"})
  {:limit 100})

```
