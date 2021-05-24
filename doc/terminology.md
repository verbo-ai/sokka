# Terminology

Some `sokka` terminology:

## Task
`Task` is the unit of work in Sokka.

``` clojure
{;; task-id unique id for a task
 :task-id  "7p9767c9hch7e6kt18nw9iwb1"

 :task-group-id "5a978"
 :task-description "daily products data sync - 10/05/21"
 :topic "products-sync"
 :sub-topic "default"
 :timeout-ms 10000
 :status :starting
 :data {
    :operation :sync-products
    :include :all
    :batch-size 100
  }
}
```

## Reservation
Reservation is the process of acquiring a lock/lease on a task.

## Extending Lease
For impentations that

## Worker
A worker is a thread that

## TaskQ
TaskQ is a durable queue of tasks.


* `topic` - a queue tasks.
* `sub-topic` - further grouping useful for filtering while listing tasks.
* `task` - the basic unit of work.
** `task-id` - unique id of the task.
** `task-group-id` - tasks can share a group-id to help make sense of related tasks.
** `status` - current status of a task. valid statuses are `:starting`, `:running`, `:snoozed`, `:failed` and `:terminated`.
    * `worker` - a background thread that de-queues tasks from a topic and
  executes them in another thread.
