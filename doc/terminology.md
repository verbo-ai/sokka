# Terminology

Some `sokka` terminology:

## Task
`Task` is the basic unit of work. Here is a quick example of what a
task looks like:

``` clojure
{:task-id  "7p9767c9hch7e6kt18nw9iwb1"
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

| Term             | Description                                                                                                     |
|------------------|-----------------------------------------------------------------------------------------------------------------|
| task-id          | Unique id for a task                                                                                            |
| task-group-id    | Tasks can be grouped together using a task-group-id.                                                            |
| task-description | Friendly description of the task.                                                                               |
| topic            | Logical grouping of tasks. Tasks that belong to a topic will be processed in FIFO order (not strictly).         |
| sub-topic        | Further classification to help organize and list tasks better.                                                  |
| status           | Current status of the task. valid statuses are `:starting`, `:running`, `:snoozed`, `:failed` and `:terminted`. |
| timeout-ms       | Time in ms after which an executing task would be cancelled and marked as failed                                |
| data             | The `task definition` that describes what needs to be done. The value must be a valid `edn`.                    |


## TaskQ
TaskQ is a durable queue that implements the [[verbo.sokka.TaskQ]]
protocol. Tasks are organized by `topic` and `sub-topic`.

## Worker
A worker is a thread that reserves (dequeues) tasks from a topic and
executes it. The worker is responsible for managing the life-cycle of
a task.

A typical life-cycle is:
- Process: reserve - execute - update status (terminate/fail/snooze)
- Keepalive: periodically extend the lease of the task.

## Reservation
Reservation is the process of acquiring a lock/lease on a task. The
actual leasing mechanism will depend on the TaskQ implementation. For
eg., the default DynamoDB implementation uses leases (time based
locks).

## Extending Lease (Keepalive)
A worker must periodically 'extend' the lease of the task that it has
reserved. Lease extension usually comprises of the following
steps:
- Check that the lease held is still valid. If the lease was revoked
  by a supervisor elsewhere, abort the current execution.
- Update the lease for the configured `lease-time`.

## Lease Supervision
Locking implementations in distributed systems are prone to issues
caused by clock skew. A process with skewed time can cause the active
and valid lease held by other processes to be revoked. This can cause
thrashing of tasks and results in the same task being attempted to run
multiple times wasting system resources and delaying sucessful task
execution.

Sokka provides an out of the box solution that solves the clock skew
issue for implementations that use time based locks (see:
`verbo.sokka.supervisor`). The solution is to maintain an in memory
list of tasks with active lease and a projected expiry time based on
the current time of the process/instance. If the task still has the
same record version after the expiry, it is considered stale and its
lease is forcefully revoked freeing it up to be reserved again.
