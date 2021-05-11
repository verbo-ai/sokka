# Terminology

Some `sokka` terminology:

* `topic` - a durable queue of similar tasks.
* `sub-topic` - further classification useful for filtering while listing tasks.
* `task` - the basic unit of work.
** `task-id` - unique id of the task.
** `task-group-id` - tasks can share a group-id to help make sense of related tasks.
** `status` - current status of a task. valid statuses are `:starting`, `:running`, `:snoozed`, `:failed` and `:terminated`.
    * `worker` - a background thread that de-queues tasks from a topic and
  executes them in another thread.
