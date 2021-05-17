# sokka

Task management and async utilities for Clojure.

`sokka` is the fictional character in the animation series **Avatar: The Last Airbender.**

![Sokka](https://upload.wikimedia.org/wikipedia/en/c/cc/Sokka.png)

## Objectives

sokka's main objective is to offer a light-weight background
processing library for Clojure, by providing a simple implementation
of `DurableQueue` and provide basic building blocks to run de-queued
tasks asynchronously.

`sokka` comes with a DynamoDB implementation out of the box, because
it is the primary storage for the [verbo](https://verbo.ai)
platform. However, it is possible to provide an alternative
implementation by implementing the `Task` protocol.


Main benefits include:

- Simple to integrate - does not require a standalone server, just add
  the library as a dependency to get started.
- Simple, but powerful worker implementation to run tasks
  asynchronously and in-process.
- Command line tool to create, list and manage tasks (WIP)
- Web UI to visualise and manage tasks.


## Documentation
- [Terminology](doc/terminology.md)
- [Getting Started](doc/getting-started.md)
- [Using the CLI](doc/cli.md)
- [Implementing a custom `TaskQ`](doc/custom-backend.md)

## What's next

- Improved task lifecycle - support retries and pause (hold task).
- Provide higher level features for task lifecycle management using FSM.

## License

Copyright © 2019-2021 verbo.ai - Distributed under the [Apache License v2.0](http://www.apache.org/licenses/LICENSE-2.0)
