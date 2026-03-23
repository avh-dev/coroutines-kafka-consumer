# Tasks

| Task   | Description                                                                                                                                                                                                | Status |
|--------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|  |
| CORE-1 | Implement `OffsetTracker`, tests, and JMH benchmarks.                                                                                                                                                      | DONE |
| CORE-2 | Move experiment candidates and benchmarks into a separate `coroutines-kafka-consumer-experiments` module.                                                                                                  | DONE |
| CORE-3 | Increase `OffsetTracker` test coverage to 100%.                                                                                                                                                            | DONE |
| CORE-3 | Refactor experiments and update experiments README.                                                                                                                                                        | DONE |
| CORE-4 | Make `OffsetTracker` internal and clean up related experiments.                                                                                                                                            | DONE |
| CORE-5 | Implement `ConsumerPollLoop` and supporting pieces: `CoroutineKafkaConsumerConfig`, `OverflowStrategy`, `PartitionState`, lock-free `PartitionRegistry`, and unit tests for lossy/backpressure flows.      | DONE |
| CORE-6 | Implement CoroutinesKafkaConsumer, Polish the public coroutine consumer DSL for OSS usage: document the builder API, add usage examples, and review the public surface before publication.                 | DONE |
| CORE-7 | Refactor the consumer internals after introducing `DeliveryStrategy`: align naming, simplify mode-specific flows, and prepare the codebase for telemetry integration.                                    | DONE |
| CORE-8 | Introduce a library-level telemetry API, wire it through consumer processing/polling paths, and prepare the ground for a Micrometer adapter.                                                            | DONE |
