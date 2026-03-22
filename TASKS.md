# Tasks

| Task   | Description                                                                                                                                                                                     | Status |
|--------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| --- |
| CORE-1 | Implement `OffsetTracker`, tests, and JMH benchmarks.                                                                                                                                           | DONE |
| CORE-2 | Move experiment candidates and benchmarks into a separate `coroutines-kafka-consumer-experiments` module.                                                                                       | DONE |
| CORE-3 | Increase `OffsetTracker` test coverage to 100%.                                                                                                                                                 | DONE |
| CORE-3 | Refactor experiments and update experiments README.                                                                                                                                             | DONE |
| CORE-4 | Make `OffsetTracker` internal and clean up related experiments.                                                                                                                                 | DONE |
| CORE-5 | Implement `ConsumerPollLoop` and supporting pieces: `CoroutineKafkaConsumerConfig`, `OverflowStrategy`, `PartitionState`, lock-free `PartitionRegistry`, and unit tests for throttling/backpressure flows. | IN PROGRESS |
| CORE-6 | Polish the public coroutine consumer DSL for OSS usage: document the builder API, add usage examples, and review the public surface before publication.                                       | TODO |
