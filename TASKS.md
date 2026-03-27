# Tasks

| Task   | Description                                                                                                                                                                                                | Status |
|--------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|
| CORE-1 | Implement `OffsetTracker`, tests, and JMH benchmarks.                                                                                                                                                      | DONE |
| CORE-2 | Move experiment candidates and benchmarks into a separate `coroutines-kafka-consumer-experiments` module.                                                                                                  | DONE |
| CORE-3 | Increase `OffsetTracker` test coverage to 100%.                                                                                                                                                            | DONE |
| CORE-3 | Refactor experiments and update experiments README.                                                                                                                                                        | DONE |
| CORE-4 | Make `OffsetTracker` internal and clean up related experiments.                                                                                                                                            | DONE |
| CORE-5 | Implement `ConsumerPollLoop` and supporting pieces: `CoroutineKafkaConsumerConfig`, `OverflowStrategy`, `PartitionState`, lock-free `PartitionRegistry`, and unit tests for lossy/backpressure flows.      | DONE |
| CORE-6 | Implement CoroutinesKafkaConsumer, Polish the public coroutine consumer DSL for OSS usage: document the builder API, add usage examples, and review the public surface before publication.                 | DONE |
| CORE-7 | Refactor the consumer internals after introducing `DeliveryStrategy`: align naming, simplify mode-specific flows, and prepare the codebase for telemetry integration.                                    | DONE |
| CORE-8 | Introduce a library-level telemetry API, wire it through consumer processing/polling paths, and prepare the ground for a Micrometer adapter.                                                            | DONE |
| CORE-9 | Add integration tests with a real Kafka broker and strengthen failure-path coverage for commit/retry/rebalance scenarios.                                                                              | DONE |
| DEMO-1 | Add a Spring Boot demo application with shared protobuf contracts, local docker-compose environment, Prometheus endpoint, and order query API for comparing CKC and Spring Kafka consumers.           | DONE |
| DEMO-2 | README added to `coroutines-kafka-consumer-demo` and `coroutines-kafka-consumer-demo-contracts`                                                                                                      | DONE |
| DEMO-3 | Extend the local demo environment with Grafana/Prometheus provisioning, a prebuilt CKC dashboard, LT-oriented WireMock profile, and improve CKC demo failure visibility in logs.                    | DONE |
| DEMO-4 | Add the initial load-test traffic generator module, wire it into the Gradle build, and make demo Kafka enablement configurable via environment variables.                                           | DONE |
