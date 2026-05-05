# Tasks

| Task   | Description                                                                                                                                                                                           | Status |
|--------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|
| CORE-1 | Implement `OffsetTracker`, tests, and JMH benchmarks.                                                                                                                                                 | DONE |
| CORE-2 | Move experiment candidates and benchmarks into a separate `ckc-experiments` module.                                                                                             | DONE |
| CORE-3 | Increase `OffsetTracker` test coverage to 100%.                                                                                                                                                       | DONE |
| CORE-3 | Refactor experiments and update experiments README.                                                                                                                                                   | DONE |
| CORE-4 | Make `OffsetTracker` internal and clean up related experiments.                                                                                                                                       | DONE |
| CORE-5 | Implement `ConsumerPollLoop` and supporting pieces: `CoroutineKafkaConsumerConfig`, `OverflowStrategy`, `PartitionState`, lock-free `PartitionRegistry`, and unit tests for lossy/backpressure flows. | DONE |
| CORE-6 | Implement CoroutinesKafkaConsumer, Polish the public coroutine consumer DSL for OSS usage: document the builder API, add usage examples, and review the public surface before publication.            | DONE |
| CORE-7 | Refactor the consumer internals after introducing `DeliveryStrategy`: align naming, simplify mode-specific flows, and prepare the codebase for telemetry integration.                                 | DONE |
| CORE-8 | Introduce a library-level telemetry API, wire it through consumer processing/polling paths, and prepare the ground for a Micrometer adapter.                                                          | DONE |
| CORE-9 | Add integration tests with a real Kafka broker and strengthen failure-path coverage for commit/retry/rebalance scenarios.                                                                             | DONE |
| CORE-10 | Harden Micrometer telemetry tag customization by introducing a shared record-tag schema, per-consumer value binding, and Prometheus-safe fallback labels.                                            | DONE |
| CORE-11 | Refactor consumer telemetry naming toward metrics-oriented abstractions as the first step for passing consumer metrics.                                                                                | DONE |
| CORE-12 | Expand consumer metrics coverage.                                                                                                                                                                      | DONE |
| DEMO-1 | Add a Spring Boot demo application with shared protobuf contracts, local docker-compose environment, Prometheus endpoint, and order query API for comparing CKC and Spring Kafka consumers.           | DONE |
| DEMO-2 | README added to `ckc-demo` and `ckc-demo-contracts`                                                                                                       | DONE |
| DEMO-3 | Extend the local demo environment with Grafana/Prometheus provisioning, a prebuilt CKC dashboard, local LT-oriented stub support, and improve CKC demo failure visibility in logs.                    | DONE |
| DEMO-4 | Add the initial load-test traffic generator module, wire it into the Gradle build, and make demo Kafka enablement configurable via environment variables.                                             | DONE |
| DEMO-6 | Replace WireMock in the local demo environment with a dedicated lightweight `demo-stubs` service that exposes `/eta` and supports configurable latency profiles for local resiliency testing.         | DONE |
| DEMO-5 | Rename the load-test module to `ckc-demo-load-test` to make demo-related modules explicit and consistent with upcoming demo-side support services.                              | DONE |
| DEMO-7 | Demo logic updated                                                                                                                                                                                    | DONE |
| DEMO-8 | Add publish-side diagnostics to the demo load-test so AWS smoke runs expose producer acknowledgements, failures, and generator heartbeats without relying on SLF4J.                                  | DONE |
| DEMO-9 | Add stdout audit records for load-test publishes and processed demo consumer records.                                                                                                                  | DONE |
| DEMO-10 | Parameterize demo consumer runtime worker and queue settings for load-test tuning.                                                                                                                    | DONE |
| DEMO-11 | Rewrite the demo load-test generator around a shared load profile with separate lifecycle and telemetry message rates, and stop when the schedule ends.                                               | DONE |
| INFRA-1 | Add AWS runner and load-lab scaffolding for reproducible cloud load and resiliency testing.                                                                                                          | DONE |
| INFRA-2 | Restructure AWS and shared observability assets, update local environment wiring, and align packaging scripts for demo services.                                                                    | DONE |
| INFRA-3 | Split lab lifecycle from test-run orchestration, move app/stubs deployment to Helm profiles, add MSK-backed minimal lab profile, and switch the AWS runner to a public-subnet SSM-only setup without NAT. | DONE |
| INFRA-4 | Revise Grafana dashboards to reflect the updated consumer metrics and add missing runtime observability panels.                                                                                      | DONE |
| INFRA-5 | Share Kubernetes test assets across AWS and local environments, add local k8s smoke orchestration, and expose topic metrics in Prometheus/Grafana.                                                   | DONE |
| INFRA-6 | Add AWS lab remote-write observability so pod-aware metrics are stored on the runner after lab destroy.                                                                                              | DONE |
| INFRA-7 | Add local Kubernetes Fluent Bit log collection that archives audit records into a temporary local folder.                                                                                              | DONE |
| INFRA-8 | Tune the local Kubernetes baseline.                                                                                                      | DONE |
| INFRA-9 | Extract local Kubernetes manifests from setup scripts.                                                                                   | DONE |
| INFRA-10 | Add Kafka lag metrics to local Kubernetes and AWS smoke observability.                                                                   | DONE |
| INFRA-11 | Clean Redis and recreate Kafka topics during lab setup from test definition deployment settings.                                          | DONE |
| INFRA-12 | Separate local-dev, local-k8s, and AWS observability ports, then validate the local Kubernetes baseline run.                              | DONE |
| GLOBAL-1 | Shorten repository module names to `ckc-*` while preserving full published artifact names.                                              | DONE |
