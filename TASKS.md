# Tasks

| Task   | Description                                                                                                                                                                                           | Status |
|--------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|
| [CORE-1](#core-1) | Implement `OffsetTracker`, tests, and JMH benchmarks.                                                                                                                                                 | DONE |
| [CORE-2](#core-2) | Move experiment candidates and benchmarks into a separate `ckc-experiments` module.                                                                                             | DONE |
| [CORE-3](#core-3) | Increase `OffsetTracker` test coverage to 100%.                                                                                                                                                       | DONE |
| [CORE-3](#core-3) | Refactor experiments and update experiments README.                                                                                                                                                   | DONE |
| [CORE-4](#core-4) | Make `OffsetTracker` internal and clean up related experiments.                                                                                                                                       | DONE |
| [CORE-5](#core-5) | Implement `ConsumerPollLoop` and supporting pieces: `CoroutineKafkaConsumerConfig`, `OverflowStrategy`, `PartitionState`, lock-free `PartitionRegistry`, and unit tests for lossy/backpressure flows. | DONE |
| [CORE-6](#core-6) | Implement CoroutinesKafkaConsumer, Polish the public coroutine consumer DSL for OSS usage: document the builder API, add usage examples, and review the public surface before publication.            | DONE |
| [CORE-7](#core-7) | Refactor the consumer internals after introducing `DeliveryStrategy`: align naming, simplify mode-specific flows, and prepare the codebase for telemetry integration.                                 | DONE |
| [CORE-8](#core-8) | Introduce a library-level telemetry API, wire it through consumer processing/polling paths, and prepare the ground for a Micrometer adapter.                                                          | DONE |
| [CORE-9](#core-9) | Add integration tests with a real Kafka broker and strengthen failure-path coverage for commit/retry/rebalance scenarios.                                                                             | DONE |
| [CORE-10](#core-10) | Harden Micrometer telemetry tag customization by introducing a shared record-tag schema, per-consumer value binding, and Prometheus-safe fallback labels.                                            | DONE |
| [CORE-11](#core-11) | Refactor consumer telemetry naming toward metrics-oriented abstractions as the first step for passing consumer metrics.                                                                                | DONE |
| [CORE-12](#core-12) | Expand consumer metrics coverage.                                                                                                                                                                      | DONE |
| [CORE-13](#core-13) | Add per-partition OffsetTracker capacity gauge.                                                                                                                                                        | DONE |
| [CORE-14](#core-14) | Expand offset metrics with committed offset advancement measurements.                                                                                                                                   | DONE |
| [CORE-15](#core-15) | Align the default CKC manual commit interval with Kafka's default auto-commit interval.                                                                                                                  | DONE |
| [CORE-16](#core-16) | Prototype OffsetTracker metadata payload compression candidates in experiments and benchmark them before moving a winner into core.                                                                      | DONE |
| [CORE-17](#core-17) | Add OffsetTracker snapshots and zstd-backed metadata encoding for restoring processed-but-not-committed offsets.                                                                                         | DONE |
| [CORE-18](#core-18) | Use OffsetTracker snapshot metadata during commits and assignment restore, and skip records already restored as processed.                                                                                | DONE |
| [CORE-19](#core-19) | Refactor core package layout around public API, metrics, partition, processing, Kafka, and poll-loop responsibilities.                                                                                   | DONE |
| [CORE-20](#core-20) | Rename processing mode semantics and split the current unordered record processing runtime wiring so existing modes can diverge cleanly later.                                                           | DONE |
| [CORE-21](#core-21) | Add drop metrics for freshness-first processing so intentionally discarded records are observable.                                                                                                      | DONE |
| [CORE-22](#core-22) | Move deserialization under the processing package so package layout reflects the raw-polling and typed-processing boundary.                                                                              | DONE |
| [CORE-23](#core-23) | Reorganize polling state packages and rename the Kafka consumer config adapter for clearer package boundaries.                                                                                         | DONE |
| [CORE-24](#core-24) | Add bounded at-least-once ordered processing modes for key and partition ordering.                                                                                                                       | DONE |
| [CORE-25](#core-25) | Delegate deserialization to Kafka poll loops and remove the custom worker-side deserialization pipeline.                                                                                                 | DONE |
| [CORE-26](#core-26) | Add ordering-queue gauges so key and partition contention are observable independently from the shared admission queue.                                                                                   | DONE |
| [CORE-27](#core-27) | Advance tracked offsets after terminal processing failures are handled successfully so skipped or DLT records do not block later commits.                                                               | DONE |
| [CORE-28](#core-28) | Add a freshness-first-by-key processing mode that keeps only the latest queued record per key and drops new keys when bounded admission is full.                                                        | DONE |
| [CORE-29](#core-29) | Refactor the Micrometer adapter module into clearer public API, implementation, naming, and documentation pieces.                                                                                       | DONE |
| [CORE-30](#core-30) | Add a Spring Boot starter that wires annotated CKC consumer beans from application configuration.                                                                                                        | DONE |
| [CORE-31](#core-31) | Rename the Micrometer metrics factory API to schema-oriented naming and simplify record-driven tag configuration.                                                                                        | DONE |
| [CORE-32](#core-32) | Add Spring Boot metrics configuration for Micrometer schemas, custom metrics, and annotated record-driven tag extractors.                                                                                | DONE |
| [CORE-33](#core-33) | Add Spring Boot retry schemas so consumers can share explicit ordered retry rules from application configuration.                                                                                         | DONE |
| [CORE-34](#core-34) | Add Spring Boot lifecycle phase, bounded shutdown, and lifecycle diagnostics for starter-managed CKC consumers.                                                                                            | DONE |
| [CORE-35](#core-35) | Add a minimal CKC startup banner with starter version diagnostics.                                                                                                                                       | DONE |
| [CORE-36](#core-36) | Add startup validation and diagnostics for Spring Boot starter consumer configuration.                                                                                                                    | DONE |
| [CORE-37](#core-37) | Add Spring Boot configuration metadata and document the full CKC starter property structure.                                                                                                               | DONE |
| [CORE-38](#core-38) | Add Spring Boot dispatcher definitions so starter-managed consumers can choose configured processing dispatchers.                                                                                          | DONE |
| [CORE-39](#core-39) | Add Spring Boot Actuator health indicators for starter-managed CKC consumers.                                                                                                                             | DONE |
| [CORE-40](#core-40) | Add runtime state snapshots for CKC consumers and surface them through Spring Boot health details.                                                                                                         | DONE |
| [CORE-41](#core-41) | Rename `ProcessingMode` values so config strings describe ordering and freshness behavior directly.                                                                                                       | DONE |
| [CORE-42](#core-42) | Add an optional freshness max record age so freshness-first runtimes can drop stale records before handling.                                                                                              | DONE |
| [CORE-43](#core-43) | Rename internal processing runtime classes to match the current processing mode terminology.                                                                                                              | DONE |
| [CORE-44](#core-44) | Add optional coroutine-safe MDC context for starter-managed record processing.                                                                                                                            | DONE |
| [CORE-45](#core-45) | Harden Spring Boot starter startup diagnostics, validation gaps, and lifecycle shutdown tests before the first release.                                                                                   | DONE |
| [CORE-46](#core-46) | Split the Spring Boot starter auto-configuration implementation into focused internal files without changing behavior.                                                                                    | DONE |
| [CORE-47](#core-47) | Add concise KDoc for the Spring Boot starter public API surface.                                                                                                                                          | DONE |
| [CORE-48](#core-48) | Polish Spring Boot starter metadata, documentation, demo configuration, and defaults before release.                                                                                                      | DONE |
| [CORE-49](#core-49) | Add record-count-triggered commits alongside the existing time-based commit interval for at-least-once processing modes.                                                                                  | DONE |
| [CORE-50](#core-50) | Compact oversized `OffsetTracker` ring buffers after transient out-of-order processing spikes.                                                                                                            | DONE |
| [CORE-51](#core-51) | Replace record-age telemetry with successful end-to-end record processing latency.                                                                                                                       | DONE |
| [DEMO-1](#demo-1) | Add a Spring Boot demo application with shared protobuf contracts, local docker-compose environment, Prometheus endpoint, and order query API for comparing CKC and Spring Kafka consumers.           | DONE |
| [DEMO-2](#demo-2) | README added to `ckc-demo` and `ckc-demo-contracts`                                                                                                       | DONE |
| [DEMO-3](#demo-3) | Extend the local demo environment with Grafana/Prometheus provisioning, a prebuilt CKC dashboard, local LT-oriented stub support, and improve CKC demo failure visibility in logs.                    | DONE |
| [DEMO-4](#demo-4) | Add the initial load-test traffic generator module, wire it into the Gradle build, and make demo Kafka enablement configurable via environment variables.                                             | DONE |
| [DEMO-6](#demo-6) | Replace WireMock in the local demo environment with a dedicated lightweight `demo-stubs` service that exposes `/eta` and supports configurable latency profiles for local resiliency testing.         | DONE |
| [DEMO-5](#demo-5) | Rename the load-test module to `ckc-demo-load-test` to make demo-related modules explicit and consistent with upcoming demo-side support services.                              | DONE |
| [DEMO-7](#demo-7) | Demo logic updated                                                                                                                                                                                    | DONE |
| [DEMO-8](#demo-8) | Add publish-side diagnostics to the demo load-test so AWS smoke runs expose producer acknowledgements, failures, and generator heartbeats without relying on SLF4J.                                  | DONE |
| [DEMO-9](#demo-9) | Add stdout audit records for load-test publishes and processed demo consumer records.                                                                                                                  | DONE |
| [DEMO-10](#demo-10) | Parameterize demo consumer runtime worker and queue settings for load-test tuning.                                                                                                                    | DONE |
| [DEMO-11](#demo-11) | Rewrite the demo load-test generator around a shared load profile with separate lifecycle and telemetry message rates, and stop when the schedule ends.                                               | DONE |
| [DEMO-12](#demo-12) | Add demo consumer experiment controls for processing enablement and deserialization dispatcher selection.                                                                                              | DONE |
| [DEMO-13](#demo-13) | Split demo business services into explicit blocking and suspend paths, and keep consumer handlers as thin service adapters.                                                                            | DONE |
| [DEMO-14](#demo-14) | Add comparable Spring Kafka record metrics with a consumer implementation tag shared with CKC metrics.                                                                                                  | DONE |
| [DEMO-15](#demo-15) | Move demo consumer implementation identity from every record metric into a dedicated static profile info metric.                                                                                        | DONE |
| [DEMO-16](#demo-16) | Add a blocking Confluent Parallel Consumer demo implementation with native Parallel Consumer metrics.                                                                                                    | DONE |
| [DEMO-17](#demo-17) | Keep the Confluent Parallel Consumer profile metric visible without registering CKC-style record metric beans.                                                                                            | DONE |
| [DEMO-18](#demo-18) | Enable percentile histogram buckets for Confluent Parallel Consumer processing-time metrics in the demo app.                                 | DONE |
| [DEMO-19](#demo-19) | Make the CKC order lifecycle processing mode configurable while preserving the unordered default.                                                                           | DONE |
| [DEMO-20](#demo-20) | Reorganize the demo domain around orders, batches, cauldrons, brewing steps, model clients, and latency-only consumer handling.             | DONE |
| [DEMO-21](#demo-21) | Remove the redundant demo handler layer and place order, batch, and cauldron business services into aggregate-specific packages.            | DONE |
| [DEMO-22](#demo-22) | Move demo sample event builders out of main sources and keep only test fixtures that are actually used.                                      | DONE |
| [DEMO-23](#demo-23) | Unify demo domain model classes for API and Redis state, and rename external ML clients to the `ml` package.                               | DONE |
| [DEMO-24](#demo-24) | Split demo domain model into top-level aggregate files and use direct domain class names instead of state aliases.                          | DONE |
| [DEMO-25](#demo-25) | Rename the order event consumer configuration and beans from lifecycle to order to avoid ambiguity with batch lifecycle events.              | DONE |
| [DEMO-26](#demo-26) | Rework the demo load-test generator around stable event-type traffic, state queues, fake fallback events, and time-based rate control.       | DONE |
| [DEMO-27](#demo-27) | Move suspend model clients to a shared coroutine-native HTTP transport and only create sync or suspend clients for the active demo profile.   | DONE |
| [DEMO-28](#demo-28) | Add an Armeria-backed suspend model-client transport option for comparing coroutine and Netty-based HTTP behavior in the CKC demo profile.   | DONE |
| [DEMO-29](#demo-29) | Replace the demo stubs Ktor Netty server with an Armeria server to reduce thread overhead during model-client load tests.                   | DONE |
| [DEMO-30](#demo-30) | Add in-process load-test workers with isolated shard state and a shared publisher so one JVM can use multiple CPU cores.                   | DONE |
| [DEMO-31](#demo-31) | Add demo model-call metrics for throughput and latency percentile analysis.                                                                 | DONE |
| [DEMO-32](#demo-32) | Add fixed-fleet cauldron telemetry load generation and retain demo Redis state through TTL instead of immediate deletes.                   | DONE |
| [DEMO-33](#demo-33) | Replace file-backed demo audit collection with synchronous Redis list writes for internal lab loss, duplicate, and resiliency analysis.      | DONE |
| [DEMO-34](#demo-34) | Add a CKC sync Spring profile that keeps CKC consumption while running blocking demo business services on the IO dispatcher.               | DONE |
| [DEMO-35](#demo-35) | Add demo application settings for Kafka consumer fetch batching and per-poll record limits.                                                | DONE |
| [DEMO-36](#demo-36) | Add CKC pause and resume metrics for observing demo consumer backpressure.                                                                 | DONE |
| [DEMO-37](#demo-37) | Remove the remaining Ktor demo model-client path and keep Armeria as the only suspend HTTP transport.                                      | DONE |
| [DEMO-38](#demo-38) | Add a configurable shared fixed worker dispatcher for suspend CKC demo consumers.                                                           | DONE |
| [DEMO-39](#demo-39) | Replace the demo application's embedded Tomcat server with Armeria while preserving query and Actuator endpoints.                           | DONE |
| [DEMO-40](#demo-40) | Add Redis audit analyzer read progress and per-topic result summaries.                                                                      | DONE |
| [DEMO-41](#demo-41) | Replace Spring Data Redis with direct Lettuce coroutine and synchronous commands for demo Redis access.                                      | DONE |
| [DEMO-42](#demo-42) | Add a Reactor-backed Confluent Parallel Consumer profile that runs the suspend demo business path without blocking worker threads.           | DONE |
| [DEMO-43](#demo-43) | Remove demo Redis state TTL so long-running load tests retain batch and cauldron state until the test runner resets Redis.                     | DONE |
| [DEMO-44](#demo-44) | Align external demo consumer processing modes with CKC settings and discard stale freshness-first records before business processing.          | DONE |
| [DEMO-45](#demo-45) | Persist demo-stubs runtime settings in Redis so configured latency profiles survive pod restarts during resiliency tests.                       | DONE |
| [DEMO-46](#demo-46) | Replace Redis-backed demo audit writes with compact TCP audit logging that carries run and writer identity for Fluent Bit ingestion.            | DONE |
| [DEMO-47](#demo-47) | Replace the demo app and load-test custom TCP audit senders with Logback TCP appenders, add failure audit records, and shrink the Fluent Bit JSON audit payload. | DONE |
| [DEMO-48](#demo-48) | Add an internal demo crash endpoint that flushes the TCP audit logger before forcing a hard JVM halt. | DONE |
| [DEMO-49](#demo-49) | Align Spring Kafka demo offset commit cadence with the Kafka auto-commit interval instead of committing after every poll. | DONE |
| [DEMO-50](#demo-50) | Rework the demo load-test generator to resolve missing state through delegated prerequisite event generation instead of fake fallback entities. | DONE |
| [DEMO-51](#demo-51) | Add configurable same-key brewing-step bursts to the demo load-test generator so ordered-by-key contention is observable. | DONE |
| [DEMO-52](#demo-52) | Add a legacy brewing-step registry HTTP acknowledgement path and persist registry receipts before completing step records. | DONE |
| [DEMO-53](#demo-53) | Add a naive Spring Kafka batch-listener profile that hands records to coroutine workers through bounded channels. | DONE |
| [DEMO-54](#demo-54) | Flush the Logback TCP audit appender in the final graceful-shutdown lifecycle phase, audit freshness-first drops, and surface closed-channel admission failures. | DONE |
| [DEMO-55](#demo-55) | Align demo consumer audit retry semantics and configure bounded retry rules for transient Redis and Armeria failures. | DONE |
| [DEMO-56](#demo-56) | Add comparable record age metrics and Grafana percentiles for alternative demo consumer implementations. | DONE |
| [DEMO-57](#demo-57) | Add Redis-backed cauldron telemetry event gap metrics and Grafana visibility. | DONE |
| [DEMO-58](#demo-58) | Unify external demo consumer record metrics and expose freshness-first dropped records. | DONE |
| [DEMO-59](#demo-59) | Stabilize demo Spring context tests that start Armeria on shared ports. | DONE |
| [DEMO-60](#demo-60) | Add a CKC sync Loom profile that runs blocking demo handlers on virtual threads instead of `Dispatchers.IO`. | DONE |
| [DEMO-61](#demo-61) | Add a `ckc-spring-boot` demo profile that uses the CKC Spring Boot starter for configuration-driven consumer wiring. | DONE |
| [DEMO-62](#demo-62) | Wire custom metrics for the `ckc-spring-boot` demo profile so retry and drop audit records are emitted by the starter-backed consumers. | DONE |
| [DEMO-63](#demo-63) | Add configurable demo processing dispatchers and replace the `ckc-sync-loom` Spring profile with a runtime dispatcher setting. | DONE |
| [DEMO-64](#demo-64) | Enable optional native Lettuce command metrics in the demo app and surface Redis client latency in Grafana. | DONE |
| [DEMO-65](#demo-65) | Prevent the load-test audit TCP appender from connecting when audit logging is disabled. | DONE |
| [DEMO-66](#demo-66) | Add demo profile timeline labels that include meaningful dispatcher variants. | DONE |
| [DEMO-67](#demo-67) | Add Thread Stats metrics to the demo application with bounded JVM thread groups. | DONE |
| [DEMO-68](#demo-68) | Split Armeria Thread Stats groups into client and server pools. | DONE |
| [DEMO-69](#demo-69) | Expose a demo Actuator endpoint that returns a current Thread Stats snapshot. | DONE |
| [DEMO-70](#demo-70) | Keep Armeria Actuator endpoints on the demo server port used by Kubernetes probes. | DONE |
| [DEMO-71](#demo-71) | Classify Spring Kafka and support threads in the demo Thread Stats groups. | DONE |
| [DEMO-72](#demo-72) | Classify JVM virtual-thread runtime support threads in the demo Thread Stats groups. | DONE |
| [DEMO-73](#demo-73) | Classify JVM common ForkJoinPool support threads in the demo Thread Stats groups. | DONE |
| [DEMO-74](#demo-74) | Add an experimental virtual executor option for the demo sync JDK HTTP clients. | DONE |
| [DEMO-75](#demo-75) | Add a Spring Kafka thread-pool demo profile for sync processing with bounded platform-worker pools. | DONE |
| [DEMO-76](#demo-76) | Use one demo Kafka consumer group and shared Spring Kafka listener-id prefixes across demo profiles. | DONE |
| [DEMO-77](#demo-77) | Adapt the demo Thread Stats actuator integration to the cached latest-interval starter behavior. | DONE |
| [DEMO-78](#demo-78) | Keep Spring Kafka listener ids from overriding the shared demo Kafka consumer group. | DONE |
| [DEMO-79](#demo-79) | Add a Spring Kafka virtual-thread worker profile for Thread Stats comparison screenshots. | DONE |
| [DEMO-80](#demo-80) | Add load-test Kafka producer batching controls for partition fanout experiments. | DONE |
| [DEMO-81](#demo-81) | Add selectable Armeria and JDK transports for suspend and synchronous demo HTTP clients. | DONE |
| [DEMO-82](#demo-82) | Align the demo Thread Stats actuator integration test with the current endpoint contract. | DONE |
| [DEMO-83](#demo-83) | Model instantaneous and duration-based demo chaos scenarios with automatic recovery and cleanup. | DONE |
| [DEMO-84](#demo-84) | Scale per-topic load-test Kafka producer pools from target throughput and expose producer diagnostics. | DONE |
| [DEMO-85](#demo-85) | Classify Confluent Parallel Consumer and Kafka metrics support threads as Kafka work. | DONE |
| [DEMO-86](#demo-86) | Install packet-capture tooling in the demo application and load-test container images. | DONE |
| [DEMO-87](#demo-87) | Generate incompressible random telemetry diagnostics payloads for realistic Kafka compression tests. | DONE |
| [DEMO-88](#demo-88) | Pass experiment target names into demo profile metrics and support topic-specific Kafka producer and consumer settings. | DONE |
| [DEMO-89](#demo-89) | Reconfigure load-test Kafka producer pools during a run and emit timestamped experiment events for each change. | DONE |
| [DEMO-90](#demo-90) | Remove runtime Kafka producer reconfiguration and return load-test producers to fixed per-run configuration. | DONE |
| [INFRA-1](#infra-1) | Add AWS runner and load-lab scaffolding for reproducible cloud load and resiliency testing.                                                                                                          | DONE |
| [INFRA-2](#infra-2) | Restructure AWS and shared observability assets, update local environment wiring, and align packaging scripts for demo services.                                                                    | DONE |
| [INFRA-3](#infra-3) | Split lab lifecycle from test-run orchestration, move app/stubs deployment to Helm profiles, add MSK-backed minimal lab profile, and switch the AWS runner to a public-subnet SSM-only setup without NAT. | DONE |
| [INFRA-4](#infra-4) | Revise Grafana dashboards to reflect the updated consumer metrics and add missing runtime observability panels.                                                                                      | DONE |
| [INFRA-5](#infra-5) | Share Kubernetes test assets across AWS and local environments, add local k8s smoke orchestration, and expose topic metrics in Prometheus/Grafana.                                                   | DONE |
| [INFRA-6](#infra-6) | Add AWS lab remote-write observability so pod-aware metrics are stored on the runner after lab destroy.                                                                                              | DONE |
| [INFRA-7](#infra-7) | Add local Kubernetes Fluent Bit log collection that archives audit records into a temporary local folder.                                                                                              | DONE |
| [INFRA-8](#infra-8) | Tune the local Kubernetes baseline.                                                                                                      | DONE |
| [INFRA-9](#infra-9) | Extract local Kubernetes manifests from setup scripts.                                                                                   | DONE |
| [INFRA-10](#infra-10) | Add Kafka lag metrics to local Kubernetes and AWS smoke observability.                                                                   | DONE |
| [INFRA-11](#infra-11) | Clean Redis and recreate Kafka topics during lab setup from test definition deployment settings.                                          | DONE |
| [INFRA-12](#infra-12) | Separate local-dev, local-k8s, and AWS observability ports, then validate the local Kubernetes baseline run.                              | DONE |
| [INFRA-13](#infra-13) | Aggregate Grafana dashboard metric rates over 30s windows for clearer local-dev observability.                                            | DONE |
| [INFRA-14](#infra-14) | Replace snapshot-based worker utilization dashboard panels with busy-time utilization queries.                                             | DONE |
| [INFRA-15](#infra-15) | Add a lightweight k3s internal lab for running the demo app on a dedicated Linux host with host-managed Kafka, Redis, Grafana, and stubs.   | DONE |
| [INFRA-16](#infra-16) | Move local-dev Kafka topic creation from compose into explicit helper scripts.                                                             | DONE |
| [INFRA-17](#infra-17) | Reorganize the Grafana consumer dashboard around lifecycle and telemetry record metrics that work for CKC and Spring Kafka.                | DONE |
| [INFRA-18](#infra-18) | Rework the Grafana dashboard around the dedicated demo consumer profile info metric and record metrics without implementation tags.        | DONE |
| [INFRA-19](#infra-19) | Centralize temporary files created by demo infrastructure scripts under the root `.demo-infra` directory.                                  | DONE |
| [INFRA-20](#infra-20) | Remove the unsupported local Kubernetes lab and keep local development focused on the local-dev environment.                                | DONE |
| [INFRA-21](#infra-21) | Add a standalone Wake-on-LAN helper for the internal lab host.                                                                              | DONE |
| [INFRA-22](#infra-22) | Update Grafana dashboards to compare consumers by Spring profile and add Confluent Parallel Consumer metric panels.                         | DONE |
| [INFRA-23](#infra-23) | Add Confluent Parallel Consumer offset encoder metrics and clearer shard/partition panels to the Grafana dashboard.                          | DONE |
| [INFRA-24](#infra-24) | Clean up the Grafana dashboard for load-test result analysis with consistent time-series panels.                                             | DONE |
| [INFRA-25](#infra-25) | Wire lifecycle processing mode through demo Helm values and add ordered-runtime-oriented CKC dashboard panels.                                | DONE |
| [INFRA-26](#infra-26) | Update demo infrastructure topics, Helm wiring, scripts, and dashboards for order, batch, and cauldron aggregate event streams.              | DONE |
| [INFRA-27](#infra-27) | Switch the local-dev Kafka broker to Redpanda while preserving the existing local Kafka endpoint and topic helper workflow.                   | DONE |
| [INFRA-28](#infra-28) | Add local-dev scripts and env profiles for running and stopping load-test and demo-stubs processes from `.demo-infra`.                       | DONE |
| [INFRA-29](#infra-29) | Switch the internal lab broker to Redpanda and run lab load tests through an interactive local process script.                                | DONE |
| [INFRA-30](#infra-30) | Add internal-lab host service CPU observability and restore Redpanda-backed Kafka lag metrics.                                                | DONE |
| [INFRA-31](#infra-31) | Increase internal-lab Redpanda and Redis resource limits for high-partition consumer load tests.                                                | DONE |
| [INFRA-32](#infra-32) | Refresh the Grafana dashboard with clearer host service CPU, Kafka offset lag, batch stream, pod CPU, and recently added metric coverage.         | DONE |
| [INFRA-33](#infra-33) | Move internal-lab image building and load-test execution onto the lab host while keeping local Gradle builds for uncommitted changes.              | DONE |
| [INFRA-34](#infra-34) | Add Helm and load-test definition support for the `ckc-sync` demo profile.                                                                         | DONE |
| [INFRA-35](#infra-35) | Add Grafana visibility for CKC backpressure pause and resume events.                                                                               | DONE |
| [INFRA-36](#infra-36) | Mount internal-lab Grafana dashboards from the synced workspace copy.                                                                              | DONE |
| [INFRA-37](#infra-37) | Wire Kafka consumer fetch settings through Helm and tune lab profiles to 8 KiB fetch minimum with 250 ms max wait.                                 | DONE |
| [INFRA-38](#infra-38) | Split internal-lab deployment profiles from load-test definitions and configure reusable stubs before each run.                                  | DONE |
| [INFRA-39](#infra-39) | Add interactive internal-lab audit logging and consumer metrics implementation settings for performance comparisons.                              | DONE |
| [INFRA-40](#infra-40) | Add an interactive internal-lab CKC worker dispatcher thread setting for suspend-consumer experiments.                                            | DONE |
| [INFRA-41](#infra-41) | Group demo Helm deployment profiles by environment and keep AWS and internal-lab runners scoped to their own profile directories.                   | DONE |
| [INFRA-42](#infra-42) | Show native Parallel Consumer metrics for both blocking and Reactor-backed Confluent demo profiles in Grafana.                                      | DONE |
| [INFRA-43](#infra-43) | Update internal-lab images incrementally and restart demo stubs only when their image changes.                                                       | DONE |
| [INFRA-44](#infra-44) | Add dedicated Fluent Bit audit ingestion, archive wiring, and fail-fast test orchestration for internal-lab and future EKS runs.                   | DONE |
| [INFRA-45](#infra-45) | Update the internal-lab audit archive and analyzer flow for compact `P/C/F` records, including fresh live-log cleanup before each run.             | DONE |
| [INFRA-46](#infra-46) | Fix internal-lab Redpanda observability and drain-wait scripts so Kafka lag checks work cleanly after the broker migration.                        | DONE |
| [INFRA-47](#infra-47) | Split internal-lab audit archives into completed chunks and analyze audit data incrementally while local lab tests are running.                    | DONE |
| [INFRA-48](#infra-48) | Restore the internal-lab Redpanda CPU dashboard panel using native Redpanda public metrics.                                                       | DONE |
| [INFRA-49](#infra-49) | Clamp the Redpanda CPU dashboard query so public-metrics counter spikes cannot display physically impossible millicore values.                    | DONE |
| [INFRA-50](#infra-50) | Add audit analyzer checks for terminal processing order by partition and by message key.                                                          | DONE |
| [INFRA-51](#infra-51) | Optimize internal-lab audit analyzer memory use and set short rolling retention for load-test Kafka topics.                                      | DONE |
| [INFRA-52](#infra-52) | Use process-exporter CPU metrics for the internal-lab Redpanda dashboard panel to avoid native busy-counter stop spikes.                         | DONE |
| [INFRA-53](#infra-53) | Reduce internal-lab audit analyzer CPU cost and benchmark the changes on real optilab audit chunks.                                             | DONE |
| [INFRA-54](#infra-54) | Restore full per-topic YAML audit summaries while keeping audit analysis off the load-test critical path.                                        | DONE |
| [INFRA-55](#infra-55) | Refactor lab infrastructure entrypoints, AWS runner workflow, Helm ownership, and shared audit analysis assets.                                  | DONE |
| [INFRA-56](#infra-56) | Add audit fairness metrics and telemetry-focused lab profiles for comparing freshness-first processing modes.                                   | DONE |
| [INFRA-57](#infra-57) | Add test-definition chaos steps and simplify internal-lab audit collection around Fluent Bit file output.                                      | DONE |
| [INFRA-58](#infra-58) | Reorganize the internal-lab server runtime layout by responsibility instead of source asset ownership.                                        | DONE |
| [INFRA-59](#infra-59) | Add internal-lab test bundles for running sequential comparison scenarios across deployment profiles.                                        | DONE |
| [INFRA-60](#infra-60) | Add audit freshness gap distributions so lossy processing modes show how many dropped records precede each processed record.                 | DONE |
| [INFRA-61](#infra-61) | Extend internal-lab Prometheus retention so Grafana keeps recent lab metrics for multi-day comparisons.                                      | DONE |
| [INFRA-62](#infra-62) | Remove native Parallel Consumer record metrics from the shared Grafana dashboard.                                                           | DONE |
| [INFRA-63](#infra-63) | Add internal-lab deployment and comparison-bundle support for the `ckc-sync-loom` demo profile.                                           | DONE |
| [INFRA-64](#infra-64) | Show selected application pod CPU and memory series alongside sum and average aggregates in Grafana.                                      | DONE |
| [INFRA-65](#infra-65) | Add internal-lab network and host-service chaos steps for Redis and Kafka resiliency scenarios.                                          | DONE |
| [INFRA-66](#infra-66) | Add a Grafana dashboard variable for toggling event-type metric aggregation.                                                             | DONE |
| [INFRA-67](#infra-67) | Add internal-lab Helm and test-definition support for the `ckc-spring-boot` demo profile.                                               | DONE |
| [INFRA-68](#infra-68) | Add selectable internal-lab Kafka broker implementation and broker CPU observability.                                                   | DONE |
| [INFRA-69](#infra-69) | Add a dynamic internal-lab run planner that computes topics, concurrency, and processing modes from profile and load settings.          | DONE |
| [INFRA-70](#infra-70) | Simplify internal-lab Helm profiles around generated run plans and remove static topic partition settings from legacy overlays.        | DONE |
| [INFRA-71](#infra-71) | Make internal-lab run replica count selectable before dynamic plan generation.                                                          | DONE |
| [INFRA-72](#infra-72) | Make internal-lab demo-stubs replica count selectable per test run.                                                                    | DONE |
| [INFRA-73](#infra-73) | Refine the shared Grafana overview with a common throughput panel and clearer event/command aggregation toggle.                       | DONE |
| [INFRA-74](#infra-74) | Add an internal-lab test definition that steps load upward in ten-percent increments.                                                  | DONE |
| [INFRA-75](#infra-75) | Fix the Grafana event breakdown variable so it exposes only split and aggregated choices.                                             | DONE |
| [INFRA-76](#infra-76) | Add persistent internal-lab pod log collection with Loki and Grafana Explore support.                                                | DONE |
| [INFRA-77](#infra-77) | Move internal-lab single-run and bundle artifacts into a results directory layout.                                                   | DONE |
| [INFRA-78](#infra-78) | Use the selected internal-lab test definition's base TPS instead of reusing stale deployment state.                                  | DONE |
| [INFRA-79](#infra-79) | Add an internal-lab result exporter that packages run and bundle artifacts with Loki logs.                                          | DONE |
| [INFRA-80](#infra-80) | Add a local restore workflow for browsing exported internal-lab Loki logs in Grafana.                                               | DONE |
| [INFRA-81](#infra-81) | Export internal-lab Prometheus metrics and add a one-command restore viewer for metrics and Loki logs.                             | DONE |
| [INFRA-82](#infra-82) | Continue internal-lab bundles after failed tests so negative results stay in bundle summaries and exports.                         | DONE |
| [INFRA-83](#infra-83) | Export result directories with bundle-scoped metrics, restore archives, summaries, and separate audit files.                       | DONE |
| [INFRA-84](#infra-84) | Print a direct Grafana dashboard URL with the exported experiment time range from restore runs.                                    | DONE |
| [INFRA-85](#infra-85) | Replace internal-lab bundles with experiment definitions built from explicit targets and planning latencies.                         | DONE |
| [INFRA-86](#infra-86) | Prebuild Loki restore data during internal-lab result export so local restore starts without log import.                            | DONE |
| [INFRA-87](#infra-87) | Replace the dispatcher experiment with a consumer capacity comparison that includes Confluent Parallel Consumer Reactor and Spring Kafka. | DONE |
| [INFRA-88](#infra-88) | Add experiment target Helm overrides for demo pod JVM and resource tuning.                                                         | DONE |
| [INFRA-89](#infra-89) | Add an internal-lab queue capacity experiment for CKC backpressure and Spring Kafka coroutine-naive comparison.                    | DONE |
| [INFRA-90](#infra-90) | Add Grafana panels for Thread Stats group CPU, allocation, and thread-count metrics. | DONE |
| [INFRA-91](#infra-91) | Apply the Grafana pod filter to Thread Stats dashboard panels. | DONE |
| [INFRA-92](#infra-92) | Restart the internal-lab demo deployment when `update-lab.sh` reloads the demo image. | DONE |
| [INFRA-93](#infra-93) | Improve interactive internal-lab `run-test.sh` defaults for dispatcher and planning-latency selection. | DONE |
| [INFRA-94](#infra-94) | Expose the demo sync JDK HTTP client executor mode in internal-lab run-test and experiments. | DONE |
| [INFRA-95](#infra-95) | Add internal-lab planner and experiment support for the Spring Kafka thread-pool demo profile. | DONE |
| [INFRA-96](#infra-96) | Use one demo Kafka consumer group across internal-lab and AWS observability wiring. | DONE |
| [INFRA-97](#infra-97) | Collect per-pod Thread Stats text snapshots during internal-lab runs. | DONE |
| [INFRA-98](#infra-98) | Make Apache Kafka the default internal-lab broker implementation while keeping Redpanda selectable. | DONE |
| [INFRA-99](#infra-99) | Attach the local Thread Stats Java agent to internal-lab Apache Kafka and add broker thread panels. | DONE |
| [INFRA-100](#infra-100) | Deploy Armeria HTTP-client defaults to optilab and prepare focused one-off comparison experiments. | DONE |
| [INFRA-101](#infra-101) | Add compact detailed and category-stacked Thread Stats dashboard panels. | DONE |
| [INFRA-102](#infra-102) | Replace successful record-age Grafana panels with end-to-end processing latency panels. | DONE |
| [INFRA-103](#infra-103) | Collect demo process context-switch metrics and expose them in the shared Grafana dashboard. | DONE |
| [INFRA-104](#infra-104) | Align application Thread Stats Grafana panels with the unified metric schema. | DONE |
| [INFRA-105](#infra-105) | Isolate application Thread Stats Grafana queries from the Kafka broker agent job. | DONE |
| [INFRA-106](#infra-106) | Generate local human-readable Markdown and SVG reports for completed internal-lab experiments. | DONE |
| [INFRA-107](#infra-107) | Render semantic chaos events and intervals on experiment load-profile timelines. | DONE |
| [INFRA-108](#infra-108) | Wire scaled load-test producer settings and Kafka producer metrics into the internal lab. | DONE |
| [INFRA-109](#infra-109) | Add a focused CKC experiment comparing coroutine worker reserves across fixed and single-carrier virtual-thread dispatchers under large Kafka poll batches. | DONE |
| [INFRA-112](#infra-112) | Store full Thread Stats snapshots as timestamped per-pod diagnostic artifacts during internal-lab runs. | DONE |
| [INFRA-113](#infra-113) | Add scheduled packet captures for application and load-test traffic in local and cloud lab runs. | DONE |
| [INFRA-114](#infra-114) | Analyze Kafka packet captures and compare producer and consumer protocol traffic in experiment reports. | DONE |
| [INFRA-115](#infra-115) | Compare uncompressed and LZ4 Kafka traffic for partition-parallel Spring Kafka and worker-parallel CKC consumers. | DONE |
| [INFRA-116](#infra-116) | Repeat the Kafka compression comparison with random and zero-length telemetry diagnostics payloads. | DONE |
| [INFRA-117](#infra-117) | Allow experiments to inherit, override, or fully define their test configuration and persist the resolved test artifact. | DONE |
| [INFRA-118](#infra-118) | Record actual experiment events and expose them in reports, live Grafana annotations, and restored result bundles. | DONE |
| [INFRA-119](#infra-119) | Add a Spring Kafka experiment sweeping producer linger from 0 to 1000 ms with LZ4 and no compression. | DONE |
| [INFRA-120](#infra-120) | Make Grafana experiment annotations opt-in and replace runtime linger sweeps with fixed five-minute targets. | DONE |
| [INFRA-121](#infra-121) | Publish one informative Grafana annotation at the start of every test run while keeping detailed event annotations optional. | DONE |
| [INFRA-122](#infra-122) | Reduce run-start Grafana annotations to the target values that distinguish experiment runs. | DONE |
| [INFRA-123](#infra-123) | Use explicit run annotation labels and emit only the annotation type as a Grafana tag. | DONE |
| [INFRA-124](#infra-124) | Interleave uncompressed and LZ4 linger targets for adjacent like-for-like comparison. | DONE |
| [INFRA-125](#infra-125) | Add a checkout-local, ephemeral AWS experiment smoke workflow with portable artifacts and verified cleanup. | DONE |
| [INFRA-126](#infra-126) | Share the mature result-bundle pipeline across internal-lab and AWS with environment-aware dashboards and complete cloud telemetry. | DONE |
| [INFRA-127](#infra-127) | Run a 20-minute, 10k/s CKC AWS capacity test on MSK and ElastiCache with a single-thread processing dispatcher. | DONE |
| [INFRA-128](#infra-128) | Unify experiment definitions and orchestration across internal-lab and AWS behind shared environment adapters. | IN_PROGRESS |
| [GLOBAL-1](#global-1) | Shorten repository module names to `ckc-*` while preserving full published artifact names.                                              | DONE |
| [GLOBAL-2](#global-2) | Separate production modules from demo, demo infrastructure, and experiment code in the repository layout.                                | DONE |
| [DOC-1](#doc-1) | Add a documentation task scope for repository documentation, task history, working rules, and project notes. | DONE |
| [DOC-2](#doc-2) | Expand `TASKS.md` with linked task entries and retrospective implementation notes restored from git history and code changes. | DONE |
| [DOC-3](#doc-3) | Add the initial OSS README with positioning, related projects, and alternative comparison boundaries. | DONE |
| [DOC-4](#doc-4) | Refine the `ckc-micrometer` README wording around ConsumerMetrics, tag customization, filtering, and histograms. | DONE |
| [DOC-5](#doc-5) | Remove the alternatives and related-projects section from the main README while OSS positioning is still being refined. | DONE |
| [DOC-6](#doc-6) | Update the main README module list so it matches the current repository modules. | DONE |

## Task Details

<a id="core-1"></a>
### CORE-1 - Implement `OffsetTracker`, tests, and JMH benchmarks

_Date: 2025-12-28_

Implemented the first offset tracking abstraction in the core module.
The task added the production `OffsetTracker` plus reference and candidate implementations used to compare behavior and performance.
Coverage was built around shared test fixtures, and JMH benchmarks were added for single-threaded and concurrent offset tracking scenarios.

<a id="core-2"></a>
### CORE-2 - Move experiment candidates and benchmarks into `ckc-experiments`

_Date: 2025-12-29_

Moved non-production offset tracker variants, their tests, and JMH benchmarks out of the core module.
The core module kept the production tracker and its focused tests, while experimental candidates moved into a dedicated experiments module.
Gradle settings and module build files were updated so benchmark and experiment code no longer shaped the library runtime surface.

<a id="core-3"></a>
### CORE-3 - Increase `OffsetTracker` coverage and refactor experiments

_Date: 2026-01-01_

Expanded `OffsetTracker` testing to cover the production behavior more completely through shared fixtures.
The experiments package layout was cleaned up so benchmark and candidate tracker code lived under explicit experiment namespaces.
The experiments README was updated to document the purpose of the module and the offset tracker variants it contains.

<a id="core-4"></a>
### CORE-4 - Make `OffsetTracker` internal and clean up experiments

_Date: 2026-01-11_

Changed `OffsetTracker` from public API candidate to an internal implementation detail of the library.
Experiment code and tests were adjusted around that boundary so alternate trackers remained available for comparison without leaking into the public contract.
This matched the pre-release API stance: keep implementation pieces movable until the consumer API stabilized.

<a id="core-5"></a>
### CORE-5 - Implement `ConsumerPollLoop` and partition runtime pieces

_Date: 2026-03-17_

Introduced the initial task tracker and then built the lower-level consumer runtime pieces over several commits.
The work added consumer configuration types, overflow strategy support, partition state management, and a lock-free partition registry.
`ConsumerPollLoop` was implemented with tests for backpressure and lossy delivery flows, giving the later public consumer a tested runtime core.

<a id="core-6"></a>
### CORE-6 - Implement `CoroutinesKafkaConsumer` and builder API

_Date: 2026-03-22_

Added the public coroutine consumer facade and builder-oriented configuration surface.
The implementation connected deserializer factories, record processors, retry policy, config adaptation, and the poll loop into one usable API.
Unit tests were added around the builder, consumer facade, deserialization path, record processing, and supporting fixtures.

<a id="core-7"></a>
### CORE-7 - Refactor internals around `DeliveryStrategy`

_Date: 2026-03-23_

Renamed the earlier overflow terminology to delivery-oriented naming and replaced throttling/lossy concepts with clearer strategy names.
The refactor touched config adaptation, poll loop behavior, builder options, record processing, and tests.
This reduced mode-specific naming noise before telemetry and metrics were layered into the runtime.

<a id="core-8"></a>
### CORE-8 - Add consumer telemetry API

_Date: 2026-03-23_

Introduced a library-level telemetry hook that could observe polling and processing without binding core to a metrics backend.
The telemetry interface was wired through `ConsumerPollLoop`, `RecordProcessor`, `CoroutinesKafkaConsumer`, and the builder.
Tests and telemetry fixtures were added so later Micrometer integration could be implemented against a stable internal event model.

<a id="core-9"></a>
### CORE-9 - Add Kafka integration tests and Micrometer adapter

_Date: 2026-03-24_

Added integration testing against a real Kafka broker to exercise consumer behavior beyond unit-level poll loop simulations.
The tests strengthened coverage around end-to-end consumption, commit behavior, retry behavior, and rebalance-sensitive paths.
The task also introduced the first Micrometer adapter module and tests, proving the telemetry API could be exported as backend metrics.

<a id="demo-1"></a>
### DEMO-1 - Add Spring Boot demo application and local environment

_Date: 2026-03-26_

Created the demo application and shared protobuf contracts for order lifecycle and cauldron telemetry events.
The demo added CKC and Spring Kafka consumer profiles, Redis-backed state, an ETA model client, Prometheus metrics, and an order query API.
The local docker-compose environment supplied Kafka, Redis, and the model stub needed to compare consumer implementations locally.

<a id="demo-2"></a>
### DEMO-2 - Add demo and contracts README files

_Date: 2026-03-26_

Documented the demo application module and the shared contracts module.
The README updates explained how the demo pieces fit together and how generated protobuf contracts are shared by producers and consumers.
This made the demo usable without reading the full Spring Boot and Gradle configuration first.

<a id="demo-3"></a>
### DEMO-3 - Add local observability and load-test stub profile

_Date: 2026-03-27_

Extended the local environment with Prometheus and Grafana provisioning.
A CKC overview dashboard was added alongside Prometheus scrape configuration and a load-test-oriented WireMock mapping.
The demo configuration and README were updated so local failure visibility and observability setup were part of the normal demo workflow.

<a id="demo-4"></a>
### DEMO-4 - Add initial load-test generator module

_Date: 2026-03-27_

Added the first load-test generator module with its own Gradle build and README.
The module generated lifecycle and telemetry traffic using a scenario parser, state machine, traffic generator, shard context, and Kafka producers.
Tests covered scenario parsing, lifecycle state transitions, and shard behavior, while the demo app gained environment-driven Kafka enablement.

<a id="demo-5"></a>
### DEMO-5 - Rename load-test module to `ckc-demo-load-test`

_Date: 2026-03-27_

Renamed the load-test module so demo-related modules followed the same explicit naming pattern.
Gradle settings, module paths, source layout, tests, and README references were moved under the demo load-test name.
The change prepared the repository for additional demo-side support services without ambiguous module ownership.

<a id="demo-6"></a>
### DEMO-6 - Replace WireMock with `demo-stubs` service

_Date: 2026-03-27_

Added a dedicated lightweight `demo-stubs` Spring Boot service to replace the local WireMock ETA stub.
The service exposed configurable `/eta` behavior with delay sampling, latency profiles, Docker packaging, and tests.
Local environment wiring and demo documentation were updated so resiliency testing used the same service shape locally and in future lab setups.

<a id="demo-7"></a>
### DEMO-7 - Update demo logic

_Date: 2026-04-23_

Reworked demo business logic after the first local and lab-oriented usage.
The changes focused on the demo domain flow around order state, ETA recalculation, model normalization, and consumer behavior.
The task made the sample application more representative for later load and resiliency tests, even though the commit messages were intentionally broad.

<a id="infra-1"></a>
### INFRA-1 - Add AWS runner and load-lab scaffolding

_Date: 2026-04-23_

Added the first AWS-oriented load lab and runner infrastructure.
The task introduced Terraform, runner scripts, ECR support, observability bootstrap pieces, and command wrappers for Linux and Windows.
It gave the project a reproducible cloud path for building images, creating a runner, deploying a lab, running tests, and tearing the setup down.

<a id="core-10"></a>
### CORE-10 - Harden Micrometer telemetry tag customization

_Date: 2026-04-27_

Hardened Micrometer tag handling by introducing a shared record tag schema.
The adapter bound tag values per consumer and added fallback labels that are safe for Prometheus-style metric storage.
Tests were updated to cover customized tags and missing-label behavior, reducing the risk of metric cardinality or label-shape surprises.

<a id="demo-8"></a>
### DEMO-8 - Add load-test publish diagnostics

_Date: 2026-04-27_

Added publish-side diagnostics to the demo load-test producer path.
The generator began exposing producer acknowledgements, failures, and heartbeat-style progress through stdout-friendly diagnostics.
This made AWS smoke runs debuggable even when relying on collected process output instead of normal application logs.

<a id="infra-2"></a>
### INFRA-2 - Restructure AWS and shared observability assets

_Date: 2026-04-29_

Reorganized early AWS lab assets into a clearer infrastructure layout.
The diff moved shared Grafana, Prometheus, Helm, Terraform, runner, and test-definition files toward the structure used by later local and AWS labs.
Packaging scripts and Docker build files for demo services were aligned so the same service images could be reused across environments.

<a id="infra-3"></a>
### INFRA-3 - Split lab lifecycle from test-run orchestration

_Date: 2026-04-29_

Separated environment lifecycle operations from test execution orchestration.
The infrastructure moved app and stub deployment into Helm profiles, added an MSK-backed minimal lab profile, and revised runner wiring.
The AWS setup shifted toward a public-subnet, SSM-only runner design without NAT, lowering lab complexity while keeping remote control available.

<a id="core-11"></a>
### CORE-11 - Refactor telemetry naming toward metrics abstractions

_Date: 2026-04-29_

Renamed and reshaped consumer telemetry abstractions into metrics-oriented contracts.
Core code, tests, and the Micrometer module were updated around `ConsumerMetrics` naming and responsibilities.
This provided a cleaner base for runtime counters, gauges, and timing measurements added in the following task.

<a id="core-12"></a>
### CORE-12 - Expand consumer metrics coverage

_Date: 2026-04-29_

Added broader runtime metric coverage for consumer behavior.
The changes introduced runtime stats and additional measurements while reducing metric cardinality where raw record-level labels were too expensive.
Core tests and Micrometer tests were updated to validate the new metric surface before dashboards were revised against it.

<a id="core-13"></a>
### CORE-13 - Add OffsetTracker capacity metric

_Date: 2026-05-12_

Expose OffsetTracker ring bit capacity as a per-partition gauge.
Wire the value through the existing consumer metrics surface so Micrometer can export it with topic and partition labels.
Add focused tests around the reported capacity and gauge lifecycle.

<a id="core-14"></a>
### CORE-14 - Expand offset metrics

_Date: 2026-05-13_

Track how many offset positions each explicit commit attempt advances across committed partitions.
Expose that advancement through the existing consumer metrics event and Micrometer summary surface.
Keep the metric tied to commit success labels so failed and successful attempts can be inspected separately.

<a id="core-15"></a>
### CORE-15 - Align default commit interval

_Date: 2026-05-14_

Reduce the default CKC backpressure commit interval from 60 seconds to 5 seconds.
Keep the default aligned with Kafka's `auto.commit.interval.ms` default while preserving explicit CKC manual commit configuration.
Update test helpers so core tests inherit the same default interval as the public builder.

<a id="core-16"></a>
### CORE-16 - Prototype OffsetTracker metadata compression

_Date: 2026-05-18_

Prototype compact encodings for processed-but-not-committed offset bitsets in the experiments module.
Compare raw bitset storage with custom RLE variants before selecting a metadata payload format for core.
Document the experiment intent and JMH workflow so compression candidates can be evaluated reproducibly.

<a id="core-17"></a>
### CORE-17 - Add OffsetTracker snapshot metadata encoding

_Date: 2026-05-18_

Add an internal OffsetTracker snapshot representation for the tracker ring state.
Introduce a focused offset tracker snapshot serializer using raw storage for small payloads and zstd for larger snapshots.
Restore OffsetTracker state from decoded snapshots while keeping compression outside the tracker lock.
Move direct offset tracking, snapshot serialization, and offset contract tests into `avh.ckc.core.offset`.

<a id="core-18"></a>
### CORE-18 - Use OffsetTracker commit metadata

_Date: 2026-05-19_

Commit OffsetTracker snapshots in Kafka offset metadata for backpressure consumers.
Restore partition trackers from committed metadata during assignment when metadata is available.
Skip records that were already processed according to restored tracker state.
Cover the behavior with unit and Kafka integration tests.

<a id="core-19"></a>
### CORE-19 - Refactor core package layout

_Date: 2026-05-20_

Split the core module package layout by responsibility while keeping the root package focused on the primary consumer API.
Move metrics, partition state, processing, Kafka plumbing, and poll-loop internals in small reviewable steps.
Keep each migration step isolated so imports, tests, and public package choices can be reviewed before committing.

<a id="core-20"></a>
### CORE-20 - Split current processing runtimes

_Date: 2026-05-21_

Renamed the public processing enum from delivery-oriented terminology to `ProcessingMode`.
The existing backpressure and lossy modes became `AT_LEAST_ONCE_NO_ORDERING` and `FRESHNESS_FIRST_DROP_OLDEST`.
The current channel-backed implementation was renamed to `UnorderedRecordProcessingRuntime`, with mode-specific overflow behavior selected at runtime creation.
Future ordered and keyed freshness modes will be added with their dedicated runtime implementations in follow-up tasks.

<a id="core-21"></a>
### CORE-21 - Add freshness-first drop metrics

_Date: 2026-05-22_

Add observability for records intentionally discarded by freshness-first processing.
Pinned down channel overflow behavior with a focused test before wiring production metrics.
Added `ConsumerMetrics.onRecordDropped` and a Micrometer `ckc.record.dropped` counter tagged by topic.
Split unordered runtime implementations so freshness-first owns drop accounting while shutdown and cancellation cleanup stay separate from intentional drops.

<a id="core-22"></a>
### CORE-22 - Move deserialization under processing

_Date: 2026-05-22_

Moved deserialization support into the processing package hierarchy.
The package layout now reflects the runtime model: poll loops fetch raw Kafka records, while processing workers deserialize keys and values before invoking handlers.
Updated imports across core runtime, processor, fixtures, and tests.
Also documented the pre-release API stance in `AGENTS.md` so package and class names can be reshaped while the library has no published release.

<a id="core-23"></a>
### CORE-23 - Reorganize polling state packages

_Date: 2026-05-22_

Moved partition and offset-tracking internals under the polling package hierarchy.
Renamed the raw Kafka consumer config adapter to `KafkaConsumerConfigAdapter` and moved it to the Kafka package so it is not confused with CKC library configuration.
Updated core runtime, polling, processing, test fixtures, and focused tests without changing behavior.

<a id="core-24"></a>
### CORE-24 - Add bounded ordered processing modes

_Date: 2026-05-22_

Implement at-least-once ordered processing modes backed by a shared bounded runtime.
Support ordering by raw Kafka key and by Kafka topic partition while preserving parallelism across independent ordering keys.
Keep all accepted records under one bounded admission budget so backpressure reaches the poll loop without hidden per-key unbounded queues.
Covered same-key ordering, partition ordering, cross-key parallelism, factory wiring, and admission backpressure with focused core tests.

<a id="core-25"></a>
### CORE-25 - Use Kafka poll-loop deserialization

_Date: 2026-05-30_

Delegate key and value deserialization to the standard Kafka consumer poll path.
Remove the custom worker-side deserialization pipeline and its per-record dispatcher handoff.
Pass typed records through bounded processing runtimes while preserving backpressure, ordering, and offset tracking.
Remove obsolete demo dispatcher settings, environment wiring, and documentation.

<a id="infra-4"></a>
### INFRA-4 - Revise Grafana dashboards for consumer metrics

_Date: 2026-04-29_

Updated the Grafana overview dashboard to match the renamed and expanded consumer metrics.
Panels were added or revised for runtime observability, including consumer activity and worker-oriented views.
This brought local and lab dashboards back in sync with the metrics emitted by the core and Micrometer modules.

<a id="infra-5"></a>
### INFRA-5 - Share Kubernetes test assets and add local k8s baseline

_Date: 2026-04-30_

Shared Helm charts, Grafana dashboards, and test definitions between AWS and local Kubernetes environments.
Added local Kubernetes smoke orchestration and baseline test assets so the same demo workloads could run outside AWS.
Prometheus and Grafana wiring gained topic-level visibility, improving the ability to compare app metrics with Kafka-side behavior.

<a id="infra-6"></a>
### INFRA-6 - Add AWS remote-write observability

_Date: 2026-04-30_

Added remote-write observability for AWS lab runs so metrics could survive lab teardown.
The runner gained Prometheus configuration templates, compose wiring, and scripts that capture pod-aware metrics on the runner.
AWS docs and Terraform outputs were updated to explain the new observability flow and its cleanup behavior.

<a id="demo-9"></a>
### DEMO-9 - Add stdout audit records for publishes and processed records

_Date: 2026-05-01_

Added structured stdout audit records for load-test publishes and processed demo consumer records.
The load-test producer path gained `LoadTestAuditLog`, while the demo application gained an `AuditLog` used by CKC and Spring Kafka consumers.
Helm values and orchestration scripts were updated so audit output could be enabled and collected consistently in lab runs.

<a id="infra-7"></a>
### INFRA-7 - Add local Kubernetes Fluent Bit audit log archive

_Date: 2026-05-01_

Added Fluent Bit log collection for the local Kubernetes lab.
The setup archived audit records into a temporary local folder so publish and consume traces could be inspected after a run.
Local k8s scripts and README guidance were updated to create the archive plumbing as part of lab setup.

<a id="demo-10"></a>
### DEMO-10 - Parameterize demo consumer runtime settings

_Date: 2026-05-01_

Exposed demo consumer worker and queue settings through application properties and Helm values.
Both CKC and Spring Kafka profile wiring were updated so load-test tuning could be changed without code edits.
Tests were added around property binding to keep configuration changes explicit and safe.

<a id="infra-8"></a>
### INFRA-8 - Tune the local Kubernetes baseline

_Date: 2026-05-02_

Adjusted the local Kubernetes baseline profile for more useful load-test behavior.
The work tuned Helm values, deployment settings, local lab scripts, and shared test definitions.
It also added or revised local capacity-search assets so baseline and exploratory runs used the same orchestration conventions.

<a id="infra-9"></a>
### INFRA-9 - Extract local Kubernetes manifests from setup scripts

_Date: 2026-05-02_

Moved local Kubernetes YAML out of setup scripts into explicit manifests and config files.
Kafka, observability, Redis values, Grafana datasource, Prometheus config, and Fluent Bit archive resources became versioned assets.
Helper scripts were kept for environment-specific values, making the lab setup easier to review and change.

<a id="global-1"></a>
### GLOBAL-1 - Shorten repository module names to `ckc-*`

_Date: 2026-05-02_

Renamed repository modules from the longer `coroutines-kafka-consumer-*` names to concise `ckc-*` directories.
The published artifact naming was preserved while source paths, Gradle settings, Docker files, docs, and tests moved to the shorter layout.
The change touched core, Micrometer, experiments, demo, contracts, stubs, load-test, and infrastructure references.

<a id="global-2"></a>
### GLOBAL-2 - Separate production, demo, infrastructure, and experiment layout

_Date: 2026-05-13_

Restructure the repository so top-level module directories represent production library artifacts.
Move demo modules under `demo/`, demo-owned infrastructure under `demo/infra/`, and benchmark/prototype code under `experiments/`.
Update Gradle wiring, scripts, and documentation references so the new layout remains buildable and discoverable.

<a id="infra-10"></a>
### INFRA-10 - Add Kafka lag metrics to local k8s and AWS smoke observability

_Date: 2026-05-02_

Added Kafka lag visibility to both local Kubernetes and AWS smoke observability flows.
The local lab gained a Kafka exporter manifest and Prometheus scrape configuration, while AWS runner update scripts and docs were adjusted.
Grafana dashboards were extended so consumer behavior could be compared against broker-side lag during smoke and load runs.

<a id="infra-11"></a>
### INFRA-11 - Clean Redis and recreate Kafka topics during lab setup

_Date: 2026-05-05_

Added setup steps that clean Redis state and recreate Kafka topics from deployment settings in the selected test definition.
Shared orchestration gained dedicated helpers for Redis flush and topic preparation, then wired them into the local and AWS run flow.
This made repeated lab runs less dependent on leftover state from previous tests.

<a id="infra-12"></a>
### INFRA-12 - Separate observability ports and validate local k8s baseline

_Date: 2026-05-05_

Separated local-dev, local-k8s, and AWS observability ports to avoid conflicts when environments coexist.
Updated tunnel scripts, runner templates, local manifests, compose files, and README instructions around the new port layout.
The task also validated the local Kubernetes baseline path and aligned test definitions with the cleaned setup flow.

<a id="demo-11"></a>
### DEMO-11 - Rewrite load-test generator around a shared load profile

_Date: 2026-05-05_

Reworked the load-test generator to use a shared load profile with separate lifecycle and telemetry rates.
The generator was changed to stop when the schedule ends instead of relying on an open-ended run.
Configuration tests and generator tests were added, and shared test definitions were updated to use the new profile shape.

<a id="infra-13"></a>
### INFRA-13 - Aggregate Grafana metric rates over 30s windows

_Date: 2026-05-05_

Changed Grafana dashboard rate queries to use 30-second windows.
The adjustment made local-dev panels less noisy while still preserving enough responsiveness for short smoke runs.
This was a dashboard-only observability refinement against the shared CKC overview dashboard.

<a id="infra-14"></a>
### INFRA-14 - Replace worker utilization dashboard panels

_Date: 2026-05-06_

Replaced snapshot-based worker utilization views with busy-time utilization queries.
The demo consumer metrics emission was adjusted so the dashboard could display utilization based on meaningful active time.
The shared Grafana dashboard was updated to make worker saturation easier to interpret during load tests.

<a id="demo-12"></a>
### DEMO-12 - Add demo consumer experiment controls

_Date: 2026-05-06_

Added experiment controls to the demo consumer configuration.
The demo can now toggle processing behavior and choose deserialization dispatcher behavior through properties.
README and property-binding tests were updated so the controls are discoverable and safe to tune for experiments.

<a id="demo-13"></a>
### DEMO-13 - Split demo services into blocking and suspend paths

_Date: 2026-05-06_

Separated demo business services into explicit blocking and suspend execution paths.
Consumer handlers were kept thin, delegating business work to services instead of mixing framework callbacks with domain logic.
Tests around order querying and lifecycle service behavior were updated to cover the reshaped service boundaries.

<a id="demo-14"></a>
### DEMO-14 - Add comparable Spring Kafka record metrics

_Date: 2026-05-06_

Added Spring Kafka record metrics so Spring and CKC consumers could be compared on the same dashboard.
The metrics include a shared consumer implementation tag, allowing dashboard queries to split CKC and Spring Kafka behavior consistently.
Spring listener and service code were updated, and Grafana panels were revised to include the new implementation dimension.

<a id="demo-15"></a>
### DEMO-15 - Add demo consumer profile info metric

_Date: 2026-05-11_

Move demo consumer implementation identity out of high-volume record metric tags.
Add a static profile info metric that identifies the active demo consumer implementation and Spring profile for timeline-style observability.
Keep record metrics focused on consumer, topic, and event labels so dashboard comparisons do not multiply all record series by implementation.

<a id="demo-16"></a>
### DEMO-16 - Add blocking Confluent Parallel Consumer demo implementation

_Date: 2026-05-15_

Added a third demo consumer implementation backed by Confluent Parallel Consumer.
The implementation reuses the existing blocking demo business services so it can be compared with the Spring Kafka blocking path.
Native Parallel Consumer Micrometer metrics are registered for the Confluent path instead of the CKC-style record metrics.
The first implementation stays focused on the demo application; infrastructure dashboards can follow separately if needed.

<a id="demo-17"></a>
### DEMO-17 - Keep Confluent profile metric without CKC record metrics

_Date: 2026-05-16_

Keep the dedicated demo consumer profile gauge available for every consumer implementation.
Scope CKC and Spring Kafka record metric beans to their own profiles so Confluent Parallel Consumer only exports the profile gauge plus native PC metrics.
This keeps Grafana profile visibility without pulling CKC-style record metrics into the Confluent profile.

<a id="demo-18"></a>
### DEMO-18 - Enable Confluent PC processing-time histograms

_Date: 2026-05-18_

Enable percentile histogram buckets for the native Confluent Parallel Consumer user-function processing timer.
Keep the setting in demo application metrics configuration so Grafana can query PC processing-time percentiles later.
Add coverage around the Confluent profile context to guard the demo metrics distribution setting.

<a id="demo-19"></a>
### DEMO-19 - Make lifecycle processing mode configurable

_Date: 2026-05-22_

Add a CKC order lifecycle consumer setting for selecting the processing mode used by the demo.
Keep the default as `AT_LEAST_ONCE_NO_ORDERING` so existing demo behavior remains unchanged.
Allow load tests and local runs to opt into key-ordered lifecycle processing through configuration.
Preserve telemetry's freshness-first default through the same runtime settings shape.

<a id="demo-20"></a>
### DEMO-20 - Reorganize demo domain around batches and cauldrons

_Date: 2026-05-23_

Redesign the demo lifecycle so orders are grouped into batches before cauldron brewing starts.
Split order, batch, and cauldron aggregate events across aggregate topics and add brewing-step and bottling events.
Add an order flavour model result stored separately in Redis, keep ETA recalculation driven by cauldron telemetry, and expose runtime model latency controls in stubs.
Reorganize demo handlers, consumer wiring, and model-client packages so business logic stays separate from consumer implementation details.

<a id="demo-21"></a>
### DEMO-21 - Clean up demo service package layout

_Date: 2026-05-23_

Remove the redundant event handler adapter layer added during the demo domain reorganization.
Place order, batch, and cauldron business logic in aggregate-specific service packages.
Keep the latency-only processing switch in consumer wiring so business services remain direct and focused.
Preserve behavior while making the demo business core easier to navigate.

<a id="demo-22"></a>
### DEMO-22 - Move sample event builders to tests

_Date: 2026-05-23_

Remove demo sample event builders from main application sources.
Keep only the cauldron telemetry fixture currently used by tests.
Drop unused sample batch event data so production demo code contains no example-only fixtures.

<a id="demo-23"></a>
### DEMO-23 - Unify demo domain model and ML package naming

_Date: 2026-05-23_

Move order, batch, flavour, and ETA context state classes into a dedicated demo domain model package.
Use the same domain model types for Redis persistence and API responses where the shapes are identical.
Rename external ML client packages to `ml` so domain `model` remains unambiguous.

<a id="demo-24"></a>
### DEMO-24 - Flatten demo domain model files

_Date: 2026-05-23_

Split the demo domain model into top-level files named after the domain classes.
Replace the temporary `*State` class names and aliases with direct `Order`, `Batch`, `EtaContext`, and `OrderFlavour` classes.
Keep repository persistence and API responses on the same domain model types.

<a id="demo-25"></a>
### DEMO-25 - Rename order consumer configuration

_Date: 2026-05-23_

Rename order-event consumer beans, variables, and runtime settings from lifecycle-oriented names to order-oriented names.
Keep lifecycle terminology only where it describes event contracts or load-test lifecycle traffic rather than a specific consumer.
Update demo Helm values and environment wiring so order consumer tuning uses order-prefixed names.

<a id="demo-26"></a>
### DEMO-26 - Rework load-test event generation

_Date: 2026-05-24_

Replace capacity-driven load-test generation with event-type generators that keep topic and event-type traffic density stable.
Use state queues for real entities and fake-prefixed fallback entities while the simulated world is warming up.
Run generators with elapsed-time permit accumulation so rates follow wall-clock time instead of a fixed tick loop.

<a id="demo-27"></a>
### DEMO-27 - Use async suspend model clients

_Date: 2026-05-25_

Replace the suspend model-client transport with a coroutine-native HTTP client shared across model endpoints.
Keep ETA and flavour clients as separate domain adapters while avoiding separate transport pools per model.
Gate sync and suspend model-client beans by demo profile so inactive consumer paths do not allocate unused HTTP resources.

<a id="demo-28"></a>
### DEMO-28 - Add Armeria model-client transport

_Date: 2026-05-25_

Add an Armeria-backed suspend model-client implementation alongside the existing Ktor CIO implementation.
Expose the selected suspend transport through demo model configuration so CKC runs can compare HTTP client behavior without changing consumer profiles.
Keep ETA and flavour domain clients separate while sharing the selected transport resources.

<a id="demo-29"></a>
### DEMO-29 - Use Armeria for demo stubs

_Date: 2026-05-26_

Replace the Ktor Netty based demo stubs server with Armeria while preserving the existing `/health`, `/config`, `/eta`, and `/flavour` endpoints.
Keep latency profile behavior and JSON response shapes unchanged so model-client load tests remain comparable.
Reduce the number of server-side event-loop threads visible during local and lab profiling.

<a id="demo-30"></a>
### DEMO-30 - Add in-process load-test workers

_Date: 2026-05-26_

Add configurable load-test generator workers inside one JVM.
Each worker runs with its own generator identity and simulation state, while sharing the configured publisher resources.
Keep external shard identity separate from internal worker identity so AWS/Kubernetes shards do not overlap with local process parallelism.
Treat `BASE_TPS` as the process-level target and split it across workers, with compact generated ids such as `order-1-5-00021212`.

<a id="demo-31"></a>
### DEMO-31 - Add model-call metrics

_Date: 2026-05-28_

Add Micrometer metrics around demo model-client calls so load tests can inspect model throughput and latency directly.
Expose model name, operation, client mode, and outcome tags without tying the metric to a specific HTTP transport.
Enable histogram buckets for the model-call timer so Prometheus percentile queries can use p50, p95, and p99.

<a id="demo-32"></a>
### DEMO-32 - Add fixed-fleet telemetry and Redis TTL retention

_Date: 2026-05-28_

Add a load-test mode that keeps a fixed cauldron fleet active so telemetry key cardinality can be controlled independently of the business pipeline.
Keep generated telemetry on the full processing path by pairing fleet cauldrons with live batch state instead of sending batchless events.
Retain demo Redis state for a bounded period with TTL so completed orders remain queryable after completion without explicit active-batch deletes.

<a id="demo-33"></a>
### DEMO-33 - Move demo audit to Redis

_Date: 2026-05-28_

Replace file-backed audit records with compact append-only Redis list entries for published and processed demo messages.
Keep processed audit writes inside record completion: suspend CKC paths await Redis without blocking dispatcher threads, while sync profiles block naturally.
Write generator publish acknowledgements asynchronously and drain outstanding Redis writes before normal generator shutdown.
Read the complete run audit from Redis for loss, duplicate, and latency analysis across pod failure scenarios.

<a id="demo-34"></a>
### DEMO-34 - Add CKC sync profile

_Date: 2026-05-29_

Add a `ckc-sync` Spring profile for comparing CKC runtime behavior with blocking demo service calls.
Keep the consumer implementation on CKC while routing record processing work through the IO dispatcher.
Reuse the existing synchronous order, batch, and cauldron business service variants so the profile models blocking application logic explicitly.
Cover the profile with a Spring context test and document the local run command.

<a id="demo-35"></a>
### DEMO-35 - Add Kafka consumer fetch settings

_Date: 2026-05-29_

Add demo application properties for `fetch.min.bytes`, `fetch.max.wait.ms`, and `max.poll.records`.
Wire those settings into all demo consumer profiles so CKC, CKC sync, Spring Kafka, and Confluent Parallel Consumer can be compared with the same Kafka consumer fetch behavior.
Keep defaults aligned with Kafka client defaults unless an experiment overrides them.

<a id="demo-36"></a>
### DEMO-36 - Add pause and resume metrics

_Date: 2026-05-29_

Add a CKC metric signal for Kafka consumer pause and resume events caused by downstream backpressure.
Expose the events through the Micrometer adapter so demo dashboards can correlate lower poll duration with paused intake.
Keep the metric lightweight and tagged by action so it can be queried as pause and resume rates.

<a id="infra-15"></a>
### INFRA-15 - Add lightweight internal k3s lab

_Date: 2026-05-08_

Added an internal k3s lab for running the demo stack on a dedicated Linux host.
The lab uses host-managed Kafka, Redis, Grafana, and stubs, with scripts for install, image loading, base deployment, test preparation, and execution.
Internal Helm values, Kubernetes assets, Grafana provisioning, host tuning scripts, and dedicated test definitions were added for this environment.

<a id="infra-16"></a>
### INFRA-16 - Move local-dev topic creation into scripts

_Date: 2026-05-11_

Removed the local-dev compose topic initialization service from the environment definition.
Added explicit local-dev helper scripts for starting, stopping, and creating Kafka topics through the running Kafka container.
Topic partition counts are now supplied through script arguments or interactive prompts, and topic recreation prints the previous and target partition counts.

<a id="infra-17"></a>
### INFRA-17 - Reorganize the consumer dashboard

_Date: 2026-05-11_

Reorganize Grafana consumer panels around record-level lifecycle and telemetry views.
Make throughput, processing duration, and record age panels work for both CKC and Spring Kafka by sourcing dashboard variables from shared record metrics.
Keep CKC-only runtime panels separate from the cross-implementation record comparisons.
Enable Prometheus percentile histogram buckets for processing duration so percentile panels can show p50, p95, and p99 after the app restarts.
Keep record age panels on average and max values to show whether messages are waiting in topics too long without adding age histogram cardinality.

<a id="infra-18"></a>
### INFRA-18 - Rework profile dashboard

_Date: 2026-05-11_

Update Grafana after moving implementation identity out of record metrics.
Remove the implementation dashboard variable and query record panels without `consumer_impl`.
Add profile visibility from the dedicated demo consumer profile info metric so active implementation periods can be inspected separately from throughput and latency.

<a id="infra-19"></a>
### INFRA-19 - Centralize demo infrastructure temporary files

_Date: 2026-05-13_

Reorganized demo infrastructure scripts so generated temporary state is written under the repository root `.demo-infra` directory.
Moved local Kubernetes runner state, internal lab state, AWS SSM temp files, and Python temporary work directories into the centralized location.
Kept script outputs predictable across local Kubernetes, AWS, and internal lab flows while reducing root-level clutter from ad hoc folders.

<a id="infra-20"></a>
### INFRA-20 - Remove local Kubernetes lab

_Date: 2026-05-14_

Removed the minikube-based local Kubernetes lab now that useful load checks have moved to the internal lab.
Kept local machine development focused on the `local-dev` environment.
Updated infrastructure documentation and references so the supported environment split is explicit.
Renamed the internal baseline test label away from the old local Kubernetes wording.

<a id="infra-21"></a>
### INFRA-21 - Add internal lab wakeup helper

_Date: 2026-05-14_

Add a standalone Python Wake-on-LAN helper for the internal lab host.
Support direct MAC/IP arguments and optional local `.demo-infra/internal-lab/lab.env` defaults.
Document how to wake the host and optionally wait for SSH readiness without depending on platform-specific tools.
Keep the user-facing entrypoint in `scripts/`, place helper implementation under `scripts/helpers/`, and refresh repo working notes for the current `demo/infra` layout.

<a id="infra-22"></a>
### INFRA-22 - Add Confluent Parallel Consumer dashboard metrics

_Date: 2026-05-16_

Update Grafana dashboards to use Spring profile identity without repeating consumer implementation labels.
Add Confluent Parallel Consumer metric panels for overlapping throughput and processing-time comparisons.
Add a Confluent-specific section for native Parallel Consumer runtime signals.

<a id="infra-23"></a>
### INFRA-23 - Add Confluent offset encoder dashboard metrics

_Date: 2026-05-16_

Extend the Confluent Parallel Consumer dashboard section with offset encoder metrics.
Add panels for offset encoding time, offset encoding usage, metadata space used, and payload ratio used.
Split the Confluent shard and partition counts into separate panels so the dashboard legends stay readable.
Keep `demo/infra/shared/grafana` as the single dashboard source and have the internal lab copy shared dashboard assets during installation.

<a id="infra-24"></a>
### INFRA-24 - Clean up Grafana load-test dashboard panels

_Date: 2026-05-18_

Rework the shared Grafana dashboard for load-test result analysis rather than live monitoring.
Keep Consumer Profile as a single full-width timeline panel and remove redundant profile summary panels.
Replace small stat panels with consistent half-width time-series panels, dropping duplicate signal views where they add noise.
Rename the record sections and panels around domain event streams: Order Events and Cauldron Events.
Add Confluent Parallel Consumer series to the Order Events throughput and processing panels, and move JVM Threads to the end of runtime panels.
Add matching Confluent Parallel Consumer series to the Cauldron Events throughput and processing panels.
Keep the MSK CloudWatch lag panels on the same dashboard row for easier load-test comparison.
Order the Java Runtime panels as CPU, GC pause, memory, GC rate, and threads.
Place MSK CloudWatch Time Lag before Offset Lag in the Kafka Lag section.
Remove duplicated Confluent Parallel Consumer throughput and processing-time panels now covered by domain event panels.
Extend the processing duration selector with default avg and max options alongside p50, p95, and p99.
Use Grafana's adaptive `$__rate_interval` for Prometheus rate windows so short load tests are not smoothed by fixed two-minute windows.

<a id="infra-25"></a>
### INFRA-25 - Add ordered lifecycle runtime observability

_Date: 2026-05-22_

Expose lifecycle and telemetry processing modes through the shared demo Helm chart.
Set CKC demo deployment profiles to key-ordered lifecycle processing while leaving telemetry freshness-first.
Update the CKC runtime dashboard section with active-worker and bounded-queue utilization panels for ordered runtime analysis.

<a id="infra-26"></a>
### INFRA-26 - Update demo infrastructure for aggregate event streams

_Date: 2026-05-23_

Update Kafka topic creation and test orchestration defaults for `order.events.v1`, `batch.events.v1`, and `cauldron.events.v1`.
Wire batch consumer runtime settings through Helm alongside order and cauldron settings.
Refresh local-dev and internal-lab topic/group reset scripts so repeated runs start from a clean aggregate-stream state.
Update Grafana metric selectors so runtime panels include the new batch consumer stream.

<a id="infra-27"></a>
### INFRA-27 - Switch local-dev Kafka to Redpanda

_Date: 2026-05-24_

Replace the local-dev Kafka container with Redpanda while keeping the external bootstrap endpoint on `localhost:9092`.
Adjust local helper scripts and documentation so topic management continues to work against the local broker.
Keep the change scoped to the local development Docker Compose environment.

<a id="infra-28"></a>
### INFRA-28 - Add local-dev process scripts

_Date: 2026-05-24_

Add local-dev helpers for running and stopping the load-test generator as a local process.
Add matching helpers for running and stopping the demo stubs service with selectable env profiles.
Add a separate local demo app runner with selectable app profiles and runtime settings.
Add a single interactive `run-test.sh` orchestration entrypoint for compose startup, stubs, topics, load-test execution, and cleanup.
Store local run configuration, pid files, and logs under `.demo-infra/local-dev` so repo-root temporary state stays centralized.

<a id="infra-29"></a>
### INFRA-29 - Use Redpanda and local interactive lab load tests

_Date: 2026-05-26_

Switch the internal lab host broker from Apache Kafka to Redpanda while keeping the same Kafka API endpoint for apps and local load tests.
Bind the external broker listener on all host interfaces and advertise the lab host IP so laptop-started generators receive a reachable broker address.
Update lab reset and install verification scripts to use `rpk` for topic and consumer-group operations.
Rework the internal lab `run-test.sh` flow so it prepares the selected test definition, starts the load generator as a local Java process, and allows interactive early stop with `q`.
Make the lab runner compare local demo image fingerprints with the lab host and automatically rebuild/load images when they are stale or absent.

<a id="infra-30"></a>
### INFRA-30 - Add host service CPU and Redpanda lag observability

_Date: 2026-05-28_

Add process-exporter to the internal-lab host Docker Compose stack so Prometheus can scrape CPU and memory for host-managed Redpanda and Redis processes.
Expose the process-exporter endpoint to in-cluster Prometheus through the existing external service pattern and add Grafana panels for host service CPU and memory.
Update Kafka exporter topic filters for the aggregate topic names used after the Redpanda migration so consumer lag panels are populated again.

<a id="infra-31"></a>
### INFRA-31 - Increase internal-lab Redpanda and Redis resources

_Date: 2026-05-28_

Raise the internal-lab Redpanda container from one CPU and a 512M Redpanda memory budget to two CPU shards and a 3G Redpanda memory budget.
Increase the Docker memory limit to 4G so Redpanda has headroom outside the Seastar allocator during high-partition consumer load tests.
Raise Redis maxmemory to 2G, align its Docker memory limit, and use noeviction so state pressure fails visibly instead of dropping keys.
Keep the change scoped to host-managed internal lab services used by optilab.

<a id="infra-32"></a>
### INFRA-32 - Refresh Grafana dashboard coverage

_Date: 2026-05-28_

Rework the shared Grafana dashboard so host service CPU is shown in millicores and split between Redpanda and Redis.
Clarify Kafka offset lag panels and fix missing lag visibility for Spring Kafka where possible.
Add missing batch-stream and pod CPU panels, remove low-value JVM CPU panels, and review panels for recently added demo metrics.
Add model-call, drop-throughput, and OffsetTracker capacity panels so newer demo and CKC metrics have dashboard coverage.

<a id="infra-33"></a>
### INFRA-33 - Run internal-lab runtime on the lab host

_Date: 2026-05-28_

Keep local Gradle builds so uncommitted workspace changes can be tested without committing them first.
Replace the local image archive workflow with an update step that syncs built runtime artifacts to the lab host.
Move Docker image rebuild/loading and load-test generator execution onto the lab host so the laptop is only used for install/update orchestration.
Update local-dev and AWS build scripts to use the same split Gradle runtime distributions instead of removed fat-jar tasks.
Keep local internal-lab scripts limited to `install-lab.sh` and `update-lab.sh`; move lab runtime scripts and Python helpers under copied assets.
Make `install-lab.sh` read or prompt for a lab host from the single local lab state file, and add a lab-side cleanup script for rebuilding the server from scratch.
Reduce local lab configuration to `LAB_HOST`, derive SSH from that host, and keep the resolved node IP only in lab-side config for Kubernetes endpoints and Redpanda advertising.

<a id="infra-34"></a>
### INFRA-34 - Add CKC sync infrastructure profile

_Date: 2026-05-29_

Add infrastructure wiring so the internal lab can deploy and test the `ckc-sync` demo profile.
Keep the new profile aligned with existing CKC baseline settings while selecting the blocking CKC Spring profile.
Expose the demo processing enablement switch through Helm as `env.processingEnabled`.
Add a dedicated test definition so sync CKC runs can be selected without editing shared baseline files.
Validate the new Helm values profile and definition references with local rendering and lint checks.

<a id="infra-35"></a>
### INFRA-35 - Add CKC pause/resume dashboard panel

_Date: 2026-05-29_

Add Grafana dashboard visibility for the CKC backpressure pause/resume metric.
Place the panel near CKC poll and queue pressure panels so paused intake can be correlated with lower poll duration.
Use per-consumer and action labels to show pause and resume rates separately.

<a id="infra-36"></a>
### INFRA-36 - Mount synced Grafana dashboards

_Date: 2026-05-29_

Make internal-lab Grafana read dashboards from the workspace path that `update-lab.sh` refreshes.
Keep generated provisioning files under the runtime Grafana directory because the Prometheus datasource still needs lab host templating.
Remove the stale dashboard copy step so dashboard updates no longer require manual copying after lab updates.
Reapply the Grafana compose service during lab updates so existing labs switch to the synced dashboard mount.
Align fresh installs with the same workspace-backed shared infra path.

<a id="infra-37"></a>
### INFRA-37 - Wire Helm consumer fetch settings

_Date: 2026-05-29_

Add Helm values and deployment environment variables for Kafka consumer `fetch.min.bytes`, `fetch.max.wait.ms`, and `max.poll.records`.
Set shared chart defaults to an 8 KiB fetch minimum, 250 ms max wait, and 500 records per poll.
Apply the same fetch settings across demo Helm profiles so CKC, CKC sync, Spring Kafka, and Confluent Parallel Consumer lab runs use comparable consumer fetch behavior.

<a id="infra-38"></a>
### INFRA-38 - Simplify internal-lab test workflow

_Date: 2026-05-30_

Separate reusable internal-lab deployment profiles from load-test definitions.
Configure the already deployed demo stubs through their HTTP API before each test run.
Replace persistent test selection with one interactive run command for choosing a deployment and a test.
Keep AWS orchestration and timed failure scenarios out of scope for this task.
Remove obsolete noop profiles by exposing processing enablement as an interactive Helm override.

<a id="doc-1"></a>
### DOC-1 - Add documentation task scope

_Date: 2026-05-10_

Added `DOC` as the scope for repository documentation, task history, working rules, and project notes.
The tracked task table gained `DOC-1` so documentation-only work no longer needs to be filed under infrastructure.
The local working rules were also updated, but that file is currently ignored by Git in this repository.

<a id="doc-2"></a>
### DOC-2 - Expand `TASKS.md` with linked task history

_Date: 2026-05-10_

Added internal links from the task table to detailed task notes below the table.
Restored retrospective descriptions by reading git history, changed files, and the current code layout rather than relying only on commit messages.
Kept the table as the high-level tracker while making `TASKS.md` usable as a chronological project history.

<a id="doc-3"></a>
### DOC-3 - Add initial OSS README

_Date: 2026-06-09_

Add the first repository-level README for OSS positioning.
Document what CKC is trying to solve and which Kafka-adjacent projects are not direct benchmark equivalents.
Capture the kotlin-kafka investigation outcome as a related-project note instead of keeping a misleading demo benchmark profile.

<a id="doc-4"></a>
### DOC-4 - Refine Micrometer README wording

_Date: 2026-07-08_

Clarify that `ConsumerMetrics` is an interface supplied to CKC consumers.
Tighten the Micrometer module introduction so it describes backend export without exposing factory details too early.
Make filtering, percentile, and histogram sections explicit that CKC relies on Micrometer or framework-level configuration for those concerns.

<a id="doc-5"></a>
### DOC-5 - Remove README alternatives

_Date: 2026-07-24_

Remove the repository-level README section that named related projects and alternative libraries.
Keep the main README focused on CKC's own processing modes, offset tracking, modules, and current project status.

<a id="doc-6"></a>
### DOC-6 - Update README modules

_Date: 2026-07-24_

Refresh the repository-level README module list to include the Spring Boot starter, demo contracts, and demo stubs.
Keep the module descriptions concise while matching the current Gradle module layout.

<a id="demo-37"></a>
### DEMO-37 - Remove Ktor remnants

_Date: 2026-05-31_

Remove the obsolete Ktor CIO model-client implementation from the demo application.
Keep Armeria as the only suspend HTTP transport for the CKC profile.
Drop transport selection settings and align demo Helm defaults and context tests with the single supported path.

<a id="infra-39"></a>
### INFRA-39 - Add interactive audit and metrics settings

_Date: 2026-05-31_

Expose audit logging as an interactive internal-lab test-run setting.
Add a selectable consumer metrics implementation for comparing Micrometer-backed and no-op record processing.
Wire both settings through the internal-lab deployment path while preserving existing defaults and explicit non-interactive flags.
Add CKC and Spring Kafka context coverage for the no-op consumer metrics configuration.
Present every interactive runner choice as a numbered list and reuse the previous test definition as the default selection.

<a id="demo-38"></a>
### DEMO-38 - Add shared CKC worker dispatcher

_Date: 2026-06-01_

Add a configurable fixed thread pool shared by suspend CKC demo consumers.
Keep per-consumer worker concurrency settings independent from the shared physical thread limit.
Retain `Dispatchers.IO` for the CKC sync profile because its handlers execute blocking service calls.

<a id="infra-40"></a>
### INFRA-40 - Add interactive CKC worker dispatcher threads

_Date: 2026-06-01_

Expose the suspend CKC shared worker dispatcher thread count through Helm.
Add an internal-lab runner prompt and CLI override for changing the thread count between runs.
Persist the previous selection so repeated experiments keep the same physical worker limit by default.

<a id="demo-39"></a>
### DEMO-39 - Replace Tomcat with Armeria

_Date: 2026-06-01_

Replace the demo application's embedded Tomcat server with Armeria to reduce thread overhead during load tests.
Serve the existing order query API through an Armeria annotated service.
Keep health and Prometheus Actuator endpoints available on the existing port and paths.
Limit consumer-side Armeria server threads and make the reference query API opt-in through the `api` profile.

<a id="demo-40"></a>
### DEMO-40 - Add audit analyzer progress and topic summaries

_Date: 2026-06-01_

Report Redis audit download progress in ten-percent increments.
Keep progress on stderr so the persisted summary remains focused on analysis results.
Print the existing loss, duplicate, and latency measurements for the complete audit and for each demo topic.

<a id="demo-41"></a>
### DEMO-41 - Use direct Lettuce Redis access

_Date: 2026-06-01_

Replace the suspend demo Redis path with direct Lettuce coroutine commands.
Keep blocking demo profiles on direct synchronous Lettuce commands.
Preserve Redis data formats and audit completion semantics so the same internal-lab baseline can compare overhead.
Retain the change after two sequential baseline comparisons showed a stable 10-15% throughput improvement.

<a id="demo-42"></a>
### DEMO-42 - Add async Confluent Parallel Consumer profile

_Date: 2026-06-01_

Add a separate Reactor-backed Confluent Parallel Consumer profile for a fair async comparison with CKC.
Bridge the existing suspend business path into `Mono` processing and complete each record only after suspend audit acknowledgement.
Run suspend work on the configured fixed worker dispatcher and avoid the Reactor adapter's default bounded-elastic pool.
Keep the existing blocking Confluent Parallel Consumer profile available as a separate baseline.

<a id="demo-43"></a>
### DEMO-43 - Remove demo Redis state TTL

_Date: 2026-06-01_

Store demo Redis state without expiration so long-running load tests retain batch and cauldron state.
Rely on the existing test-runner Redis reset before each run instead of time-based cleanup.
Apply the same behavior to synchronous and suspend repository paths.

<a id="demo-44"></a>
### DEMO-44 - Align external consumer processing modes

_Date: 2026-06-01_

Map CKC processing-mode settings to the corresponding Confluent Parallel Consumer ordering.
Discard stale records before external-adapter business processing when freshness-first mode is selected.
Reject processing modes that Spring Kafka cannot model faithfully instead of silently running a different comparison.

<a id="infra-41"></a>
### INFRA-41 - Group Helm profiles by environment

_Date: 2026-06-01_

Split demo Helm deployment profiles into explicit `aws` and `internal-lab` directories.
Keep environment runners scoped to their own profiles so interactive selection does not mix unrelated presets.
Duplicate shared smoke and HPA presets where both environments need an independently discoverable profile.
Replace obsolete demo-stubs latency-named profiles with fixed `internal-lab` and `aws-hpa` deployment presets.
Reduce internal-lab demo profiles to concise consumer-oriented names and remove unused HPA and alternate CKC presets.

<a id="core-26"></a>
### CORE-26 - Add ordering queue metrics

_Date: 2026-06-02_

Expose current and maximum ordered-runtime queue sizes separately from the shared admission queue.
Count only records waiting behind an in-flight key or partition so low-contention traffic is visible directly.
Publish the gauges through the Micrometer adapter with stable zero values for unordered runtimes.

<a id="core-27"></a>
### CORE-27 - Advance offsets after handled processing failures

_Date: 2026-06-02_

Treat a successfully handled terminal processing failure as a completed record for offset tracking.
Preserve at-least-once behavior when the failure handler itself fails by leaving the offset unprocessed.
Add regression coverage for skipped records and failing terminal handlers.

<a id="core-28"></a>
### CORE-28 - Add freshness-first processing by key

_Date: 2026-06-12_

Add a bounded freshness-first-by-key runtime for telemetry-style streams with finite active key cardinality.
Keep only the newest queued record for each key and drop older queued records for the same key intentionally.
Drop records for new keys when queued key-lane admission is full, and document capacity tuning around active keys and drop metrics.
Expose stable drop reasons so replacement drops can be distinguished from key-lane saturation.

<a id="core-29"></a>
### CORE-29 - Refactor Micrometer module documentation

_Date: 2026-07-05_

Split the Micrometer adapter module into clearer API, implementation, naming, and record-tag schema pieces.
Document the metric prefix, custom tag schema, Prometheus label-set constraints, and the exported metric surface.
Remove counters duplicated by timer counts and export record age as a timer so histogram and percentile configuration is consistent for duration-like metrics.
Rename the configured Micrometer entry point to `MicrometerConsumerMetricsSchema` and expose `metricPrefix` instead of an application-specific prefix name.
Require a metric prefix while keeping the `ckc` namespace permanent in every factory-created metric name.
Keep `consumer_id` present on all CKC meters, using `default` when no logical consumer id is supplied, so Prometheus label sets stay stable.
Replace object-handle custom tags with string-key record-driven tag schemas, per-consumer extractor maps, and default schema values for missing record-driven tags.
Add README examples, exported metric tables, filtering/histogram notes, and Grafana screenshots that explain record-driven custom tags.

<a id="infra-42"></a>
### INFRA-42 - Show Reactor Parallel Consumer metrics

_Date: 2026-06-02_

Expand Grafana Parallel Consumer selectors to include blocking and Reactor-backed Confluent profiles.
Keep native PC metric tags distinct so historical runs remain attributable to the selected Spring profile.
Show CKC ordering-queue current and maximum values next to the existing runtime pressure panels.
Aggregate application CKC, Parallel Consumer, model, pod-resource, and JVM series across pods by default and provide an optional per-pod dashboard view.
Apply the shared duration statistic selector to successful record-age panels, using average outside the max view.
Restart Grafana during internal-lab updates so synchronized dashboard definitions are reloaded immediately.
Collapse dashboard sections by default while keeping the shorter profile timeline visible on initial load.

<a id="infra-43"></a>
### INFRA-43 - Update internal-lab images incrementally

_Date: 2026-06-02_

Avoid synchronizing unchanged Docker build contexts during routine lab updates.
Rebuild only images whose service-specific fingerprints changed or are missing from k3s.
Restart demo stubs only when their image changes, while preserving first-time deployment behavior.

<a id="demo-45"></a>
### DEMO-45 - Persist demo-stubs settings in Redis

_Date: 2026-06-02_

Persist the active demo-stubs latency and failure profile under a fixed Redis key.
Restore the last configured profile when a stubs pod starts so resiliency tests keep stable model behavior.
Publish settings updates through Redis so every live stubs pod applies endpoint changes without a restart.
Use the existing lab Redis service and retain baseline defaults when no saved profile exists.

<a id="demo-46"></a>
### DEMO-46 - Replace Redis audit with TCP audit logging

_Date: 2026-06-03_

Replace Redis audit writes in the demo application and load-test generator with compact line-based TCP audit logging.
Include the test run id and stable writer identity in every audit record so downstream collection stays stateless.
Keep audit failures visible enough to invalidate a run instead of silently dropping experiment data.

<a id="demo-47"></a>
### DEMO-47 - Switch demo audit transport to Logback TCP appenders

_Date: 2026-06-03_

Replace the demo application's and load-test generator's custom TCP audit senders with Logback-managed TCP appenders.
Keep the current Fluent Bit JSON-over-TCP ingestion contract so internal-lab audit collection stays unchanged during the transport swap.
Remove the remaining self-managed socket audit transport code once both sender paths use the shared logging-based contract.
Add `F` audit records for terminal processing failures across CKC, Spring Kafka, and Confluent demo consumer paths.
Shrink audit payloads by removing run and writer identifiers from each line, using `|` separators, and splitting publish and consumer record schemas.

<a id="demo-48"></a>
### DEMO-48 - Add crash endpoint with audit flush

_Date: 2026-06-04_

Add an internal demo HTTP endpoint that forces the application to die on demand for resiliency experiments.
Flush and stop the dedicated `AUDIT_TCP` Logback appender before halting the JVM so in-flight audit lines have a chance to leave the process.
Keep the shutdown path intentionally hard by using a direct JVM halt instead of a normal Spring shutdown.

<a id="demo-49"></a>
### DEMO-49 - Align Spring Kafka commit cadence

_Date: 2026-06-05_

Configure Spring Kafka lifecycle listeners to use time-based commits instead of the default batch-after-poll acknowledgement.
Keep the default interval aligned with Kafka's `auto.commit.interval.ms` default so lifecycle and telemetry streams are comparable.
Make the auto-commit split explicit: lifecycle streams use Spring-managed commits, while telemetry keeps Kafka auto-commit.

<a id="demo-50"></a>
### DEMO-50 - Rework load generator delegation

_Date: 2026-06-05_

Replace fake fallback entities in the demo load-test generator with bounded prerequisite-driven generation.
Keep the string-based load profile and existing per-topic traffic split while letting requested events emit real upstream events when state is cold.
Report generator outcomes as emitted, delegated, and blocked instead of real versus fake.
Remove the obsolete fake entity prefix configuration from load-test config and local/internal-lab launch paths.

<a id="demo-51"></a>
### DEMO-51 - Add brewing-step key bursts

_Date: 2026-06-05_

Add configurable same-key burst generation for `BATCH_BREWING_STEP_COMPLETED` load-test events.
Keep each burst within the remaining brewing steps for the selected batch so lifecycle state remains valid.
Count burst messages in generator diagnostics so reported emitted traffic matches producer output.

<a id="demo-52"></a>
### DEMO-52 - Add brewing-step registry receipts

_Date: 2026-06-05_

Add a legacy HTTP registry acknowledgement for `BATCH_BREWING_STEP_COMPLETED` processing.
Persist registry receipt data so step processing has a realistic external acceptance gate before audit completion.
Keep the registry endpoint configurable separately from existing model endpoints while allowing local stubs to serve all dependencies.

<a id="demo-53"></a>
### DEMO-53 - Add naive Spring Kafka coroutine profile

_Date: 2026-06-07_

Add a separate `spring-kafka-coroutines-naive` profile for comparing a simple Spring Kafka batch-listener admission path with coroutine workers.
Keep the implementation isolated from CKC runtime code while reusing demo contracts, suspend business services, audit logging, and shared record metrics.
Use bounded channels and blocking listener-side admission so committed/enqueued records can be lost during graceful shutdown or crash scenarios.

<a id="demo-54"></a>
### DEMO-54 - Flush audit last on graceful shutdown

_Date: 2026-06-11_

Add a demo application lifecycle component that keeps the dedicated TCP audit appender alive until the final Spring shutdown phase.
Reuse the existing Logback audit flusher so normal graceful shutdown and the internal crash endpoint share the same flush-and-stop path.
Emit a compact shutdown marker before flushing and log audit appender state so shutdown ordering can be verified from pod logs and packet captures.
Log freshness-first dropped records as explicit audit terminal outcomes and report them separately in the audit analyzer.
Let closed-channel admission failures escape the naive Spring Kafka listener instead of swallowing them as successful batch handling.
Recover naive Spring Kafka batch admission failures without retry backoff, audit recovered records as dropped, and stop listener containers promptly before closing worker channels.
Add an optional local-dev Docker Compose audit profile that runs Fluent Bit and archives audit chunks under `.demo-infra`.

<a id="demo-55"></a>
### DEMO-55 - Align consumer retry audit semantics

_Date: 2026-06-19_

Make demo audit records distinguish retryable processing attempts from terminal consumer outcomes.
Keep `C`, `D`, and `F` aligned across CKC, Spring Kafka, Confluent Parallel Consumer, and the naive coroutine profile.
Configure bounded retry behavior for demo failures that are expected to come mostly from Redis and Armeria client calls.
Update the shared audit analyzer and tests so retry attempts do not look like conflicting terminal outcomes.
Add drop reason details to demo audit records and analyzer summaries, including already-processed metadata skips after rebalance.
Switch the demo telemetry default to freshness-first-by-key for the CKC comparison profile and raise internal-lab CKC queue capacities.

<a id="demo-56"></a>
### DEMO-56 - Add alternative record age metrics

_Date: 2026-06-23_

Emit CKC-style record age measurements from alternative demo consumer implementations.
Enable percentile histograms for the record age metric so freshness-first runs expose queueing and staleness behavior.
Update the shared Grafana dashboard to compare record age percentiles across CKC and alternative consumer profiles.

<a id="demo-57"></a>
### DEMO-57 - Add cauldron telemetry gap metrics

_Date: 2026-06-24_

Measure the event-time gap between consecutive processed cauldron telemetry updates using Redis-backed ETA context state.
Keep the metric resilient to consumer rebalance by deriving the previous timestamp from persisted business state instead of per-pod memory.
Expose the gap distribution in Grafana so freshness-first runs show how often each cauldron state is refreshed.

<a id="demo-58"></a>
### DEMO-58 - Unify external record metrics

_Date: 2026-07-01_

Bring Confluent Parallel Consumer profiles onto the same CKC-style record metric adapter used by Spring Kafka external profiles.
Record freshness-first stale drops as `ckc.record.dropped` in addition to audit records.
Keep adapter-specific retry and acknowledgement behavior while sharing processed, failed, and dropped metric semantics.

<a id="demo-59"></a>
### DEMO-59 - Stabilize demo context tests

_Date: 2026-07-02_

Fix demo Spring context tests that can fail with Armeria bind conflicts when multiple profile contexts run in the same Gradle test task.
Keep the full `ckc-demo` test task runnable so merge checks do not rely only on targeted test subsets.

<a id="demo-60"></a>
### DEMO-60 - Add CKC sync Loom profile

_Date: 2026-07-03_

Add a `ckc-sync-loom` Spring profile for comparing blocking CKC demo handlers on virtual threads.
Reuse the existing synchronous demo services while making the processing dispatcher differ from the `ckc-sync` IO baseline.
Cover the profile identity and bean wiring with demo Spring context tests.

<a id="infra-44"></a>
### INFRA-44 - Add dedicated Fluent Bit audit ingestion

_Date: 2026-06-03_

Run a dedicated Fluent Bit audit collector alongside internal-lab host services and prepare the same ingestion model for AWS EKS.
Archive compact audit records per run without reusing Redis as a temporary store.
Fail internal-lab runs fast when the audit collector is unavailable or cannot keep up with the generated load.

<a id="infra-45"></a>
### INFRA-45 - Update compact audit archive and analyzer flow

_Date: 2026-06-04_

Update the internal-lab audit analyzer to understand the compact `P`, `C`, and `F` payload formats now emitted by the demo app and load-test.
Drop dependence on per-record run and writer identifiers by cleaning the shared live audit log before each test run and archiving only fresh records.
Keep the existing Fluent Bit JSON-over-TCP collector and internal-lab orchestration, but align the file parsing and summary outputs with the new payload contract.

<a id="infra-46"></a>
### INFRA-46 - Fix internal-lab Redpanda script compatibility

_Date: 2026-06-04_

Fix the internal-lab Kafka exporter startup flags so the observability container starts cleanly against the current exporter image.
Reduce noisy exporter readiness failures during `update-lab.sh` while keeping lag metrics optional for test execution.
Keep `run-test.sh` drain waiting functional on Redpanda by preserving or tightening the existing `rpk`-based fallback when Prometheus lag metrics are unavailable.
Persist internal-lab Prometheus TSDB on the lab host and avoid unconditional Prometheus restarts during routine lab updates.
Split internal-lab update fingerprints so test-definition and Helm-only changes can skip unnecessary Gradle builds, base redeploys, and stubs restarts.

<a id="infra-47"></a>
### INFRA-47 - Add chunked local audit archive analysis

_Date: 2026-06-05_

Routed internal-lab Fluent Bit audit records into a local audit archiver instead of one long-lived file.
The archiver writes completed gzip chunks under the live audit directory and resets cleanly before each run.
The audit analyzer now supports chunk directories, gzip input, watch mode, stop-file finalization, and visible run-test progress during test execution.
Filtered internal-lab Redpanda lag fallback series so the dashboard does not show blank consumer-group legends.
Kept the AWS audit path untouched until the local lab flow has stabilized.

<a id="infra-48"></a>
### INFRA-48 - Restore Redpanda CPU dashboard panel

_Date: 2026-06-05_

Diagnose the empty Redpanda CPU panel in the internal-lab Grafana dashboard.
Replace the process-exporter-based CPU and memory queries with Redpanda's native public metrics.
Keep the CPU panel unit in millicores and the memory panel in bytes so they remain comparable with the existing host service panels.

<a id="infra-49"></a>
### INFRA-49 - Clamp Redpanda CPU panel

_Date: 2026-06-05_

Clamp the Redpanda CPU dashboard query to the internal-lab Redpanda shard capacity.
Prevent native public-metrics counter spikes from rendering impossible CPU values in Grafana.
Keep the panel in millicores so normal readings stay comparable with the other host-service CPU panels.

<a id="infra-50"></a>
### INFRA-50 - Add audit processing order analysis

_Date: 2026-06-05_

Expand the internal-lab audit analyzer with terminal processing order checks.
Report out-of-order terminal records by Kafka partition and by `(topic, partition, message key)`.
Keep the checks usable for comparing CKC and Confluent ordered-by-key and ordered-by-partition modes.

<a id="infra-51"></a>
### INFRA-51 - Optimize audit analyzer memory use

_Date: 2026-06-06_

Investigate why chunked internal-lab audit analysis still grows memory without bound.
Change the analyzer from full-record retention to bounded aggregation: close and evict records after both publish and terminal outcomes are accounted for.
Keep only open records for a short timeout window and preserve aggregate counters, latency buckets, duplicate counts, conflict counts, and order checks.
Set internal-lab load-test topics to short rolling retention, about five minutes by default, so multi-hour runs do not retain obsolete Kafka data.

<a id="infra-52"></a>
### INFRA-52 - Use process-exporter Redpanda CPU metrics

_Date: 2026-06-06_

Switch the internal-lab Redpanda CPU dashboard panel from native `redpanda_cpu_busy_seconds_total` to process-exporter CPU accounting.
Keep Redpanda and Redis host-service CPU panels based on the same metric family so stop-time native counter spikes do not hide normal CPU load.
Restart process-exporter after host services are up so the Redpanda process group is present before Grafana uses it.

<a id="infra-53"></a>
### INFRA-53 - Reduce audit analyzer CPU cost

_Date: 2026-06-06_

Benchmark the Python audit analyzer on real internal-lab gzip chunks before and after optimization.
Reduce per-record CPU overhead in the streaming hot path so live analysis does not compete with the load test for a full CPU core.
Keep the bounded-memory behavior, processing-order checks, duplicate counters, and latency summaries added by the previous analyzer tasks.
Use a lighter live-run summary mode and tuple-backed record keys after validating the change against 10.85M real audit records on optilab.

<a id="infra-54"></a>
### INFRA-54 - Restore per-topic audit order summaries

_Date: 2026-06-07_

Restore topic-level visibility for audit processing-order checks after the optimized live-run summary hid mixed-topic behavior.
Track per-topic order counters inside the existing partition/key order pass instead of rebuilding full per-topic audit state.
Restore full per-topic audit summaries in internal-lab runs, including published, processed, missing, duplicate, without-publish, order, and latency counters.
Move internal-lab audit analysis out of the load-test runtime path: collect chunks during the run, then run the analyzer once after the generator exits and consumer lag drains.
Replace the legacy text report with `summary.yaml`, remove online-analysis flags from the analyzer path, and keep ordering output focused on out-of-order counters only.
Include the selected run configuration in the YAML report and remove static latency summaries so delivery correctness stays the focus.

<a id="infra-55"></a>
### INFRA-55 - Refactor lab infrastructure entrypoints

_Date: 2026-06-15_

Refactor demo infrastructure so local scripts only create, update, or connect to target lab hosts.
Move long-running test execution behind target-host entrypoints that can be run from `tmux`.
Split environment-owned Helm assets while keeping shared orchestration and audit-analysis logic reusable.
Remove the PowerShell script surface and keep Windows usage centered on Git Bash-compatible shell entrypoints.
Move internal-lab entrypoints into `assets/bin`, implementation scripts into `assets/libexec`, and reusable audit tools into `shared/audit`.
Reshape AWS so local scripts create/update/connect, while lab lifecycle and test commands live under runner-side assets.

<a id="infra-56"></a>
### INFRA-56 - Add freshness fairness audit metrics

_Date: 2026-06-15_

Add audit analyzer metrics that show per-key telemetry processing fairness under lossy freshness-first modes.
Report distribution skew, starvation gaps, and processed/dropped ratios for cauldron telemetry keys.
Add internal-lab test and deployment profiles that generate telemetry-only fixed-fleet pressure for comparing `FRESHNESS_FIRST_DROP_OLDEST`, `FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY`, and stale-threshold discard behavior.

<a id="infra-57"></a>
### INFRA-57 - Add test definition chaos steps

_Date: 2026-06-16_

Start adding a chaos-steps section to test definitions with offsets from test start, scenario type, and scenario parameters.
Cover initial Kubernetes pod disruption scenarios: deleting a random pod and triggering the internal crash endpoint in a random pod.
Cover demo-stubs runtime profile changes, including switching to a named profile and resetting back to the default profile.
Add an internal-lab chaos test definition for the 15-minute 3000 TPS scenario.
Replace the custom audit archiver with Fluent Bit file output, analyze a single completed audit log offline, and gzip it after analysis.
Remove noisy per-ack load-test stdout logging and make analyzer delivery matching exact by default for long chaos delays.

<a id="infra-58"></a>
### INFRA-58 - Reorganize internal-lab server runtime layout

_Date: 2026-06-27_

Replace source-oriented server directories such as `assets`, `shared`, and `workspace` with responsibility-oriented runtime paths.
Keep local repository ownership intact while mapping uploaded files to lab-host `bin`, `libexec`, `helpers`, `helm`, `docker`, `grafana`, and `test-definitions` directories.
Clean up install and update scripts so fresh installs and incremental updates produce the same lab-host structure.

<a id="infra-59"></a>
### INFRA-59 - Add internal-lab test bundles

_Date: 2026-06-27_

Add bundle definitions for sequentially running the same internal-lab test against multiple deployment profiles.
Provide a lab-host bundle runner that calls the existing single-test runner for each bundle entry and records a per-bundle summary.
Start with a comparison bundle based on the current lab settings plus Confluent Reactor and naive Spring Kafka profiles.

<a id="infra-60"></a>
### INFRA-60 - Add audit freshness gap distributions

_Date: 2026-06-30_

Add audit analyzer output that counts how many dropped same-key records preceded each processed record.
Report the distribution alongside existing freshness fairness summaries so freshness-first-by-key runs show actual per-key refresh behavior.
Include timing context for dropped-to-processed gaps where audit timestamps allow it.

<a id="infra-61"></a>
### INFRA-61 - Extend internal-lab Prometheus retention

_Date: 2026-07-01_

Keep internal-lab Prometheus scraping at a 15-second interval for load-test charts with lower storage churn.
Increase TSDB retention from a short same-day window to three days so recent runs remain visible after midnight.
Raise the retention size cap while keeping an explicit disk guard for the dedicated lab host.

<a id="infra-62"></a>
### INFRA-62 - Remove native PC record dashboard metrics

_Date: 2026-07-02_

Remove native Confluent Parallel Consumer record throughput, failure, and processing-duration series from the shared Grafana dashboard.
Use the unified `ckc.record.*` metrics for Confluent profiles after the external metric adapter change.

<a id="infra-63"></a>
### INFRA-63 - Add CKC sync Loom lab profile

_Date: 2026-07-03_

Add an internal-lab Helm deployment profile for the `ckc-sync-loom` demo profile.
Keep the profile aligned with the existing `ckc-sync` baseline so tests can compare dispatcher behavior directly.
Add a small internal-lab bundle for running the IO and Loom CKC sync profiles back to back.

<a id="infra-64"></a>
### INFRA-64 - Add application pod aggregate panels

_Date: 2026-07-03_

Update the Grafana Application Pods CPU and memory panels to honor selected pod filters.
Keep per-pod series visible while adding sum and average aggregate series for the selected pods.
Make deployment-level and typical-pod resource usage visible in the same panel.

<a id="infra-65"></a>
### INFRA-65 - Add internal-lab network chaos

_Date: 2026-07-04_

Add scheduled internal-lab chaos steps for Redis and Kafka network degradation.
Support reversible host-service disruption scenarios that fit the existing test-definition DSL.
Document example scenarios so lab runs can exercise slow downstream state, slow Kafka access, and service restarts.
Add a network-chaos smoke definition for validating the host-level tc/iptables path.
Add a targeted queue-backlog crash comparison with large order/batch worker queues and low worker concurrency to expose listener-accepted records lost on app crashes.

<a id="infra-66"></a>
### INFRA-66 - Add event-type aggregation toggle

_Date: 2026-07-07_

Add a Grafana dashboard variable that switches CKC order and batch record panels between per-event-type series and aggregated event-type series.
Keep the existing split-by-event-type view as the default so current dashboard behavior is preserved.

<a id="core-30"></a>
### CORE-30 - Add Spring Boot starter

_Date: 2026-07-08_

Added a production Spring Boot starter module for configuring CKC consumers from application properties.
The starter discovers annotated `CkcConsumer` beans, binds each bean to a named consumer configuration, and manages CKC lifecycle through Spring.
The first iteration keeps the API focused on class-level consumer declarations instead of Spring Kafka-style method listener annotations.

<a id="core-31"></a>
### CORE-31 - Rename Micrometer metrics schema API

_Date: 2026-07-09_

Rename the Micrometer consumer metrics entry point from factory-oriented naming to schema-oriented naming.
Fold record-driven tag schema declarations into the metrics schema API so Spring configuration can model metric families directly.
Update KDoc, README examples, demo wiring, and tests around the new naming.

<a id="core-32"></a>
### CORE-32 - Add Spring Boot metrics configuration

_Date: 2026-07-09_

Add Spring Boot configuration for choosing CKC metrics implementation and declaring Micrometer metric schemas.
Move record-driven Micrometer tag extractors and custom `ConsumerMetrics` resolution to annotated beans.
Keep business consumer interfaces focused on processing while metrics wiring stays in Spring infrastructure.

<a id="core-33"></a>
### CORE-33 - Add Spring Boot retry schemas

_Date: 2026-07-09_

Add named retry schemas to the Spring Boot starter so consumers can share ordered retry rules.
Resolve exception class names from configuration into the existing core `RetryPolicy` model.
Support a default retry schema and per-consumer schema overrides without inline retry rule shortcuts.

<a id="core-34"></a>
### CORE-34 - Add Spring Boot lifecycle controls

_Date: 2026-07-09_

Add configurable Spring lifecycle phase and bounded shutdown for starter-managed CKC consumers.
Keep graceful drain and commit semantics in core while making starter shutdown wait time explicit.
Log consumer startup and shutdown diagnostics so configured consumers are visible during application boot.

<a id="core-35"></a>
### CORE-35 - Add Spring Boot startup banner

_Date: 2026-07-09_

Add a minimal CKC banner to the Spring Boot starter startup logs.
Expose the starter implementation version through jar manifest metadata and fall back to a development marker on plain classpaths.

<a id="core-36"></a>
### CORE-36 - Add Spring Boot startup validation

_Date: 2026-07-09_

Validate starter-managed consumer declarations before building CKC runtimes.
Fail fast on duplicate handlers, missing handlers or configuration, invalid subscriptions, missing Kafka essentials, and unknown schemas.
Improve startup diagnostics so each resolved consumer logs its handler, cluster, subscription, group, processing, retry, and metrics configuration.

<a id="core-37"></a>
### CORE-37 - Add Spring Boot configuration metadata

_Date: 2026-07-09_

Add starter configuration metadata so IDEs can offer completion and descriptions for `ckc.*` properties.
Document the full configuration structure in the starter README, including lifecycle, metrics, retry schemas, clusters, and consumers.

<a id="core-38"></a>
### CORE-38 - Add Spring Boot dispatcher definitions

_Date: 2026-07-10_

Add named processing dispatcher definitions to the Spring Boot starter.
Keep built-in coroutine dispatchers available through reserved names while allowing configured fixed-thread and virtual-thread dispatchers.
Let consumers reference dispatchers by name so runtime threading remains visible in application configuration.
Update the CKC Spring Boot demo profile to use a configured fixed-thread dispatcher definition.

<a id="core-39"></a>
### CORE-39 - Add Spring Boot health indicators

_Date: 2026-07-10_

Add optional Spring Boot Actuator health integration for starter-managed CKC consumers.
Report registered consumers, auto-startup settings, running state, subscriptions, cluster, and processing configuration.
Keep this first health layer based on starter lifecycle state until core exposes deeper poll-loop and partition runtime snapshots.

<a id="core-40"></a>
### CORE-40 - Add runtime state snapshots

_Date: 2026-07-10_

Expose lightweight runtime state snapshots from CKC consumers.
Include lifecycle, poll-loop, partition, and processing-runtime state that can be read without blocking Kafka poll threads.
Use the snapshots in Spring Boot health details as the next step toward richer readiness and degraded-state diagnostics.

<a id="core-41"></a>
### CORE-41 - Rename ProcessingMode values

_Date: 2026-07-13_

Rename public `ProcessingMode` enum values to make ordering and freshness behavior clearer in configuration.
Update repository-owned demo, infra, starter metadata, and documentation references to the new names.
Do not keep compatibility aliases for the old pre-release names.

<a id="core-42"></a>
### CORE-42 - Add freshness max record age

_Date: 2026-07-13_

Add an optional freshness-first max record age setting for dropping stale records before user handling.
Reject non-null freshness expiry configuration for at-least-once modes so delivery semantics stay explicit.
Expose the setting through repository-owned Spring Boot and demo configuration.

<a id="core-43"></a>
### CORE-43 - Rename processing runtimes

_Date: 2026-07-13_

Rename internal record processing runtime classes so their names align with the current `ProcessingMode` values.
Keep behavior unchanged while updating core wiring and tests.

<a id="core-44"></a>
### CORE-44 - Add processing MDC context

_Date: 2026-07-15_

Add an optional record processing context hook to CKC core.
Wire the Spring Boot starter to provide coroutine-safe MDC context for record processing and failure handling.
Keep the disabled path lightweight so applications can turn MDC off without per-record map or MDCContext allocation.

<a id="core-45"></a>
### CORE-45 - Harden starter startup diagnostics

_Date: 2026-07-15_

Audit the Spring Boot starter validation surface before the first release.
Close gaps in startup diagnostics and lifecycle shutdown behavior tests.
Keep the work focused on starter-managed consumers without changing demo infrastructure.
Align `SmartLifecycle.isRunning()` with the lifecycle bean state so manual-only consumers still receive shutdown callbacks.

<a id="core-46"></a>
### CORE-46 - Split starter auto-configuration internals

_Date: 2026-07-15_

Refactor the large Spring Boot starter auto-configuration source file into focused internal implementation files.
Keep the existing public annotations, properties, lifecycle, registry, and configuration behavior unchanged.
Use the existing starter and demo profile tests as regression coverage.
Leave `CkcSpringBootAutoConfiguration` as the small Spring entrypoint and move lifecycle, runtime resolution, validation, metrics, retry, dispatcher, MDC, and banner logic into internal files.

<a id="core-47"></a>
### CORE-47 - Add starter public API KDoc

_Date: 2026-07-15_

Add concise KDoc to the Spring Boot starter contracts that users and code readers are expected to see.
Document the consumer contract, binding annotation, registry, properties root, and metrics annotations.
Keep implementation helpers undocumented unless a short comment clarifies their role.
Cover lifecycle and auto-configuration entrypoints without adding noisy comments to internal helper code.

<a id="core-48"></a>
### CORE-48 - Polish starter release surface

_Date: 2026-07-15_

Review the Spring Boot starter release surface after the recent configuration additions.
Keep configuration metadata, README examples, demo profile settings, and documented defaults aligned with the implementation.
Use starter and demo profile tests as regression coverage.
Document the full demo profile as the runnable reference and add profile assertions for lifecycle, health, MDC, dispatcher, retry, and cluster defaults.

<a id="demo-61"></a>
### DEMO-61 - Add CKC Spring Boot demo profile

_Date: 2026-07-09_

Added a parallel demo profile named `ckc-spring-boot` that exercises the Spring Boot starter in a normal application shape.
Kept the existing hand-wired `ckc` profiles intact while moving the new profile's CKC runtime settings into application configuration.
The demo profile keeps code focused on annotated consumer classes and business handling.

<a id="demo-62"></a>
### DEMO-62 - Add CKC Spring Boot audit metrics

_Date: 2026-07-09_

Use the starter custom metrics hook in the `ckc-spring-boot` demo profile.
Keep Micrometer record metrics while wrapping retry and drop callbacks with demo audit logging.
This lets internal-lab audit analysis see the same terminal and retry signals for starter-backed CKC consumers.

<a id="demo-63"></a>
### DEMO-63 - Add configurable demo processing dispatchers

_Date: 2026-07-13_

Replace profile-specific dispatcher experiments with a runtime dispatcher setting for demo consumers that use coroutine dispatchers.
Keep existing default behavior while allowing fixed threads, `Dispatchers.Default`, `Dispatchers.IO`, or virtual threads where the profile supports them.
Remove the need for a separate `ckc-sync-loom` Spring profile by selecting virtual threads through configuration.
Keep the legacy internal-lab `ckc-sync-loom` overlay as a compatibility alias for `ckc-sync` plus virtual dispatcher mode until the Helm profile cleanup task removes it.

<a id="demo-64"></a>
### DEMO-64 - Add optional Lettuce metrics

_Date: 2026-07-16_

Enable native Lettuce Micrometer command latency metrics for the demo application Redis client.
Keep the setting optional so Redis client metric cardinality can be disabled when it is not needed.
Expose the setting through internal-lab test launch as `--lettuce-metrics true|false` and bundle metadata.
Add Grafana visibility for Redis command throughput and latency on the shared dashboard.

<a id="demo-65"></a>
### DEMO-65 - Disable load-test audit appender when audit is off

_Date: 2026-07-17_

Prevent the load-test process from initializing the audit TCP appender when `AUDIT_LOG_ENABLED=false`.
Keep publish audit records available when audit logging is enabled.
Cover the disabled path so smoke runs without audit do not emit connection-refused warnings.

<a id="demo-66"></a>
### DEMO-66 - Add profile timeline labels

_Date: 2026-07-20_

Add a small demo profile label component for the profile timeline metric.
Keep plain names for profiles where dispatcher variants are not meaningful.
Include dispatcher names and fixed worker counts for CKC, CKC sync, and comparable coroutine/parallel-consumer demo profiles.

<a id="demo-67"></a>
### DEMO-67 - Add Thread Stats metrics

_Date: 2026-07-26_

Connect the demo application to the local Thread Stats Spring Boot starter.
Expose bounded JVM platform-thread activity metrics through the existing Micrometer/Prometheus endpoint.
Start with reusable thread-name groups for CKC workers, Kafka clients, Armeria, model clients, Redis, and JVM/system threads.
Leave Grafana dashboard panels and raw thread dump artifact collection for follow-up tasks.

<a id="infra-67"></a>
### INFRA-67 - Add CKC Spring Boot lab profile

_Date: 2026-07-09_

Added internal-lab Helm deployment and test-bundle support for running the `ckc-spring-boot` demo profile.
Kept it parallel to existing CKC profiles so the starter-backed application shape can be smoke-tested without replacing hand-wired CKC deployments.

<a id="infra-68"></a>
### INFRA-68 - Add selectable internal-lab Kafka broker

_Date: 2026-07-10_

Add an internal-lab broker selector so tests can run against either Redpanda or Apache Kafka while preserving the existing Kafka API endpoint.
Run the Apache Kafka option on the official `apache/kafka:4.3.1` image with share-group broker settings enabled for single-node experiments.
Keep topic/group reset scripts compatible with both broker implementations.
Generalize Grafana broker CPU visibility so Redpanda and Apache Kafka runs can be inspected from the same dashboard.
Carry the selected broker through run metadata, drain waiting, chaos service actions, and internal-lab documentation.

<a id="infra-69"></a>
### INFRA-69 - Add dynamic internal-lab run planner

_Date: 2026-07-11_

Replace static internal-lab deployment profile selection with a generated run plan based on consumer profile, base TPS, capacity factor, stubs latency, and processing modes.
Keep existing runner controls for processing, audit logging, metrics, worker dispatcher threads, Kafka implementation, and environment overrides.
Print the full computed plan before destructive setup and persist it in run metadata and generated lab state for post-run analysis.
Use measured per-topic capacity models from test definitions when available, support explicit per-topic processing modes, and allow profile-scoped manual plan edits before setup.

<a id="infra-70"></a>
### INFRA-70 - Simplify internal-lab Helm profiles

_Date: 2026-07-13_

Reduce internal-lab Helm overlays to a single generic manual/debug profile while generated run plans own Spring profile, topic partitions, and tuning.
Move experiment variants such as partition ordering, freshness-first, queue-loss, and virtual dispatcher selection into explicit run-test flags and bundle entries.
Move test definitions and internal-lab bundles out of `demo/infra/shared` into `demo/infra/internal-lab/assets` and AWS-owned directories so shared infra only carries orchestration, audit, and Grafana assets.
Add a bundle snippet helper that converts the latest tuned single run into a ready-to-paste bundle `tests:` item.
Make dispatcher selection profile-aware and allow fixed worker thread tuning only when the selected dispatcher is `FIXED`.
Show external hardcoded stale-record filtering as `HARDCODED_FRESHNESS_FIRST_DROP_EXPIRED` while mapping generated application values to the existing enum.
Give worker-based `AT_LEAST_ONCE_PARTITION_ORDERING` topics enough partitions while keeping pollers fixed and scaling workers.
Reuse previous processing modes only when the selected run profile is unchanged; otherwise fall back to the new profile defaults.
Let interactive run-plan edits change per-topic processing modes and show each editable field on its own prompt block.
Prompt for per-topic processing modes before generating the run plan so capacity calculations use the selected modes.

<a id="infra-71"></a>
### INFRA-71 - Add selectable run replicas

_Date: 2026-07-15_

Make the internal-lab dynamic run replica count selectable before plan generation.
Use the selected replica count when rounding topic partitions and computing per-pod workers or pollers.
Keep the profile default as the fallback for non-interactive runs and unchanged previous-profile launches.
Persist the chosen replica count into run metadata/current deployment state and carry it through bundle snippets.
Keep generated worker-based topic partitions at least equal to replica count so every pod can receive an assignment.

<a id="infra-72"></a>
### INFRA-72 - Add selectable demo-stubs replicas

_Date: 2026-07-16_

Add an internal-lab test-run parameter for selecting how many demo-stubs pods to run.
Pass the selected count through stubs deployment, current deployment state, and run metadata.
Keep the chart default as the fallback for existing non-interactive runs.

<a id="infra-73"></a>
### INFRA-73 - Refine Grafana overview controls

_Date: 2026-07-16_

Add a common top-level throughput panel so load-test progress can be compared against the target rate at a glance.
Rename the top dashboard row to cover shared run-level signals instead of only the active consumer profile.
Restore the event-type breakdown control wording while keeping split and aggregated behavior for event and Redis command panels.

<a id="infra-74"></a>
### INFRA-74 - Add step-rate test definition

_Date: 2026-07-16_

Add an internal-lab `step-rate` test definition for controlled throughput discovery.
Ramp the load by ten percentage points over one minute, then hold each step for three minutes.
Continue the sequence through 100 percent of the selected base TPS and finish with a short cool-down.

<a id="infra-75"></a>
### INFRA-75 - Fix Grafana breakdown toggle

_Date: 2026-07-16_

Keep the Grafana event breakdown variable to exactly two visible choices: split and aggregated.
Use a normalized `breakdown` label so event panels split by event type and Lettuce panels split by Redis command.
Avoid comma-separated custom variable values that Grafana expands into extra choices.

<a id="infra-76"></a>
### INFRA-76 - Add internal-lab Loki log collection

_Date: 2026-07-17_

Add persistent internal-lab application log collection so pod restarts do not erase useful runtime logs.
Wire Loki into the host observability stack and provision it as a Grafana datasource.
Deploy a Kubernetes log collector that forwards pod stdout/stderr with run, namespace, pod, container, and application labels.

<a id="infra-77"></a>
### INFRA-77 - Move internal-lab artifacts into results

_Date: 2026-07-17_

Move internal-lab run artifacts out of the historical top-level audit folder and into `/opt/ckc-lab/results/runs/<run-id>`.
Keep audit records, analyzer output, run metadata, and runner logs together under each run directory.
Store bundle logs and summaries under `/opt/ckc-lab/results/bundles/<bundle-set-id>` so later bundle export work has a stable root.

<a id="infra-78"></a>
### INFRA-78 - Use test-definition base TPS defaults

_Date: 2026-07-17_

Default internal-lab dynamic run planning to the selected test definition's `load_test.base_tps`.
Stop carrying a previous deployment's `BASE_TPS` into unrelated smoke or comparison runs.
Keep explicit `--base-rate` and interactive overrides available for intentional load changes.

<a id="infra-79"></a>
### INFRA-79 - Add internal-lab result export

_Date: 2026-07-17_

Add a lab-side export command for packaging run and bundle result artifacts into a portable archive.
Include copied run/bundle files, Grafana dashboard provisioning, a manifest, and Loki log extracts keyed by run id.
Keep Prometheus/Grafana replay out of scope for this first export step.

<a id="infra-80"></a>
### INFRA-80 - Add internal-lab result restore

_Date: 2026-07-17_

Add a local restore workflow for exported internal-lab result archives.
Provide Docker Compose wiring for Grafana and Loki using the exported dashboards and provisioning.
Import exported Loki JSONL records back into Loki so archived pod logs can be inspected after the lab run is gone.

<a id="infra-81"></a>
### INFRA-81 - Export and restore internal-lab metrics

_Date: 2026-07-18_

Export internal-lab Prometheus metrics together with result archives.
Add a restore runner that starts Docker Compose, imports exported metrics and Loki logs, waits for `q`, then tears down the local stack.
Remove redundant per-run load-test stdout files now that pod logs are exported through Loki.

<a id="infra-82"></a>
### INFRA-82 - Continue bundles after failed tests

_Date: 2026-07-18_

Keep internal-lab bundles running remaining tests after one test fails.
Preserve failed run directories in bundle summaries so exports include negative results for comparison.
Keep explicit user interruption as a stop condition.

<a id="infra-83"></a>
### INFRA-83 - Restructure result exports

_Date: 2026-07-19_

Export internal-lab results as directories instead of a single archive.
Limit Prometheus snapshot blocks to the selected run or bundle time window.
Keep restore-ready logs and metrics in a tar archive, write a readable summary, and copy audit files separately per run.

<a id="infra-84"></a>
### INFRA-84 - Print restore dashboard link

_Date: 2026-07-19_

Print a direct Grafana dashboard link when running a restored export.
Use the exported metrics or Loki time window from `manifest.json` so the link opens on the experiment interval.
Keep the plain Grafana, Prometheus, and Loki service links visible for fallback navigation.

<a id="infra-85"></a>
### INFRA-85 - Replace bundles with experiment targets

_Date: 2026-07-20_

Rename the internal-lab comparison bundle concept to experiments.
Model each experiment as one test definition and base TPS executed against explicit consumer targets.
Keep per-target planning latency assumptions in the experiment instead of using a capacity factor.
Move mutable workload YAML under a dedicated internal-lab workloads catalog.
Move reusable consumer-profile planner capabilities into the shared workloads catalog and keep deployment/env overrides in experiment definitions.
Keep the manual-run helper by turning it into a target draft generator for copy-pasting into experiments.
Have the planner emit work-channel capacity, including freshness-by-key telemetry capacity based on the effective fixed fleet.
Make result export default to the latest experiment and autodetect explicit run or experiment ids.
Export Prometheus metrics through bounded query-range blocks instead of copying whole snapshot blocks.
Keep the exported dashboard summary compact and preserve bare Prometheus metrics used by state-timeline panels.
Export raw Prometheus samples instead of query-range evaluation samples so timeline panels do not get lookback overlap.
Use a short restored-Prometheus query lookback to avoid extending rebuilt info gauges across target boundaries.
Keep export helper links readable and print direct Dashboard, Prometheus, and Loki restore links.

<a id="infra-86"></a>
### INFRA-86 - Prebuild Loki restore data

_Date: 2026-07-23_

Move Loki log ingestion from local restore startup into the export phase.
Build a ready-to-mount Loki data directory with the same Loki image and config carried by the export.
Keep restore startup focused on copying prepared Prometheus and Loki data into runtime directories and starting Grafana.

<a id="infra-87"></a>
### INFRA-87 - Expand sync dispatcher comparison

_Date: 2026-07-23_

Replace the dispatcher-focused experiment with `consumer-capacity-comparison` and extend it with external consumer baselines.
Add a `capacity_6min` test definition for order and batch capacity runs.
Add Confluent Parallel Consumer Reactor and Spring Kafka targets alongside the existing CKC sync dispatcher variants.
Keep the experiment focused on comparable internal-lab target configuration without changing runtime code.

<a id="infra-88"></a>
### INFRA-88 - Add experiment target Helm overrides

_Date: 2026-07-23_

Add target-scoped Helm override support for generated internal-lab experiment runs.
Allow experiments to tune demo pod `env.javaToolOptions` and Kubernetes resource requests or limits per target.
Use the override path to give the Spring Kafka baseline more JVM and pod memory without changing every experiment target.

<a id="infra-89"></a>
### INFRA-89 - Add queue capacity experiment

_Date: 2026-07-24_

Add target-level queue capacity overrides to generated internal-lab profile runs.
Create a queue capacity comparison experiment for CKC fixed dispatcher variants.
Include a Spring Kafka coroutine-naive target to compare its simpler admission/backpressure behavior against CKC pause/resume.

<a id="infra-90"></a>
### INFRA-90 - Add Thread Stats dashboard panels

_Date: 2026-07-26_

Extend the shared Grafana overview dashboard with Thread Stats panels.
Show grouped platform-thread counts, state counts, CPU usage, CPU-time rate, and allocation rate.
Keep the panels based on bounded `group` and `state` labels emitted by the Thread Stats Micrometer integration.
Leave raw thread dump collection and exported report artifacts to a separate follow-up task.

<a id="infra-91"></a>
### INFRA-91 - Apply pod filter to Thread Stats panels

_Date: 2026-07-26_

Apply the dashboard `pod` variable to all Thread Stats panel PromQL expressions.
Keep the existing `pod_grouping` selector behavior so panels can still switch between total and per-pod views.
Verify that a concrete pod selector restricts Thread Stats series to that pod.

<a id="demo-68"></a>
### DEMO-68 - Split Armeria Thread Stats groups

_Date: 2026-07-26_

Split the demo Thread Stats Armeria catch-all group into client and server groups.
Classify Armeria boss, event-loop, and blocking-task threads as server activity so idle server threads do not hide client/common activity.
Keep a final `armeria-other` fallback for future Armeria-managed thread names.

<a id="demo-69"></a>
### DEMO-69 - Expose Thread Stats Actuator snapshot

_Date: 2026-07-28_

Expose the Thread Stats Actuator endpoint through the demo application's Armeria-only HTTP setup.
Provide Boot 3.5 web endpoint discovery beans for the non-web Armeria context so Armeria's actuator starter can mount exposed endpoints.
Return grouped snapshot JSON from `/actuator/threadstats/groups` for ad hoc load-test inspection.
Remove stale demo assertions and config for Thread Stats history, logging, and dump properties that are no longer present in the current starter snapshot.
Cover the endpoint with a Spring Boot demo HTTP test.

<a id="demo-70"></a>
### DEMO-70 - Keep Armeria Actuator on the probe port

_Date: 2026-07-28_

Restore the demo Armeria internal-services port to `${SERVER_PORT:8080}` so Kubernetes startup, readiness, and liveness probes keep hitting `/actuator/health` on the application port.
Tighten the HTTP test to call Actuator endpoints through the primary Armeria server port instead of accepting any active server port.
Keep Thread Stats snapshot exposure on the same Actuator surface as health and Prometheus.

<a id="demo-71"></a>
### DEMO-71 - Classify Spring Kafka Thread Stats groups

_Date: 2026-07-28_

Add Thread Stats rules for regular Spring Kafka listener threads so lab runs no longer put the main consumer pools in `other`.
Classify Spring task scheduler, audit TCP appender, JDK HTTP client, Spring lifecycle, and `DestroyJavaVM` support threads.
Cover the expected rule bindings in the demo Spring Boot context test.

<a id="demo-72"></a>
### DEMO-72 - Classify virtual-thread runtime threads

_Date: 2026-07-28_

Add Thread Stats rules for JVM virtual-thread carrier workers and the virtual-thread unparker.
Keep these JVM runtime support threads separate from CKC workers, Kafka clients, and HTTP client pools.
Cover the expected rule bindings in the demo Thread Stats configuration test.

<a id="demo-73"></a>
### DEMO-73 - Classify common ForkJoinPool threads

_Date: 2026-07-28_

Add a Thread Stats rule for JVM common ForkJoinPool worker threads observed during CKC sync virtual-thread runs.
Keep common-pool runtime support separate from the virtual-thread carrier pool group.
Cover the rule in the demo Thread Stats configuration test.

<a id="demo-74"></a>
### DEMO-74 - Add sync JDK HTTP virtual executor flag

_Date: 2026-07-28_

Add an experimental demo setting for building sync JDK HTTP clients with a virtual-thread executor.
Share one configured JDK HTTP client across sync model and registry clients.
Keep the default JDK HTTP client executor behavior unchanged unless the flag is enabled.
Cover the default and virtual executor bindings in demo tests.

<a id="demo-75"></a>
### DEMO-75 - Add Spring Kafka thread-pool profile

_Date: 2026-07-28_

Add a Spring Kafka profile that consumes batches with low poller concurrency and hands records to bounded fixed platform-thread pools.
Use the existing synchronous demo business services so the profile isolates OS-thread processing from coroutine and virtual-thread processing.
Reuse Spring Kafka-style metrics, audit, retry, and freshness handling while giving the profile a distinct timeline label.

<a id="demo-76"></a>
### DEMO-76 - Use one demo Kafka consumer group

_Date: 2026-07-29_

Replace the per-topic demo Kafka consumer group configuration with one application-level group id.
Keep topic offsets separate through Kafka's group/topic/partition offset key instead of separate group names.
Rename Spring Kafka listener ids to a shared `spring-kafka-consumer-*` prefix so Thread Stats rules do not need one prefix per Spring Kafka profile.

<a id="demo-77"></a>
### DEMO-77 - Adapt Thread Stats cached actuator interval

_Date: 2026-07-29_

Remove the demo's manual Armeria web endpoint discovery workaround now that the Thread Stats starter orders itself before Armeria actuator discovery.
Treat `/actuator/threadstats/groups` as a cached latest-interval view instead of an on-demand sampler.
Update the actuator test to wait for `available=true` with a short sampling interval.

<a id="demo-78"></a>
### DEMO-78 - Keep Spring Kafka listener ids separate from group ids

_Date: 2026-07-29_

Set Spring Kafka listener annotations to keep their stable listener ids from becoming Kafka consumer group ids.
Preserve the shared demo group id supplied by the listener container consumer factories.
Cover the annotation contract so future listener-id changes do not reintroduce per-listener groups.

<a id="demo-79"></a>
### DEMO-79 - Add Spring Kafka virtual-thread workers

_Date: 2026-08-02_

Add a `spring-kafka-virtual-thread-pool` demo profile that keeps Spring Kafka batch pollers separate from business processing.
Run accepted records on virtual-thread-per-task workers behind the configured worker concurrency and queue admission limits.
Expose Thread Stats grouping and profile metrics for the virtual worker profile.
Allow internal-lab experiment targets to override generated parallelism knobs while keeping concrete partitions, workers, and pollers calculated by the planner.
Add a 10-minute 3m/6m/1m load-test definition and a Spring Kafka Thread Stats progression experiment for screenshot runs at 5k TPS.
Allow experiment defaults to provide `planning_latency` so only targets with different latency assumptions need local overrides.
Force that progression experiment to use the default JDK HTTP client executor so ordinary Spring Kafka runs do not inherit virtual threads from a previous deployment.
Tune Kafka consumer fetch batching in that progression experiment to reduce small-response broker noise during high-partition screenshot runs.
Raise worker queue capacities for thread-pool progression targets to twice the tuned `max.poll.records` value so worker admission does not dominate the comparison.
Pass Kafka consumer fetch overrides from experiment environment through lab prepare into generated demo Helm values.

Verification: targeted `:ckc-demo:test` profile/config tests passed, `plan-run.py` accepted the new manual partition/worker/poller shape, changed Python helpers passed `py_compile`, and changed shell scripts passed `bash -n`.

<a id="demo-80"></a>
### DEMO-80 - Add load-test producer batching controls

_Date: 2026-08-03_

Expose load-test Kafka producer `linger.ms`, `batch.size`, `compression.type`, and `buffer.memory` through environment-backed configuration.
Pass producer batching overrides through the internal lab runner and include them in run metadata for later pcap and broker CPU analysis.
Set the Thread Stats progression experiment defaults to use a larger producer linger, batch size, and buffer for many-partition comparisons.

<a id="infra-92"></a>
### INFRA-92 - Restart demo after lab image update

_Date: 2026-07-28_

Teach `update-lab.sh` to restart the existing `ckc-demo` deployment when the demo image fingerprint changes.
Wait for the rollout to complete so a lab update does not leave the cluster running stale pods against a freshly loaded `latest` image.
Report whether the demo deployment was restarted in the update summary.

<a id="infra-93"></a>
### INFRA-93 - Improve interactive run-test defaults

_Date: 2026-07-28_

Improve the internal-lab interactive `run-test.sh` path so dispatcher and planning-latency inputs use profile-aware defaults.
Keep generated run plans reproducible by storing the defaults in the shared consumer-profile catalog instead of ad hoc Bash constants.
Allow dynamic run planning to use those defaults when explicit latency flags are omitted.
Print an explicit interactive note when the selected profile does not support dispatcher selection.

<a id="infra-94"></a>
### INFRA-94 - Expose JDK HTTP executor run setting

_Date: 2026-07-28_

Add a first-class internal-lab `run-test.sh` flag for selecting the demo sync JDK HTTP client executor mode.
Wire the setting through generated Helm values, prepared deployments, current deployment state, run metadata, and experiment target env mapping.
Document how experiments can enable `JDK_HTTP_CLIENT_EXECUTOR: VIRTUAL` for sync model-client targets.

<a id="infra-95"></a>
### INFRA-95 - Add Spring Kafka thread-pool lab profile

_Date: 2026-07-28_

Add the Spring Kafka thread-pool profile to the shared internal-lab consumer profile catalog.
Make run planning expose the profile with OS-thread worker concurrency while keeping poll loop concurrency low.
Wire the profile into comparison experiments and consumer-group reset/drain helpers so it can run from both interactive tests and experiment definitions.

<a id="infra-96"></a>
### INFRA-96 - Use one demo Kafka consumer group in infra

_Date: 2026-07-29_

Expose the unified demo Kafka consumer group id through Helm values and deployment environment.
Collapse internal-lab reset and drain helpers from per-topic Spring Kafka group lists to the single `ckc-demo` group.
Update Kafka exporter filters, Grafana lag queries, and lab documentation to follow the unified group id.

<a id="infra-97"></a>
### INFRA-97 - Collect Thread Stats snapshots during runs

_Date: 2026-07-29_

Collect Thread Stats text snapshots from every running demo pod while an internal-lab load test is active.
Write timestamped, pod-delimited blocks to each run's result directory so experiment targets keep their own thread evidence.
Use the Kubernetes pod proxy to address pods directly without requiring HTTP tools inside the demo container.

<a id="infra-98"></a>
### INFRA-98 - Default internal-lab tests to Apache Kafka

_Date: 2026-08-01_

Make Apache Kafka the default broker implementation for internal-lab test runs.
Keep Redpanda available through the existing `--kafka-implementation redpanda` override and environment variable.
Align helper fallbacks and documentation so non-interactive runs use the same default as interactive runs.

Verification: `bash -n` passed for changed shell scripts, `python -m py_compile` passed for changed Python helpers, and `git diff --check` passed.

<a id="infra-99"></a>
### INFRA-99 - Attach Thread Stats agent to Apache Kafka

_Date: 2026-08-01_

Build the local Thread Stats Java agent from the sibling repository during internal-lab install and update flows.
Mount the agent into the Apache Kafka host container and expose its Prometheus endpoint on a separate port and scrape job.
Add Grafana broker Thread Stats panels that query `ckc-kafka-thread-stats` so Kafka JVM metrics stay separate from demo app metrics.
Clear `KAFKA_OPTS` for Kafka CLI helper commands so admin tools do not try to start a second agent on the broker metrics port.
Polish the Kafka agent grouping from the live optilab `/threadstats` report and restart Kafka during base redeploys so bind-mounted agent config changes take effect.
Make the Grafana application pod selector time-range aware and hide pod names from app legends when the current range/filter resolves to a single pod.

Verification: changed shell scripts passed `bash -n`, changed Python helpers passed `py_compile`, Docker Compose rendered with the Apache Kafka profile, dashboard JSON parsed, `thread-stats-agent` Maven package succeeded, and a local `-javaagent` smoke run scraped Prometheus metrics.

<a id="demo-81"></a>
### DEMO-81 - Add selectable demo HTTP clients

_Date: 2026-08-04_

Add JDK `HttpClient.sendAsync` implementations for the suspend demo model and registry clients.
Select Armeria or JDK transport with one application property while preserving the existing client interfaces and processing profiles.
Add the complementary sync-client selector so virtual-thread processing can compare JDK `send` with Armeria async I/O followed by synchronous completion waiting.
Use the option for a one-off optilab comparison before deciding whether to remove Armeria from the demo application.
Keep Armeria as the default transport for both suspend and synchronous clients after it proved more CPU-efficient in the comparison; retain JDK as an explicit experimental option.

Verification: property-binding, JDK suspend-profile, explicit JDK sync-profile, and default Armeria sync-profile context tests passed, including confirmation that each option omits the unselected client beans. The full demo suite passed 93 of 94 tests; the unrelated existing Thread Stats `/groups` endpoint test fails against the locally installed newer Thread Stats endpoint format.

<a id="infra-100"></a>
### INFRA-100 - Deploy Armeria HTTP defaults

_Date: 2026-08-04_

Deploy the updated demo image with Armeria as the synchronous and suspend HTTP-client default to optilab.
Reduce the one-off HTTP comparison to Armeria virtual-thread and coroutine targets followed by one explicit JDK coroutine target.
Make every retained transport selection explicit so stale application defaults cannot change the experiment meaning.
Add a small server-only comparison of Spring Kafka coroutine-naive and CKC, each using one fixed dispatcher thread and Armeria.

Verification: `update-lab.sh` rebuilt and loaded the demo image, restarted the deployment, and completed its rollout. The server-side HTTP experiment parser resolved three targets with explicit Armeria, Armeria, and JDK transports respectively; the replacement pod is ready on the new image. Both naive-versus-CKC targets passed server-side dry-run planning with matching load, queue, dispatcher, and Armeria settings.

<a id="infra-101"></a>
### INFRA-101 - Add grouped Thread Stats panels

_Date: 2026-08-05_

Pair detailed and smoothly stacked Thread Stats panels for CPU usage, CPU time rate, and allocation rate.
Aggregate stacked series into stable Business, Kafka, HTTP client, Redis client, and Other categories.
Treat CKC poll threads as Kafka work and virtual-thread runtime threads as business work for the demo profiles.
Use compact color-only legends throughout the Thread Stats section and keep thread counts below the comparison rows.

Verification: dashboard JSON parsed successfully, Thread Stats panel ids and layout were validated, the repository Thread Stats row matched the reviewed live dashboard exactly, and all 20 panel PromQL expressions parsed successfully against the optilab Prometheus API.

<a id="infra-102"></a>
### INFRA-102 - Show end-to-end processing latency in Grafana

_Date: 2026-08-06_

Replace the obsolete successful record-age queries with the new successful end-to-end processing latency metric.
Rename the order, batch, and cauldron panels so the dashboard presents the consumer's primary latency SLA directly.
Remove the obsolete success error-label filter from queries because the replacement metric is emitted only for successful records.

Verification: dashboard JSON parsed successfully, all three target panels retained five statistic queries, and all 15 changed PromQL expressions parsed successfully against the optilab Prometheus API.

<a id="core-49"></a>
### CORE-49 - Add adaptive at-least-once commit triggers

_Date: 2026-08-05_

Separate the contiguous processed frontier from the last successfully committed offset so failed commits remain retryable.
Advance partition trackers between commits and trigger a commit when either the pending record threshold or the time interval is reached.
Expose the record threshold through the core API and Spring Boot configuration while retaining the interval as a low-traffic fallback.

Verification: focused core offset, poll-loop, builder, and processing tests passed; the complete Spring Boot starter suite passed.
The unrelated timing-sensitive runtime queue-size test remains flaky in the complete core suite and passes in isolation.

<a id="core-50"></a>
### CORE-50 - Compact oversized OffsetTracker buffers

_Date: 2026-08-06_

Add an internal compaction operation that preserves live processed offsets while shrinking unused ring capacity.
Use hysteresis to avoid grow/shrink churn during ordinary workload variation.
Compact before commit snapshots so metadata serialization and post-restart tracker capacity reflect the useful state.

Verification: OffsetTracker, partition commit-data, metadata serializer, and poll-loop tests passed.

<a id="core-51"></a>
### CORE-51 - Replace record age with end-to-end latency

_Date: 2026-08-06_

Replace the processing-start record-age timer with latency measured from the Kafka record timestamp through successful terminal processing.
Remove the obsolete record-age metric and its failure series rather than retaining overlapping telemetry before the first release.
Align the core metrics contract, Micrometer schema, tests, and module documentation with the new SLA-oriented meaning.

Verification: the complete core, Micrometer, and Spring Boot starter test suites passed, as did focused demo metric and CKC profile tests.
The complete demo suite passed 93 of 94 tests; the unrelated existing Thread Stats endpoint-format test still fails.

<a id="demo-82"></a>
### DEMO-82 - Fix the Thread Stats actuator endpoint test

_Date: 2026-08-06_

Diagnose the persistent demo test failure against the current Thread Stats Spring Boot starter.
Align the endpoint request and assertions with the supported cached-report representation without weakening readiness coverage.
Run the focused test repeatedly and restore a clean complete demo test suite.

Verification: the focused endpoint test passed in three independent runs, and the complete demo suite passed all 94 tests.

<a id="infra-103"></a>
### INFRA-103 - Add demo context-switch observability

_Date: 2026-08-06_

Track the Kubernetes demo JVM through the internal-lab host process exporter.
Expose voluntary and nonvoluntary process context-switch rates in the shared Grafana dashboard.
Keep the process matcher specific enough to exclude other Java workloads on optilab.

Use thread-level exporter counters so the panel covers every JVM platform thread and avoids the process-level counter underflow observed when a pod PID changes.
Verification: the matcher selected exactly one live demo JVM, the exporter exposed both switch types, dashboard JSON and PromQL validation passed, and the deployed optilab Prometheus and Grafana APIs returned the new series and panel.

<a id="infra-104"></a>
### INFRA-104 - Align application Thread Stats dashboard metrics

_Date: 2026-08-06_

Replace obsolete application Thread Stats CPU, thread-count, and thread-state metric names with the unified schema.
Keep the allocation panels unchanged because their metric name remains compatible.
Validate the updated detailed and category queries against the active optilab experiment before updating the live dashboard without restarting workloads.

Verification: dashboard JSON parsed successfully, all 14 changed PromQL expressions parsed against the active optilab Prometheus, and the live Grafana API loaded the corrected panels while preserving the in-progress context-switch panel.

<a id="infra-105"></a>
### INFRA-105 - Isolate application Thread Stats panels

_Date: 2026-08-06_

Add an explicit demo scrape-job matcher to every application Thread Stats query.
Keep the existing pod selector while preventing its all-value regex from matching Kafka agent series that have no pod label.
Validate detailed and category panels against the active optilab experiment and update live Grafana without restarting workloads.

Verification: dashboard JSON parsed successfully, all 20 application Thread Stats targets executed against optilab Prometheus, broker-only groups were excluded, and live Grafana reloaded all scoped queries without restarting the experiment.

<a id="infra-106"></a>
### INFRA-106 - Generate experiment reports

_Date: 2026-08-07_

Generate a local human-readable Markdown report for every completed internal-lab experiment.
Build a stable report model from experiment definitions, run metadata, audit summaries, reusable SLA profiles, and Prometheus measurements.
Render deterministic GitHub-compatible SVG diagrams and comparison charts without coupling analysis to Markdown generation.
Evaluate exact per-record end-to-end latency from audit publish/processed pairs, including post-load drain, against profile limits and allowed violation percentages.
Show processed records, latency misses, miss percentage, maximum observed latency, and separate delivery/latency/overall results in the comparison table.
Plot load in TPS against elapsed `HH:MM`, align phase names and minute/second durations with their segments, and place chronological chaos annotations on separate levels below the chart.
Keep Evidence Bundle export and repository publication as explicit manual operations.

Verification: all 17 experiment-report and audit-analyzer tests passed, Python sources compiled, and the historical queue-backlog preview regenerated successfully.

<a id="demo-83"></a>
### DEMO-83 - Add duration-based chaos scenarios

_Date: 2026-08-08_

Replace command-style paired chaos steps with semantic instantaneous and duration-based scenarios.
Generate recovery actions automatically for duration-based degradation and outage scenarios.
Guarantee cleanup when a run finishes, fails, or is interrupted, and reject ambiguous overlapping scenarios for the same target.
Migrate the current demo test definitions without retaining compatibility with historical experiment definitions.

Verification: 24 chaos-scenario, experiment-report, and audit-analyzer tests passed; Python and Bash syntax checks passed; all current chaos definitions normalized through the new contract.

<a id="infra-107"></a>
### INFRA-107 - Render semantic chaos timelines

_Date: 2026-08-08_

Render duration-based chaos scenarios as translucent ranges and instantaneous scenarios as timeline markers.
Keep load phases aligned with the load curve and chaos descriptions in non-overlapping lanes below the chart.
Protect elapsed-time labels from vertical marker lines and add separate action and service icon slots with self-contained SVG output.
Bundle selected Kubernetes, Kafka, Redis, and Fluent Bit artwork, add a project-owned HTTP-stubs gear glyph, and retain deterministic fallbacks for unknown targets.
Keep elapsed-time labels below the plot, add solid full-height vertical grid lines, gently round load-profile corners, and align each colored scenario line with its action icon.
Use width-aware elapsed ticks, higher-contrast axes and chaos overlays, and a subtle rounded frame around the complete timeline.
Format elapsed positions as compact durations rather than clock times and remove redundant tick marks from the gridded axis.
Render stub-degradation events as expanded comparison cards containing only affected HTTP downstreams and highlighting changed p90/p95/p99/p100 delay and error-rate values.

Verification: all 27 chaos-scenario, experiment-report, and audit-analyzer tests passed; Python and Bash syntax checks passed; the queue-backlog preview rendered a three-row stub comparison, one instant marker, compact elapsed labels, and paired icon slots for both cards.

<a id="demo-84"></a>
### DEMO-84 - Scale load-test producers by topic throughput

_Date: 2026-08-11_

Model each topic as an independently scaled producer service with a configurable messages-per-second capacity per producer.
Build stable per-topic Kafka producer pools from the peak load-profile throughput while preserving same-key producer affinity.
Expose bounded Kafka producer and generator diagnostics for inspecting batching, compression, throughput, latency, backpressure, and failures.

Verification: all 28 load-test tests passed, the application distribution built successfully, and a live dry-run scrape returned per-topic pool and sent/acked/failed metrics from the embedded Prometheus endpoint.

<a id="infra-108"></a>
### INFRA-108 - Observe scaled load-test producers

_Date: 2026-08-11_

Pass per-topic producer capacity from internal-lab test definitions into the load-test runtime and keep its lab metrics endpoint fixed on port 9405.
Expose the host load-test metrics endpoint to Kubernetes Prometheus without changing the single-process internal-lab execution model.
Add Grafana producer panels for pool size, throughput, batching, compression, latency, buffer pressure, retries, and failures.

Verification: all 21 internal-lab tests passed; Python, Bash, POSIX shell, YAML, dashboard JSON, and 20 PromQL expressions validated; optilab smoke run `20260811T131027Z` completed successfully with configured pool sizes `2/2/2`, an `UP` Prometheus target, persisted capacity metadata, and samples in all 10 live Grafana producer panels.

<a id="demo-85"></a>
### DEMO-85 - Classify CPC Kafka threads

_Date: 2026-08-14_

Classify Confluent Parallel Consumer `pc-broker-poll`, `pc-control`, and `pc-pool-` threads in the existing `kafka-client` Thread Stats group.
Classify the Kafka Micrometer binder's `micrometer-kafka-metrics` threads in the same group.
Keep the Reactor processing dispatcher in the separate `confluent-parallel-worker` business group.
Cover the CPC thread-name rules in the demo configuration test.

Verification: the focused Thread Stats configuration test passed, all CPC names from the completed run resolved to the intended Kafka and business groups, and the updated demo image rolled out successfully on optilab. A live CPC actuator snapshot classified `micrometer-kafka-metrics` under Kafka and contained no `OTHER` section.

<a id="infra-109"></a>
### INFRA-109 - Compare reserved CKC workers under large poll batches

_Date: 2026-08-22_

Add a single-topic internal-lab experiment that compares 40 and 200 CKC coroutine workers on one fixed dispatcher thread and on a `ckc-sync` virtual dispatcher restricted to one carrier thread.
Shape Kafka producer and consumer batching so steady-state polls deliver at least 500 telemetry records while 40 workers remain sufficient for the configured suspend-only processing latency.
Run the real Redis/HTTP processing path and degrade all demo-stub latencies from 1 ms to 10 ms for one minute so the reserved workers are exercised during a controlled latency spike.
Keep Lettuce command metrics enabled so downstream Redis rate and latency remain observable during the comparison.
Expose poll-batch, active-worker, processing-worker CPU/allocation, and context-switch measurements in generated experiment reports so the workload premise and worker fan-out are visible alongside CPU and throughput.
Automatically disable the telemetry freshness-age limit in generated plans that select an at-least-once processing mode.
Avoid false missing-image failures in lab preparation by making the container-image checks safe under Bash `pipefail`.

Verification: all 22 internal-lab tests passed; Python and Bash syntax checks passed; all generated run plans resolved to one partition, one poll loop, required parallelism 36, and configured concurrency 40/200. Live processing-enabled optilab experiment `20260823T154958Z`, with Lettuce metrics enabled and both fail-fast VT targets first, completed all four targets and passed the large-poll-batch SLA with 597-674 average records per poll and 933-1000 maximum records per poll. During the one-minute slowdown, both 40-worker targets fell to about 3.2K records/s and accumulated about 66K-69K lag, while both 200-worker targets sustained about 4.5K records/s without additional lag. The actuator and Prometheus contained per-command Lettuce completion histograms, and the live VT deployment retained exactly one `ForkJoinPool-1-worker-1` carrier.

<a id="infra-112"></a>
### INFRA-112 - Store per-pod Thread Stats diagnostic artifacts

_Date: 2026-08-24_

Replace the monolithic Thread Stats snapshot log with paired timestamped JSON and readable text artifacts grouped by application pod.
Collect a full snapshot from every running demo pod once per minute while preserving pod identity and collection status in a machine-readable index.
Expose collection coverage in run metadata and experiment reports, and keep the artifact layout compatible with local and cloud Kubernetes access.

Verification: all 26 internal-lab tests passed; Python and Bash syntax checks passed; a live optilab collection discovered the running demo pod through Kubernetes, stored a readable 4.6 KiB text report beside its normalized 14.2 KiB full JSON snapshot, and reported 100% coverage without discovery or snapshot failures.

<a id="demo-86"></a>
### DEMO-86 - Add packet-capture tooling to lab images

_Date: 2026-08-24_

Install `tcpdump` in the demo application and load-test container images used by local and cloud lab runs.
Keep packet capture dormant by default; Kubernetes capabilities, scheduling, collection, and analysis remain infrastructure concerns.
Verify both images expose the tool without changing their application entrypoints.
Allow the load-test Gradle distribution into its Docker build context so the declared image can be built by the shared AWS workflow.

Verification: both Gradle distributions and Docker images built successfully; both containers expose tcpdump 4.99.6 with libpcap 1.10.6 while retaining their original application entrypoints.

<a id="infra-113"></a>
### INFRA-113 - Add scheduled packet captures

_Date: 2026-08-24_

Add semantic `diagnostic_steps` that schedule bounded packet captures from the load-test timeline without treating observation as chaos.
Grant `NET_RAW` and mount bounded capture storage only when diagnostics are enabled, then retrieve completed files before pod or Job cleanup.
Use the same artifact contract for application and load-test targets across optilab and AWS, with host capture adapting the current optilab load generator.
Compress captures on the runner, preserve structured status and checksums, and expose capture coverage in experiment reports and Evidence Bundles.
Validate the full optilab path with a required two-point smoke capture: 180 consumer-side packets and 348 producer-side packets, with no files left in the pod.

<a id="infra-114"></a>
### INFRA-114 - Analyze Kafka packet captures

_Date: 2026-08-24_

Analyze every captured Kafka connection and aggregate request/response message counts by API type for producer and consumer observation points.
Report connection counts, wire bytes, TCP/IP overhead, Kafka payload, record-batch overhead, and compression savings with explicit unavailable estimates for encrypted traffic.
Persist machine-readable JSON plus a readable text summary beside each run, and render paired producer/consumer comparison bars for every experiment target.
Rebuild the missing analyzer around tshark and native Kafka RecordBatch decompression, keeping the same post-workload path suitable for optilab and AWS artifacts.

Verification: all 36 internal-lab tests and 3 focused analyzer tests passed; Python and Bash syntax checks passed. Live optilab smoke run `20260824T142956Z` resolved `eth0` for the application and `br-cf23c25e43fb` from the active Kafka Docker network for the host producer. Both captures had zero retransmissions and no duplicate interface observations; TCP payload equalled Kafka PDU bytes exactly at both points. Automatic JSON/text analysis reported 16 Produce request/response messages and 8 LZ4 batches on the producer side, plus 106 Kafka messages and 7 LZ4 batches on the consumer side.

<a id="infra-115"></a>
### INFRA-115 - Compare Kafka compression across parallelism models

_Date: 2026-08-24_

Add a ten-minute, 5K TPS experiment comparing Spring Kafka parallelism backed by 200 partitions with CKC parallelism backed by 200 workers over three partitions.
Run both consumer layouts first without producer compression and then with LZ4, keeping workload, batching, processing, and resource settings fixed.
Capture producer and consumer traffic twice during steady load and report wire/batch compression alongside application, producer, and broker resource cost.
Classify the load generator in process-exporter and include its CPU/RSS in the report; raise the analyzer CSV field limit for reassembled Kafka payloads larger than 128 KiB.
Add per-configuration totals for producer wire, consumer wire, message values, record attributes, useful logical payload, and record metadata.
Normalize differing fixed-duration capture samples with separate producer and consumer logical-payload-to-wire ratios.

<a id="demo-87"></a>
### DEMO-87 - Randomize telemetry diagnostics payloads

_Date: 2026-08-24_

Replace the repeating telemetry diagnostics byte pattern with independently generated random bytes for every event.
Preserve the configured payload size, including zero-length payloads, and cover size and per-event variation with focused tests.
Keep compression experiments representative by preventing large Kafka record batches from exploiting an artificial cross-record pattern.

Verification: `./gradlew :ckc-demo-load-test:test` passed, including focused coverage for independently randomized 256-byte payloads and zero-length payloads.

<a id="infra-116"></a>
### INFRA-116 - Compare random and empty telemetry payload compression

_Date: 2026-08-24_

Extend the ten-minute, 5K TPS compression comparison to a full Spring/CKC, none/LZ4, random-256/empty payload matrix.
Keep the application topology, processing cost, producer batching, resource limits, Thread Stats cadence, and packet-capture points identical across all eight targets.
Use payload-to-wire ratios and the existing record-batch decomposition to separate realistic compression from protocol and polling overhead.

Verification: all 36 internal-lab tests passed and all eight dry-run plans resolved the intended topology, codec, and diagnostics payload size. Optilab experiment set `20260824T180504Z` completed all eight ten-minute targets with throughput SLA PASS, 10/10 Thread Stats snapshots, and 4/4 successful captures per target. Random 256-byte payloads raised the LZ4 ratio to 70.95% for Spring and 62.25% for CKC, while empty payloads reached 37.97% and 18.39%; CKC used about 0.94-0.95 application cores versus 2.31-2.35 for Spring.

<a id="demo-88"></a>
### DEMO-88 - Add experiment identity and topic-specific Kafka settings

_Date: 2026-08-27_

Pass the experiment target name into the demo profile-info metric while retaining the Spring profile as diagnostic metadata.
Allow load-test producer batching settings and demo consumer fetch settings to vary by topic with shared settings as backwards-compatible fallbacks.
Expose the resolved settings in diagnostics so reports can describe the actual Kafka client configuration used by each run.

Verification: `./gradlew :ckc-demo:test :ckc-demo-load-test:test` passed with focused coverage for explicit experiment profile labels, topic consumer override resolution, Spring Kafka factory properties, and topic producer fallback behavior.

<a id="demo-89"></a>
### DEMO-89 - Reconfigure load-test producers during a run

_Date: 2026-08-27_

Accept a scheduled sequence of per-topic Kafka producer configuration changes in the load-test process.
Replace active producer pools safely without racing concurrent sends, preserving asynchronous acknowledgements through a bounded flush and close transition.
Emit structured events with actual start, completion, and failure timestamps so orchestration can build reports and Grafana annotations from observed execution.

Verification: `./gradlew :ckc-demo-load-test:test` passed with schedule parsing, lazy and active pool replacement, generation-scoped Kafka metrics cleanup, and actual structured start/success event coverage.

<a id="infra-117"></a>
### INFRA-117 - Make experiment tests self-contained

_Date: 2026-08-27_

Allow an experiment-level `test` section to extend a reusable test-definition preset, override any mapping value, replace lists, or define a complete test inline.
Materialize and validate the resolved test before planning targets, then use that exact artifact for every run and later report generation.
Retain compatibility with existing `test_definition` experiments while making presets optional for experiment-specific workloads.
Wire readable target names, per-topic Kafka producer/consumer settings, and scheduled producer reconfiguration from the resolved experiment into local and shared runners.

Verification: all 42 internal-lab unit tests passed; modified Python helpers compiled, Bash scripts passed syntax checks, and the internal-lab demo Helm chart rendered successfully.

<a id="infra-118"></a>
### INFRA-118 - Add experiment event annotations

_Date: 2026-08-27_

Collect actual producer reconfiguration, chaos, and diagnostic lifecycle events into one per-run JSONL timeline.
Show observed events in the experiment report and publish them as Grafana annotations during live runs.
Include the timeline in exported bundles and replay its annotations into the bundle's local Grafana instance.

The run wrapper now preserves load-test stdout, consumes its structured producer events, and combines them with actual chaos and diagnostic lifecycle records under a locked JSONL journal.
Experiment reports render the observed target-aware timeline and retain the raw journal; the live and restored dashboards expose the same events through Grafana's annotations API.

Verification: all 44 internal-lab unit tests passed; the load-test event wrapper was exercised end to end, modified Python files compiled, Bash scripts passed syntax checks, the Helm chart rendered, and the dashboard JSON annotation layer was validated.

<a id="infra-119"></a>
### INFRA-119 - Compare Spring Kafka producer linger sweeps

_Date: 2026-08-27_

Add a 5000 TPS telemetry experiment for the standard Spring Kafka consumer with identical LZ4 and uncompressed targets.
Start producers at `linger.ms=0`, warm up for two minutes, then increase linger by 100 ms every minute through 1000 ms.
Capture representative Kafka traffic near the beginning, middle, and end of the sweep while runtime events mark every actual reconfiguration.

Verification: the resolved experiment validated as a 13-minute load profile with ten ordered producer changes, three diagnostic captures, and two Spring Kafka targets; all 44 internal-lab unit tests passed. The experiment was not executed.

<a id="demo-90"></a>
### DEMO-90 - Remove runtime producer reconfiguration

_Date: 2026-08-27_

Remove scheduled producer configuration changes, switchable producer pools, and producer-generation metrics from the load-test application.
Return each topic producer pool to one immutable configuration for the full process lifetime while retaining topic-specific producer settings.
This avoids accumulating retired Kafka producer metrics and resources during a run.

Verification: `./gradlew :ckc-demo-load-test:test` passed.

<a id="infra-120"></a>
### INFRA-120 - Make experiment annotations optional

_Date: 2026-08-27_

Disable live and restored-bundle Grafana annotations by default while retaining the experiment event journal and report timeline.
Remove producer reconfiguration schedules and their load-test event wrapper from local and shared orchestration.
Replace the runtime linger sweep with independent five-minute Spring Kafka targets for every compression and linger configuration.

The comparison now contains 22 static targets: `linger.ms=0..1000` in 100 ms increments for both LZ4 and no compression. Each target includes one steady-state packet capture, while Grafana annotation publishing and bundle replay remain explicitly opt-in.

Verification: all 44 internal-lab unit tests passed; modified Python files compiled, Bash scripts passed syntax checks, the dashboard JSON parsed, and the resolved experiment validated as 22 fixed five-minute targets. The experiment was not executed.

<a id="infra-121"></a>
### INFRA-121 - Annotate test-run starts in Grafana

_Date: 2026-08-28_

Publish one compact, filterable Grafana annotation when each target load-test process starts.
Include the experiment target, run id, profile, load, parallelism, and relevant Kafka producer settings.
Keep detailed chaos and diagnostic annotations behind their existing opt-in flag and retain the complete JSONL report timeline.

Run-start annotations are enabled by default and carry filterable experiment, target, profile, compression, linger, and run tags plus compact topology, load, producer, and consumer details. Exported bundles replay run starts by default while detailed event replay remains opt-in.

Verification: all 46 internal-lab unit tests passed; modified Python files compiled, Bash scripts passed syntax checks, and the annotation helper was exercised against real run metadata. `update-lab.sh` completed on optilab; installed assets matched the repository, Grafana reported healthy, and all lab Kubernetes workloads were Ready. No experiment was started.

<a id="infra-122"></a>
### INFRA-122 - Keep run annotations variant-only

_Date: 2026-08-28_

Replace the verbose run-start annotation body with a compact label derived from the part of target names that differs between experiment runs.
Keep complete run configuration in run metadata and the exported experiment summary instead of duplicating it in Grafana markers.

For the Spring Kafka linger comparison the 22 markers now read only `lz4 · lingerN` or `none · lingerN`; `run_id` remains a service tag rather than annotation text.

Verification: all 48 internal-lab unit tests passed; modified Python files compiled, Bash scripts passed syntax checks, and all 22 resolved experiment labels were checked. Optilab was updated, installed assets matched the repository, the installed helper produced a variant-only marker, and Grafana remained healthy. No experiment was started.

<a id="infra-123"></a>
### INFRA-123 - Make run annotation labels explicit

_Date: 2026-08-28_

Allow experiment targets to define the exact short text shown in their run-start annotation instead of inferring parameter semantics from target names.
Reduce Grafana annotation tags to one tag containing only the event type.

The short lab comparison now uses `compression.type=lz4` and `compression.type=none`, while both targets retain the same `linger.ms=200` outside the annotation text.

Verification: all 48 internal-lab unit tests passed; modified Python files compiled and Bash scripts passed syntax checks. Optilab was updated, installed helpers matched the repository, a mocked installed publisher produced exactly one `run_started` tag, and the lab-only experiment resolved with no diagnostics or chaos. No experiment was started.

<a id="infra-124"></a>
### INFRA-124 - Interleave compression targets by linger

_Date: 2026-08-28_

Order the long Spring Kafka linger comparison as adjacent uncompressed and LZ4 target pairs for each increasing `linger.ms` value.
Give every target an explicit annotation label naming both varying producer parameters.

The sequence now runs `none` then `lz4` at each linger value from 0 through 1000 ms before advancing by 100 ms.

Verification: all 48 internal-lab unit tests passed; the resolved experiment validated as 11 ordered compression pairs with 22 five-minute targets and explicit labels. Optilab was updated and its installed experiment matched the repository byte-for-byte. The experiment was not started.

<a id="infra-125"></a>
### INFRA-125 - Add an ephemeral AWS experiment smoke workflow

_Date: 2026-08-29_

Run an end-to-end AWS smoke experiment from any prepared checkout without depending on optilab-specific paths or a pre-existing runner.
Create the runner, disposable lab, and artifact transport for one session; download a portable local result before deleting every session-owned AWS resource.
Keep Terraform state and resumable lifecycle metadata under the checkout-local ignored work area, tag and inventory every resource for later cost attribution, and verify cleanup independently after teardown.
Reuse the shared test, audit, diagnostics, metrics, logs, and report assets so the first smoke is a vertical slice toward longer production-like experiments.

Live verification: `smoke-20260829-a6` completed successfully in `eu-central-1` with an ephemeral EKS cluster, three Kubernetes Kafka brokers, Kubernetes Redis, and a checkout-created SSM runner. All 517 published records reached one terminal processed outcome with no failures, missing terminal records, duplicates, or terminal records without publish.

The audit, application and runner logs, resolved test, Grafana dashboard, and VictoriaMetrics data were downloaded and bundled locally before teardown. Independent post-cleanup checks found no EKS clusters, live EC2 instances, EBS volumes, NAT gateways, or session S3 buckets; only the intentionally persistent ECR image repositories remain.

Follow-up verification found an empty, untagged EKS control-plane CloudWatch log group created outside Terraform ownership. The live residue was deleted, EKS control-plane logging was disabled for the disposable lab, and cleanup now explicitly deletes and independently verifies the exact log-group name.

The final result bundle now embeds a self-contained Docker restore kit with VictoriaMetrics, Grafana, dashboard/data-source provisioning, and one-command startup. Finalization rebuilds the manifest after local audit and session metadata are added, so the manifest covers the complete portable report rather than only the S3 transport payload.

Portable restore verification used a freshly extracted `smoke-20260829-a6` archive outside the repository: Grafana 11.6 started successfully, provisioned the 17-panel CKC Overview dashboard, reported a healthy Prometheus datasource, and queried 221 metric names from the archived VictoriaMetrics database. The bundled close script then removed both containers and their Docker network.

Grafana restore now binds to `0.0.0.0:3002` by default for access from another host, with optional port and bind-address arguments for local-only or alternate bindings. Docker inspection verified the rebuilt portable bundle published the test port on `HostIp=0.0.0.0`; documentation warns that the default Grafana credentials must not be exposed to an untrusted network.

<a id="infra-126"></a>
### INFRA-126 - Share result bundles across local and AWS labs

_Date: 2026-08-30_

Extract the mature internal-lab dashboard patching, experiment summary, time-window selection, manifest, and Docker restore behavior into a shared result-bundle foundation.
Use thin internal-lab and AWS adapters, explicit environment capabilities, and a stable telemetry label schema so each bundle keeps relevant panels without showing known-empty environment-specific sections.
Complete AWS application, load-generator, Kafka, pod-resource, logs, and annotation collection, and preserve anonymous read-only Grafana access in portable reports.
Verify the result with an approximately ten-minute AWS smoke reaching at least 5,000 TPS, then validate dashboard data coverage, audit correctness, autonomous restore, and complete AWS teardown.

Live verification: `smoke-20260830-5k-b` ran the 600-second `smoke-5k` profile in `eu-central-1` with two load shards targeting 5,000 messages/s. The portable 74 MiB result contains 8,840,853 audit records, 9,224 Loki log lines, the exact experiment time range, anonymous read-only Grafana access, and environment-aware dashboard capabilities.

The restored VictoriaMetrics database exposes application, load-test, Kafka lag, Thread Stats, Lettuce, and per-pod CPU/memory series; Grafana and Loki were queried successfully through their anonymous datasource proxies. AWS-only finalization excludes the three host-service rows and the unsupported demo process context-switch panel while retaining the shared dashboard source and useful pod panels.

The load deliberately exceeded the small three-node smoke lab: 5,717,532 records were published, 1,168,972 processed, 1,954,323 cauldron records dropped with `queue_overflow`, and 2,594,229 order/batch records remained without terminal outcomes when the run ended. The result therefore verifies high-volume collection and overload visibility, not a correctness/pass capacity target.

Post-teardown verification reported `CLEAN`: no active instances, volumes, NAT gateways, EIPs, ENIs, subnets, security groups, VPCs, endpoints, peerings, session bucket, or EKS log group remained. Cleanup now distinguishes authoritative service state from stale Resource Groups Tagging API history, preventing deleted resources from creating a false incomplete result.

_Follow-up: 2026-08-31_

Reopened after restored-bundle inspection exposed incomplete sharing: AWS still generated a separate experiment summary, its logs/reset links and annotations were non-functional, mode-specific empty panels remained, and cAdvisor samples started late without blocking the workload. Complete the shared presentation/event pipeline, add telemetry readiness and coverage validation, link each target to its exact time range, then repeat the ten-minute AWS smoke with 100 load-generator workers.

The shared presentation now renders the same experiment summary and target table for internal-lab and AWS, including exact reset/log links and target-name time links. Portable restore imports run events as Grafana annotations with authenticated administration while preserving anonymous dashboard and Explore access; capability filtering removes MSK-only panels from the Kubernetes Kafka result.

`smoke-20260831-5k-d` completed the 600-second, 5,000 messages/s profile with one load shard, 100 load-generator workers, two application replicas, and 100 order/batch/telemetry workers per replica. Telemetry readiness blocked workload start until application, Kafka exporter, Thread Stats, cAdvisor, CPU, and memory signals were live; coverage passed all seven required families with first samples 3.876-67.0 seconds after the recorded workload start.

The result contains 2,867,428 published records and 2,512,573 terminal outcomes. The deliberate overload left 354,855 non-terminal order/batch records and a 372,387-record lag at the optional one-minute drain deadline, which is recorded as `TIMEOUT` without misclassifying the telemetry smoke as failed.

Portable verification restored 69 shared dashboard panels, 44,252 Loki records, one visible run-start annotation, working anonymous Explore/reset/profile links, and no irrelevant MSK panels. Teardown pre-deleted the managed node group, found no detached CNI ENIs, destroyed all Terraform stacks and the S3 bucket, and independently reported `CLEAN` with no active billable session resources.

<a id="infra-127"></a>
### INFRA-127 - Run a 20-minute MSK and ElastiCache capacity test

_Date: 2026-09-01_

Replace the undersized in-cluster Redis/Kafka smoke dependencies with an explicitly sized ElastiCache replication group and non-burstable MSK brokers.
Run CKC with a fixed one-thread processing dispatcher at 10,000 messages/s for 20 minutes, including a ten-minute ramp for HPA and JVM stabilization, so dependency stability, backlog, and steady-state behavior are visible.
Install the EKS metrics-server add-on so application and stub CPU-based HPAs receive the resource metrics required to scale.
Use three `m7i.xlarge` workers, explicit load-generator and observability resources, stronger stub CPU limits, and topology spreading so shared-node contention does not masquerade as application saturation.
Capture dependency health and restart evidence in the portable result, verify dashboard and audit artifacts, then destroy and independently audit every session-owned AWS resource.

The completed run exposed one-minute default Alloy scrapes and a five-minute default HPA downscale window: the latter removed a healthy third application pod while the ten-minute ramp was still rising. Use explicit 15-second scrapes for in-cluster signals and a 600-second downscale stabilization window for the AWS HPA profiles; MSK CloudWatch panels remain one-minute by design.

Continuously stream Kubernetes workload logs through the shared Alloy/Loki path so logs survive HPA pod deletion. Preserve `application`, `namespace`, `pod`, `container`, `node`, `profile`, `environment`, and `run_id` labels in the portable Loki export, while retaining labeled runner-file logs as a fallback.

Separate the existing Thread Stats `audit` group from `Other` in the three shared category panels for CPU time, allocations, and CPU usage, making audit transport overhead directly visible in both internal-lab and AWS result dashboards.

<a id="infra-128"></a>
### INFRA-128 - Unify experiment orchestration across internal-lab and AWS

_Date: 2026-09-01_

Move the experiment, resolved-test, target, consumer-profile, planning, lifecycle-event, and comparative-report model into `demo/infra/shared`, retaining compatibility entrypoints while the two environments migrate.
Keep one immutable lab profile for an experiment, allow each target to select its consumer profile and override deployment or any nested test setting, and persist the fully resolved test and lab configuration for every target.
Use environment adapters for installed internal-lab services versus disposable AWS provisioning, artifact transport, CloudWatch/cost data, provider-specific chaos, and verified cloud teardown.
Replace the AWS-only `app_profile` presets with explicit target consumer profiles plus deployment/resources/scaling policy, migrate existing definitions, and prove the shared path with internal tests followed by a small disposable AWS smoke.
