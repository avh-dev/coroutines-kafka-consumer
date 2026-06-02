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
| [GLOBAL-1](#global-1) | Shorten repository module names to `ckc-*` while preserving full published artifact names.                                              | DONE |
| [GLOBAL-2](#global-2) | Separate production modules from demo, demo infrastructure, and experiment code in the repository layout.                                | DONE |
| [DOC-1](#doc-1) | Add a documentation task scope for repository documentation, task history, working rules, and project notes. | DONE |
| [DOC-2](#doc-2) | Expand `TASKS.md` with linked task entries and retrospective implementation notes restored from git history and code changes. | DONE |

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
The existing backpressure and lossy modes became `AT_LEAST_ONCE_UNORDERED` and `FRESHNESS_FIRST`.
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
Keep the default as `AT_LEAST_ONCE_UNORDERED` so existing demo behavior remains unchanged.
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
