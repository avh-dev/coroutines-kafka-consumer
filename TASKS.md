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
