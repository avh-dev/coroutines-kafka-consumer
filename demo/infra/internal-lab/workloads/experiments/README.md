# Internal-lab Experiments

`demo/infra/internal-lab/workloads/experiments` contains experiment definitions.
An experiment runs one resolved test at one explicit `base_tps` against one
or more inline consumer targets. The test can extend a reusable definition or
be written completely inline. Each experiment may select a reusable
`sla_profile` from `../sla-profiles`; completed experiment sets generate local
Markdown and SVG reports under their result directory.

Internal-lab experiments are synced to `/opt/ckc-lab/workloads/experiments`.
Start the interactive selector with:

```sh
LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/run-experiment.sh
```

Run one or more named experiments with:

```sh
LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/run-experiment.sh telemetry-fairness-profile-comparison consumer-capacity-comparison
```

Run every synced experiment sequentially with:

```sh
LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/run-experiment.sh all
```

After the initial selection and experiment-wide settings prompt, targets run
unattended. Type `q` and press Enter to stop the current target gracefully,
finalize its raw audit log, and abort the remaining targets. Experiment runs
collect raw audit logs first and run audit analysis as a separate final phase.

Experiment-wide overrides can be passed without prompts:

```sh
LAB_ROOT=/opt/ckc-lab /opt/ckc-lab/bin/run-experiment.sh telemetry-fairness-profile-comparison \
  --env PROCESSING_ENABLED=true \
  --env AUDIT_LOG_ENABLED=true \
  --env METRICS_IMPLEMENTATION=MICROMETER
```

Experiment schema:

```yaml
name: telemetry-fairness-profile-comparison
description: Compare telemetry fairness on the same fixed-fleet overload test.
test_definition: telemetry-freshness-fairness
base_tps: 2000
sla_profile: consumer-baseline

defaults:
  application:
    replicas: 2
    resources:
      requests: {cpu: 500m, memory: 768Mi}
      limits: {cpu: "2", memory: 2Gi}
    hpa:
      enabled: true
      min_replicas: 2
      max_replicas: 6
      target_cpu_utilization_percentage: 70
      scale_down_stabilization_window_seconds: 600
  env:
    PROCESSING_ENABLED: true
    AUDIT_LOG_ENABLED: true
    METRICS_IMPLEMENTATION: MICROMETER
    JDK_HTTP_CLIENT_EXECUTOR: DEFAULT

targets:
  - name: ckc.fixed.2
    profile: ckc
    planning_latency:
      order_ms: 2
      batch_ms: 5
      telemetry_ms: 35
    env:
      PROCESSING_DISPATCHER_TYPE: FIXED
      WORKER_DISPATCHER_THREADS: 2

  - name: spring-kafka-coroutines-naive.fixed.2
    profile: spring-kafka-coroutines-naive
    planning_latency:
      order_ms: 2
      batch_ms: 5
      telemetry_ms: 35
    telemetry_processing_mode: HARDCODED_FRESHNESS_FIRST_DROP_EXPIRED
    env:
      PROCESSING_DISPATCHER_TYPE: FIXED
      WORKER_DISPATCHER_THREADS: 2
    order_queue_capacity: 4096
    batch_queue_capacity: 4096

  - name: spring-kafka
    profile: spring-kafka
    planning_latency:
      order_ms: 2
      batch_ms: 5
      telemetry_ms: 35
    helm:
      env:
        javaToolOptions: "-Xms512m -Xmx1536m -Xss256k -XX:+UseSerialGC"
      resources:
        requests:
          memory: 2Gi
        limits:
          memory: 3Gi
```

`application` is environment-neutral: internal-lab and AWS feed it to the same
planner and shared Helm chart. Top-level `replicas` and `helm.resources` remain
accepted for existing experiment definitions.

`test_definition` remains supported as the short legacy form. New experiments
can use `test.extends` and override any nested test field. Objects merge
recursively, while lists (including chaos and diagnostics) replace the
inherited list. `null` removes an inherited field:

```yaml
test:
  extends: telemetry-freshness-fairness
  load_test:
    kafka_producer_linger_ms: 50
    kafka_producer_batch_size: 131072
  chaos_steps:
    - at: 4m
      type: service_restart
      target: redis
  diagnostic_steps: null
```

Omit `extends` to define the complete `stubs` and `load_test` sections directly
inside `test`. Every run stores the fully resolved YAML beside the experiment
summary, and report generation uses that snapshot.

Targets may override any nested part of the resolved experiment test. Objects
merge recursively, lists replace the inherited list, and `null` removes a
field. Each target receives and archives its own resolved snapshot:

```yaml
test:
  extends: production-like

targets:
  - name: baseline
    profile: spring-kafka

  - name: ckc-with-slower-registry
    profile: ckc
    test:
      load_test:
        workers: 200
      stubs:
        registry:
          delay_p99_ms: 100
      diagnostic_steps: null
```

Use `targets[].test.extends` to select another reusable test for one target, or
`targets[].test.replace: true` for a complete target-local test. A target may
not override `lab`/`lab_profile`: the environment stays immutable for the
whole experiment, and a different lab configuration is a different experiment.

The target `name` is automatically passed to the demo as
`EXPERIMENT_TARGET_NAME`, so `ckc.demo.consumer.profile.info` uses the readable
experiment name for its `profile` tag while retaining the Spring profile in
`spring_profile`.

Initial producer settings may be shared (`kafka_producer_linger_ms`,
`kafka_producer_batch_size`, `kafka_producer_compression_type`, and
`kafka_producer_buffer_memory`) or prefixed with `order_`, `batch_`, or
`telemetry_` under `load_test`. Producer settings remain fixed for the complete
target run; use separate targets to compare different settings.

Consumer fetch settings are target-specific environment overrides. Shared
`KAFKA_CONSUMER_*` values and corresponding `ORDER_`, `BATCH_`, and
`TELEMETRY_` variants are passed through the internal-lab Helm deployment; this
allows unlike topics to use different fetch sizes, waits, and poll limits.

Each target run publishes one `run_started` Grafana annotation by default.
Set `targets[].annotation_label` to the exact short parameter/value text, for
example `compression.type=lz4` or `linger.ms=500`. When it is omitted, the
runner falls back to the part of the readable target name after its common
dot-separated prefix. Complete configuration remains in run metadata and the
report summary. Grafana annotations carry only one tag: their event type. Set
`EXPERIMENT_GRAFANA_RUN_ANNOTATIONS_ENABLED=false` to disable these markers.

Chaos actions and diagnostic captures also append their actual lifecycle
timestamps to `experiment-events.jsonl`, so they remain visible in reports and
exported bundles. Their noisier live Grafana annotations stay disabled by
default; set `EXPERIMENT_GRAFANA_ANNOTATIONS_ENABLED=true` to publish them.
Grafana downtime never fails the workload because the JSONL file remains
authoritative.

SLA profiles support inheritance, declarative delivery criteria, and exact
per-record latency rules. The standard experiment profile is:

```yaml
name: consumer-baseline
extends: delivery-integrity
latency:
  rules:
    - id: business-events
      title: Business event end-to-end latency
      topics: [order.events.v1, batch.events.v1]
      max_ms: 2000
      allowed_exceed_percent: 1.0
    - id: telemetry
      title: Telemetry end-to-end latency
      topics: [cauldron.events.v1]
      max_ms: 1000
      allowed_exceed_percent: 5.0
```

For every unique processed record with a matching publish, the audit analyzer
subtracts the Kafka timestamp from the successful terminal timestamp. A record
at exactly `max_ms` passes; only a larger value increments `exceeded`. The rule
passes when `exceeded / measured * 100` is less than or equal to
`allowed_exceed_percent`. Topics may occur in only one latency rule so headline
counts do not double-count records. Missing publish timestamps or negative
latencies make the rule incomplete rather than silently passing.

Generic audit and Prometheus measurement criteria remain available. Supported
operators are `eq`, `lte`, `lt`, `gte`, and `gt`; standard diagnostic
measurements are `latency_p95_ms`, `latency_p99_ms`,
`freshness_gap_p95_ms`, `throughput_average_rps`, `cpu_average_cores`,
`telemetry_poll_batch_average_records`, `telemetry_poll_batch_max_records`,
`telemetry_active_workers_average`, `telemetry_active_workers_max`,
`processing_worker_cpu_average_cores`,
`processing_worker_allocation_average_bytes_per_second`, and
`context_switches_average_per_second`.

`defaults.replicas`, target-level parallelism overrides, and target-level
`helm` overrides are passed to the generated Helm values. The shared
`demo/infra/shared/workloads/consumer-profiles.yaml` catalog only describes the
reusable consumer-profile capabilities used by the planner; environment and
deployment choices should be visible in the experiment.

`helm.env.javaToolOptions` can tune the demo JVM for a specific target.
`helm.resources.requests` and `helm.resources.limits` currently support `cpu`
and `memory` values for the demo pod.

`JDK_HTTP_CLIENT_EXECUTOR: VIRTUAL` in `defaults.env` or a target `env` enables
the demo sync JDK HTTP client virtual-thread executor for profiles that use the
sync model and registry clients.

Use `spring-kafka-thread-pool` when a comparison should keep Spring Kafka poller
concurrency low and move synchronous processing onto bounded platform-thread
worker pools.

Use `spring-kafka-virtual-thread-pool` for the same batch-listener admission
shape, but with accepted records executed on virtual-thread-per-task workers
behind the configured worker concurrency limit.

`planning_latency` is an experiment input for generated partitions, workers,
and pollers. It should be calibrated for this experiment's `base_tps`, target
consumer profile, and current lab infrastructure.

`order_queue_capacity`, `batch_queue_capacity`, and
`telemetry_queue_capacity` override the generated work channel capacity for a
target. Use them when comparing CKC pause/resume behavior or external consumer
admission behavior under the same traffic shape.

Use `/opt/ckc-lab/bin/target-draft.sh` after a tuned `run-test.sh` run to print
a ready-to-paste target draft with the run's profile, planning latencies,
replicas, processing modes, generated partitions, workers, pollers, and
environment settings.
