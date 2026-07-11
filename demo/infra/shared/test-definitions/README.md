# Test Definitions

`demo/infra/shared/test-definitions` contains YAML files that the runner uses to execute a test run inside an existing lab.

AWS definitions currently select:

- deployment profiles for the app
- Kafka topics and partition counts prepared during lab setup
- load-test runtime settings such as shards, base TPS, traffic mix, domain limits, and load profile

Internal-lab tests live under `internal-lab/` and contain only:

- mandatory demo stub baseline settings applied through `POST /settings`
- reusable load-test runtime settings

The stubs block has separate latency profiles for ETA model calls, order flavour model calls, and the legacy brewing registry acknowledgement used by completed brewing-step processing.

Internal-lab definitions may also include an optional `chaos_steps` list. Each step is executed at a fixed offset from load-test start and has:

- `at`: non-negative offset from load-test start, as seconds or a compact duration such as `30s`, `2m30s`, or `1h`.
- `type`: one of `delete_random_pod`, `crash_random_pod`, `set_stubs_profile`, `reset_stubs_profile`,
  `set_service_netem`, `reset_service_netem`, `pause_service`, `resume_service`, or `restart_service`.
- `params`: scenario-specific parameters.

Pod disruption steps default to the demo application in the internal-lab namespace:

```yaml
chaos_steps:
  - at: 2m
    type: delete_random_pod
    params:
      namespace: ckc-perf
      selector: app.kubernetes.io/name=ckc-demo
  - at: 5m
    type: crash_random_pod
    params:
      namespace: ckc-perf
      selector: app.kubernetes.io/name=ckc-demo
      endpoint: /internal/crash
```

`delete_random_pod` deletes one randomly selected matching pod through `kubectl delete pod`. `crash_random_pod` selects one matching pod, opens a temporary `kubectl port-forward` to that pod, and posts to the demo application's internal crash endpoint from the lab host. `namespace` and `selector` are optional for both types. `endpoint` is optional for `crash_random_pod` and defaults to `/internal/crash`.

Stub profile steps use the same latency keys as the baseline `stubs` block. `set_stubs_profile` applies the supplied profile through the demo-stubs settings endpoint, and `reset_stubs_profile` restores the baseline `stubs` settings from the same test definition.

```yaml
chaos_steps:
  - at: 8m
    type: set_stubs_profile
    params:
      error_rate_percent: 10
      eta:
        delay_p90_ms: 200
        delay_p95_ms: 500
        delay_p99_ms: 1000
        delay_p100_ms: 2000
      flavour:
        delay_p90_ms: 200
        delay_p95_ms: 500
        delay_p99_ms: 1000
        delay_p100_ms: 2000
      registry:
        delay_p90_ms: 20
        delay_p95_ms: 50
        delay_p99_ms: 100
        delay_p100_ms: 200
  - at: 12m
    type: reset_stubs_profile
```

Host-service network chaos targets Redis, Kafka, or the audit TCP endpoint from the lab host. It uses Linux `tc netem`
and `iptables` marking on the host-service response path to Kubernetes pods. By default the executor uses `cni0` when
present, then `flannel.1`, then the default-route interface. Set `CHAOS_NETEM_DEV` when a lab host uses a different
pod-facing interface.

`set_service_netem` supports `target`, optional `delay_ms`, optional `jitter_ms`, optional `loss_percent`, optional
`rate`, and optional `duration`. At least one of `delay_ms`, `loss_percent`, or `rate` must be set. When `duration` is
defined, the test-definition normalizer inserts a matching `reset_service_netem` step automatically.

```yaml
chaos_steps:
  - at: 5m
    type: set_service_netem
    params:
      target: redis
      delay_ms: 120
      jitter_ms: 30
      duration: 2m
  - at: 9m
    type: set_service_netem
    params:
      target: kafka
      delay_ms: 250
      jitter_ms: 75
      loss_percent: 0.2
      rate: 10mbit
      duration: 3m
```

`reset_service_netem` can also be scheduled explicitly:

```yaml
chaos_steps:
  - at: 5m
    type: set_service_netem
    params:
      target: kafka
      delay_ms: 250
  - at: 8m
    type: reset_service_netem
    params:
      target: kafka
```

Host service disruption steps act on the lab Docker containers. `pause_service` can include a `duration`, which inserts
a matching `resume_service` automatically. `restart_service` restarts the selected container immediately.

```yaml
chaos_steps:
  - at: 6m
    type: pause_service
    params:
      target: redis
      duration: 30s
  - at: 10m
    type: restart_service
    params:
      target: kafka
```

Supported service targets are:

- `kafka`: selected Kafka API broker on port `9092`, container `ckc-perf-redpanda` or `ckc-perf-kafka`.
- `redis`: Redis on port `6379`, container `ckc-perf-redis`.
- `audit`: Fluent Bit TCP audit input on port `5170`, container `ckc-internal-fluent-bit`.

The runner resets known internal-lab network chaos state during test preparation and after a run stops, so aborted
tests do not leave `tc` or `iptables` rules behind.

Internal-lab deployment settings are stored directly in local Helm profiles under
`demo/infra/internal-lab/assets/helm/demo/profiles/internal-lab`. AWS deployment profiles live under
`demo/infra/aws/helm/demo/profiles/aws`. The `lab.kafkaTopics` block associates topic
partition counts with app scaling settings without affecting rendered Kubernetes resources.
Noop runs use the runtime Helm override `--set env.processingEnabled=false`.

Available internal-lab tests:

- `internal-lab/smoke.yaml`: short functional check with low traffic.
- `internal-lab/chaos-smoke.yaml`: short resiliency check that schedules demo pod deletion, demo pod crash, and demo-stubs profile changes.
- `internal-lab/network-chaos-smoke.yaml`: short resiliency check for Redis/Kafka network degradation and host-service restart steps.
- `internal-lab/baseline.yaml`: comparison baseline for the dedicated Linux host.
- `internal-lab/queue_backlog_crash.yaml`: targeted backlog-loss check that slows downstream services, uses order/batch-heavy traffic, and crashes app pods while large worker queues can hold records already accepted by listeners.
- `internal-lab/telemetry-freshness-fairness.yaml`: telemetry-only fixed-fleet load for comparing freshness-first key fairness under intentional overload.

Available AWS definitions remain at the top level:

- `smoke-test.yaml`: short functional check with low traffic.
- `ckc-baseline.yaml`: AWS baseline intended for larger, more production-like load.

`load_profile` is a shared percentage schedule. `base_tps` is applied per load-test shard, and `order_event_percent`, `batch_event_percent`, and `cauldron_telemetry_percent` split that event budget across order, batch, and cauldron telemetry topics. `telemetry_source_mode` defaults to `ACTIVE_BATCHES`; set it to `FIXED_FLEET` when a test needs stable cauldron-key cardinality instead of business-pipeline active-batch cardinality. The load-test job exits when the profile schedule ends.

For freshness fairness comparisons, run `internal-lab/telemetry-freshness-fairness.yaml` against `ckc-telemetry-freshness-first`, `ckc-telemetry-freshness-first-by-key`, and `spring-kafka-coroutines-naive-telemetry-threshold`. The definition intentionally uses one load-test worker so fixed-fleet telemetry is a single round-robin stream across the configured cauldron keys instead of one fleet per worker. Compare `audit.topics.cauldron.events.v1.key_fairness.freshness_gap.dropped_before_processed_histogram`, the adjacent dropped-before-processed and drop-to-processed millisecond summaries, and `record_age` in each run's `summary.yaml`.

For AWS definitions, `deployment.kafka_topics` is consumed by lab setup, not by the application deployment itself. `create-lab` flushes Redis and deletes and recreates these topics before workloads are deployed so a test definition can change partition counts without leaving old topic metadata behind.
