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
- `type`: one of `delete_random_pod`, `crash_random_pod`, `set_stubs_profile`, or `reset_stubs_profile`.
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
```

`delete_random_pod` deletes one randomly selected matching pod through `kubectl delete pod`. `crash_random_pod` execs into one randomly selected matching pod and posts to the demo application's internal crash endpoint. `namespace` and `selector` are optional for both types.

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

Internal-lab deployment settings are stored directly in local Helm profiles under
`demo/infra/internal-lab/helm/demo/profiles/internal-lab`. AWS deployment profiles live under
`demo/infra/aws/helm/demo/profiles/aws`. The `lab.kafkaTopics` block associates topic
partition counts with app scaling settings without affecting rendered Kubernetes resources.
Noop runs use the runtime Helm override `--set env.processingEnabled=false`.

Available internal-lab tests:

- `internal-lab/smoke.yaml`: short functional check with low traffic.
- `internal-lab/baseline.yaml`: comparison baseline for the dedicated Linux host.
- `internal-lab/telemetry-freshness-fairness.yaml`: telemetry-only fixed-fleet load for comparing freshness-first key fairness under intentional overload.

Available AWS definitions remain at the top level:

- `smoke-test.yaml`: short functional check with low traffic.
- `ckc-baseline.yaml`: AWS baseline intended for larger, more production-like load.

`load_profile` is a shared percentage schedule. `base_tps` is applied per load-test shard, and `order_event_percent`, `batch_event_percent`, and `cauldron_telemetry_percent` split that event budget across order, batch, and cauldron telemetry topics. `telemetry_source_mode` defaults to `ACTIVE_BATCHES`; set it to `FIXED_FLEET` when a test needs stable cauldron-key cardinality instead of business-pipeline active-batch cardinality. The load-test job exits when the profile schedule ends.

For freshness fairness comparisons, run `internal-lab/telemetry-freshness-fairness.yaml` against `ckc-telemetry-freshness-first`, `ckc-telemetry-freshness-first-by-key`, and `spring-kafka-coroutines-naive-telemetry-threshold`. The definition intentionally uses one load-test worker so fixed-fleet telemetry is a single round-robin stream across the configured cauldron keys instead of one fleet per worker. Compare `audit.topics.cauldron.events.v1.key_fairness.processed_ratio`, `dropped_ratio`, `processed_max_gap_ms`, and `record_age` in each run's `summary.yaml`.

For AWS definitions, `deployment.kafka_topics` is consumed by lab setup, not by the application deployment itself. `create-lab` flushes Redis and deletes and recreates these topics before workloads are deployed so a test definition can change partition counts without leaving old topic metadata behind.
