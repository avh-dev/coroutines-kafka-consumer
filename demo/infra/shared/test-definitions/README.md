# Test Definitions

`demo/infra/shared/test-definitions` contains YAML files that the runner uses to execute a test run inside an existing lab.

AWS definitions currently select:

- deployment profiles for the app
- Kafka topics and partition counts prepared during lab setup
- load-test runtime settings such as shards, base TPS, traffic mix, domain limits, and load profile

Internal-lab tests live under `internal-lab/` and contain only:

- mandatory demo stub baseline settings applied through `POST /settings`
- reusable load-test runtime settings

Internal-lab deployment settings are stored directly in local Helm profiles under
`demo/infra/shared/helm/demo/profiles/internal-lab`. AWS deployment profiles live under
`demo/infra/shared/helm/demo/profiles/aws`. The `lab.kafkaTopics` block associates topic
partition counts with app scaling settings without affecting rendered Kubernetes resources.
Noop runs use the runtime Helm override `--set env.processingEnabled=false`.

Available internal-lab tests:

- `internal-lab/smoke.yaml`: short functional check with low traffic.
- `internal-lab/baseline.yaml`: comparison baseline for the dedicated Linux host.

Available AWS definitions remain at the top level:

- `smoke-test.yaml`: short functional check with low traffic.
- `ckc-baseline.yaml`: AWS baseline intended for larger, more production-like load.

`load_profile` is a shared percentage schedule. `base_tps` is applied per load-test shard, and `order_event_percent`, `batch_event_percent`, and `cauldron_telemetry_percent` split that event budget across order, batch, and cauldron telemetry topics. `telemetry_source_mode` defaults to `ACTIVE_BATCHES`; set it to `FIXED_FLEET` when a test needs stable cauldron-key cardinality instead of business-pipeline active-batch cardinality. The load-test job exits when the profile schedule ends.

For AWS definitions, `deployment.kafka_topics` is consumed by lab setup, not by the application deployment itself. `create-lab` flushes Redis and deletes and recreates these topics before workloads are deployed so a test definition can change partition counts without leaving old topic metadata behind.
