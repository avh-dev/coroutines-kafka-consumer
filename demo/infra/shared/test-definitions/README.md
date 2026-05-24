# Test Definitions

`demo/infra/shared/test-definitions` contains YAML files that the runner uses to execute a test run inside an existing lab.

Each definition selects:

- deployment profiles for the app and stubs
- Kafka topics and partition counts prepared during lab setup
- load-test runtime settings such as shards, base TPS, traffic mix, domain limits, and load profile

Available definitions:

- `smoke-test.yaml`: short functional check with low traffic.
- `ckc-baseline-internal.yaml`: internal k3s lab baseline for the dedicated Linux host.
- `ckc-baseline.yaml`: AWS baseline intended for larger, more production-like load.

`load_profile` is a shared percentage schedule. `base_tps` is applied per load-test shard, and `order_event_percent`, `batch_event_percent`, and `cauldron_telemetry_percent` split that event budget across order, batch, and cauldron telemetry topics. The load-test job exits when the profile schedule ends.

`deployment.kafka_topics` is consumed by lab setup, not by the application deployment itself. `create-lab` flushes Redis and deletes and recreates these topics before workloads are deployed so a test definition can change partition counts without leaving old topic metadata behind.
