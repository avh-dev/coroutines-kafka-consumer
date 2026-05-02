# Test Definitions

`infra/shared/test-definitions` contains YAML files that the runner uses to execute a test run inside an existing lab.

Each definition selects:

- deployment profiles for the app and stubs
- load-test runtime settings such as shards, rates, and load profile

Available definitions:

- `smoke-test.yaml`: short functional check with low traffic.
- `ckc-capacity-search-local.yaml`: short constant-load local profile for quickly probing the two-pod one-core baseline.
- `ckc-baseline-local.yaml`: local minikube baseline, lower than AWS but large enough to exercise Kafka, Redis, app metrics, Prometheus, and Grafana.
- `ckc-baseline.yaml`: AWS baseline intended for larger, more production-like load.

`base_rate` is applied per load-test shard. Total generated message rate is higher than `shards * base_rate` because each order produces multiple lifecycle events and telemetry is generated separately.
