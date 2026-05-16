# Demo Application

This module contains a Spring Boot demo application for `coroutines-kafka-consumer`.

The demo uses a potion workshop domain:

- business-critical `order lifecycle` events must be processed without loss;
- high-frequency `cauldron telemetry` may be processed in a lossy/latest-wins mode;
- telemetry triggers an external REST call to an arcane ETA model and a small CPU-bound normalization step.

The application is intended for functional checks and for comparing consumer implementations under the same workload:

- `ckc` profile uses `coroutines-kafka-consumer`;
- `spring-kafka` profile uses `@KafkaListener` as a legacy baseline;
- `confluent-parallel` profile uses a blocking Confluent Parallel Consumer implementation with key-ordered parallel processing.

## Runtime Features

- protobuf payloads in Kafka via `:ckc-demo-contracts`
- Redis-backed order and batch state
- external ETA model stub via `:ckc-demo-stubs`
- Prometheus metrics endpoint at `/actuator/prometheus`
- read API for current order state at `/api/orders/{orderId}`

## Local Environment

Local Kafka, Redis, demo stubs, Prometheus, and Grafana are defined in `demo/infra/local-dev/README.md`.

Start the local environment:

```bash
docker compose -f demo/infra/local-dev/docker-compose.yml up -d
```

Run the demo with the coroutine-based consumer:

```bash
./gradlew :ckc-demo:bootRun --args='--spring.profiles.active=ckc --demo.kafka.enabled=true'
```

Run the demo with Spring Kafka listeners:

```bash
./gradlew :ckc-demo:bootRun --args='--spring.profiles.active=spring-kafka --demo.kafka.enabled=true'
```

Run the demo with Confluent Parallel Consumer:

```bash
./gradlew :ckc-demo:bootRun --args='--spring.profiles.active=confluent-parallel --demo.kafka.enabled=true'
```

If `8080` is already occupied, override the port:

```bash
./gradlew :ckc-demo:bootRun --args='--server.port=8081 --demo.kafka.enabled=true'
```

If you override the app port, update `demo/infra/local-dev/prometheus/prometheus.yml` to match it.

## CKC Experiment Controls

The `ckc` profile exposes demo-only switches for consumer experiments:

- `DEMO_CONSUMER_PROCESSING_ENABLED=false` keeps consuming and deserializing records, but skips the demo business handler and processed audit log.
- `DEMO_CONSUMER_DESERIALIZATION_DISPATCHER=DEFAULT|IO|CUSTOM_THREAD_POOL` selects the coroutine dispatcher used for Kafka deserializers.
- `DEMO_CONSUMER_DESERIALIZATION_THREADS=8` sets the custom thread pool size when `CUSTOM_THREAD_POOL` is selected.
- `DEMO_CONSUMER_DESERIALIZATION_THREAD_PREFIX=ckc-demo-deserializer` sets custom deserializer thread names.

Example:

```bash
DEMO_CONSUMER_PROCESSING_ENABLED=false \
DEMO_CONSUMER_DESERIALIZATION_DISPATCHER=CUSTOM_THREAD_POOL \
DEMO_CONSUMER_DESERIALIZATION_THREADS=8 \
./gradlew :ckc-demo:bootRun --args='--spring.profiles.active=ckc --demo.kafka.enabled=true'
```

## Confluent Parallel Consumer Metrics

The `confluent-parallel` profile publishes native Parallel Consumer Micrometer metrics instead of CKC-style record metrics.
Parallel Consumer meters use the `pc.*` prefix and include tags such as `consumer_id`, `spring_profile`, `topic`, and `pcinstance`.
Kafka client metrics are also bound for the underlying Kafka consumers.

## Endpoints

- `GET /actuator/prometheus`
- `GET /api/orders/{orderId}`

## Observability

- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`
- Dashboard: `CKC Overview` is provisioned automatically
- CKC Micrometer record metrics use a shared tag schema. Custom consumer tags must be declared once in the metrics bean and per-consumer bindings only provide values; omitted values fall back to `NONE`.

## Topics

- `potion.orders.lifecycle.v1`
- `potion.cauldrons.telemetry.v1`

## Tests

```bash
./gradlew :ckc-demo:test
```
