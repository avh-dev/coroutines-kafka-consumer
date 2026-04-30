# Demo Application

This module contains a Spring Boot demo application for `coroutines-kafka-consumer`.

The demo uses a potion workshop domain:

- business-critical `order lifecycle` events must be processed without loss;
- high-frequency `cauldron telemetry` may be processed in a lossy/latest-wins mode;
- telemetry triggers an external REST call to an arcane ETA model and a small CPU-bound normalization step.

The application is intended for functional checks and for comparing two consumer implementations under the same workload:

- `ckc` profile uses `coroutines-kafka-consumer`;
- `spring-kafka` profile uses `@KafkaListener` as a legacy baseline.

## Runtime Features

- protobuf payloads in Kafka via `:coroutines-kafka-consumer-demo-contracts`
- Redis-backed order and batch state
- external ETA model stub via `:coroutines-kafka-consumer-demo-stubs`
- Prometheus metrics endpoint at `/actuator/prometheus`
- read API for current order state at `/api/orders/{orderId}`

## Local Environment

Local Kafka, Redis, demo stubs, Prometheus, and Grafana are defined in `infra/local-dev/README.md`.

Start the local environment:

```bash
docker compose -f infra/local-dev/docker-compose.yml up -d
```

Run the demo with the coroutine-based consumer:

```bash
./gradlew :coroutines-kafka-consumer-demo:bootRun --args='--spring.profiles.active=ckc --demo.kafka.enabled=true'
```

Run the demo with Spring Kafka listeners:

```bash
./gradlew :coroutines-kafka-consumer-demo:bootRun --args='--spring.profiles.active=spring-kafka --demo.kafka.enabled=true'
```

If `8080` is already occupied, override the port:

```bash
./gradlew :coroutines-kafka-consumer-demo:bootRun --args='--server.port=8081 --demo.kafka.enabled=true'
```

If you override the app port, update `infra/local-dev/prometheus/prometheus.yml` to match it.

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
./gradlew :coroutines-kafka-consumer-demo:test
```
