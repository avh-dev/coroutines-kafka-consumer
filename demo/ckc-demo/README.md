# Demo Application

This module contains a Spring Boot demo application for `coroutines-kafka-consumer`.

The demo uses a potion workshop domain:

- business-critical order events track customer-facing order state;
- batch events track reagent preparation, cauldron assignment, brewing steps, and bottling;
- high-frequency cauldron events carry telemetry only for active brewing batches;
- cauldron telemetry triggers an external REST call to an arcane ETA model and a small CPU-bound normalization step;
- order creation triggers an order flavour model whose result is stored separately in Redis;
- completed brewing steps are acknowledged by a legacy brewing registry, and the returned receipt is stored before the record is considered processed.

The application is intended for functional checks and for comparing consumer implementations under the same workload:

- `ckc` profile uses `coroutines-kafka-consumer` with suspend business services;
- `ckc-sync` profile uses `coroutines-kafka-consumer` with blocking business services on `Dispatchers.IO`;
- `ckc-sync-loom` profile uses `coroutines-kafka-consumer` with blocking business services on virtual threads;
- `spring-kafka` profile uses `@KafkaListener` as a legacy baseline;
- `spring-kafka-coroutines-naive` profile uses Spring Kafka batch listeners that enqueue records into bounded channels drained by coroutine workers;
- `confluent-parallel` profile uses a blocking Confluent Parallel Consumer implementation with key-ordered parallel processing.
- `confluent-parallel-reactor` profile uses the Reactor Parallel Consumer adapter with the suspend business path.

## Runtime Features

- protobuf payloads in Kafka via `:ckc-demo-contracts`
- Redis-backed order and batch state
- external ETA, order flavour, and legacy brewing registry stubs via `:ckc-demo-stubs`
- Prometheus metrics endpoint at `/actuator/prometheus`
- Armeria HTTP server for the query API, health checks, and Prometheus scrapes
- optional reference read API for current order state at `/api/orders/{orderId}` under the `api` profile

External HTTP dependencies use separate base URL settings: `ETA_MODEL_BASE_URL`, `FLAVOUR_MODEL_BASE_URL`, and `REGISTRY_BASE_URL`.
Local and lab defaults point all three at `ckc-demo-stubs`, but the application keeps separate clients so production-like deployments can route them to distinct services.

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

Run the demo with CKC and blocking business services:

```bash
./gradlew :ckc-demo:bootRun --args='--spring.profiles.active=ckc-sync --demo.kafka.enabled=true'
```

Run the demo with CKC and blocking business services on virtual threads:

```bash
./gradlew :ckc-demo:bootRun --args='--spring.profiles.active=ckc-sync-loom --demo.kafka.enabled=true'
```

Run the demo with Spring Kafka listeners:

```bash
./gradlew :ckc-demo:bootRun --args='--spring.profiles.active=spring-kafka --demo.kafka.enabled=true'
```

Run the demo with naive Spring Kafka batch listeners and coroutine workers:

```bash
./gradlew :ckc-demo:bootRun --args='--spring.profiles.active=spring-kafka-coroutines-naive --demo.kafka.enabled=true'
```

Run the demo with Confluent Parallel Consumer:

```bash
./gradlew :ckc-demo:bootRun --args='--spring.profiles.active=confluent-parallel --demo.kafka.enabled=true'
```

Run the reference order query API locally:

```bash
./gradlew :ckc-demo:bootRun --args='--spring.profiles.active=api --demo.kafka.enabled=false'
```

If `8080` is already occupied, override the port:

```bash
./gradlew :ckc-demo:bootRun --args='--server.port=8081 --demo.kafka.enabled=true'
```

If you override the app port, update `demo/infra/local-dev/prometheus/prometheus.yml` to match it.

## CKC Experiment Controls

The consumer profiles expose demo-only switches for consumer experiments:

- `DEMO_CONSUMER_PROCESSING_ENABLED=false` keeps consuming and deserializing records, but replaces the demo business handler with a small consumer-layer latency-only delay.
- `WORKER_DISPATCHER_THREADS=8` limits the shared fixed worker pool used by all consumers in the suspend `ckc`, `spring-kafka-coroutines-naive`, and `confluent-parallel-reactor` profiles. Per-consumer `*_WORKER_CONCURRENCY` settings remain independent upper bounds. The blocking `ckc-sync` profile continues to use `Dispatchers.IO`, while `ckc-sync-loom` uses virtual threads.
- `TELEMETRY_PROCESSING_MODE=FRESHNESS_FIRST_DROP_OLDEST` selects the CKC telemetry processing mode. `FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY` is available for CKC telemetry runs when only the latest queued cauldron update per key should be retained.
- `TELEMETRY_WORK_CHANNEL_CAPACITY=256` is the telemetry runtime queue capacity. With `FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY`, it limits queued distinct telemetry keys, not total Kafka records. Records for already queued keys can replace older queued values without consuming extra capacity; records for new keys are dropped when the queued key-lane capacity is full.
- `FRESHNESS_FIRST_MAX_RECORD_AGE_SECONDS=10` limits record age for freshness-first handling. CKC telemetry consumers pass it to core `freshnessMaxRecordAge`; external adapter profiles apply the same stale-record check before demo business logic. Stale records are acknowledged without running demo business logic, but still emit dropped audit records and `demo.ckc.record.dropped{reason="stale_age"}` metrics.

The external demo profiles reject `FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY` because Spring Kafka and Confluent Parallel Consumer are not configured here to model CKC's key-coalescing freshness semantics exactly.

For drop monitoring, `demo.ckc.record.dropped` includes a `reason` tag. In `FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY`, `replaced_by_newer_key_record` is expected during same-key telemetry bursts, while `new_key_queue_full` indicates the active key cardinality exceeded `TELEMETRY_WORK_CHANNEL_CAPACITY` and should normally stay near zero.

Example:

```bash
DEMO_CONSUMER_PROCESSING_ENABLED=false \
./gradlew :ckc-demo:bootRun --args='--spring.profiles.active=ckc --demo.kafka.enabled=true'
```

## Confluent Parallel Consumer Metrics

The `confluent-parallel` profile publishes native Parallel Consumer Micrometer metrics instead of CKC-style record metrics.
Parallel Consumer meters use the `pc.*` prefix and include tags such as `consumer_id`, `spring_profile`, `topic`, and `pcinstance`.
Kafka client metrics are also bound for the underlying Kafka consumers.

## Endpoints

- `GET /actuator/prometheus`
- `GET /api/orders/{orderId}` when the `api` profile is active

## Performance-Test Scope

The consumer profiles do not enable the order query API. This repository keeps
the API as a reference implementation for inspecting demo state locally.

In a production layout, consumers and query APIs would normally be separate
deployments. The API would read from a read-only replica of the persistence
layer, so API traffic and blocking query work would not affect consumer
performance measurements.

## Observability

- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`
- Dashboard: `CKC Overview` is provisioned automatically
- CKC Micrometer record metrics use a shared tag schema. Custom consumer tags must be declared once in the metrics bean and per-consumer bindings only provide values; omitted values fall back to `NONE`.

## Topics

- `order.events.v1`
- `batch.events.v1`
- `cauldron.events.v1`

## Tests

```bash
./gradlew :ckc-demo:test
```
