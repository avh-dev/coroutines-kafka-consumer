# Coroutines Kafka Consumer

`coroutines-kafka-consumer` is an experimental Kotlin library for building coroutine-native Kafka consumers with explicit processing modes, backpressure, metrics, and offset advancement that remains safe when records are processed concurrently.

The project is pre-release. APIs, package names, and configuration names may still change while the library shape is being refined.

## What CKC Is For

CKC targets Kotlin services that need more control than a conventional listener container, but do not want to build a custom Kafka processing runtime from scratch.

The core problem is not just polling Kafka from coroutines. The hard part is allowing useful parallelism while preserving the processing guarantees the application asked for:

- at-least-once processing with no ordering guarantee;
- at-least-once processing with key ordering;
- at-least-once processing with partition ordering;
- freshness-first processing that drops the oldest buffered records;
- freshness-first processing that replaces a pending same-key record with the newer record;
- safe offset advancement when records complete out of order;
- runtime metrics that explain throughput, latency, backpressure, and offset progress.

## Processing Modes

CKC supports tracked at-least-once modes and lossy freshness-first modes.

`AT_LEAST_ONCE_NO_ORDERING`, `AT_LEAST_ONCE_KEY_ORDERING`, and
`AT_LEAST_ONCE_PARTITION_ORDERING` track processed offsets and commit only the
safe contiguous frontier.

`FRESHNESS_FIRST_DROP_OLDEST` and `FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY`
intentionally trade reliability for throughput and freshness. They require Kafka auto-commit and do not use CKC
offset tracking. Dropped records are not processed by the current consumer
instance and may be redelivered only if partition ownership changes before Kafka
commits past them.

Freshness-first modes can also be configured with `freshnessMaxRecordAge`.
When set, a worker drops records older than that duration before invoking the
handler and reports `stale_age` as the drop reason. This option is rejected for
tracked at-least-once modes because age-based drops would violate their delivery
semantics.

`FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY` is intended for finite-key telemetry
streams, such as sensor, vehicle, courier, or device state updates. The runtime
keeps at most one queued record per deserialized Kafka key. When a newer record
arrives for a key that is already waiting, it replaces the older queued record.
Records with a null Kafka key share one freshness lane.

For `FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY`, `workChannelCapacity` limits the number of
distinct keys that may wait in the runtime queue at the same time. It is not a
total Kafka-record buffer size. Set it at or above the expected concurrently
active key cardinality per consumer instance, with headroom for bursts. If the
capacity is full and a record for a new key arrives, CKC drops that incoming
record instead of applying backpressure.

Drop metrics include a `reason` tag. `replaced_by_newer_key_record` is expected
during same-key bursts in `FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY`; `new_key_queue_full` means
the queued key-lane capacity is saturated and should normally stay near zero.
If `new_key_queue_full` appears regularly, increase `workChannelCapacity`,
reduce active keys per consumer instance, increase worker throughput, or split
the workload.

## Why Offset Tracking Matters

Kafka commits offsets per topic partition. Committing offset `N + 1` means that every record up to offset `N` is safe to skip after a restart.

That becomes non-trivial when processing is concurrent:

```text
offset 10 processed
offset 11 processed
offset 12 processed
offset 8 still running
offset 9 still running
```

A consumer runtime must not commit past offsets `8` and `9` just because later records completed first. CKC tracks processed-but-not-yet-committable offsets and advances the commit frontier only when it is safe for the selected processing mode.

## Modules

- `ckc-core` contains the consumer runtime and public Kotlin DSL.
- `ckc-micrometer` adapts CKC metrics to Micrometer.
- `ckc-spring-boot-starter` wires CKC consumers into Spring Boot applications.
- `demo/ckc-demo-contracts` contains shared demo protobuf contracts and serialization helpers.
- `demo/ckc-demo` is a Spring Boot demo application used for functional checks and implementation comparisons.
- `demo/ckc-demo-load-test` generates reproducible load-test traffic.
- `demo/ckc-demo-stubs` provides local model-service stubs used by demo and load-test flows.
- `experiments/ckc-experiments` contains offset-tracker and metadata-compression experiments.

## Current Status

The project currently prioritizes:

- validating processing semantics and offset correctness;
- improving benchmark/demo clarity;
- hardening metrics and observability;
- preparing a stable public API before an initial release.

Expect documentation, packaging, and examples to evolve quickly.
