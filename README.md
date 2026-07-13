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
- `demo/ckc-demo` is a Spring Boot demo application used for functional checks and implementation comparisons.
- `demo/ckc-demo-load-test` generates reproducible load-test traffic.
- `experiments/ckc-experiments` contains offset-tracker and metadata-compression experiments.

## Related Projects And Alternatives

This section is intentionally early in the README while the project is being positioned for OSS. It will likely move down once installation and usage documentation mature.

### Spring Kafka

Spring Kafka is the default choice for many Spring services. It provides listener containers, annotation-based listeners, consumer factories, error handling, and Spring Boot integration.

CKC is not trying to replace Spring Kafka for ordinary listener use cases. The comparison is interesting when a service needs coroutine-native business handlers, explicit processing modes, and safe offset advancement under concurrency.

The demo keeps Spring Kafka profiles as practical baselines:

- `spring-kafka` for conventional blocking listener processing;
- `spring-kafka-coroutines-naive` for a deliberately simple coroutine-worker adapter.

The naive coroutine profile is useful because it demonstrates a common DIY path: enqueue records into coroutine workers. That can improve throughput, but without a real offset tracker it does not provide the same at-least-once safety as CKC under out-of-order completion.

### Confluent Parallel Consumer

[Confluent Parallel Consumer](https://github.com/confluentinc/parallel-consumer) is the closest direct alternative in this project. It is a Kafka client wrapper with per-message acknowledgement, client-side queueing, key concurrency, ordering modes, and offset metadata encoding.

CKC should be compared against Parallel Consumer because both projects address the same class of problem: processing records concurrently while preserving Kafka offset correctness and configurable ordering semantics.

The demo includes both blocking and Reactor-backed Parallel Consumer profiles.

### kotlin-kafka

[kotlin-kafka](https://github.com/nomisRev/kotlin-kafka) provides Kafka bindings for Kotlin `suspend` and streaming operators for KotlinX Flow.

It is useful when the goal is a lightweight Flow-oriented wrapper around Kafka clients. It is not, by itself, a parallel processing runtime equivalent to CKC or Parallel Consumer.

Flow can be made concurrent with operators such as `flatMapMerge`, `buffer`, or custom worker flows. That is not enough to make Kafka offset commits safe under out-of-order completion. Once a project adds lane scheduling, backpressure policy, terminal failure handling, and offset frontier tracking on top, it is effectively building a processing runtime.

For that reason, kotlin-kafka is documented here as a related project rather than included in the benchmark matrix as a direct competitor.

### Quafka

[Quafka](https://github.com/Trendyol/quafka) is a Kotlin Kafka client from Trendyol. Its README describes a non-blocking, coroutine-first client with separated polling and processing, backpressure, worker-per-topic-partition processing, per-partition ordering, optional batch handling, retry orchestration, delayed processing, and middleware pipelines.

Quafka looks closer to an internal processing framework than a small client wrapper. That may be a good fit for teams that want its full model.

It is not currently a direct CKC benchmark target. Based on the public model, Quafka preserves safety through ordered partition processing rather than unordered per-partition processing with explicit offset-gap tracking. That makes it different from CKC and Confluent Parallel Consumer, where the interesting problem is processing records concurrently and still committing only the safe offset frontier.

Quafka remains relevant related work, but it should not be presented as equivalent to CKC unless a future review finds first-class unordered processing with gap-aware offset advancement.

### Kafka Streams

[Kafka Streams](https://kafka.apache.org/33/streams/core-concepts/) is a stream processing library for building topologies with transformations, joins, aggregations, state stores, and Kafka-backed scaling.

That is a different problem space from CKC. Kafka Streams is appropriate when the application is a stream-processing topology. CKC is aimed at services that consume records and run application business logic with explicit processing semantics.

### Alpakka Kafka

[Alpakka Kafka](https://doc.akka.io/libraries/alpakka-kafka/current/home.html) connects Kafka to Akka Streams. It belongs to the Akka Streams ecosystem and brings that ecosystem's stream model, dependencies, operational assumptions, and licensing context.

It is relevant related work, but not a primary benchmark target for this project. Teams already invested in Akka Streams should evaluate Alpakka Kafka within that ecosystem. CKC is aimed at Kotlin/Spring-style services that do not want to adopt Akka as the surrounding runtime.

### Reactor Kafka

[Reactor Kafka](https://github.com/reactor/reactor-kafka) is a reactive wrapper around Kafka clients. It is relevant historically and conceptually, but it is not the main comparison target for this Kotlin coroutine project.

The Project Reactor team announced on 2025-05-20 that Reactor Kafka would be discontinued. Reactor Kafka 1.3 is the final minor release, future maintenance is limited to necessary updates and fixes until the published support dates, and Reactor Kafka is removed from future Reactor BOM releases.

The demo already includes a Reactor-backed Confluent Parallel Consumer profile where Reactor is part of a parallel processing runtime rather than just a Kafka wrapper.

## Current Status

The project currently prioritizes:

- validating processing semantics and offset correctness;
- improving benchmark/demo clarity;
- hardening metrics and observability;
- preparing a stable public API before an initial release.

Expect documentation, packaging, and examples to evolve quickly.
