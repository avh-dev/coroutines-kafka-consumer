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
intentionally trade reliability for throughput and freshness. CKC explicitly commits the latest position accepted
from each partition without waiting for processing completion, including records intentionally dropped by the
freshness policy. They do not use processed-offset tracking.

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

Every mode attempts an explicit commit when either `commitRecordsThreshold` commit-ready records are pending
within a poll loop or `commitIntervalMs` elapses. At-least-once modes select only the safe contiguous processed
frontier; freshness-first modes select the latest accepted position. Kafka auto-commit is always disabled, and an
explicit `enable.auto.commit=true` property is rejected.

The at-least-once tracker uses a small ring that grows on demand and is bounded by
`maxTrackedOffsetsPerPartition` (1,048,576 bits, or 128 KiB per partition, by default).
When its window fills, CKC pauses that partition without stopping polls or work already accepted from other
partitions, and resumes it after the contiguous processed frontier releases tracker space.

### Replay and Ordering After Failures

CKC does not support Kafka transactions or exactly-once processing. In tracked
at-least-once modes, successful processing and committing its progress are
separate operations. Commit metadata preserves processed offsets beyond the
contiguous frontier so they can be skipped on recovery. Records whose completion
was not included in a successful commit can still be processed again after a
failure, including an abrupt process termination or an interrupted shutdown.

Key ordering serializes records of the same key during normal processing; it
does not prevent replay of an already processed tail after recovery. For example,
two records for the same key may produce this sequence of handler invocations:

```text
A -> B -> failure before progress is committed -> A -> B
```

The replay follows the original order, but an application that unconditionally
writes the state carried by each record can temporarily restore an older state.
Replaying through the latest record can restore the latest state for such
handlers; it does not undo repeated external side effects. Partition ordering
has the same replay limitation. Successful graceful draining and committing
preserve completed progress; an incomplete drain or failed commit can leave
records eligible for replay.

This is an intentional at-least-once trade-off. Applications that cannot tolerate
repeated effects or temporary state regression must provide appropriate atomic
deduplication or version-checked updates at the destination. CKC's ordering modes
do not provide that application-level guarantee.

## Modules

- `ckc-core` contains the consumer runtime, public Kotlin DSL, and optional [Micrometer adapter](ckc-core/README.md).
- `ckc-warmup` contains the opt-in traffic warm-up DSL, runtime, and v6 protocol.
- `ckc-spring-boot-starter` wires CKC consumers into Spring Boot applications.
- `demo/ckc-demo-contracts` contains shared demo protobuf contracts and serialization helpers.
- `demo/ckc-demo` is a Spring Boot demo application used for functional checks and implementation comparisons.
- `demo/ckc-demo-load-test` generates reproducible load-test traffic.
- `demo/ckc-demo-stubs` provides local model-service stubs used by demo and load-test flows.
- `experiments/ckc-experiments` contains offset-tracker and metadata-compression experiments.

## Using CKC

### Standard consumer

```kotlin
// build.gradle.kts
dependencies {
    implementation(project(":ckc-core"))
}
```

```kotlin
import avh.ckc.core.coroutinesKafkaConsumer

val consumer = coroutinesKafkaConsumer<String, String>(kafkaProperties) {
    topics("orders")
    handle { record -> processOrder(record.value()) }
}
```

Supply the main Kafka bootstrap servers, group ID, and deserializers in `kafkaProperties`.
Call `consumer.start()` and use the suspending `consumer.stop()` during shutdown.
The base Spring Boot starter and Micrometer adapter also work without the warm-up JAR.

### Consumer with traffic warm-up

```kotlin
// build.gradle.kts
dependencies {
    implementation(project(":ckc-warmup"))
}
```

```kotlin
import avh.ckc.core.coroutinesKafkaConsumer
import avh.ckc.warmup.trafficWarmup
import avh.ckc.warmup.trafficWarmupContext
import org.apache.kafka.common.serialization.StringSerializer

val warmupKafka = mapOf("bootstrap.servers" to "localhost:9092")
val context = trafficWarmupContext {
    namespace = "orders-service"
    controlTopic = "warmup.control"
    controlProducerProperties = warmupKafka
    controlConsumerProperties = warmupKafka
    maxInflightDelegations = 4
}

val consumer = coroutinesKafkaConsumer<String, String>(kafkaProperties) {
    topics("orders")
    trafficWarmup(context, "orders") {
        mapTopic("orders", "warmup.orders")
        warmupConsumerProperties = warmupKafka
        forwardingProducerProperties = warmupKafka + mapOf(
            "key.serializer" to StringSerializer::class.java,
            "value.serializer" to StringSerializer::class.java
        )
    }
    handle { record -> processOrder(record.value()) }
}
```

Provision the control topic and a single-partition destination topic before starting the consumer.
Instances that share a namespace use the same participant IDs and mappings.

A donor staggers quota windows for concurrent recipient sessions that use the same
`quotaInterval`. For a one-second interval, the first three recipients get window starts
at 0, 500, and 250 ms within each second of the donor's schedule. The first can delegate
immediately. Each mapping can send its available quota as a batch once its window opens.
Mappings retain their own quota profiles and share a session's phase only when their
intervals match. Unused quota expires at the next window boundary.

The mapping quota limits how many records can be delegated per interval.
`maxInflightDelegations` limits how many delegations the donor can have outstanding at once,
including when it serves a single recipient whose results stop arriving.

`maxInflightDelegations` accepts values from `1` to `Int.MAX_VALUE` and defaults to `Int.MAX_VALUE`.
Each `TrafficWarmupContext` has its own limit shared by all its donor sessions, participants, and source topics.
A delegation holds a place from admission before the Kafka send through the producer ACK, the recipient result,
and any required local fallback. When all places are occupied, the record is processed locally immediately.
Saturation preserves the remaining mapping quota and the session can delegate later records as places become free.
The `warmup.pending` metric counts only sends acknowledged by Kafka that are still awaiting a recipient result.
Use each consumer's `workerConcurrency` when sizing the limit if local processing needs spare workers.
Any participant can use all available places up to its own worker concurrency.

The `coroutines-kafka-consumer-warmup` JAR includes the protocol in `avh.ckc.warmup.protocol`.
When the consumer uses a CKC Micrometer metrics binding, enabling the DSL also registers the
warm-up meters with that binding's prefix and tags.
For custom callbacks, implement `WarmupMetrics<K, V>` and set `metrics` inside the warm-up block.

For a temporary broker configured for coordinated shutdown, point each participating context at its
dedicated lifecycle topic and broker connection.

```kotlin
brokerLifecycle {
    topic = "ckc.traffic-warmup.lifecycle"
    kafkaProperties = warmupKafka
}
```

The broker supervisor publishes a fresh epoch at startup. A context joins that epoch before
serving warm-up and uses the ordinary main consumer path when the broker is draining.
The stop coordinator waits for the contexts present at the first drain request's offset.
Stage the standalone CLI and its dependencies with `./gradlew :ckc-warmup:stageWarmupBrokerCli`.

## Current Status

The project currently prioritizes:

- validating processing semantics and offset correctness;
- improving benchmark/demo clarity;
- hardening metrics and observability;
- preparing a stable public API before an initial release.

Expect documentation, packaging, and examples to evolve quickly.
