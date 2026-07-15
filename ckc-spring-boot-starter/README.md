# CKC Spring Boot Starter

Spring Boot starter for wiring CKC consumers from application configuration.

```kotlin
@CkcKafkaConsumer(name = "orders")
class OrdersConsumer(
    private val service: OrderService
) : CkcConsumer<String, OrderEvent> {
    override suspend fun process(record: ConsumerRecord<String, OrderEvent>) {
        service.handle(record.value())
    }

    override suspend fun handleFailure(record: ConsumerRecord<String, OrderEvent>, reason: Throwable) {
        // Publish to a DLT, audit, or intentionally skip the record.
    }
}
```

```yaml
ckc:
  lifecycle:
    phase: 0
    shutdown-timeout: 30s
  health:
    enabled: true
  observability:
    mdc:
      enabled: true
      include-key: false
      max-key-length: 128
  metrics:
    implementation: MICROMETER
    micrometer:
      schemas:
        default:
          metric-prefix: myapp
          static-tags:
            - name: app_name
              value: orders-service
          record-driven-tags:
            - name: event_type
              default: UNKNOWN
  default-cluster: main
  default-retry-schema: transient-errors
  default-processing-dispatcher: shared-workers
  dispatchers:
    shared-workers:
      type: FIXED_THREAD_POOL
      threads: 16
      thread-name-prefix: orders-worker-
  retry-schemas:
    transient-errors:
      rules:
        - exceptions:
            - java.io.IOException
            - java.net.SocketTimeoutException
          max-retries: 3
          delay: 100ms
  clusters:
    main:
      kafka-properties:
        bootstrap.servers: localhost:9092
        auto.offset.reset: earliest
  consumers:
    orders:
      auto-startup: false
      cluster: main
      topics:
        - orders.v1
      group-id: orders-service
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: com.example.OrderEventDeserializer
      processing-mode: at-least-once-key-ordering
      worker-concurrency: 64
      processing-dispatcher: shared-workers
      work-channel-capacity: 10000
      kafka-properties:
        max.poll.records: 500
```

Each `@CkcKafkaConsumer` bean is bound to `ckc.consumers.<name>`. The starter keeps runtime
settings in Spring configuration and leaves record handling in a dedicated consumer class instead
of using method-level listener annotations.

For a full runnable configuration, see the demo application's
[`application-ckc-spring-boot.yml`](../demo/ckc-demo/src/main/resources/application-ckc-spring-boot.yml).

Startup validation is strict: every annotated consumer must have matching configuration, every
configured consumer must have a matching annotated bean, consumer names must be unique, and merged
Kafka properties must include `bootstrap.servers`, `group.id`, `key.deserializer`, and
`value.deserializer`.

Kafka client properties are shared through `ckc.clusters.<name>.kafka-properties`. A consumer selects
its cluster with `cluster`. If `cluster` is omitted, the starter uses `ckc.default-cluster`; if that is
also omitted and exactly one cluster is configured, that single cluster is used as the default.

Per-consumer `kafka-properties` override cluster properties. Convenience fields such as `group-id`,
`client-id`, `key-deserializer`, and `value-deserializer` are applied last and therefore override both
cluster and per-consumer raw Kafka properties.

Consumers with `auto-startup: false` can be started explicitly through `CkcConsumerRegistry`:

```kotlin
class OrdersWarmup(
    private val ckcConsumers: CkcConsumerRegistry
) {
    fun startAfterWarmup() {
        ckcConsumers.start("orders")
    }
}
```

Retry schemas are ordered lists of core retry rules. Each rule lists fully qualified exception class
names, `max-retries`, and `delay`; the first matching rule is used. Consumers use
`ckc.default-retry-schema` unless they override it with `ckc.consumers.<name>.retry-schema`.

Processing dispatchers are named definitions under `ckc.dispatchers`. Consumers use
`ckc.default-processing-dispatcher` unless they override it with
`ckc.consumers.<name>.processing-dispatcher`. The reserved names `dispatchers-default` and
`dispatchers-io` always map to Kotlin `Dispatchers.Default` and `Dispatchers.IO`.
Starter-created fixed-thread and virtual-thread dispatchers are closed with the Spring context.

The starter is managed by Spring `SmartLifecycle`. `ckc.lifecycle.phase` controls start/stop order
relative to other lifecycle beans, and `ckc.lifecycle.shutdown-timeout` bounds how long the starter
waits for CKC consumers to stop gracefully.

If Spring Boot Actuator is on the classpath, the starter contributes a `ckcHealthIndicator` bean.
It reports starter lifecycle state and per-consumer configuration/state details: registered
consumers, running consumers, auto-startup flags, subscription, cluster, group, processing mode,
dispatcher, retry schema, metrics mode, core lifecycle flags, processing worker/queue counters,
poll-loop state, assigned partitions, and last local poll/commit observations. Kafka lag and
downstream dependency health require separate probes and are not inferred by this indicator.

Starter-managed consumers add coroutine-safe MDC around `process` and `handleFailure` callbacks by
default. The MDC context includes `ckc.consumer`, `ckc.processing.mode`, `kafka.topic`,
`kafka.partition`, and `kafka.offset`. `kafka.key` is disabled by default to avoid per-record key
string conversion and high-cardinality log context; enable it with `ckc.observability.mdc.include-key`.
Set `ckc.observability.mdc.enabled=false` to keep the record processing path direct.

## Configuration Reference

The full starter property structure is:

```yaml
ckc:
  enabled: true

  lifecycle:
    phase: 0
    shutdown-timeout: 30s

  health:
    enabled: true

  observability:
    mdc:
      enabled: true
      include-key: false
      max-key-length: 128

  default-processing-dispatcher: dispatchers-default
  dispatchers:
    shared-workers:
      type: FIXED_THREAD_POOL # DISPATCHERS_DEFAULT | DISPATCHERS_IO | FIXED_THREAD_POOL | VIRTUAL_THREAD_PER_TASK | BEAN
      threads: 16
      thread-name-prefix: ckc-worker-
    loom-workers:
      type: VIRTUAL_THREAD_PER_TASK
      thread-name-prefix: ckc-loom-

  metrics:
    enabled: true
    implementation: MICROMETER # MICROMETER | CUSTOM | NONE
    prefix: app # fallback when no Micrometer schemas are configured
    micrometer:
      default-schema: default
      schemas:
        default:
          metric-prefix: app
          static-tags:
            - name: app_name
              value: my-service
          record-driven-tags:
            - name: event_type
              default: NONE

  default-retry-schema: default
  retry-schemas:
    default:
      rules:
        - exceptions:
            - java.io.IOException
          max-retries: 3
          delay: 100ms

  default-cluster: main
  clusters:
    main:
      kafka-properties:
        bootstrap.servers: localhost:9092
        auto.offset.reset: earliest

  consumers:
    orders:
      auto-startup: true
      cluster: main
      topics:
        - orders.v1
      # topic-pattern: orders\..*
      group-id: orders-service
      client-id: orders-service-1
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: com.example.OrderEventDeserializer
      processing-mode: AT_LEAST_ONCE_NO_ORDERING
      worker-concurrency: 1
      consumer-poll-loop-concurrency: 1
      commit-interval: 5s
      work-channel-capacity: 1024
      # freshness-max-record-age: 10s
      processing-dispatcher: shared-workers
      metrics:
        schema: default
      retry-schema: default
      kafka-properties:
        max.poll.records: 500
```

Map keys under `clusters`, `dispatchers`, `retry-schemas`, `metrics.micrometer.schemas`, and
`consumers` are application-defined names, except for reserved dispatcher names
`dispatchers-default` and `dispatchers-io`. Per-consumer convenience properties are applied after
raw Kafka properties, so they override values from `kafka-properties`.
`freshness-max-record-age` is supported only for freshness-first processing
modes and causes stale records to be dropped before the user handler runs.

## Metrics

Set `ckc.metrics.implementation` to `MICROMETER`, `CUSTOM`, or `NONE`. `MICROMETER` uses the
configured `ckc.metrics.micrometer.schemas` entries to create CKC Micrometer metrics. If no schema is
configured, the starter keeps the legacy fallback and creates one schema from `ckc.metrics.prefix`.

Record-driven Micrometer tag schemas are declared in configuration. The per-record extraction logic
lives in Spring beans annotated with `@CkcMicrometerRecordTags`:

```kotlin
import avh.ckc.micrometer.RecordDrivenTagExtractors
import avh.ckc.micrometer.recordDrivenTagExtractors
import avh.ckc.spring.CkcMicrometerRecordTags

@Bean
@CkcMicrometerRecordTags(consumer = "orders")
fun orderRecordTags(): RecordDrivenTagExtractors<String, OrderEvent> =
    recordDrivenTagExtractors {
        tag("event_type") { record -> record.value()?.type }
    }
```

If an extractor is absent or returns `null`, the tag value falls back to the `default` declared in
the selected schema.

For `CUSTOM`, provide `ConsumerMetrics` beans annotated with `@CkcConsumerMetrics`:

```kotlin
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.spring.CkcConsumerMetrics

@Bean
@CkcConsumerMetrics(consumer = "orders")
fun orderMetrics(): ConsumerMetrics<String, OrderEvent> =
    MyOrderMetrics()
```

Leaving `consumer` blank declares a default bean. For custom metrics, a single unannotated
`ConsumerMetrics` bean is also accepted as the default.

The demo `ckc-spring-boot` profile uses `CUSTOM` metrics to wrap Micrometer metrics with demo audit
side effects while still reading the Micrometer schema section from application configuration.
