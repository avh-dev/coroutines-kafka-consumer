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
      processing-mode: at-least-once-ordered-by-key
      worker-concurrency: 64
      work-channel-capacity: 10000
      kafka-properties:
        max.poll.records: 500
```

Each `@CkcKafkaConsumer` bean is bound to `ckc.consumers.<name>`. The starter keeps runtime
settings in Spring configuration and leaves record handling in a dedicated consumer class instead
of using method-level listener annotations.

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

The starter is managed by Spring `SmartLifecycle`. `ckc.lifecycle.phase` controls start/stop order
relative to other lifecycle beans, and `ckc.lifecycle.shutdown-timeout` bounds how long the starter
waits for CKC consumers to stop gracefully.

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
