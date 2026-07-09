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
  metrics:
    prefix: myapp
  default-cluster: main
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
      retry:
        max-retries: 3
        delay: 100ms
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

If a `MeterRegistry` is present, the starter creates a default `MicrometerConsumerMetricsSchema`
using `ckc.metrics.prefix`. Applications can provide their own schema bean to customize tags and
record-driven tag schemas.
