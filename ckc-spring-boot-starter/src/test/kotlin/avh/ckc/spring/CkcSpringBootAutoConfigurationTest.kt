package avh.ckc.spring

import avh.ckc.core.ProcessingMode
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.micrometer.RecordDrivenTagExtractors
import avh.ckc.micrometer.recordDrivenTagExtractors
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

class CkcSpringBootAutoConfigurationTest {
    private val contextRunner = ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(CkcSpringBootAutoConfiguration::class.java))

    @Test
    fun `creates lifecycle for annotated consumer configured by name`() {
        contextRunner
            .withUserConfiguration(OrdersConsumerConfiguration::class.java)
            .withPropertyValues(
                "ckc.clusters.main.kafka-properties.${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}=localhost:9092",
                "ckc.clusters.main.kafka-properties.${ConsumerConfig.AUTO_OFFSET_RESET_CONFIG}=earliest",
                "ckc.consumers.orders.auto-startup=false",
                "ckc.consumers.orders.cluster=main",
                "ckc.consumers.orders.topics[0]=orders.v1",
                "ckc.consumers.orders.group-id=orders-service",
                "ckc.consumers.orders.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer",
                "ckc.consumers.orders.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer",
                "ckc.consumers.orders.processing-mode=at-least-once-ordered-by-key",
                "ckc.consumers.orders.worker-concurrency=4",
                "ckc.consumers.orders.consumer-poll-loop-concurrency=2",
                "ckc.consumers.orders.commit-interval=2s",
                "ckc.consumers.orders.work-channel-capacity=256",
                "ckc.consumers.orders.kafka-properties.${ConsumerConfig.MAX_POLL_RECORDS_CONFIG}=500"
            )
            .run { context ->
                assertThat(context).hasSingleBean(CkcConsumersLifecycle::class.java)
                assertThat(context).hasSingleBean(CkcConsumerRegistry::class.java)

                val registry = context.getBean(CkcConsumerRegistry::class.java)
                assertThat(registry.consumerNames)
                    .containsExactly("orders")
                assertThat(registry.isRunning("orders"))
                    .isFalse()

                val properties = context.getBean(CkcConsumerProperties::class.java)
                assertThat(properties.consumers["orders"]?.processingMode)
                    .isEqualTo(ProcessingMode.AT_LEAST_ONCE_ORDERED_BY_KEY)
                assertThat(
                    properties.consumers["orders"]?.kafkaProperties(
                        properties.clusters.getValue("main").kafkaProperties
                    )
                )
                    .containsEntry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
                    .containsEntry(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                    .containsEntry(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500")
            }
    }

    @Test
    fun `uses configured default cluster when consumer cluster is omitted`() {
        contextRunner
            .withUserConfiguration(OrdersConsumerConfiguration::class.java)
            .withPropertyValues(
                "ckc.default-cluster=main",
                "ckc.clusters.main.kafka-properties.${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}=localhost:9092",
                "ckc.clusters.secondary.kafka-properties.${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}=localhost:19092",
                "ckc.consumers.orders.auto-startup=false",
                "ckc.consumers.orders.topics[0]=orders.v1",
                "ckc.consumers.orders.group-id=orders-service",
                "ckc.consumers.orders.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer",
                "ckc.consumers.orders.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer"
            )
            .run { context ->
                val registry = context.getBean(CkcConsumerRegistry::class.java)

                assertThat(registry.consumerNames)
                    .containsExactly("orders")
            }
    }

    @Test
    fun `uses the only cluster as default when consumer cluster is omitted`() {
        contextRunner
            .withUserConfiguration(OrdersConsumerConfiguration::class.java)
            .withPropertyValues(
                "ckc.clusters.main.kafka-properties.${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}=localhost:9092",
                "ckc.consumers.orders.auto-startup=false",
                "ckc.consumers.orders.topics[0]=orders.v1",
                "ckc.consumers.orders.group-id=orders-service",
                "ckc.consumers.orders.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer",
                "ckc.consumers.orders.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer"
            )
            .run { context ->
                val registry = context.getBean(CkcConsumerRegistry::class.java)

                assertThat(registry.consumerNames)
                    .containsExactly("orders")
            }
    }

    @Test
    fun `does not create lifecycle when starter is disabled`() {
        contextRunner
            .withUserConfiguration(OrdersConsumerConfiguration::class.java)
            .withPropertyValues("ckc.enabled=false")
            .run { context ->
                assertThat(context).doesNotHaveBean(CkcConsumersLifecycle::class.java)
            }
    }

    @Test
    fun `binds configured micrometer metrics schema`() {
        contextRunner
            .withUserConfiguration(OrdersConsumerConfiguration::class.java, MetricsConfiguration::class.java)
            .withPropertyValues(
                "ckc.clusters.main.kafka-properties.${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}=localhost:9092",
                "ckc.consumers.orders.auto-startup=false",
                "ckc.consumers.orders.topics[0]=orders.v1",
                "ckc.consumers.orders.group-id=orders-service",
                "ckc.consumers.orders.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer",
                "ckc.consumers.orders.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer",
                "ckc.metrics.implementation=micrometer",
                "ckc.metrics.micrometer.schemas.default.metric-prefix=testapp",
                "ckc.metrics.micrometer.schemas.default.static-tags[0].name=app_name",
                "ckc.metrics.micrometer.schemas.default.static-tags[0].value=test",
                "ckc.metrics.micrometer.schemas.default.record-driven-tags[0].name=event_type",
                "ckc.metrics.micrometer.schemas.default.record-driven-tags[0].default=UNKNOWN"
            )
            .run { context ->
                val registry = context.getBean(CkcConsumerRegistry::class.java)
                assertThat(registry.consumerNames)
                    .containsExactly("orders")

                val metrics = context.getBean(CkcConsumerProperties::class.java).metrics
                assertThat(metrics.implementation)
                    .isEqualTo(CkcConsumerProperties.MetricsImplementation.MICROMETER)
                assertThat(metrics.micrometer.schemas.getValue("default").metricPrefix)
                    .isEqualTo("testapp")
                assertThat(metrics.micrometer.schemas.getValue("default").recordDrivenTags.single().default)
                    .isEqualTo("UNKNOWN")
            }
    }

    @Test
    fun `uses annotated custom metrics bean`() {
        contextRunner
            .withUserConfiguration(OrdersConsumerConfiguration::class.java, CustomMetricsConfiguration::class.java)
            .withPropertyValues(
                "ckc.clusters.main.kafka-properties.${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}=localhost:9092",
                "ckc.consumers.orders.auto-startup=false",
                "ckc.consumers.orders.topics[0]=orders.v1",
                "ckc.consumers.orders.group-id=orders-service",
                "ckc.consumers.orders.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer",
                "ckc.consumers.orders.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer",
                "ckc.metrics.implementation=custom"
            )
            .run { context ->
                val registry = context.getBean(CkcConsumerRegistry::class.java)
                assertThat(registry.consumerNames)
                    .containsExactly("orders")
            }
    }

    @Test
    fun `fails when custom metrics are enabled without a metrics bean`() {
        contextRunner
            .withUserConfiguration(OrdersConsumerConfiguration::class.java)
            .withPropertyValues(
                "ckc.clusters.main.kafka-properties.${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}=localhost:9092",
                "ckc.consumers.orders.auto-startup=false",
                "ckc.consumers.orders.topics[0]=orders.v1",
                "ckc.consumers.orders.group-id=orders-service",
                "ckc.consumers.orders.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer",
                "ckc.consumers.orders.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer",
                "ckc.metrics.implementation=custom"
            )
            .run { context ->
                assertThatThrownBy {
                    context.getBean(CkcConsumerRegistry::class.java).consumerNames
                }
                    .hasStackTraceContaining("CKC custom metrics are enabled")
            }
    }

    @Test
    fun `uses configured retry schema`() {
        contextRunner
            .withUserConfiguration(OrdersConsumerConfiguration::class.java)
            .withPropertyValues(
                "ckc.clusters.main.kafka-properties.${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}=localhost:9092",
                "ckc.consumers.orders.auto-startup=false",
                "ckc.consumers.orders.topics[0]=orders.v1",
                "ckc.consumers.orders.group-id=orders-service",
                "ckc.consumers.orders.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer",
                "ckc.consumers.orders.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer",
                "ckc.consumers.orders.retry-schema=transient-errors",
                "ckc.retry-schemas.transient-errors.rules[0].exceptions[0]=java.io.IOException",
                "ckc.retry-schemas.transient-errors.rules[0].exceptions[1]=java.net.SocketTimeoutException",
                "ckc.retry-schemas.transient-errors.rules[0].max-retries=3",
                "ckc.retry-schemas.transient-errors.rules[0].delay=50ms",
                "ckc.retry-schemas.transient-errors.rules[1].exceptions[0]=java.lang.IllegalStateException",
                "ckc.retry-schemas.transient-errors.rules[1].max-retries=1",
                "ckc.retry-schemas.transient-errors.rules[1].delay=10ms"
            )
            .run { context ->
                val registry = context.getBean(CkcConsumerRegistry::class.java)
                assertThat(registry.consumerNames)
                    .containsExactly("orders")

                val properties = context.getBean(CkcConsumerProperties::class.java)
                assertThat(properties.consumers.getValue("orders").retrySchema)
                    .isEqualTo("transient-errors")
                assertThat(properties.retrySchemas.getValue("transient-errors").rules)
                    .hasSize(2)
            }
    }

    @Test
    fun `uses default retry schema when consumer retry schema is omitted`() {
        contextRunner
            .withUserConfiguration(OrdersConsumerConfiguration::class.java)
            .withPropertyValues(
                "ckc.clusters.main.kafka-properties.${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}=localhost:9092",
                "ckc.consumers.orders.auto-startup=false",
                "ckc.consumers.orders.topics[0]=orders.v1",
                "ckc.consumers.orders.group-id=orders-service",
                "ckc.consumers.orders.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer",
                "ckc.consumers.orders.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer",
                "ckc.default-retry-schema=default",
                "ckc.retry-schemas.default.rules[0].exceptions[0]=java.io.IOException",
                "ckc.retry-schemas.default.rules[0].max-retries=2",
                "ckc.retry-schemas.default.rules[0].delay=25ms"
            )
            .run { context ->
                val registry = context.getBean(CkcConsumerRegistry::class.java)
                assertThat(registry.consumerNames)
                    .containsExactly("orders")

                val properties = context.getBean(CkcConsumerProperties::class.java)
                assertThat(properties.defaultRetrySchema)
                    .isEqualTo("default")
                assertThat(properties.consumers.getValue("orders").retrySchema)
                    .isNull()
            }
    }

    @Test
    fun `fails when retry schema is unknown`() {
        contextRunner
            .withUserConfiguration(OrdersConsumerConfiguration::class.java)
            .withPropertyValues(
                "ckc.clusters.main.kafka-properties.${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}=localhost:9092",
                "ckc.consumers.orders.auto-startup=false",
                "ckc.consumers.orders.topics[0]=orders.v1",
                "ckc.consumers.orders.group-id=orders-service",
                "ckc.consumers.orders.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer",
                "ckc.consumers.orders.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer",
                "ckc.consumers.orders.retry-schema=missing"
            )
            .run { context ->
                assertThatThrownBy {
                    context.getBean(CkcConsumerRegistry::class.java).consumerNames
                }
                    .hasStackTraceContaining("references unknown retry schema 'missing'")
            }
    }

    @Test
    fun `fails when retry schema exception is not throwable`() {
        contextRunner
            .withUserConfiguration(OrdersConsumerConfiguration::class.java)
            .withPropertyValues(
                "ckc.clusters.main.kafka-properties.${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}=localhost:9092",
                "ckc.consumers.orders.auto-startup=false",
                "ckc.consumers.orders.topics[0]=orders.v1",
                "ckc.consumers.orders.group-id=orders-service",
                "ckc.consumers.orders.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer",
                "ckc.consumers.orders.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer",
                "ckc.consumers.orders.retry-schema=bad",
                "ckc.retry-schemas.bad.rules[0].exceptions[0]=java.lang.String",
                "ckc.retry-schemas.bad.rules[0].max-retries=1"
            )
            .run { context ->
                assertThatThrownBy {
                    context.getBean(CkcConsumerRegistry::class.java).consumerNames
                }
                    .hasStackTraceContaining("but it is not a Throwable")
            }
    }

}

@Configuration(proxyBeanMethods = false)
private class OrdersConsumerConfiguration {
    @Bean
    fun ordersConsumer(): OrdersConsumer =
        OrdersConsumer()
}

@CkcKafkaConsumer(name = "orders")
private class OrdersConsumer : CkcConsumer<String, String> {
    override suspend fun process(record: ConsumerRecord<String, String>) = Unit
}

@Configuration(proxyBeanMethods = false)
private class MetricsConfiguration {
    @Bean
    fun meterRegistry(): SimpleMeterRegistry =
        SimpleMeterRegistry()

    @Bean
    @CkcMicrometerRecordTags(consumer = "orders")
    fun orderRecordTags(): RecordDrivenTagExtractors<String, String> =
        recordDrivenTagExtractors {
            tag("event_type") { "ORDER" }
        }
}

@Configuration(proxyBeanMethods = false)
private class CustomMetricsConfiguration {
    @Bean
    @CkcConsumerMetrics(consumer = "orders")
    @Suppress("UNCHECKED_CAST")
    fun orderMetrics(): ConsumerMetrics<String, String> =
        ConsumerMetrics.NOOP as ConsumerMetrics<String, String>
}
