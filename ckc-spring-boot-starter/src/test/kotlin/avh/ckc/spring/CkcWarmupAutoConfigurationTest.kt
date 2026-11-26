package avh.ckc.spring

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.micrometer.MicrometerConsumerMetricsSchema
import avh.ckc.core.metrics.micrometer.micrometerConsumerMetrics
import avh.ckc.warmup.TrafficWarmupLifecycleEvent
import avh.ckc.warmup.TrafficWarmupLifecyclePhase
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.util.concurrent.CopyOnWriteArrayList
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.springframework.boot.actuate.health.HealthIndicator
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.assertj.AssertableApplicationContext
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.event.EventListener

class CkcWarmupAutoConfigurationTest {
    @Test
    fun `a late waiting event cannot replace main readiness`() {
        val context = CkcWarmupContext(
            "shared",
            CkcWarmupProperties.Context(namespace = "test", controlTopic = "control", controlCluster = "control"),
            mapOf("control" to CkcConsumerProperties.Cluster(linkedMapOf("bootstrap.servers" to "localhost:19093"))),
            ApplicationEventPublisher { }
        )

        context.recordPhase(TrafficWarmupLifecycleEvent(TrafficWarmupLifecyclePhase.MAIN_READY, "orders"))
        context.recordPhase(TrafficWarmupLifecycleEvent(TrafficWarmupLifecyclePhase.WAITING_FOR_PARTICIPANTS, "orders"))

        assertThat(context.snapshot("orders")).isEqualTo(CkcWarmupStateSnapshot("shared", "MAIN_READY", true))
    }

    @Test
    fun `disabled warmup ignores an unknown context`() {
        val events = WarmupTestEvents()
        warmupTestRunner(events)
            .withPropertyValues(*WarmupTestTopics("disabled").properties(false))
            .withPropertyValues(
                "ckc.warmup.enabled=false",
                "ckc.consumers.orders.warmup.context=unknown"
            )
            .run { context ->
                assertThat(context).hasNotFailed()
                assertThat(context.consumerDetails("orders")).doesNotContainKey("warmup")
                assertThat(context.getBean(SimpleMeterRegistry::class.java).find("app.ckc.warmup.pending").gauges())
                    .isEmpty()
                assertThat(events.lifecycle).isEmpty()
            }
    }

    @Test
    fun `unknown warmup context names the consumer in the startup error`() {
        warmupTestRunner(WarmupTestEvents())
            .withPropertyValues(*WarmupTestTopics("unknown").properties(false))
            .withPropertyValues("ckc.consumers.orders.warmup.context=unknown")
            .run { context ->
                assertThat(context.startupFailure)
                    .hasStackTraceContaining("ckc.consumers.orders.warmup references unknown warm-up context 'unknown'")
            }
    }

    @Test
    fun `warmup rejects pattern subscriptions with the consumer name`() {
        warmupTestRunner(WarmupTestEvents())
            .withPropertyValues(*WarmupTestTopics("pattern").properties(false, patternOrders = true))
            .run { context ->
                assertThat(context.startupFailure)
                    .hasStackTraceContaining("ckc.consumers.orders.warmup requires explicit topics")
            }
    }

    @ParameterizedTest
    @EnumSource(CkcConsumerProperties.MetricsImplementation::class)
    fun `warmup uses the existing consumer metrics selection`(mode: CkcConsumerProperties.MetricsImplementation) {
        warmupTestRunner(WarmupTestEvents())
            .withPropertyValues(*WarmupTestTopics("metrics").properties(false))
            .withPropertyValues("ckc.metrics.implementation=${mode.name}")
            .run { context ->
                assertThat(context).hasNotFailed()
                val meters = context.getBean(SimpleMeterRegistry::class.java)
                val prefix = when (mode) {
                    CkcConsumerProperties.MetricsImplementation.CUSTOM -> "custom"
                    CkcConsumerProperties.MetricsImplementation.MICROMETER -> "app"
                    CkcConsumerProperties.MetricsImplementation.NONE -> null
                }
                val warmupGauges = meters.meters.filter { it.id.name.endsWith(".warmup.pending") }
                if (prefix == null) {
                    assertThat(warmupGauges).isEmpty()
                } else {
                    assertThat(warmupGauges.map { it.id.name }.toSet()).containsExactly("$prefix.ckc.warmup.pending")
                    assertThat(warmupGauges.map { it.id.getTag("consumer_id") }).containsExactlyInAnyOrder("orders", "batches")
                }
                assertThat(context.consumerDetails("orders")["warmup"])
                    .isEqualTo(mapOf("context" to "shared", "phase" to "REGISTERED", "mainReady" to false))
            }
    }
}

internal fun warmupTestRunner(events: WarmupTestEvents): ApplicationContextRunner =
    ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                CkcSpringBootAutoConfiguration::class.java,
                CkcSpringBootHealthAutoConfiguration::class.java
            )
        )
        .withUserConfiguration(WarmupTestConfiguration::class.java)
        .withBean(WarmupTestEvents::class.java, { events })

@Suppress("UNCHECKED_CAST")
internal fun AssertableApplicationContext.consumerDetails(name: String): Map<String, Any?> {
    val health = getBean("ckcHealthIndicator", HealthIndicator::class.java).health()
    return (health.details.getValue("consumers") as Map<*, *>)[name] as Map<String, Any?>
}

internal class WarmupTestTopics(private val prefix: String) {
    val orders = "$prefix.orders"
    val batches = "$prefix.batches"
    val warmupOrders = "$prefix.warmup-orders"
    val warmupBatches = "$prefix.warmup-batches"
    val control = "$prefix.control"

    fun properties(autoStartup: Boolean, patternOrders: Boolean = false): Array<String> = buildList {
        val deserializer = StringDeserializer::class.java.name
        val serializer = StringSerializer::class.java.name
        addAll(listOf(
            "ckc.default-cluster=main",
            "ckc.clusters.main.kafka-properties[bootstrap.servers]=localhost:19092",
            "ckc.clusters.main.kafka-properties[auto.offset.reset]=earliest",
            "ckc.clusters.control.kafka-properties[bootstrap.servers]=localhost:19093",
            "ckc.clusters.destination.kafka-properties[bootstrap.servers]=localhost:19094",
            "ckc.warmup.enabled=true",
            "ckc.warmup.contexts.shared.namespace=$prefix",
            "ckc.warmup.contexts.shared.control-topic=$control",
            "ckc.warmup.contexts.shared.control-cluster=control",
            "ckc.warmup.contexts.shared.warmup-duration=12s",
            "ckc.warmup.contexts.shared.offer-window=1s",
            "ckc.warmup.contexts.shared.max-inflight-delegations=32"
        ))
        listOf(Triple("orders", orders, warmupOrders), Triple("batches", batches, warmupBatches)).forEach {
            (consumer, source, destination) ->
            val path = "ckc.consumers.$consumer"
            val subscription = if (consumer == "orders" && patternOrders) "topic-pattern" else "topics[0]"
            addAll(listOf(
                "$path.auto-startup=$autoStartup",
                "$path.$subscription=$source",
                "$path.group-id=$prefix-$consumer",
                "$path.key-deserializer=$deserializer",
                "$path.value-deserializer=$deserializer",
                "$path.warmup.context=shared",
                "$path.warmup.destination-cluster=destination",
                "$path.warmup.key-serializer=$serializer",
                "$path.warmup.value-serializer=$serializer",
                "$path.warmup.mappings[0].source-topic=$source",
                "$path.warmup.mappings[0].warmup-topic=$destination",
                "$path.warmup.mappings[0].quota-profile=100",
                "$path.warmup.mappings[0].quota-interval=1s"
            ))
        }
    }.toTypedArray()
}

internal class WarmupTestEvents(
    private val beforeProcessed: suspend (ConsumerRecord<String, String>) -> Unit = {}
) {
    val lifecycle = CopyOnWriteArrayList<CkcWarmupLifecycleEvent>()
    val forwarded = CopyOnWriteArrayList<CkcWarmupForwardedEvent>()
    val processed = CopyOnWriteArrayList<ConsumerRecord<String, String>>()

    suspend fun process(record: ConsumerRecord<String, String>) {
        beforeProcessed(record)
        processed += record
    }

    @EventListener
    fun onLifecycle(event: CkcWarmupLifecycleEvent) {
        lifecycle += event
    }

    @EventListener
    fun onForwarded(event: CkcWarmupForwardedEvent) {
        forwarded += event
    }
}

@Configuration(proxyBeanMethods = false)
internal class WarmupTestConfiguration {
    @Bean
    fun meterRegistry() = SimpleMeterRegistry()

    @Bean
    @CkcConsumerMetrics(consumer = "orders")
    fun ordersMetrics(registry: SimpleMeterRegistry) = wrappedMetrics(registry, "orders")

    @Bean
    @CkcConsumerMetrics(consumer = "batches")
    fun batchesMetrics(registry: SimpleMeterRegistry) = wrappedMetrics(registry, "batches")

    private fun wrappedMetrics(registry: SimpleMeterRegistry, name: String): ConsumerMetrics<String, String> {
        val delegate = micrometerConsumerMetrics<String, String>(
            MicrometerConsumerMetricsSchema(registry, metricPrefix = "custom")
        ) { consumerId = name }
        return object : ConsumerMetrics<String, String> by delegate {}
    }

    @CkcKafkaConsumer(name = "orders")
    class Orders(private val events: WarmupTestEvents) : CkcConsumer<String, String> {
        override suspend fun process(record: ConsumerRecord<String, String>) {
            events.process(record)
        }
    }

    @CkcKafkaConsumer(name = "batches")
    class Batches(private val events: WarmupTestEvents) : CkcConsumer<String, String> {
        override suspend fun process(record: ConsumerRecord<String, String>) {
            events.process(record)
        }
    }
}
