package avh.ckc.demo.consumer.ckc

import avh.ckc.core.metrics.micrometer.MicrometerConsumerMetricsSchema
import avh.ckc.core.metrics.micrometer.micrometerConsumerMetrics
import avh.ckc.demo.DemoTopics
import avh.ckc.demo.config.DemoApplicationProperties
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.serialization.OrderLifecycleEventSerializer
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.boot.env.YamlPropertySourceLoader
import org.springframework.core.io.support.PathMatchingResourcePatternResolver
import org.springframework.mock.env.MockEnvironment
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName

@Testcontainers(disabledWithoutDocker = true)
class WarmupInflightLimitIntegrationTest {
    @Test
    fun `deployment limit keeps the next record on the donor while a result is pending`() = runBlocking {
        val topics = listOf(
            DemoTopics.ORDER_EVENTS, DemoTopics.BATCH_EVENTS, DemoTopics.CAULDRON_EVENTS,
            DemoTopics.WARMUP_ORDER_EVENTS, DemoTopics.WARMUP_BATCH_EVENTS, DemoTopics.WARMUP_CAULDRON_EVENTS,
            DemoTopics.WARMUP_CONTROL
        )
        val connection = mapOf(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers)
        Admin.create(connection).use { admin ->
            admin.createTopics(topics.map { NewTopic(it, 1, 1.toShort()) }).all().get(10, TimeUnit.SECONDS)
        }

        val donorReady = CompletableDeferred<Unit>()
        val local = CompletableDeferred<Unit>()
        val recipientEntered = CompletableDeferred<Unit>()
        val recipientHandledNext = CompletableDeferred<Unit>()
        val releaseHeld = CompletableDeferred<Unit>()
        val donorMeters = SimpleMeterRegistry()
        val recipientMeters = SimpleMeterRegistry()
        try {
            val donor = runtime("donor", donorMeters) { key ->
                when (key) {
                    "ready" -> donorReady.complete(Unit)
                    "next" -> local.complete(Unit)
                }
            }
            val recipient = runtime("recipient", recipientMeters) { key ->
                when (key) {
                    "held" -> {
                        recipientEntered.complete(Unit)
                        releaseHeld.await()
                    }
                    "next" -> recipientHandledNext.complete(Unit)
                }
            }

            KafkaProducer(connection, StringSerializer(), OrderLifecycleEventSerializer()).use { producer ->
                fun publish(key: String) {
                    producer.send(ProducerRecord(DemoTopics.ORDER_EVENTS, key, OrderLifecycleEvent.getDefaultInstance()))
                        .get(5, TimeUnit.SECONDS)
                }

                try {
                    donor.start()
                    publish("ready")
                    withTimeout(20.seconds) { donorReady.await() }

                    recipient.start()
                    // Records published before the recipient activates its lane are processed by the donor.
                    withTimeout(20.seconds) {
                        while (!recipientEntered.isCompleted) {
                            publish("held")
                            delay(200)
                        }
                    }
                    val pending = donorMeters.get("demo.ckc.warmup.pending").tag("consumer_id", "order").gauge()
                    assertEquals(1.0, pending.value())

                    publish("next")
                    withTimeout(5.seconds) { local.await() }
                    assertEquals(1.0, pending.value())
                    assertFalse(recipientHandledNext.isCompleted)
                } finally {
                    releaseHeld.complete(Unit)
                    try {
                        recipient.stop()
                    } finally {
                        donor.stop()
                    }
                }
            }
        } finally {
            recipientMeters.close()
            donorMeters.close()
        }
    }

    private fun runtime(
        instance: String,
        registry: SimpleMeterRegistry,
        handleOrder: suspend (String?) -> Unit
    ): CkcConsumerRuntime {
        val environment = MockEnvironment()
            .withProperty("demo.kafka.bootstrap-servers", kafka.bootstrapServers)
            .withProperty("AUDIT_LOG_ENABLED", "false")
            .withProperty("TRAFFIC_WARMUP_ENABLED", "true")
            .withProperty("TRAFFIC_WARMUP_INSTANCE_ID", instance)
            .withProperty("TRAFFIC_WARMUP_CONTROL_KAFKA_BOOTSTRAP_SERVERS", kafka.bootstrapServers)
            .withProperty("TRAFFIC_WARMUP_DESTINATION_KAFKA_BOOTSTRAP_SERVERS", kafka.bootstrapServers)
            .withProperty("TRAFFIC_WARMUP_OFFER_WINDOW_MS", "250")
            .withProperty("TRAFFIC_WARMUP_DELEGATION_TIMEOUT_SECONDS", "30")
            .withProperty("TRAFFIC_WARMUP_ORDER_QUOTA_PROFILE", "100")
            .withProperty("TRAFFIC_WARMUP_MAX_INFLIGHT_DELEGATIONS", "1")
        val loader = YamlPropertySourceLoader()
        PathMatchingResourcePatternResolver().getResources("classpath*:application.yml").forEach { resource ->
            loader.load(resource.description, resource).forEach(environment.propertySources::addLast)
        }
        val properties = Binder.get(environment).bind("demo", DemoApplicationProperties::class.java).get()
        assertEquals(1, properties.trafficWarmup.maxInflightDelegations)
        val schema = MicrometerConsumerMetricsSchema(registry, metricPrefix = "demo")
        return CkcConsumerRuntime(
            properties,
            orderHandler = { key, _ -> handleOrder(key) },
            batchHandler = { _, _ -> },
            telemetryHandler = { _, _ -> },
            consumerMetrics = micrometerConsumerMetrics(schema) { consumerId = "telemetry" },
            orderConsumerMetrics = micrometerConsumerMetrics(schema) { consumerId = "order" },
            batchConsumerMetrics = micrometerConsumerMetrics(schema) { consumerId = "batch" },
            processingDispatcher = Dispatchers.Default,
            meterRegistry = registry
        )
    }

    companion object {
        @Container
        @JvmStatic
        val kafka = KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"))
    }
}
