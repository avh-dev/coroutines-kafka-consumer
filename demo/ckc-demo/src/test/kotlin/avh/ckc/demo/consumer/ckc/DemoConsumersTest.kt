package avh.ckc.demo.consumer.ckc

import avh.ckc.core.ProcessingMode
import avh.ckc.core.consumerContainer
import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.core.metrics.micrometer.MicrometerConsumerMetricsSchema
import avh.ckc.core.metrics.micrometer.micrometerConsumerMetrics
import avh.ckc.demo.DemoTopics
import avh.ckc.demo.config.DemoApplicationProperties
import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.CauldronTelemetryEvent
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.warmup.trafficWarmupContext
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.slf4j.LoggerFactory

class DemoConsumersTest {
    @ParameterizedTest
    @ValueSource(booleans = [false, true])
    fun `three typed demo consumers register warmup meters only when configured`(enabled: Boolean) {
        val connection = mapOf<String, Any?>(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG to "warmup-kafka:9092"
        )
        val configuration = DemoApplicationProperties.TrafficWarmup(enabled = enabled)
        val warmup = if (configuration.enabled) {
            val context = trafficWarmupContext {
                instanceId = "pod-7"
                namespace = "global5-v6-test"
                controlTopic = DemoTopics.WARMUP_CONTROL
                controlProducerProperties = connection
                controlConsumerProperties = connection
            }
            DemoConsumers.TrafficWarmup(context, connection, connection, configuration)
        } else null
        val mainProperties = mapOf<String, Any>(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "main-kafka:9092",
            ConsumerConfig.GROUP_ID_CONFIG to "demo-test"
        )
        val runtime = DemoApplicationProperties.ConsumerRuntime()
        val telemetryRuntime = DemoApplicationProperties.ConsumerRuntime(
            processingMode = ProcessingMode.FRESHNESS_FIRST_REPLACE_PENDING_BY_KEY
        )
        val retry = DemoApplicationProperties.Retry()
        val audit = DemoApplicationProperties.Audit(enabled = false)

        val registry = SimpleMeterRegistry()
        val schema = MicrometerConsumerMetricsSchema(registry, metricPrefix = "demo")
        val order = DemoConsumers.orderConsumer(
            mainProperties,
            micrometerConsumerMetrics(schema) { consumerId = "orders" },
            audit,
            runtime,
            retry,
            Dispatchers.Default,
            processingEnabled = false,
            trafficWarmup = warmup
        ) { _, _: OrderLifecycleEvent -> }
        val batch = DemoConsumers.batchConsumer(
            mainProperties,
            micrometerConsumerMetrics(schema) { consumerId = "batches" },
            audit,
            runtime,
            retry,
            Dispatchers.Default,
            processingEnabled = false,
            trafficWarmup = warmup
        ) { _, _: BatchLifecycleEvent -> }
        val telemetry = DemoConsumers.telemetryConsumer(
            mainProperties,
            micrometerConsumerMetrics(schema) { consumerId = "telemetry" },
            audit,
            telemetryRuntime,
            freshnessMaxRecordAgeSeconds = 10,
            retry = retry,
            processingDispatcher = Dispatchers.Default,
            processingEnabled = false,
            trafficWarmup = warmup
        ) { _, _: CauldronTelemetryEvent -> }

        try {
            assertEquals(3, registry.find("demo.ckc.recovery.active").gauges().size)
            val pending = registry.find("demo.ckc.warmup.pending").gauges()
            val expectedIds = if (enabled) setOf("orders", "batches", "telemetry") else emptySet()
            assertEquals(expectedIds, pending.mapNotNull { it.id.getTag("consumer_id") }.toSet())
        } finally {
            runBlocking { consumerContainer(order, batch, telemetry).stop() }
            registry.close()
        }
    }

    @Test
    fun `remote donor success preserves audit and processed remotely marker`() {
        val appender = ListAppender<ILoggingEvent>().also { it.start() }
        val logger = LoggerFactory.getLogger("AUDIT_TCP") as Logger
        val markers = CopyOnWriteArrayList<Boolean>()
        val delegate = recordingMetrics(markers)
        val metrics = DemoConsumers.auditMetrics(
            delegate,
            DemoApplicationProperties.Audit(enabled = true)
        )
        val recipientRecord = ConsumerRecord(
            DemoTopics.WARMUP_ORDER_EVENTS,
            0,
            11L,
            "order-11",
            "value"
        )
        val donorRecord = ConsumerRecord(DemoTopics.ORDER_EVENTS, 1, 7L, "order-7", "value")
        logger.addAppender(appender)

        try {
            metrics.runtimeBinding("warmup-orders").onRecordProcessed(
                "order-11",
                "value",
                recipientRecord,
                10,
                20,
                processedRemotely = false
            )
            metrics.onRecordProcessed(
                "order-7",
                "value",
                donorRecord,
                30,
                40,
                processedRemotely = true
            )
        } finally {
            logger.detachAppender(appender)
            appender.stop()
        }

        assertEquals(listOf(false, true), markers)
        assertEquals(listOf("order-11", "order-7"), appender.list.map {
            it.formattedMessage.split('|').last()
        })
    }

    private fun recordingMetrics(markers: MutableList<Boolean>): ConsumerMetrics<String, String> =
        object : ConsumerMetrics<String, String> {
            override fun runtimeBinding(bindingId: String): ConsumerMetrics<String, String> = this

            override fun onRecordProcessed(
                key: String?,
                value: String?,
                record: ConsumerRecord<String, String>,
                endToEndLatencyMillis: Long,
                durationNanos: Long,
                processedRemotely: Boolean
            ) {
                markers += processedRemotely
            }
        }
}
