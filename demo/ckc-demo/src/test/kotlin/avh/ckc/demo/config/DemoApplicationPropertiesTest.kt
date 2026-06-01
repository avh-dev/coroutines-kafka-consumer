package avh.ckc.demo.config

import avh.ckc.core.ProcessingMode
import kotlin.test.Test
import kotlin.test.assertEquals
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource

class DemoApplicationPropertiesTest {
    @Test
    fun `consumer runtime defaults preserve existing demo settings`() {
        val properties = bind(emptyMap())

        assertEquals(true, properties.consumers.processingEnabled)
        assertEquals(DemoApplicationProperties.MetricsImplementation.MICROMETER, properties.consumers.metricsImplementation)
        assertEquals(8, properties.consumers.workerDispatcherThreads)
        assertEquals(1, properties.kafka.consumer.fetchMinBytes)
        assertEquals(500, properties.kafka.consumer.fetchMaxWaitMs)
        assertEquals(500, properties.kafka.consumer.maxPollRecords)
        assertEquals(2, properties.consumers.order.workerConcurrency)
        assertEquals(1, properties.consumers.order.pollLoopConcurrency)
        assertEquals(1024, properties.consumers.order.workChannelCapacity)
        assertEquals(ProcessingMode.AT_LEAST_ONCE_UNORDERED, properties.consumers.order.processingMode)
        assertEquals(4, properties.consumers.telemetry.workerConcurrency)
        assertEquals(1, properties.consumers.telemetry.pollLoopConcurrency)
        assertEquals(256, properties.consumers.telemetry.workChannelCapacity)
        assertEquals(ProcessingMode.FRESHNESS_FIRST, properties.consumers.telemetry.processingMode)
        assertEquals(true, properties.audit.enabled)
    }

    @Test
    fun `consumer runtime settings can be overridden`() {
        val properties = bind(
            mapOf(
                "demo.consumers.processing-enabled" to "false",
                "demo.consumers.metrics-implementation" to "noop",
                "demo.consumers.worker-dispatcher-threads" to "6",
                "demo.kafka.consumer.fetch-min-bytes" to "65536",
                "demo.kafka.consumer.fetch-max-wait-ms" to "100",
                "demo.kafka.consumer.max-poll-records" to "200",
                "demo.consumers.order.worker-concurrency" to "12",
                "demo.consumers.order.poll-loop-concurrency" to "3",
                "demo.consumers.order.work-channel-capacity" to "2048",
                "demo.consumers.order.processing-mode" to "at-least-once-ordered-by-key",
                "demo.consumers.telemetry.worker-concurrency" to "8",
                "demo.consumers.telemetry.poll-loop-concurrency" to "2",
                "demo.consumers.telemetry.work-channel-capacity" to "512",
                "demo.consumers.telemetry.processing-mode" to "freshness-first",
                "demo.audit.enabled" to "false"
            )
        )

        assertEquals(false, properties.consumers.processingEnabled)
        assertEquals(DemoApplicationProperties.MetricsImplementation.NOOP, properties.consumers.metricsImplementation)
        assertEquals(6, properties.consumers.workerDispatcherThreads)
        assertEquals(65536, properties.kafka.consumer.fetchMinBytes)
        assertEquals(100, properties.kafka.consumer.fetchMaxWaitMs)
        assertEquals(200, properties.kafka.consumer.maxPollRecords)
        assertEquals(12, properties.consumers.order.workerConcurrency)
        assertEquals(3, properties.consumers.order.pollLoopConcurrency)
        assertEquals(2048, properties.consumers.order.workChannelCapacity)
        assertEquals(ProcessingMode.AT_LEAST_ONCE_ORDERED_BY_KEY, properties.consumers.order.processingMode)
        assertEquals(8, properties.consumers.telemetry.workerConcurrency)
        assertEquals(2, properties.consumers.telemetry.pollLoopConcurrency)
        assertEquals(512, properties.consumers.telemetry.workChannelCapacity)
        assertEquals(ProcessingMode.FRESHNESS_FIRST, properties.consumers.telemetry.processingMode)
        assertEquals(false, properties.audit.enabled)
    }

    private fun bind(values: Map<String, String>): DemoApplicationProperties =
        Binder(MapConfigurationPropertySource(values))
            .bind("demo", DemoApplicationProperties::class.java)
            .orElseGet(::DemoApplicationProperties)
}
