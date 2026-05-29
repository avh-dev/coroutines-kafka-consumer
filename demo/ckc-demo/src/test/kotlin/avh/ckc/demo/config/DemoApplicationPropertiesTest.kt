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
        assertEquals(1, properties.kafka.consumer.fetchMinBytes)
        assertEquals(500, properties.kafka.consumer.fetchMaxWaitMs)
        assertEquals(500, properties.kafka.consumer.maxPollRecords)
        assertEquals(DemoApplicationProperties.ModelClient.KTOR_CIO, properties.model.client)
        assertEquals(
            DemoApplicationProperties.DeserializationDispatcherMode.DEFAULT,
            properties.consumers.deserializationDispatcher.mode
        )
        assertEquals(8, properties.consumers.deserializationDispatcher.customThreadPoolSize)
        assertEquals("ckc-demo-deserializer", properties.consumers.deserializationDispatcher.customThreadNamePrefix)
        assertEquals(2, properties.consumers.order.workerConcurrency)
        assertEquals(1, properties.consumers.order.pollLoopConcurrency)
        assertEquals(1024, properties.consumers.order.workChannelCapacity)
        assertEquals(ProcessingMode.AT_LEAST_ONCE_UNORDERED, properties.consumers.order.processingMode)
        assertEquals(4, properties.consumers.telemetry.workerConcurrency)
        assertEquals(1, properties.consumers.telemetry.pollLoopConcurrency)
        assertEquals(256, properties.consumers.telemetry.workChannelCapacity)
        assertEquals(ProcessingMode.FRESHNESS_FIRST, properties.consumers.telemetry.processingMode)
        assertEquals(true, properties.audit.enabled)
        assertEquals(".demo-audit", properties.audit.directory)
        assertEquals("", properties.audit.filePrefix)
        assertEquals(65_536, properties.audit.queueCapacity)
        assertEquals(1_024, properties.audit.flushRecords)
        assertEquals(50, properties.audit.flushIntervalMs)
        assertEquals(1_000, properties.audit.fsyncIntervalMs)
        assertEquals(256L * 1024L * 1024L, properties.audit.maxSegmentBytes)
    }

    @Test
    fun `consumer runtime settings can be overridden`() {
        val properties = bind(
            mapOf(
                "demo.consumers.processing-enabled" to "false",
                "demo.kafka.consumer.fetch-min-bytes" to "65536",
                "demo.kafka.consumer.fetch-max-wait-ms" to "100",
                "demo.kafka.consumer.max-poll-records" to "200",
                "demo.model.client" to "armeria",
                "demo.consumers.deserialization-dispatcher.mode" to "custom-thread-pool",
                "demo.consumers.deserialization-dispatcher.custom-thread-pool-size" to "16",
                "demo.consumers.deserialization-dispatcher.custom-thread-name-prefix" to "experiment-deserializer",
                "demo.consumers.order.worker-concurrency" to "12",
                "demo.consumers.order.poll-loop-concurrency" to "3",
                "demo.consumers.order.work-channel-capacity" to "2048",
                "demo.consumers.order.processing-mode" to "at-least-once-ordered-by-key",
                "demo.consumers.telemetry.worker-concurrency" to "8",
                "demo.consumers.telemetry.poll-loop-concurrency" to "2",
                "demo.consumers.telemetry.work-channel-capacity" to "512",
                "demo.consumers.telemetry.processing-mode" to "freshness-first",
                "demo.audit.enabled" to "false",
                "demo.audit.directory" to "/audit",
                "demo.audit.file-prefix" to "processed-test",
                "demo.audit.queue-capacity" to "17",
                "demo.audit.flush-records" to "5",
                "demo.audit.flush-interval-ms" to "25",
                "demo.audit.fsync-interval-ms" to "250",
                "demo.audit.max-segment-bytes" to "4096"
            )
        )

        assertEquals(false, properties.consumers.processingEnabled)
        assertEquals(65536, properties.kafka.consumer.fetchMinBytes)
        assertEquals(100, properties.kafka.consumer.fetchMaxWaitMs)
        assertEquals(200, properties.kafka.consumer.maxPollRecords)
        assertEquals(DemoApplicationProperties.ModelClient.ARMERIA, properties.model.client)
        assertEquals(
            DemoApplicationProperties.DeserializationDispatcherMode.CUSTOM_THREAD_POOL,
            properties.consumers.deserializationDispatcher.mode
        )
        assertEquals(16, properties.consumers.deserializationDispatcher.customThreadPoolSize)
        assertEquals("experiment-deserializer", properties.consumers.deserializationDispatcher.customThreadNamePrefix)
        assertEquals(12, properties.consumers.order.workerConcurrency)
        assertEquals(3, properties.consumers.order.pollLoopConcurrency)
        assertEquals(2048, properties.consumers.order.workChannelCapacity)
        assertEquals(ProcessingMode.AT_LEAST_ONCE_ORDERED_BY_KEY, properties.consumers.order.processingMode)
        assertEquals(8, properties.consumers.telemetry.workerConcurrency)
        assertEquals(2, properties.consumers.telemetry.pollLoopConcurrency)
        assertEquals(512, properties.consumers.telemetry.workChannelCapacity)
        assertEquals(ProcessingMode.FRESHNESS_FIRST, properties.consumers.telemetry.processingMode)
        assertEquals(false, properties.audit.enabled)
        assertEquals("/audit", properties.audit.directory)
        assertEquals("processed-test", properties.audit.filePrefix)
        assertEquals(17, properties.audit.queueCapacity)
        assertEquals(5, properties.audit.flushRecords)
        assertEquals(25, properties.audit.flushIntervalMs)
        assertEquals(250, properties.audit.fsyncIntervalMs)
        assertEquals(4096, properties.audit.maxSegmentBytes)
    }

    private fun bind(values: Map<String, String>): DemoApplicationProperties =
        Binder(MapConfigurationPropertySource(values))
            .bind("demo", DemoApplicationProperties::class.java)
            .orElseGet(::DemoApplicationProperties)
}
