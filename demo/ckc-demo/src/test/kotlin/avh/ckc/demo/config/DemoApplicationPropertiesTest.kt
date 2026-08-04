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
        assertEquals(DemoApplicationProperties.ProcessingDispatcherType.AUTO, properties.consumers.processingDispatcherType)
        assertEquals(8, properties.consumers.workerDispatcherThreads)
        assertEquals("demo-processing-virtual-", properties.consumers.virtualThreadNamePrefix)
        assertEquals(10, properties.consumers.freshnessFirstMaxRecordAgeSeconds)
        assertEquals(3, properties.consumers.retry.maxAttempts)
        assertEquals(250, properties.consumers.retry.backoffMs)
        assertEquals(2, properties.consumers.retry.maxRetries)
        assertEquals(1, properties.kafka.consumer.fetchMinBytes)
        assertEquals(500, properties.kafka.consumer.fetchMaxWaitMs)
        assertEquals(500, properties.kafka.consumer.maxPollRecords)
        assertEquals("ckc-demo", properties.kafka.groupId)
        assertEquals(2, properties.consumers.order.workerConcurrency)
        assertEquals(1, properties.consumers.order.pollLoopConcurrency)
        assertEquals(1024, properties.consumers.order.workChannelCapacity)
        assertEquals(ProcessingMode.AT_LEAST_ONCE_NO_ORDERING, properties.consumers.order.processingMode)
        assertEquals(4, properties.consumers.telemetry.workerConcurrency)
        assertEquals(1, properties.consumers.telemetry.pollLoopConcurrency)
        assertEquals(256, properties.consumers.telemetry.workChannelCapacity)
        assertEquals(ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST, properties.consumers.telemetry.processingMode)
        assertEquals(true, properties.audit.enabled)
        assertEquals("127.0.0.1", properties.audit.host)
        assertEquals(5170, properties.audit.port)
        assertEquals("local", properties.audit.runId)
        assertEquals("demo", properties.audit.writerId)
        assertEquals("http://127.0.0.1:18080", properties.model.baseUrl)
        assertEquals("", properties.model.etaBaseUrl)
        assertEquals("", properties.model.flavourBaseUrl)
        assertEquals(DemoApplicationProperties.ModelHttpClient.ARMERIA, properties.model.httpClient)
        assertEquals(DemoApplicationProperties.ModelHttpClient.JDK, properties.model.syncHttpClient)
        assertEquals(DemoApplicationProperties.JdkHttpClientExecutor.DEFAULT, properties.model.jdkHttpClientExecutor)
        assertEquals("HttpClient-virtual-", properties.model.jdkHttpClientVirtualThreadNamePrefix)
        assertEquals("http://127.0.0.1:18080", properties.registry.baseUrl)
    }

    @Test
    fun `consumer runtime settings can be overridden`() {
        val properties = bind(
            mapOf(
                "demo.consumers.processing-enabled" to "false",
                "demo.consumers.metrics-implementation" to "noop",
                "demo.consumers.processing-dispatcher-type" to "virtual",
                "demo.consumers.worker-dispatcher-threads" to "6",
                "demo.consumers.virtual-thread-name-prefix" to "demo-test-virtual-",
                "demo.consumers.freshness-first-max-record-age-seconds" to "15",
                "demo.consumers.retry.max-attempts" to "5",
                "demo.consumers.retry.backoff-ms" to "750",
                "demo.kafka.consumer.fetch-min-bytes" to "65536",
                "demo.kafka.consumer.fetch-max-wait-ms" to "100",
                "demo.kafka.consumer.max-poll-records" to "200",
                "demo.kafka.group-id" to "demo-test-group",
                "demo.consumers.order.worker-concurrency" to "12",
                "demo.consumers.order.poll-loop-concurrency" to "3",
                "demo.consumers.order.work-channel-capacity" to "2048",
                "demo.consumers.order.processing-mode" to "at-least-once-key-ordering",
                "demo.consumers.telemetry.worker-concurrency" to "8",
                "demo.consumers.telemetry.poll-loop-concurrency" to "2",
                "demo.consumers.telemetry.work-channel-capacity" to "512",
                "demo.consumers.telemetry.processing-mode" to "freshness-first-drop-oldest",
                "demo.audit.enabled" to "false",
                "demo.audit.host" to "audit-host",
                "demo.audit.port" to "5511",
                "demo.audit.run-id" to "run-9",
                "demo.audit.writer-id" to "demo-pod-1",
                "demo.model.base-url" to "http://models.example",
                "demo.model.eta-base-url" to "http://eta.example",
                "demo.model.flavour-base-url" to "http://flavour.example",
                "demo.model.http-client" to "jdk",
                "demo.model.sync-http-client" to "armeria",
                "demo.model.jdk-http-client-executor" to "virtual",
                "demo.model.jdk-http-client-virtual-thread-name-prefix" to "http-vt-test-",
                "demo.registry.base-url" to "http://registry.example"
            )
        )

        assertEquals(false, properties.consumers.processingEnabled)
        assertEquals(DemoApplicationProperties.MetricsImplementation.NOOP, properties.consumers.metricsImplementation)
        assertEquals(DemoApplicationProperties.ProcessingDispatcherType.VIRTUAL, properties.consumers.processingDispatcherType)
        assertEquals(6, properties.consumers.workerDispatcherThreads)
        assertEquals("demo-test-virtual-", properties.consumers.virtualThreadNamePrefix)
        assertEquals(15, properties.consumers.freshnessFirstMaxRecordAgeSeconds)
        assertEquals(5, properties.consumers.retry.maxAttempts)
        assertEquals(750, properties.consumers.retry.backoffMs)
        assertEquals(4, properties.consumers.retry.maxRetries)
        assertEquals(65536, properties.kafka.consumer.fetchMinBytes)
        assertEquals(100, properties.kafka.consumer.fetchMaxWaitMs)
        assertEquals(200, properties.kafka.consumer.maxPollRecords)
        assertEquals("demo-test-group", properties.kafka.groupId)
        assertEquals(12, properties.consumers.order.workerConcurrency)
        assertEquals(3, properties.consumers.order.pollLoopConcurrency)
        assertEquals(2048, properties.consumers.order.workChannelCapacity)
        assertEquals(ProcessingMode.AT_LEAST_ONCE_KEY_ORDERING, properties.consumers.order.processingMode)
        assertEquals(8, properties.consumers.telemetry.workerConcurrency)
        assertEquals(2, properties.consumers.telemetry.pollLoopConcurrency)
        assertEquals(512, properties.consumers.telemetry.workChannelCapacity)
        assertEquals(ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST, properties.consumers.telemetry.processingMode)
        assertEquals(false, properties.audit.enabled)
        assertEquals("audit-host", properties.audit.host)
        assertEquals(5511, properties.audit.port)
        assertEquals("run-9", properties.audit.runId)
        assertEquals("demo-pod-1", properties.audit.writerId)
        assertEquals("http://models.example", properties.model.baseUrl)
        assertEquals("http://eta.example", properties.model.etaBaseUrl)
        assertEquals("http://flavour.example", properties.model.flavourBaseUrl)
        assertEquals(DemoApplicationProperties.ModelHttpClient.JDK, properties.model.httpClient)
        assertEquals(DemoApplicationProperties.ModelHttpClient.ARMERIA, properties.model.syncHttpClient)
        assertEquals(DemoApplicationProperties.JdkHttpClientExecutor.VIRTUAL, properties.model.jdkHttpClientExecutor)
        assertEquals("http-vt-test-", properties.model.jdkHttpClientVirtualThreadNamePrefix)
        assertEquals("http://registry.example", properties.registry.baseUrl)
    }

    private fun bind(values: Map<String, String>): DemoApplicationProperties =
        Binder(MapConfigurationPropertySource(values))
            .bind("demo", DemoApplicationProperties::class.java)
            .orElseGet(::DemoApplicationProperties)
}
