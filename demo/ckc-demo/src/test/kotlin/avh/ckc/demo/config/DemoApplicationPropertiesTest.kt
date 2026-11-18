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
        assertEquals(false, properties.consumers.kafkaClientMetricsEnabled)
        assertEquals(false, properties.consumers.springKafkaListenerMetricsEnabled)
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
        assertEquals(300_000, properties.kafka.consumer.maxPollIntervalMs)
        assertEquals(50 * 1024 * 1024, properties.kafka.consumer.fetchMaxBytes)
        assertEquals(1024 * 1024, properties.kafka.consumer.maxPartitionFetchBytes)
        assertEquals("", properties.experimentTargetName)
        assertEquals("ckc-demo", properties.kafka.groupId)
        assertEquals(true, properties.consumers.order.enabled)
        assertEquals("", properties.consumers.order.groupId)
        assertEquals("ckc-demo", properties.consumers.order.groupIdOr(properties.kafka.groupId))
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
        assertEquals(false, properties.trafficWarmup.enabled)
        assertEquals("demo", properties.trafficWarmup.instanceId)
        assertEquals("global5-shared-partition-v6-local", properties.trafficWarmup.namespace)
        assertEquals("ckc.traffic-warmup.v6.control", properties.trafficWarmup.controlTopic)
        assertEquals("localhost:9092", properties.trafficWarmup.controlKafka.bootstrapServers)
        assertEquals("localhost:9092", properties.trafficWarmup.destinationKafka.bootstrapServers)
        assertEquals("10", properties.trafficWarmup.mappings.orderEvents.quotaProfile)
        assertEquals(1_000, properties.trafficWarmup.mappings.cauldronEvents.quotaIntervalMs)
        assertEquals(90, properties.trafficWarmup.durationSeconds)
        assertEquals(4, properties.trafficWarmup.maxDonorSessions)
        assertEquals(5, properties.trafficWarmup.producerLingerMs)
        assertEquals(16_384, properties.trafficWarmup.producerBatchSize)
        assertEquals("http://127.0.0.1:18080", properties.model.baseUrl)
        assertEquals("", properties.model.etaBaseUrl)
        assertEquals("", properties.model.flavourBaseUrl)
        assertEquals(DemoApplicationProperties.ModelHttpClient.ARMERIA, properties.model.httpClient)
        assertEquals(DemoApplicationProperties.ModelHttpClient.ARMERIA, properties.model.syncHttpClient)
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
                "demo.consumers.kafka-client-metrics-enabled" to "true",
                "demo.consumers.spring-kafka-listener-metrics-enabled" to "true",
                "demo.consumers.processing-dispatcher-type" to "virtual",
                "demo.consumers.worker-dispatcher-threads" to "6",
                "demo.consumers.virtual-thread-name-prefix" to "demo-test-virtual-",
                "demo.consumers.freshness-first-max-record-age-seconds" to "15",
                "demo.consumers.retry.max-attempts" to "5",
                "demo.consumers.retry.backoff-ms" to "750",
                "demo.kafka.consumer.fetch-min-bytes" to "65536",
                "demo.kafka.consumer.fetch-max-wait-ms" to "100",
                "demo.kafka.consumer.max-poll-records" to "200",
                "demo.kafka.consumer.max-poll-interval-ms" to "1800000",
                "demo.kafka.consumer.fetch-max-bytes" to "16777216",
                "demo.kafka.consumer.max-partition-fetch-bytes" to "2097152",
                "demo.experiment-target-name" to "spring.many-consumers.linger50",
                "demo.kafka.group-id" to "demo-test-group",
                "demo.consumers.order.enabled" to "false",
                "demo.consumers.order.group-id" to "demo-order-group",
                "demo.consumers.order.worker-concurrency" to "12",
                "demo.consumers.order.poll-loop-concurrency" to "3",
                "demo.consumers.order.work-channel-capacity" to "2048",
                "demo.consumers.order.processing-mode" to "at-least-once-key-ordering",
                "demo.consumers.order.kafka.fetch-min-bytes" to "4096",
                "demo.consumers.order.kafka.max-poll-records" to "50",
                "demo.consumers.telemetry.worker-concurrency" to "8",
                "demo.consumers.telemetry.poll-loop-concurrency" to "2",
                "demo.consumers.telemetry.work-channel-capacity" to "512",
                "demo.consumers.telemetry.processing-mode" to "freshness-first-drop-oldest",
                "demo.audit.enabled" to "false",
                "demo.audit.host" to "audit-host",
                "demo.audit.port" to "5511",
                "demo.audit.run-id" to "run-9",
                "demo.audit.writer-id" to "demo-pod-1",
                "demo.traffic-warmup.enabled" to "true",
                "demo.traffic-warmup.instance-id" to "pod-7",
                "demo.traffic-warmup.namespace" to "global5-v6-test",
                "demo.traffic-warmup.control-topic" to "warmup.control.test",
                "demo.traffic-warmup.control-kafka.bootstrap-servers" to "control-kafka:9092",
                "demo.traffic-warmup.control-kafka.security-protocol" to "SSL",
                "demo.traffic-warmup.destination-kafka.bootstrap-servers" to "destination-kafka:9092",
                "demo.traffic-warmup.destination-kafka.security-protocol" to "SASL_SSL",
                "demo.traffic-warmup.mappings.order-events.quota-profile" to "2 -> (45s, order ramp) -> 12",
                "demo.traffic-warmup.mappings.order-events.quota-interval-ms" to "2000",
                "demo.traffic-warmup.mappings.batch-events.quota-profile" to "3 -> (46s) -> 23",
                "demo.traffic-warmup.mappings.batch-events.quota-interval-ms" to "3000",
                "demo.traffic-warmup.mappings.cauldron-events.quota-profile" to "4 -> (47s) -> 34",
                "demo.traffic-warmup.mappings.cauldron-events.quota-interval-ms" to "4000",
                "demo.traffic-warmup.duration-seconds" to "80",
                "demo.traffic-warmup.offer-window-ms" to "1500",
                "demo.traffic-warmup.participant-start-timeout-seconds" to "11",
                "demo.traffic-warmup.preparation-timeout-seconds" to "12",
                "demo.traffic-warmup.completion-timeout-seconds" to "13",
                "demo.traffic-warmup.delegation-timeout-seconds" to "14",
                "demo.traffic-warmup.max-donor-sessions" to "3",
                "demo.traffic-warmup.max-aggregate-quota-per-second" to "250",
                "demo.traffic-warmup.producer-linger-ms" to "9",
                "demo.traffic-warmup.producer-batch-size" to "32768",
                "demo.model.base-url" to "http://models.example",
                "demo.model.eta-base-url" to "http://eta.example",
                "demo.model.flavour-base-url" to "http://flavour.example",
                "demo.model.http-client" to "jdk",
                "demo.model.sync-http-client" to "jdk",
                "demo.model.jdk-http-client-executor" to "virtual",
                "demo.model.jdk-http-client-virtual-thread-name-prefix" to "http-vt-test-",
                "demo.registry.base-url" to "http://registry.example"
            )
        )

        assertEquals(false, properties.consumers.processingEnabled)
        assertEquals(DemoApplicationProperties.MetricsImplementation.NOOP, properties.consumers.metricsImplementation)
        assertEquals(true, properties.consumers.kafkaClientMetricsEnabled)
        assertEquals(true, properties.consumers.springKafkaListenerMetricsEnabled)
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
        assertEquals(1_800_000, properties.kafka.consumer.maxPollIntervalMs)
        assertEquals(16777216, properties.kafka.consumer.fetchMaxBytes)
        assertEquals(2097152, properties.kafka.consumer.maxPartitionFetchBytes)
        assertEquals("spring.many-consumers.linger50", properties.experimentTargetName)
        assertEquals("demo-test-group", properties.kafka.groupId)
        assertEquals(false, properties.consumers.order.enabled)
        assertEquals("demo-order-group", properties.consumers.order.groupId)
        assertEquals("demo-order-group", properties.consumers.order.groupIdOr(properties.kafka.groupId))
        assertEquals(12, properties.consumers.order.workerConcurrency)
        assertEquals(3, properties.consumers.order.pollLoopConcurrency)
        assertEquals(2048, properties.consumers.order.workChannelCapacity)
        assertEquals(ProcessingMode.AT_LEAST_ONCE_KEY_ORDERING, properties.consumers.order.processingMode)
        assertEquals(4096, properties.consumers.order.kafka.fetchMinBytes)
        assertEquals(50, properties.consumers.order.kafka.maxPollRecords)
        assertEquals(8, properties.consumers.telemetry.workerConcurrency)
        assertEquals(2, properties.consumers.telemetry.pollLoopConcurrency)
        assertEquals(512, properties.consumers.telemetry.workChannelCapacity)
        assertEquals(ProcessingMode.FRESHNESS_FIRST_DROP_OLDEST, properties.consumers.telemetry.processingMode)
        assertEquals(false, properties.audit.enabled)
        assertEquals("audit-host", properties.audit.host)
        assertEquals(5511, properties.audit.port)
        assertEquals("run-9", properties.audit.runId)
        assertEquals("demo-pod-1", properties.audit.writerId)
        assertEquals(true, properties.trafficWarmup.enabled)
        assertEquals("pod-7", properties.trafficWarmup.instanceId)
        assertEquals("global5-v6-test", properties.trafficWarmup.namespace)
        assertEquals("warmup.control.test", properties.trafficWarmup.controlTopic)
        assertEquals("control-kafka:9092", properties.trafficWarmup.controlKafka.bootstrapServers)
        assertEquals("SSL", properties.trafficWarmup.controlKafka.securityProtocol)
        assertEquals("destination-kafka:9092", properties.trafficWarmup.destinationKafka.bootstrapServers)
        assertEquals("SASL_SSL", properties.trafficWarmup.destinationKafka.securityProtocol)
        with(properties.trafficWarmup.mappings.orderEvents) {
            assertEquals("2 -> (45s, order ramp) -> 12", quotaProfile)
            assertEquals(2_000, quotaIntervalMs)
        }
        with(properties.trafficWarmup.mappings.batchEvents) {
            assertEquals("3 -> (46s) -> 23", quotaProfile)
            assertEquals(3_000, quotaIntervalMs)
        }
        with(properties.trafficWarmup.mappings.cauldronEvents) {
            assertEquals("4 -> (47s) -> 34", quotaProfile)
            assertEquals(4_000, quotaIntervalMs)
        }
        assertEquals(80, properties.trafficWarmup.durationSeconds)
        assertEquals(1_500, properties.trafficWarmup.offerWindowMs)
        assertEquals(11, properties.trafficWarmup.participantStartTimeoutSeconds)
        assertEquals(12, properties.trafficWarmup.preparationTimeoutSeconds)
        assertEquals(13, properties.trafficWarmup.completionTimeoutSeconds)
        assertEquals(14, properties.trafficWarmup.delegationTimeoutSeconds)
        assertEquals(3, properties.trafficWarmup.maxDonorSessions)
        assertEquals(250, properties.trafficWarmup.maxAggregateQuotaPerSecond)
        assertEquals(9, properties.trafficWarmup.producerLingerMs)
        assertEquals(32_768, properties.trafficWarmup.producerBatchSize)
        assertEquals("http://models.example", properties.model.baseUrl)
        assertEquals("http://eta.example", properties.model.etaBaseUrl)
        assertEquals("http://flavour.example", properties.model.flavourBaseUrl)
        assertEquals(DemoApplicationProperties.ModelHttpClient.JDK, properties.model.httpClient)
        assertEquals(DemoApplicationProperties.ModelHttpClient.JDK, properties.model.syncHttpClient)
        assertEquals(DemoApplicationProperties.JdkHttpClientExecutor.VIRTUAL, properties.model.jdkHttpClientExecutor)
        assertEquals("http-vt-test-", properties.model.jdkHttpClientVirtualThreadNamePrefix)
        assertEquals("http://registry.example", properties.registry.baseUrl)
    }

    private fun bind(values: Map<String, String>): DemoApplicationProperties =
        Binder(MapConfigurationPropertySource(values))
            .bind("demo", DemoApplicationProperties::class.java)
            .orElseGet(::DemoApplicationProperties)
}
