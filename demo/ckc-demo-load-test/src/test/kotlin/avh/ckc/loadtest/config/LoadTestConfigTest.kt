package avh.ckc.loadtest.config

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class LoadTestConfigTest {
    @Test
    fun `audit log is enabled by default`() {
        val config = LoadTestConfig.fromEnvironment(emptyMap())

        assertTrue(config.auditLogEnabled)
        assertEquals("127.0.0.1", config.auditHost)
        assertEquals(5170, config.auditPort)
        assertEquals("local", config.auditRunId)
        assertEquals(Runtime.getRuntime().availableProcessors().coerceAtLeast(1), config.generatorWorkers)
    }

    @Test
    fun `audit log can be disabled through environment`() {
        val config = LoadTestConfig.fromEnvironment(mapOf("AUDIT_LOG_ENABLED" to "false"))

        assertFalse(config.auditLogEnabled)
    }

    @Test
    fun `reads traffic mix and domain settings from environment`() {
        val config = LoadTestConfig.fromEnvironment(
            mapOf(
                "BASE_TPS" to "250",
                "ORDER_EVENT_PERCENT" to "45",
                "BATCH_EVENT_PERCENT" to "15",
                "CAULDRON_TELEMETRY_PERCENT" to "40",
                "CAULDRON_COUNT" to "16",
                "MIN_ORDERS_PER_BATCH" to "4",
                "MAX_ORDERS_PER_BATCH" to "9",
                "MIN_BREWING_STEPS" to "5",
                "MAX_BREWING_STEPS" to "8",
                "BREWING_STEP_BURST_EVERY" to "7",
                "MIN_BREWING_STEP_BURST" to "3",
                "MAX_BREWING_STEP_BURST" to "6",
                "MAX_BURST" to "77",
                "STATS_LOG_INTERVAL_SECONDS" to "9",
                "TELEMETRY_SOURCE_MODE" to "FIXED_FLEET",
                "PUBLISH_ENABLED" to "false",
                "AUDIT_TCP_HOST" to "audit-host",
                "AUDIT_TCP_PORT" to "5511",
                "TEST_RUN_ID" to "run-12",
                "LOAD_TEST_WORKERS" to "4",
                "KAFKA_PRODUCER_LINGER_MS" to "75",
                "KAFKA_PRODUCER_BATCH_SIZE" to "131072",
                "KAFKA_PRODUCER_COMPRESSION_TYPE" to "zstd",
                "KAFKA_PRODUCER_BUFFER_MEMORY" to "134217728",
                "ORDER_TPS_PER_PRODUCER" to "700",
                "BATCH_TPS_PER_PRODUCER" to "800",
                "CAULDRON_TELEMETRY_TPS_PER_PRODUCER" to "900",
                "LOAD_TEST_METRICS_PORT" to "19405"
            )
        )

        assertEquals(250, config.baseTps)
        assertEquals(45, config.orderEventPercent)
        assertEquals(15, config.batchEventPercent)
        assertEquals(40, config.cauldronTelemetryPercent)
        assertEquals(16, config.cauldronCount)
        assertEquals(4, config.minOrdersPerBatch)
        assertEquals(9, config.maxOrdersPerBatch)
        assertEquals(5, config.minBrewingSteps)
        assertEquals(8, config.maxBrewingSteps)
        assertEquals(7, config.brewingStepBurstEvery)
        assertEquals(3, config.minBrewingStepBurst)
        assertEquals(6, config.maxBrewingStepBurst)
        assertEquals(77, config.maxBurst)
        assertEquals(9, config.statsLogInterval.seconds)
        assertEquals(TelemetrySourceMode.FIXED_FLEET, config.telemetrySourceMode)
        assertEquals(false, config.publishEnabled)
        assertEquals("audit-host", config.auditHost)
        assertEquals(5511, config.auditPort)
        assertEquals("run-12", config.auditRunId)
        assertEquals(4, config.generatorWorkers)
        assertEquals(75, config.kafkaProducer.lingerMs)
        assertEquals(131072, config.kafkaProducer.batchSize)
        assertEquals("zstd", config.kafkaProducer.compressionType)
        assertEquals(134217728L, config.kafkaProducer.bufferMemory)
        assertEquals(700, config.producerCapacity.orderTps)
        assertEquals(800, config.producerCapacity.batchTps)
        assertEquals(900, config.producerCapacity.cauldronTelemetryTps)
        assertEquals(19405, config.metricsPort)
    }

    @Test
    fun `topic producer settings override shared settings independently`() {
        val config = LoadTestConfig.fromEnvironment(
            mapOf(
                "KAFKA_PRODUCER_LINGER_MS" to "20",
                "KAFKA_PRODUCER_BATCH_SIZE" to "65536",
                "KAFKA_PRODUCER_COMPRESSION_TYPE" to "lz4",
                "KAFKA_PRODUCER_BUFFER_MEMORY" to "33554432",
                "ORDER_KAFKA_PRODUCER_LINGER_MS" to "5",
                "BATCH_KAFKA_PRODUCER_BATCH_SIZE" to "131072",
                "TELEMETRY_KAFKA_PRODUCER_COMPRESSION_TYPE" to "zstd",
                "TELEMETRY_KAFKA_PRODUCER_BUFFER_MEMORY" to "67108864"
            )
        )

        assertEquals(5, config.topicKafkaProducers.order.lingerMs)
        assertEquals(65536, config.topicKafkaProducers.order.batchSize)
        assertEquals(20, config.topicKafkaProducers.batch.lingerMs)
        assertEquals(131072, config.topicKafkaProducers.batch.batchSize)
        assertEquals("zstd", config.topicKafkaProducers.telemetry.compressionType)
        assertEquals(67108864L, config.topicKafkaProducers.telemetry.bufferMemory)
    }

    @Test
    fun `reads ordered producer configuration steps`() {
        val config = LoadTestConfig.fromEnvironment(
            mapOf(
                "PRODUCER_CONFIG_STEPS_JSON" to
                    """[{"atSeconds":60,"topic":"telemetry","lingerMs":50},{"atSeconds":120,"topic":"all","batchSize":131072}]"""
            )
        )

        assertEquals(2, config.producerConfigSteps.size)
        assertEquals(60, config.producerConfigSteps[0].atSeconds)
        assertEquals(ProducerTopic.TELEMETRY, config.producerConfigSteps[0].topic)
        assertEquals(50, config.producerConfigSteps[0].lingerMs)
        assertEquals(ProducerTopic.ALL, config.producerConfigSteps[1].topic)
        assertEquals(131072, config.producerConfigSteps[1].batchSize)
    }
}
