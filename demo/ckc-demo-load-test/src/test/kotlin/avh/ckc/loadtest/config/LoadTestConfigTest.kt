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
                "MAX_BURST" to "77",
                "STATS_LOG_INTERVAL_SECONDS" to "9",
                "TELEMETRY_SOURCE_MODE" to "FIXED_FLEET",
                "PUBLISH_ENABLED" to "false",
                "AUDIT_TCP_HOST" to "audit-host",
                "AUDIT_TCP_PORT" to "5511",
                "TEST_RUN_ID" to "run-12",
                "LOAD_TEST_WORKERS" to "4"
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
        assertEquals(77, config.maxBurst)
        assertEquals(9, config.statsLogInterval.seconds)
        assertEquals(TelemetrySourceMode.FIXED_FLEET, config.telemetrySourceMode)
        assertEquals(false, config.publishEnabled)
        assertEquals("audit-host", config.auditHost)
        assertEquals(5511, config.auditPort)
        assertEquals("run-12", config.auditRunId)
        assertEquals(4, config.generatorWorkers)
    }
}
