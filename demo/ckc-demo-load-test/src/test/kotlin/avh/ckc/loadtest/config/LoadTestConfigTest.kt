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
                "FAKE_ENTITY_PREFIX" to "fake-test",
                "STATS_LOG_INTERVAL_SECONDS" to "9",
                "PUBLISH_ENABLED" to "false"
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
        assertEquals("fake-test", config.fakeEntityPrefix)
        assertEquals(9, config.statsLogInterval.seconds)
        assertEquals(false, config.publishEnabled)
    }
}
