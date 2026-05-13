package avh.ckc.loadtest.config

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import java.time.Duration

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
    fun `reads independent stream rates from environment`() {
        val config = LoadTestConfig.fromEnvironment(
            mapOf(
                "LIFECYCLE_BASE_RATE" to "25",
                "TELEMETRY_BASE_RATE" to "250",
                "LIFECYCLE_ORDERS_PER_BATCH" to "4",
                "TELEMETRY_INTERVAL_SECONDS" to "7"
            )
        )

        assertEquals(25, config.lifecycleBaseRate)
        assertEquals(250, config.telemetryBaseRate)
        assertEquals(4, config.lifecycleOrdersPerBatch)
        assertEquals(Duration.ofSeconds(7), config.telemetryInterval)
    }
}
