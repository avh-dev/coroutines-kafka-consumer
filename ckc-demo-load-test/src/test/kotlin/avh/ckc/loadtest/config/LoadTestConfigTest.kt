package avh.ckc.loadtest.config

import kotlin.test.Test
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
}
