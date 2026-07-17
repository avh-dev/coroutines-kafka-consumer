package avh.ckc.loadtest.kafka

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.joran.JoranConfigurator
import kotlin.test.Test
import kotlin.test.assertNull

class LoadTestLogbackConfigTest {
    @Test
    fun `audit tcp appender is not configured when audit logging is disabled`() {
        val context = LoggerContext().apply {
            putProperty("AUDIT_LOG_ENABLED", "false")
        }
        try {
            JoranConfigurator().apply {
                this.context = context
                doConfigure(javaClass.classLoader.getResource("logback.xml"))
            }

            val logger = context.getLogger("AUDIT_TCP")

            assertNull(logger.getAppender("AUDIT_TCP"))
        } finally {
            context.stop()
        }
    }
}
