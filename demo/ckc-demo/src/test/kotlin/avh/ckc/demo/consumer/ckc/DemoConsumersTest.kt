package avh.ckc.demo.consumer.ckc

import avh.ckc.core.metrics.ConsumerMetrics
import avh.ckc.demo.config.DemoApplicationProperties
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import kotlin.test.Test
import kotlin.test.assertEquals

class DemoConsumersTest {
    @Test
    fun `terminal processed callback writes standard audit outside the business handler`() {
        val appender = ListAppender<ILoggingEvent>().also { it.start() }
        val logger = LoggerFactory.getLogger("AUDIT_TCP") as Logger
        logger.addAppender(appender)
        @Suppress("UNCHECKED_CAST")
        val metrics = DemoConsumers.auditMetrics(
            ConsumerMetrics.NOOP as ConsumerMetrics<String, String>,
            DemoApplicationProperties.Audit(enabled = true)
        )
        val record = ConsumerRecord("order.events.v1", 1, 7L, "order-7", "value")

        try {
            metrics.onRecordProcessed("order-7", "value", record, 10, 20)
        } finally {
            logger.detachAppender(appender)
            appender.stop()
        }

        assertEquals(1, appender.list.size)
        val fields = appender.list.single().formattedMessage.split('|')
        assertEquals(listOf("C", "1", "1", "7"), fields.take(4))
        assertEquals("order-7", fields.last())
    }
}
