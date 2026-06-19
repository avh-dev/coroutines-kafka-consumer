package avh.ckc.demo

import avh.ckc.demo.config.DemoApplicationProperties
import org.apache.kafka.clients.consumer.ConsumerRecord
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse

class AuditLogTest {
    @Test
    fun `encodes processed record as compact TCP audit payload`() {
        val encoded = encodeConsumerAuditRecord(
            type = "C",
            topic = DemoTopics.CAULDRON_EVENTS,
            partition = 4,
            offset = 77,
            key = "cauldron-1",
            auditTimestampMs = 1234
        )

        assertEquals("C|3|4|77|1234|cauldron-1", encoded)
    }

    @Test
    fun `encodes failed record as compact TCP audit payload`() {
        val encoded = encodeConsumerAuditRecord(
            type = "F",
            topic = DemoTopics.ORDER_EVENTS,
            partition = 1,
            offset = 12,
            key = "order-1",
            auditTimestampMs = 5678
        )

        assertEquals("F|1|1|12|5678|order-1", encoded)
    }

    @Test
    fun `encodes retry attempt as compact TCP audit payload`() {
        val encoded = encodeConsumerAuditRecord(
            type = "R",
            topic = DemoTopics.ORDER_EVENTS,
            partition = 1,
            offset = 12,
            key = "order-1",
            auditTimestampMs = 5678
        )

        assertEquals("R|1|1|12|5678|order-1", encoded)
    }

    @Test
    fun `encodes dropped record as compact TCP audit payload`() {
        val encoded = encodeConsumerAuditRecord(
            type = "D",
            topic = DemoTopics.CAULDRON_EVENTS,
            partition = 2,
            offset = 99,
            key = "cauldron-1",
            auditTimestampMs = 9012
        )

        assertEquals("D|3|2|99|9012|cauldron-1", encoded)
    }

    @Test
    fun `audit helper can be skipped when audit is disabled`() {
        val properties = DemoApplicationProperties(audit = DemoApplicationProperties.Audit(enabled = false))
        val record = ConsumerRecord(DemoTopics.ORDER_EVENTS, 0, 1, "order-1", "value")

        logProcessed(record, properties.audit)

        assertFalse(properties.audit.enabled)
    }
}
