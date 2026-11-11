package avh.ckc.demo

import avh.ckc.core.warmup.WarmupForwardedEvent
import avh.ckc.core.warmup.WarmupSessionId
import avh.ckc.demo.config.DemoApplicationProperties
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
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
    fun `encodes dropped record reason as optional audit detail`() {
        val encoded = encodeConsumerAuditRecord(
            type = "D",
            topic = DemoTopics.CAULDRON_EVENTS,
            partition = 2,
            offset = 99,
            key = "cauldron-1",
            auditTimestampMs = 9012,
            detail = AuditDropReasons.STALE_AGE
        )

        assertEquals("D|3|2|99|9012|cauldron-1|stale_age", encoded)
    }

    @Test
    fun `when record is handed off then audit payload uses nonterminal handoff type`() {
        val encoded = encodeConsumerAuditRecord(
            type = "H",
            topic = DemoTopics.CAULDRON_EVENTS,
            partition = 2,
            offset = 99,
            key = "cauldron-1",
            auditTimestampMs = 9012,
            detail = "assignment_ended"
        )

        assertEquals("H|3|2|99|9012|cauldron-1|assignment_ended", encoded)
    }

    @Test
    fun `audit helper can be skipped when audit is disabled`() {
        val properties = DemoApplicationProperties(audit = DemoApplicationProperties.Audit(enabled = false))
        val record = ConsumerRecord(DemoTopics.ORDER_EVENTS, 0, 1, "order-1", "value")

        logProcessed(record, properties.audit)

        assertFalse(properties.audit.enabled)
    }

    @Test
    fun `encodes warmup forwarding coordinates as diagnostic audit payload`() {
        val encoded = encodeWarmupForwardedAuditRecord(
            WarmupForwardedEvent(
                sessionId = WarmupSessionId("session-1"),
                sourceTopicPartition = TopicPartition(DemoTopics.ORDER_EVENTS, 2),
                sourceOffset = 17,
                destinationTopicPartition = TopicPartition(DemoTopics.WARMUP_ORDER_EVENTS, 0),
                destinationOffset = 9
            ),
            auditTimestampMs = 1234
        )

        assertEquals("W|session-1|1|2|17|101|0|9|1234", encoded)
    }

    @Test
    fun `warmup topics have separate compact audit ids`() {
        val encoded = encodeConsumerAuditRecord(
            type = "C",
            topic = DemoTopics.WARMUP_CAULDRON_EVENTS,
            partition = 0,
            offset = 4,
            key = "cauldron-1",
            auditTimestampMs = 1234
        )

        assertEquals("C|103|0|4|1234|cauldron-1", encoded)
    }
}
