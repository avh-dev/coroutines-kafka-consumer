package avh.ckc.loadtest.kafka

import kotlin.test.Test
import kotlin.test.assertEquals

class LoadTestAuditLogTest {
    @Test
    fun `encodes producer acknowledgement as compact TCP audit payload`() {
        val encoded = encodePublishedAuditRecord(
            topic = "order.events.v1",
            partition = 2,
            offset = 123,
            kafkaTimestampMs = 1_000,
            key = "order-1",
            auditTimestampMs = 1234
        )

        assertEquals("P|1|2|123|1000|1234|order-1", encoded)
    }
}
