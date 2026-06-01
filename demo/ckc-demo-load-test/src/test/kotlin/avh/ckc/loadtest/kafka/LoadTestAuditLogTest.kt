package avh.ckc.loadtest.kafka

import kotlin.test.Test
import kotlin.test.assertTrue

class LoadTestAuditLogTest {
    @Test
    fun `encodes producer acknowledgement as compact Redis audit payload`() {
        val encoded = encodeAuditRecord("P", "order.events.v1", 2, 123, 1_000, "order\t1")

        assertTrue(encoded.matches(Regex("""P\t1\t2\t123\t1000\t\d+\torder 1""")))
    }
}
