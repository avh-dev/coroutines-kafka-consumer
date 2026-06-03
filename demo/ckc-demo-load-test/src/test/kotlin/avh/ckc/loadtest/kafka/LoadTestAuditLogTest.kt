package avh.ckc.loadtest.kafka

import avh.ckc.demo.audit.encodeAuditRecord
import avh.ckc.loadtest.runtime.ShardContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class LoadTestAuditLogTest {
    @Test
    fun `encodes producer acknowledgement as compact TCP audit payload`() {
        val encoded = encodeAuditRecord(
            type = "P",
            runId = "run-2",
            writerId = "loadtest-shard-0",
            topic = "order.events.v1",
            partition = 2,
            offset = 123,
            kafkaTimestampMs = 1_000,
            messageKey = "order\t1"
        )

        assertTrue(encoded.matches(Regex("""P\trun-2\tloadtest-shard-0\t1\t2\t123\t1000\t\d+\torder 1""")))
    }

    @Test
    fun `writer id is derived from shard context`() {
        assertEquals(
            "loadtest-shard-7",
            writerId(ShardContext(shardIndex = 7, totalShards = 16, testRunId = "run-2", testRunStartedAt = null))
        )
    }
}
