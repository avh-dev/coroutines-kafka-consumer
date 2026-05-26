package avh.ckc.loadtest.runtime

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class ShardContextTest {
    @Test
    fun `reads shard values from environment map`() {
        val context = ShardContext.fromEnvironment(
            mapOf(
                "JOB_COMPLETION_INDEX" to "3",
                "TOTAL_SHARDS" to "12",
                "TEST_RUN_ID" to "demo-1",
                "TEST_RUN_STARTED_AT" to "2026-03-26T10:15:00Z"
            )
        )

        assertEquals(3, context.shardIndex)
        assertEquals(12, context.totalShards)
        assertEquals("demo-1", context.testRunId)
    }

    @Test
    fun `uses local defaults when cloud env vars are absent`() {
        val context = ShardContext.fromEnvironment(emptyMap())

        assertEquals(0, context.shardIndex)
        assertEquals(1, context.totalShards)
        assertNull(context.testRunId)
        assertNull(context.testRunStartedAt)
    }
}
