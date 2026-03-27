package avh.ckc.loadtest.domain

import avh.ckc.demo.proto.OrderLifecycleEventType
import avh.ckc.loadtest.runtime.ShardContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class OrderLifecycleStateMachineTest {
    @Test
    fun `creates deterministic batch ids and lifecycle sequence`() {
        val batch = OrderLifecycleStateMachine(
            ShardContext(
                shardIndex = 2,
                totalShards = 8,
                testRunId = "demo-1",
                testRunStartedAt = null
            )
        ).createOrderBatch(orderIndex = 10, batchSlot = 4, ordersPerBatch = 2)

        assertEquals("batch-shard-002-000004", batch.batchId)
        assertEquals(listOf("ord-shard-002-00000010", "ord-shard-002-00000011"), batch.orderIds)
        assertEquals(10, batch.lifecycleEvents.size)
        assertEquals(OrderLifecycleEventType.ORDER_CREATED, batch.lifecycleEvents.first().eventType)
        assertEquals(OrderLifecycleEventType.BREWING_COMPLETED, batch.lifecycleEvents.last().eventType)
        assertTrue(batch.lifecycleEvents.all { it.batchId == batch.batchId })
    }
}
