package avh.ckc.loadtest.domain

import avh.ckc.demo.proto.BatchLifecycleEventType
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
        assertEquals(8, batch.orderEvents.size)
        assertEquals(12, batch.batchEvents.size)
        assertEquals(OrderLifecycleEventType.ORDER_CREATED, batch.orderEvents.first().eventType)
        assertEquals(OrderLifecycleEventType.ORDER_COMPLETED, batch.orderEvents.last().eventType)
        assertEquals(BatchLifecycleEventType.BATCH_CREATED, batch.batchEvents.first().eventType)
        assertEquals(BatchLifecycleEventType.BATCH_BOTTLING_COMPLETED, batch.batchEvents.last().eventType)
        assertTrue(batch.orderEvents.filter { it.batchId.isNotBlank() }.all { it.batchId == batch.batchId })
        assertTrue(batch.batchEvents.all { it.batchId == batch.batchId })
    }
}
