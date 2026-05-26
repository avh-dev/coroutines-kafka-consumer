package avh.ckc.loadtest.domain

import avh.ckc.demo.proto.BatchLifecycleEventType
import avh.ckc.demo.proto.OrderLifecycleEventType
import avh.ckc.loadtest.runtime.GeneratorIdentity
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class OrderLifecycleStateMachineTest {
    @Test
    fun `creates deterministic batch ids and lifecycle sequence`() {
        val batch = OrderLifecycleStateMachine(
            GeneratorIdentity(externalShardIndex = 2, totalExternalShards = 8, workerIndex = 5, totalWorkers = 6)
        ).createOrderBatch(orderIndex = 10, batchSlot = 4, ordersPerBatch = 2)

        assertEquals("batch-2-5-000004", batch.batchId)
        assertEquals(listOf("order-2-5-00000010", "order-2-5-00000011"), batch.orderIds)
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
