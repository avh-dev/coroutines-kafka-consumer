package avh.ckc.demo.service.batch

import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.BatchLifecycleEventType
import avh.ckc.demo.proto.EventMetadata
import avh.ckc.demo.model.BatchState
import avh.ckc.demo.model.ModelContextState
import avh.ckc.demo.model.OrderFlavourState
import avh.ckc.demo.model.OrderState
import avh.ckc.demo.repository.SyncBrewingStateRepository
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

class BatchLifecycleServiceTest {
    @Test
    fun `applies batch event by merging and persisting state`() {
        val repository = FakeBrewingStateRepository(
            batches = mutableMapOf(
                "batch-11" to BatchState(
                    batchId = "batch-11",
                    recipeId = "healing-v1",
                    potionId = "healing-elixir",
                    cauldronId = null,
                    status = "ALLOCATED",
                    orderIds = listOf("ord-2"),
                    updatedAt = "2026-03-26T09:00:00Z"
                )
            )
        )
        val service = SyncBatchLifecycleService(repository)

        service.apply(
            batchEvent(
                eventType = BatchLifecycleEventType.BATCH_BREWING_STARTED,
                batchId = "batch-11",
                cauldronId = "cauldron-3",
                recipeId = "healing-v2"
            )
        )

        val batch = repository.batches.getValue("batch-11")
        assertEquals(listOf("ord-2", "ord-1"), batch.orderIds)
        assertEquals("cauldron-3", batch.cauldronId)
        assertEquals("BREWING", batch.status)
        assertEquals("batch-11", repository.activeBatchIds["cauldron-3"])
    }

    @Test
    fun `does not delete active batch when completed batch is no longer active`() {
        val repository = FakeBrewingStateRepository(
            activeBatchIds = mutableMapOf("cauldron-3" to "batch-22")
        )
        val service = SyncBatchLifecycleService(repository)

        service.apply(
            batchEvent(
                eventType = BatchLifecycleEventType.BATCH_BOTTLING_COMPLETED,
                batchId = "batch-11",
                cauldronId = "cauldron-3"
            )
        )

        assertEquals("batch-22", repository.activeBatchIds["cauldron-3"])
    }

    @Test
    fun `deletes active batch when completed batch matches active one`() {
        val repository = FakeBrewingStateRepository(
            activeBatchIds = mutableMapOf("cauldron-3" to "batch-11")
        )
        val service = SyncBatchLifecycleService(repository)

        service.apply(
            batchEvent(
                eventType = BatchLifecycleEventType.BATCH_BOTTLING_COMPLETED,
                batchId = "batch-11",
                cauldronId = "cauldron-3"
            )
        )

        assertNull(repository.activeBatchIds["cauldron-3"])
    }

    private fun batchEvent(
        eventType: BatchLifecycleEventType,
        batchId: String,
        cauldronId: String,
        recipeId: String = ""
    ): BatchLifecycleEvent =
        BatchLifecycleEvent.newBuilder()
            .setBatchId(batchId)
            .setEventType(eventType)
            .setPotionId("healing-elixir")
            .setRecipeId(recipeId)
            .setCauldronId(cauldronId)
            .addOrderIds("ord-1")
            .setMetadata(
                EventMetadata.newBuilder()
                    .setEventId("evt-1")
                    .setOccurredAt("2026-03-26T09:10:11Z")
                    .setEventVersion(1)
                    .build()
            )
            .build()
}

private class FakeBrewingStateRepository(
    val orders: MutableMap<String, OrderState> = mutableMapOf(),
    val batches: MutableMap<String, BatchState> = mutableMapOf(),
    val activeBatchIds: MutableMap<String, String> = mutableMapOf(),
    val modelContexts: MutableMap<String, ModelContextState> = mutableMapOf(),
    val flavours: MutableMap<String, OrderFlavourState> = mutableMapOf()
) : SyncBrewingStateRepository {
    override fun findOrder(orderId: String): OrderState? = orders[orderId]

    override fun saveOrder(orderState: OrderState) {
        orders[orderState.orderId] = orderState
    }

    override fun findBatch(batchId: String): BatchState? = batches[batchId]

    override fun saveBatch(batchState: BatchState) {
        batches[batchState.batchId] = batchState
    }

    override fun findActiveBatchId(cauldronId: String): String? = activeBatchIds[cauldronId]

    override fun saveActiveBatchId(cauldronId: String, batchId: String) {
        activeBatchIds[cauldronId] = batchId
    }

    override fun deleteActiveBatchId(cauldronId: String) {
        activeBatchIds.remove(cauldronId)
    }

    override fun findModelContext(batchId: String): ModelContextState? = modelContexts[batchId]

    override fun saveModelContext(context: ModelContextState) {
        modelContexts[context.batchId] = context
    }

    override fun findOrderFlavour(orderId: String): OrderFlavourState? = flavours[orderId]

    override fun saveOrderFlavour(state: OrderFlavourState) {
        flavours[state.orderId] = state
    }
}
