package avh.ckc.demo.service

import avh.ckc.demo.proto.EventMetadata
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.proto.OrderLifecycleEventType
import avh.ckc.demo.repository.BatchState
import avh.ckc.demo.repository.BrewingStateRepository
import avh.ckc.demo.repository.ModelContextState
import avh.ckc.demo.repository.OrderState
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

class BrewingLifecycleServiceTest {
    @Test
    fun `applies lifecycle event by merging and persisting state`() {
        val repository = FakeBrewingStateRepository(
            orders = mutableMapOf(
                "ord-1" to OrderState(
                    orderId = "ord-1",
                    batchId = null,
                    potionId = "healing-elixir",
                    recipeId = null,
                    customerId = "guild-17",
                    cauldronId = null,
                    status = "CREATED",
                    updatedAt = "2026-03-26T09:00:00Z"
                )
            ),
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
        val service = BrewingLifecycleService(repository)

        service.applyLifecycleEvent(
            lifecycleEvent(
                eventType = OrderLifecycleEventType.BREWING_STARTED,
                batchId = "batch-11",
                cauldronId = "cauldron-3",
                recipeId = "healing-v2"
            )
        ).toCompletableFuture().join()

        val order = repository.orders.getValue("ord-1")
        assertEquals("batch-11", order.batchId)
        assertEquals("healing-v2", order.recipeId)
        assertEquals("BREWING_STARTED", order.status)

        val batch = repository.batches.getValue("batch-11")
        assertEquals(listOf("ord-2", "ord-1"), batch.orderIds)
        assertEquals("cauldron-3", batch.cauldronId)
        assertEquals("BREWING_STARTED", batch.status)
        assertEquals("batch-11", repository.activeBatchIds["cauldron-3"])
    }

    @Test
    fun `does not delete active batch when completed batch is no longer active`() {
        val repository = FakeBrewingStateRepository(
            activeBatchIds = mutableMapOf("cauldron-3" to "batch-22")
        )
        val service = BrewingLifecycleService(repository)

        service.applyLifecycleEvent(
            lifecycleEvent(
                eventType = OrderLifecycleEventType.BREWING_COMPLETED,
                batchId = "batch-11",
                cauldronId = "cauldron-3"
            )
        ).toCompletableFuture().join()

        assertEquals("batch-22", repository.activeBatchIds["cauldron-3"])
    }

    @Test
    fun `deletes active batch when completed batch matches active one`() {
        val repository = FakeBrewingStateRepository(
            activeBatchIds = mutableMapOf("cauldron-3" to "batch-11")
        )
        val service = BrewingLifecycleService(repository)

        service.applyLifecycleEvent(
            lifecycleEvent(
                eventType = OrderLifecycleEventType.BREWING_COMPLETED,
                batchId = "batch-11",
                cauldronId = "cauldron-3"
            )
        ).toCompletableFuture().join()

        assertNull(repository.activeBatchIds["cauldron-3"])
    }

    private fun lifecycleEvent(
        eventType: OrderLifecycleEventType,
        batchId: String,
        cauldronId: String,
        recipeId: String = ""
    ): OrderLifecycleEvent =
        OrderLifecycleEvent.newBuilder()
            .setOrderId("ord-1")
            .setEventType(eventType)
            .setPotionId("healing-elixir")
            .setBatchId(batchId)
            .setRecipeId(recipeId)
            .setCustomerId("guild-17")
            .setCauldronId(cauldronId)
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
    val modelContexts: MutableMap<String, ModelContextState> = mutableMapOf()
) : BrewingStateRepository {
    override fun findOrder(orderId: String): CompletionStage<OrderState?> =
        CompletableFuture.completedFuture(orders[orderId])

    override fun saveOrder(orderState: OrderState): CompletionStage<Void> {
        orders[orderState.orderId] = orderState
        return CompletableFuture.completedFuture(null)
    }

    override fun findBatch(batchId: String): CompletionStage<BatchState?> =
        CompletableFuture.completedFuture(batches[batchId])

    override fun saveBatch(batchState: BatchState): CompletionStage<Void> {
        batches[batchState.batchId] = batchState
        return CompletableFuture.completedFuture(null)
    }

    override fun findActiveBatchId(cauldronId: String): CompletionStage<String?> =
        CompletableFuture.completedFuture(activeBatchIds[cauldronId])

    override fun saveActiveBatchId(cauldronId: String, batchId: String): CompletionStage<Void> {
        activeBatchIds[cauldronId] = batchId
        return CompletableFuture.completedFuture(null)
    }

    override fun deleteActiveBatchId(cauldronId: String): CompletionStage<Void> {
        activeBatchIds.remove(cauldronId)
        return CompletableFuture.completedFuture(null)
    }

    override fun findModelContext(batchId: String): CompletionStage<ModelContextState?> =
        CompletableFuture.completedFuture(modelContexts[batchId])

    override fun saveModelContext(context: ModelContextState): CompletionStage<Void> {
        modelContexts[context.batchId] = context
        return CompletableFuture.completedFuture(null)
    }
}
