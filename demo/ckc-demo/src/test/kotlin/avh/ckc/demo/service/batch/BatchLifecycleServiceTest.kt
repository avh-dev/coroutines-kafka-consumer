package avh.ckc.demo.service.batch

import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.BatchLifecycleEventType
import avh.ckc.demo.proto.BatchBrewingStepCompletedPayload
import avh.ckc.demo.proto.EventMetadata
import avh.ckc.demo.model.Batch
import avh.ckc.demo.model.BrewingStepReceipt
import avh.ckc.demo.model.EtaContext
import avh.ckc.demo.model.OrderFlavour
import avh.ckc.demo.model.Order
import avh.ckc.demo.registry.BrewingStepRegistryRequest
import avh.ckc.demo.registry.BrewingStepRegistryResponse
import avh.ckc.demo.registry.SyncBrewingStepRegistryClient
import avh.ckc.demo.repository.SyncBrewingStateRepository
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class BatchLifecycleServiceTest {
    @Test
    fun `applies batch event by merging and persisting state`() {
        val repository = FakeBrewingStateRepository(
            batches = mutableMapOf(
                "batch-11" to Batch(
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
        val registryClient = FakeBrewingStepRegistryClient()
        val service = SyncBatchLifecycleService(repository, registryClient)

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
        assertEquals(0, registryClient.requests.size)
    }

    @Test
    fun `reports completed brewing step to registry and persists receipt`() {
        val repository = FakeBrewingStateRepository()
        val registryClient = FakeBrewingStepRegistryClient(
            response = BrewingStepRegistryResponse(
                receiptId = "acr-batch-11-3",
                acceptedAt = "2026-03-26T09:10:12Z",
                registryShard = "acr-shard-03"
            )
        )
        val service = SyncBatchLifecycleService(repository, registryClient)

        service.apply(
            batchEvent(
                eventType = BatchLifecycleEventType.BATCH_BREWING_STEP_COMPLETED,
                batchId = "batch-11",
                cauldronId = "cauldron-3",
                stepNumber = 3,
                stepCode = "PHOENIX_ASH_FOLD"
            )
        )

        assertEquals(
            BrewingStepRegistryRequest(
                batchId = "batch-11",
                cauldronId = "cauldron-3",
                stepNumber = 3,
                stepCode = "PHOENIX_ASH_FOLD",
                completedAt = "2026-03-26T09:10:11Z",
                regulatoryTraceId = "mrb-test"
            ),
            registryClient.requests.single()
        )
        assertEquals("acr-batch-11-3", repository.receipts.getValue("batch-11:3").receiptId)
        assertEquals("acr-shard-03", repository.receipts.getValue("batch-11:3").registryShard)
    }

    @Test
    fun `retains active batch when completed batch matches active one`() {
        val repository = FakeBrewingStateRepository(
            activeBatchIds = mutableMapOf("cauldron-3" to "batch-11")
        )
        val service = SyncBatchLifecycleService(repository, FakeBrewingStepRegistryClient())

        service.apply(
            batchEvent(
                eventType = BatchLifecycleEventType.BATCH_BOTTLING_COMPLETED,
                batchId = "batch-11",
                cauldronId = "cauldron-3"
            )
        )

        assertEquals("batch-11", repository.activeBatchIds["cauldron-3"])
    }

    private fun batchEvent(
        eventType: BatchLifecycleEventType,
        batchId: String,
        cauldronId: String,
        recipeId: String = "",
        stepNumber: Int = 0,
        stepCode: String = ""
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
                    .setRegulatoryTraceId("mrb-test")
                    .build()
            )
            .apply {
                if (eventType == BatchLifecycleEventType.BATCH_BREWING_STEP_COMPLETED) {
                    setBatchBrewingStepCompleted(
                        BatchBrewingStepCompletedPayload.newBuilder()
                            .setStepNumber(stepNumber)
                            .setStepCode(stepCode)
                            .setCompletedAt("2026-03-26T09:10:11Z")
                    )
                }
            }
            .build()
}

private class FakeBrewingStateRepository(
    val orders: MutableMap<String, Order> = mutableMapOf(),
    val batches: MutableMap<String, Batch> = mutableMapOf(),
    val activeBatchIds: MutableMap<String, String> = mutableMapOf(),
    val etaContexts: MutableMap<String, EtaContext> = mutableMapOf(),
    val flavours: MutableMap<String, OrderFlavour> = mutableMapOf(),
    val receipts: MutableMap<String, BrewingStepReceipt> = mutableMapOf()
) : SyncBrewingStateRepository {
    override fun findOrder(orderId: String): Order? = orders[orderId]

    override fun saveOrder(order: Order) {
        orders[order.orderId] = order
    }

    override fun findBatch(batchId: String): Batch? = batches[batchId]

    override fun saveBatch(batch: Batch) {
        batches[batch.batchId] = batch
    }

    override fun findActiveBatchId(cauldronId: String): String? = activeBatchIds[cauldronId]

    override fun saveActiveBatchId(cauldronId: String, batchId: String) {
        activeBatchIds[cauldronId] = batchId
    }

    override fun findEtaContext(batchId: String): EtaContext? = etaContexts[batchId]

    override fun saveEtaContext(context: EtaContext) {
        etaContexts[context.batchId] = context
    }

    override fun findOrderFlavour(orderId: String): OrderFlavour? = flavours[orderId]

    override fun saveOrderFlavour(state: OrderFlavour) {
        flavours[state.orderId] = state
    }

    override fun saveBrewingStepReceipt(receipt: BrewingStepReceipt) {
        receipts["${receipt.batchId}:${receipt.stepNumber}"] = receipt
    }
}

private class FakeBrewingStepRegistryClient(
    private val response: BrewingStepRegistryResponse = BrewingStepRegistryResponse(
        receiptId = "acr-default",
        acceptedAt = "2026-03-26T09:10:12Z",
        registryShard = "acr-shard-01"
    )
) : SyncBrewingStepRegistryClient {
    val requests = mutableListOf<BrewingStepRegistryRequest>()

    override fun reportStep(request: BrewingStepRegistryRequest): BrewingStepRegistryResponse {
        requests += request
        return response
    }
}
