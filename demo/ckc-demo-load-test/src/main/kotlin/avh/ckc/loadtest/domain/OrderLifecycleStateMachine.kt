package avh.ckc.loadtest.domain

import avh.ckc.demo.proto.BatchBottlingCompletedPayload
import avh.ckc.demo.proto.BatchBottlingStartedPayload
import avh.ckc.demo.proto.BatchBrewingCompletedPayload
import avh.ckc.demo.proto.BatchBrewingStartedPayload
import avh.ckc.demo.proto.BatchBrewingStepCompletedPayload
import avh.ckc.demo.proto.BatchCauldronAssignedPayload
import avh.ckc.demo.proto.BatchCauldronRequestedPayload
import avh.ckc.demo.proto.BatchCreatedPayload
import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.BatchLifecycleEventType
import avh.ckc.demo.proto.BatchReagentsPreparationStartedPayload
import avh.ckc.demo.proto.BatchReagentsPreparedPayload
import avh.ckc.demo.proto.EventMetadata
import avh.ckc.demo.proto.OrderBatchAssignedPayload
import avh.ckc.demo.proto.OrderCompletedPayload
import avh.ckc.demo.proto.OrderCreatedPayload
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.proto.OrderLifecycleEventType
import avh.ckc.demo.proto.OrderWaitingForBottlingPayload
import avh.ckc.loadtest.runtime.GeneratorIdentity
import java.time.Instant

class OrderLifecycleStateMachine(
    private val identity: GeneratorIdentity
) {
    fun createOrderCreated(order: PendingOrder): OrderLifecycleEvent =
        orderEvent(
            orderId = order.orderId,
            customerId = order.customerId,
            batchId = "",
            potionId = order.potion.potionId,
            recipeId = order.potion.recipeId,
            eventType = OrderLifecycleEventType.ORDER_CREATED,
            occurredAt = order.createdAt,
            payloadSetter = {
                setOrderCreated(
                    OrderCreatedPayload.newBuilder()
                        .setRecipeId(order.potion.recipeId)
                        .setPotionId(order.potion.potionId)
                        .setBottleSizeMl(750)
                        .setRequestedAt(order.createdAt.toString())
                )
            }
        )

    fun createAssignedBatch(
        batchSlot: Int,
        cauldronId: String,
        orders: List<PendingOrder>,
        brewDuration: java.time.Duration,
        startedAt: Instant
    ): GeneratedBatch {
        require(batchSlot >= 0) { "batchSlot must be non-negative" }
        require(orders.isNotEmpty()) { "orders must not be empty" }

        val batchId = identity.entityId("batch", batchSlot.toLong(), width = 6)
        val recipe = orders.first().potion
        return buildBatch(batchId, cauldronId, orders, recipe, startedAt, brewDuration)
    }

    fun createCompletedEvents(batch: ActiveBatch, completedAt: Instant): List<OrderLifecycleEvent> =
        batch.orders.mapIndexed { index, order ->
            orderCompleted(order, batch.batchId, batch.potion, completedAt.plusMillis(index * 25L))
        }

    fun createOrderBatch(
        orderIndex: Int,
        batchSlot: Int,
        ordersPerBatch: Int = 3,
        potionId: String = "healing-elixir",
        recipeId: String = "healing-elixir-v2"
    ): GeneratedBatch {
        require(orderIndex >= 0) { "orderIndex must be non-negative" }
        require(batchSlot >= 0) { "batchSlot must be non-negative" }
        require(ordersPerBatch > 0) { "ordersPerBatch must be positive" }

        val occurredAt = Instant.now()
        val recipe = PotionRecipe(potionId, recipeId)
        val orders = (0 until ordersPerBatch).map { offset ->
            PendingOrder(
                orderId = identity.entityId("order", (orderIndex + offset).toLong(), width = 8),
                customerId = identity.entityId("customer", offset.toLong(), width = 4),
                potion = recipe,
                createdAt = occurredAt.plusMillis(offset * 50L)
            )
        }
        val batchId = identity.entityId("batch", batchSlot.toLong(), width = 6)
        val cauldronId = identity.entityId("cauldron", ((identity.externalShardIndex % 8) + 1).toLong(), width = 4)

        return buildBatch(batchId, cauldronId, orders, recipe, occurredAt, java.time.Duration.ofSeconds(120))
    }

    private fun buildBatch(
        batchId: String,
        cauldronId: String,
        orders: List<PendingOrder>,
        recipe: PotionRecipe,
        occurredAt: Instant,
        brewDuration: java.time.Duration
    ): GeneratedBatch {
        val orderCreated = orders.map(::createOrderCreated)
        val orderAssigned = orders.map { order ->
            orderEvent(
                orderId = order.orderId,
                customerId = order.customerId,
                batchId = batchId,
                potionId = recipe.potionId,
                recipeId = recipe.recipeId,
                eventType = OrderLifecycleEventType.ORDER_BATCH_ASSIGNED,
                occurredAt = occurredAt.plusSeconds(3),
                payloadSetter = {
                    setOrderBatchAssigned(
                        OrderBatchAssignedPayload.newBuilder()
                            .setBatchId(batchId)
                            .setAssignedAt(occurredAt.plusSeconds(3).toString())
                    )
                }
            )
        }
        val waitingForBottling = orders.map { order ->
            orderEvent(
                orderId = order.orderId,
                customerId = order.customerId,
                batchId = batchId,
                potionId = recipe.potionId,
                recipeId = recipe.recipeId,
                eventType = OrderLifecycleEventType.ORDER_WAITING_FOR_BOTTLING,
                occurredAt = occurredAt.plus(brewDuration),
                payloadSetter = {
                    setOrderWaitingForBottling(
                        OrderWaitingForBottlingPayload.newBuilder()
                            .setBatchId(batchId)
                            .setBrewingCompletedAt(occurredAt.plus(brewDuration).toString())
                    )
                }
            )
        }
        val completed = orders.mapIndexed { index, order ->
            orderCompleted(order, batchId, recipe, occurredAt.plus(brewDuration).plusSeconds(10).plusMillis(index * 25L))
        }

        return GeneratedBatch(
            batchId = batchId,
            cauldronId = cauldronId,
            orderIds = orders.map { it.orderId },
            orderEvents = orderCreated + orderAssigned + waitingForBottling + completed,
            batchEvents = batchEvents(batchId, cauldronId, recipe, orders.map { it.orderId }, occurredAt, brewDuration)
        )
    }

    private fun orderCompleted(
        order: PendingOrder,
        batchId: String,
        recipe: PotionRecipe,
        occurredAt: Instant
    ): OrderLifecycleEvent =
        orderEvent(
            orderId = order.orderId,
            customerId = order.customerId,
            batchId = batchId,
            potionId = recipe.potionId,
            recipeId = recipe.recipeId,
            eventType = OrderLifecycleEventType.ORDER_COMPLETED,
            occurredAt = occurredAt,
            payloadSetter = {
                setOrderCompleted(
                    OrderCompletedPayload.newBuilder()
                        .setBatchId(batchId)
                        .setBottleId("bottle-$batchId-${order.orderId}")
                        .setBottledAt(occurredAt.toString())
                )
            }
        )

    private fun batchEvents(
        batchId: String,
        cauldronId: String,
        recipe: PotionRecipe,
        orderIds: List<String>,
        occurredAt: Instant,
        brewDuration: java.time.Duration
    ): List<BatchLifecycleEvent> =
        listOf(
            batchEvent(batchId, recipe, "", orderIds, BatchLifecycleEventType.BATCH_CREATED, occurredAt) {
                setBatchCreated(BatchCreatedPayload.newBuilder().addAllOrderIds(orderIds))
            },
            batchEvent(batchId, recipe, "", orderIds, BatchLifecycleEventType.BATCH_REAGENTS_PREPARATION_STARTED, occurredAt.plusSeconds(1)) {
                setBatchReagentsPreparationStarted(reagentList(BatchReagentsPreparationStartedPayload.newBuilder()))
            },
            batchEvent(batchId, recipe, "", orderIds, BatchLifecycleEventType.BATCH_REAGENTS_PREPARED, occurredAt.plusSeconds(2)) {
                setBatchReagentsPrepared(
                    BatchReagentsPreparedPayload.newBuilder()
                        .addReagentCodes("mandrake-root")
                        .addReagentCodes("moonwater")
                        .addReagentCodes("phoenix-ash")
                        .setPreparedAt(occurredAt.plusSeconds(2).toString())
                )
            },
            batchEvent(batchId, recipe, "", orderIds, BatchLifecycleEventType.BATCH_CAULDRON_REQUESTED, occurredAt.plusSeconds(3)) {
                setBatchCauldronRequested(
                    BatchCauldronRequestedPayload.newBuilder()
                        .setQueueName("moon-cycle-priority")
                        .setRequestedAt(occurredAt.plusSeconds(3).toString())
                )
            },
            batchEvent(batchId, recipe, cauldronId, orderIds, BatchLifecycleEventType.BATCH_CAULDRON_ASSIGNED, occurredAt.plusSeconds(4)) {
                setBatchCauldronAssigned(
                    BatchCauldronAssignedPayload.newBuilder()
                        .setCauldronId(cauldronId)
                        .setAssignedAt(occurredAt.plusSeconds(4).toString())
                )
            },
            batchEvent(batchId, recipe, cauldronId, orderIds, BatchLifecycleEventType.BATCH_BREWING_STARTED, occurredAt.plusSeconds(5)) {
                setBatchBrewingStarted(BatchBrewingStartedPayload.newBuilder().setStartedAt(occurredAt.plusSeconds(5).toString()))
            },
            brewingStep(batchId, recipe, cauldronId, orderIds, 1, "ADD_GARLIC", "Add garlic; brew until tiny bubbles appear.", occurredAt.plusSeconds(25)),
            brewingStep(batchId, recipe, cauldronId, orderIds, 2, "UNICORN_HORN_POWDER", "Add unicorn horn powder; stir counterclockwise.", occurredAt.plusSeconds(55)),
            brewingStep(batchId, recipe, cauldronId, orderIds, 3, "SETTLE", "Let the surface settle to silver ripples.", occurredAt.plusSeconds(85)),
            batchEvent(batchId, recipe, cauldronId, orderIds, BatchLifecycleEventType.BATCH_BREWING_COMPLETED, occurredAt.plus(brewDuration)) {
                setBatchBrewingCompleted(BatchBrewingCompletedPayload.newBuilder().setCompletedAt(occurredAt.plus(brewDuration).toString()))
            },
            batchEvent(batchId, recipe, cauldronId, orderIds, BatchLifecycleEventType.BATCH_BOTTLING_STARTED, occurredAt.plus(brewDuration).plusSeconds(2)) {
                setBatchBottlingStarted(BatchBottlingStartedPayload.newBuilder().setStartedAt(occurredAt.plus(brewDuration).plusSeconds(2).toString()))
            },
            batchEvent(batchId, recipe, cauldronId, orderIds, BatchLifecycleEventType.BATCH_BOTTLING_COMPLETED, occurredAt.plus(brewDuration).plusSeconds(12)) {
                setBatchBottlingCompleted(
                    BatchBottlingCompletedPayload.newBuilder()
                        .setCompletedAt(occurredAt.plus(brewDuration).plusSeconds(12).toString())
                        .setBottlesCompleted(orderIds.size)
                )
            }
        )

    private fun brewingStep(
        batchId: String,
        recipe: PotionRecipe,
        cauldronId: String,
        orderIds: List<String>,
        stepNumber: Int,
        stepCode: String,
        instruction: String,
        occurredAt: Instant
    ): BatchLifecycleEvent =
        batchEvent(batchId, recipe, cauldronId, orderIds, BatchLifecycleEventType.BATCH_BREWING_STEP_COMPLETED, occurredAt) {
            setBatchBrewingStepCompleted(
                BatchBrewingStepCompletedPayload.newBuilder()
                    .setStepNumber(stepNumber)
                    .setStepCode(stepCode)
                    .setInstruction(instruction)
                    .setCompletedAt(occurredAt.toString())
            )
        }

    private fun reagentList(builder: BatchReagentsPreparationStartedPayload.Builder): BatchReagentsPreparationStartedPayload.Builder =
        builder.addReagentCodes("mandrake-root")
            .addReagentCodes("moonwater")
            .addReagentCodes("phoenix-ash")

    private fun orderEvent(
        orderId: String,
        customerId: String,
        batchId: String,
        potionId: String,
        recipeId: String,
        eventType: OrderLifecycleEventType,
        occurredAt: Instant,
        payloadSetter: OrderLifecycleEvent.Builder.() -> Unit
    ): OrderLifecycleEvent =
        OrderLifecycleEvent.newBuilder()
            .setMetadata(metadata("evt-$orderId-${eventType.name.lowercase()}", occurredAt))
            .setOrderId(orderId)
            .setCustomerId(customerId)
            .setBatchId(batchId)
            .setPotionId(potionId)
            .setRecipeId(recipeId)
            .setEventType(eventType)
            .apply(payloadSetter)
            .build()

    private fun batchEvent(
        batchId: String,
        recipe: PotionRecipe,
        cauldronId: String,
        orderIds: List<String>,
        eventType: BatchLifecycleEventType,
        occurredAt: Instant,
        payloadSetter: BatchLifecycleEvent.Builder.() -> Unit
    ): BatchLifecycleEvent =
        BatchLifecycleEvent.newBuilder()
            .setMetadata(metadata("evt-$batchId-${eventType.name.lowercase()}-${occurredAt.epochSecond}", occurredAt))
            .setBatchId(batchId)
            .setPotionId(recipe.potionId)
            .setRecipeId(recipe.recipeId)
            .setCauldronId(cauldronId)
            .addAllOrderIds(orderIds)
            .setEventType(eventType)
            .apply(payloadSetter)
            .build()

    private fun metadata(eventId: String, occurredAt: Instant): EventMetadata =
        EventMetadata.newBuilder()
            .setEventId(eventId)
            .setOccurredAt(occurredAt.toString())
            .setEventVersion(1)
            .setRegulatoryTraceId(identity.regulatoryTraceId(occurredAt))
            .build()
}
