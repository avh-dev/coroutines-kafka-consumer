package avh.ckc.loadtest.domain

import avh.ckc.demo.proto.BrewingCompletedPayload
import avh.ckc.demo.proto.BrewingStartedPayload
import avh.ckc.demo.proto.CauldronAssignedPayload
import avh.ckc.demo.proto.EventMetadata
import avh.ckc.demo.proto.IngredientsPreparedPayload
import avh.ckc.demo.proto.OrderCreatedPayload
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.proto.OrderLifecycleEventType
import avh.ckc.loadtest.runtime.ShardContext
import java.time.Instant

class OrderLifecycleStateMachine(
    private val shardContext: ShardContext
) {
    fun createOrderCreated(order: PendingOrder): OrderLifecycleEvent =
        lifecycleEvent(
            orderId = order.orderId,
            customerId = order.customerId,
            batchId = "",
            cauldronId = "",
            potionId = order.potion.potionId,
            recipeId = order.potion.recipeId,
            eventType = OrderLifecycleEventType.ORDER_CREATED,
            occurredAt = order.createdAt,
            payloadSetter = {
                setOrderCreated(
                    OrderCreatedPayload.newBuilder()
                        .setRecipeId(order.potion.recipeId)
                        .setBatchSizeMl(750)
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

        val batchId = "batch-${shardContext.shardToken()}-${batchSlot.toString().padStart(6, '0')}"
        val recipe = orders.first().potion
        val lifecycleEvents = orders.flatMap { order ->
            listOf(
                lifecycleEvent(
                    orderId = order.orderId,
                    customerId = order.customerId,
                    batchId = batchId,
                    cauldronId = "",
                    potionId = recipe.potionId,
                    recipeId = recipe.recipeId,
                    eventType = OrderLifecycleEventType.INGREDIENTS_PREPARED,
                    occurredAt = startedAt.minusSeconds(2),
                    payloadSetter = {
                        setIngredientsPrepared(
                            IngredientsPreparedPayload.newBuilder()
                                .addIngredientCodes("mandrake-root")
                                .addIngredientCodes("moonwater")
                                .addIngredientCodes("phoenix-ash")
                        )
                    }
                ),
                lifecycleEvent(
                    orderId = order.orderId,
                    customerId = order.customerId,
                    batchId = batchId,
                    cauldronId = cauldronId,
                    potionId = recipe.potionId,
                    recipeId = recipe.recipeId,
                    eventType = OrderLifecycleEventType.CAULDRON_ASSIGNED,
                    occurredAt = startedAt.minusSeconds(1),
                    payloadSetter = {
                        setCauldronAssigned(
                            CauldronAssignedPayload.newBuilder()
                                .setCauldronId(cauldronId)
                                .setQueueName("moon-cycle-priority")
                        )
                    }
                ),
                lifecycleEvent(
                    orderId = order.orderId,
                    customerId = order.customerId,
                    batchId = batchId,
                    cauldronId = cauldronId,
                    potionId = recipe.potionId,
                    recipeId = recipe.recipeId,
                    eventType = OrderLifecycleEventType.BREWING_STARTED,
                    occurredAt = startedAt,
                    payloadSetter = {
                        setBrewingStarted(
                            BrewingStartedPayload.newBuilder()
                                .setStartedAt(startedAt.toString())
                        )
                    }
                )
            )
        }

        return GeneratedBatch(
            batchId = batchId,
            cauldronId = cauldronId,
            orderIds = orders.map { it.orderId },
            lifecycleEvents = lifecycleEvents
        )
    }

    fun createCompletedEvents(batch: ActiveBatch, completedAt: Instant): List<OrderLifecycleEvent> =
        batch.orders.map { order ->
            lifecycleEvent(
                orderId = order.orderId,
                customerId = order.customerId,
                batchId = batch.batchId,
                cauldronId = batch.cauldronId,
                potionId = batch.potion.potionId,
                recipeId = batch.potion.recipeId,
                eventType = OrderLifecycleEventType.BREWING_COMPLETED,
                occurredAt = completedAt,
                payloadSetter = {
                    setBrewingCompleted(
                        BrewingCompletedPayload.newBuilder()
                            .setCompletedAt(completedAt.toString())
                    )
                }
            )
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

        val batchId = "batch-${shardContext.shardToken()}-${batchSlot.toString().padStart(6, '0')}"
        val cauldronId = "cauldron-${(shardContext.shardIndex % 8) + 1}"
        val occurredAt = Instant.now()

        val orderIds = (0 until ordersPerBatch).map { offset ->
            "ord-${shardContext.shardToken()}-${(orderIndex + offset).toString().padStart(8, '0')}"
        }

        val events = orderIds.flatMapIndexed { idx, orderId ->
            orderLifecycle(
                orderId = orderId,
                customerId = "customer-${shardContext.shardToken()}-${idx.toString().padStart(4, '0')}",
                batchId = batchId,
                cauldronId = cauldronId,
                potionId = potionId,
                recipeId = recipeId,
                occurredAt = occurredAt.plusMillis((idx * 50L))
            )
        }

        return GeneratedBatch(
            batchId = batchId,
            cauldronId = cauldronId,
            orderIds = orderIds,
            lifecycleEvents = events
        )
    }

    private fun orderLifecycle(
        orderId: String,
        customerId: String,
        batchId: String,
        cauldronId: String,
        potionId: String,
        recipeId: String,
        occurredAt: Instant
    ): List<OrderLifecycleEvent> = listOf(
        lifecycleEvent(
            orderId = orderId,
            customerId = customerId,
            batchId = batchId,
            cauldronId = "",
            potionId = potionId,
            recipeId = recipeId,
            eventType = OrderLifecycleEventType.ORDER_CREATED,
            occurredAt = occurredAt,
            payloadSetter = { setOrderCreated(OrderCreatedPayload.newBuilder().setRecipeId(recipeId).setBatchSizeMl(750)) }
        ),
        lifecycleEvent(
            orderId = orderId,
            customerId = customerId,
            batchId = batchId,
            cauldronId = "",
            potionId = potionId,
            recipeId = recipeId,
            eventType = OrderLifecycleEventType.INGREDIENTS_PREPARED,
            occurredAt = occurredAt.plusSeconds(2),
            payloadSetter = {
                setIngredientsPrepared(
                    IngredientsPreparedPayload.newBuilder()
                        .addIngredientCodes("mandrake-root")
                        .addIngredientCodes("moonwater")
                        .addIngredientCodes("phoenix-ash")
                )
            }
        ),
        lifecycleEvent(
            orderId = orderId,
            customerId = customerId,
            batchId = batchId,
            cauldronId = cauldronId,
            potionId = potionId,
            recipeId = recipeId,
            eventType = OrderLifecycleEventType.CAULDRON_ASSIGNED,
            occurredAt = occurredAt.plusSeconds(5),
            payloadSetter = {
                setCauldronAssigned(
                    CauldronAssignedPayload.newBuilder()
                        .setCauldronId(cauldronId)
                        .setQueueName("moon-cycle-priority")
                )
            }
        ),
        lifecycleEvent(
            orderId = orderId,
            customerId = customerId,
            batchId = batchId,
            cauldronId = cauldronId,
            potionId = potionId,
            recipeId = recipeId,
            eventType = OrderLifecycleEventType.BREWING_STARTED,
            occurredAt = occurredAt.plusSeconds(15),
            payloadSetter = {
                setBrewingStarted(
                    BrewingStartedPayload.newBuilder()
                        .setStartedAt(occurredAt.plusSeconds(15).toString())
                )
            }
        ),
        lifecycleEvent(
            orderId = orderId,
            customerId = customerId,
            batchId = batchId,
            cauldronId = cauldronId,
            potionId = potionId,
            recipeId = recipeId,
            eventType = OrderLifecycleEventType.BREWING_COMPLETED,
            occurredAt = occurredAt.plusSeconds(120),
            payloadSetter = {
                setBrewingCompleted(
                    BrewingCompletedPayload.newBuilder()
                        .setCompletedAt(occurredAt.plusSeconds(120).toString())
                )
            }
        )
    )

    private fun lifecycleEvent(
        orderId: String,
        customerId: String,
        batchId: String,
        cauldronId: String,
        potionId: String,
        recipeId: String,
        eventType: OrderLifecycleEventType,
        occurredAt: Instant,
        payloadSetter: OrderLifecycleEvent.Builder.() -> Unit
    ): OrderLifecycleEvent =
        OrderLifecycleEvent.newBuilder()
            .setMetadata(
                EventMetadata.newBuilder()
                    .setEventId("evt-$orderId-${eventType.name.lowercase()}")
                    .setOccurredAt(occurredAt.toString())
                    .setEventVersion(1)
                    .setRegulatoryTraceId("mrb-${shardContext.shardToken()}-${occurredAt.epochSecond}")
                    .build()
            )
            .setOrderId(orderId)
            .setCustomerId(customerId)
            .setBatchId(batchId)
            .setPotionId(potionId)
            .setRecipeId(recipeId)
            .setCauldronId(cauldronId)
            .setEventType(eventType)
            .apply(payloadSetter)
            .build()
}
