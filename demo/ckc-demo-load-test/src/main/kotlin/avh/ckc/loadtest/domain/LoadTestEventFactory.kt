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
import avh.ckc.loadtest.runtime.ShardContext
import java.time.Instant

class LoadTestEventFactory(
    private val shardContext: ShardContext
) {
    fun orderCreated(order: PendingOrder, now: Instant): OrderLifecycleEvent =
        orderEvent(order, "", OrderLifecycleEventType.ORDER_CREATED, now) {
            setOrderCreated(
                OrderCreatedPayload.newBuilder()
                    .setRecipeId(order.potion.recipeId)
                    .setPotionId(order.potion.potionId)
                    .setBottleSizeMl(750)
                    .setRequestedAt(order.createdAt.toString())
            )
        }

    fun orderBatchAssigned(order: PendingOrder, now: Instant): OrderLifecycleEvent =
        orderEvent(order, order.batchId.orEmpty(), OrderLifecycleEventType.ORDER_BATCH_ASSIGNED, now) {
            setOrderBatchAssigned(
                OrderBatchAssignedPayload.newBuilder()
                    .setBatchId(order.batchId.orEmpty())
                    .setAssignedAt(now.toString())
            )
        }

    fun orderWaitingForBottling(order: PendingOrder, now: Instant): OrderLifecycleEvent =
        orderEvent(order, order.batchId.orEmpty(), OrderLifecycleEventType.ORDER_WAITING_FOR_BOTTLING, now) {
            setOrderWaitingForBottling(
                OrderWaitingForBottlingPayload.newBuilder()
                    .setBatchId(order.batchId.orEmpty())
                    .setBrewingCompletedAt(now.toString())
            )
        }

    fun orderCompleted(order: PendingOrder, now: Instant): OrderLifecycleEvent =
        orderEvent(order, order.batchId.orEmpty(), OrderLifecycleEventType.ORDER_COMPLETED, now) {
            setOrderCompleted(
                OrderCompletedPayload.newBuilder()
                    .setBatchId(order.batchId.orEmpty())
                    .setBottleId("bottle-${order.batchId}-${order.orderId}")
                    .setBottledAt(now.toString())
            )
        }

    fun batchCreated(batch: SimulatedBatch, now: Instant): BatchLifecycleEvent =
        batchEvent(batch, BatchLifecycleEventType.BATCH_CREATED, now) {
            setBatchCreated(BatchCreatedPayload.newBuilder().addAllOrderIds(batch.orders.map { it.orderId }))
        }

    fun batchReagentsPreparationStarted(batch: SimulatedBatch, now: Instant): BatchLifecycleEvent =
        batchEvent(batch, BatchLifecycleEventType.BATCH_REAGENTS_PREPARATION_STARTED, now) {
            setBatchReagentsPreparationStarted(
                BatchReagentsPreparationStartedPayload.newBuilder()
                    .addReagentCodes("mandrake-root")
                    .addReagentCodes("moonwater")
                    .addReagentCodes("phoenix-ash")
            )
        }

    fun batchReagentsPrepared(batch: SimulatedBatch, now: Instant): BatchLifecycleEvent =
        batchEvent(batch, BatchLifecycleEventType.BATCH_REAGENTS_PREPARED, now) {
            setBatchReagentsPrepared(
                BatchReagentsPreparedPayload.newBuilder()
                    .addReagentCodes("mandrake-root")
                    .addReagentCodes("moonwater")
                    .addReagentCodes("phoenix-ash")
                    .setPreparedAt(now.toString())
            )
        }

    fun batchCauldronRequested(batch: SimulatedBatch, now: Instant): BatchLifecycleEvent =
        batchEvent(batch, BatchLifecycleEventType.BATCH_CAULDRON_REQUESTED, now) {
            setBatchCauldronRequested(
                BatchCauldronRequestedPayload.newBuilder()
                    .setQueueName("moon-cycle-priority")
                    .setRequestedAt(now.toString())
            )
        }

    fun batchCauldronAssigned(batch: SimulatedBatch, now: Instant): BatchLifecycleEvent =
        batchEvent(batch, BatchLifecycleEventType.BATCH_CAULDRON_ASSIGNED, now) {
            setBatchCauldronAssigned(
                BatchCauldronAssignedPayload.newBuilder()
                    .setCauldronId(batch.cauldronId.orEmpty())
                    .setAssignedAt(now.toString())
            )
        }

    fun batchBrewingStarted(batch: SimulatedBatch, now: Instant): BatchLifecycleEvent =
        batchEvent(batch, BatchLifecycleEventType.BATCH_BREWING_STARTED, now) {
            setBatchBrewingStarted(BatchBrewingStartedPayload.newBuilder().setStartedAt(now.toString()))
        }

    fun batchBrewingStepCompleted(batch: SimulatedBatch, now: Instant): BatchLifecycleEvent {
        val stepNumber = batch.brewingStepsCompleted + 1
        val template = brewingStepTemplates[(stepNumber - 1) % brewingStepTemplates.size]
        return batchEvent(batch, BatchLifecycleEventType.BATCH_BREWING_STEP_COMPLETED, now) {
            setBatchBrewingStepCompleted(
                BatchBrewingStepCompletedPayload.newBuilder()
                    .setStepNumber(stepNumber)
                    .setStepCode(template.stepCode)
                    .setInstruction(template.instruction)
                    .setCompletedAt(now.toString())
            )
        }
    }

    fun batchBrewingCompleted(batch: SimulatedBatch, now: Instant): BatchLifecycleEvent =
        batchEvent(batch, BatchLifecycleEventType.BATCH_BREWING_COMPLETED, now) {
            setBatchBrewingCompleted(BatchBrewingCompletedPayload.newBuilder().setCompletedAt(now.toString()))
        }

    fun batchBottlingStarted(batch: SimulatedBatch, now: Instant): BatchLifecycleEvent =
        batchEvent(batch, BatchLifecycleEventType.BATCH_BOTTLING_STARTED, now) {
            setBatchBottlingStarted(BatchBottlingStartedPayload.newBuilder().setStartedAt(now.toString()))
        }

    fun batchBottlingCompleted(batch: SimulatedBatch, now: Instant): BatchLifecycleEvent =
        batchEvent(batch, BatchLifecycleEventType.BATCH_BOTTLING_COMPLETED, now) {
            setBatchBottlingCompleted(
                BatchBottlingCompletedPayload.newBuilder()
                    .setCompletedAt(now.toString())
                    .setBottlesCompleted(batch.orders.size)
            )
        }

    private fun orderEvent(
        order: PendingOrder,
        batchId: String,
        eventType: OrderLifecycleEventType,
        now: Instant,
        payloadSetter: OrderLifecycleEvent.Builder.() -> Unit
    ): OrderLifecycleEvent =
        OrderLifecycleEvent.newBuilder()
            .setMetadata(metadata("evt-${order.orderId}-${eventType.name.lowercase()}", now))
            .setOrderId(order.orderId)
            .setCustomerId(order.customerId)
            .setBatchId(batchId)
            .setPotionId(order.potion.potionId)
            .setRecipeId(order.potion.recipeId)
            .setEventType(eventType)
            .apply(payloadSetter)
            .build()

    private fun batchEvent(
        batch: SimulatedBatch,
        eventType: BatchLifecycleEventType,
        now: Instant,
        payloadSetter: BatchLifecycleEvent.Builder.() -> Unit
    ): BatchLifecycleEvent =
        BatchLifecycleEvent.newBuilder()
            .setMetadata(metadata("evt-${batch.batchId}-${eventType.name.lowercase()}-${now.epochSecond}", now))
            .setBatchId(batch.batchId)
            .setPotionId(batch.potion.potionId)
            .setRecipeId(batch.potion.recipeId)
            .setCauldronId(batch.cauldronId.orEmpty())
            .addAllOrderIds(batch.orders.map { it.orderId })
            .setEventType(eventType)
            .apply(payloadSetter)
            .build()

    private fun metadata(eventId: String, now: Instant): EventMetadata =
        EventMetadata.newBuilder()
            .setEventId(eventId)
            .setOccurredAt(now.toString())
            .setEventVersion(1)
            .setRegulatoryTraceId("mrb-${shardContext.shardToken()}-${now.epochSecond}")
            .build()

    private data class BrewingStepTemplate(
        val stepCode: String,
        val instruction: String
    )

    private companion object {
        private val brewingStepTemplates = listOf(
            BrewingStepTemplate("ADD_GARLIC", "Add garlic; brew until tiny bubbles appear."),
            BrewingStepTemplate("UNICORN_HORN_POWDER", "Add unicorn horn powder; stir counterclockwise."),
            BrewingStepTemplate("SILVER_RIPPLE_SETTLE", "Let the surface settle to silver ripples."),
            BrewingStepTemplate("MOONWATER_DRIZZLE", "Drizzle moonwater along the cauldron rim."),
            BrewingStepTemplate("PHOENIX_ASH_FOLD", "Fold phoenix ash into the vortex."),
            BrewingStepTemplate("MANDRAKE_STEEP", "Steep mandrake root until the brew hums."),
            BrewingStepTemplate("CRYSTAL_CHIME", "Chime the calibration crystal three times."),
            BrewingStepTemplate("VAPOR_BINDING", "Bind violet vapor with a clockwise sweep."),
            BrewingStepTemplate("EMBER_REST", "Lower embers and rest the surface."),
            BrewingStepTemplate("FINAL_STIR", "Finish with a slow figure-eight stir.")
        )
    }
}
