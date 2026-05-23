package avh.ckc.demo.service.order

import avh.ckc.demo.modelclient.flavour.OrderFlavourRequest
import avh.ckc.demo.modelclient.flavour.OrderFlavourResponse
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.proto.OrderLifecycleEventType
import avh.ckc.demo.repository.OrderFlavourState
import avh.ckc.demo.repository.OrderState

internal fun mergeOrderState(event: OrderLifecycleEvent, existing: OrderState?): OrderState =
    OrderState(
        orderId = event.orderId,
        batchId = event.batchId.ifBlank { existing?.batchId },
        potionId = event.potionId.ifBlank { existing?.potionId ?: "" },
        recipeId = event.recipeId.ifBlank { existing?.recipeId },
        customerId = event.customerId.ifBlank { existing?.customerId ?: "" },
        status = orderStatus(event),
        updatedAt = event.metadata.occurredAt
    )

private fun orderStatus(event: OrderLifecycleEvent): String =
    when (event.eventType) {
        OrderLifecycleEventType.ORDER_CREATED -> "CREATED"
        OrderLifecycleEventType.ORDER_FLAVOUR_ANALYSED -> "CREATED"
        OrderLifecycleEventType.ORDER_BATCH_ASSIGNED -> "BATCH_ASSIGNED"
        OrderLifecycleEventType.ORDER_WAITING_FOR_BOTTLING -> "WAITING_FOR_BOTTLING"
        OrderLifecycleEventType.ORDER_COMPLETED -> "COMPLETED"
        OrderLifecycleEventType.ORDER_CANCELLED -> "CANCELLED"
        OrderLifecycleEventType.ORDER_FAILED -> "FAILED"
        else -> event.eventType.name
    }

internal fun flavourRequest(event: OrderLifecycleEvent): OrderFlavourRequest =
    OrderFlavourRequest(
        orderId = event.orderId,
        customerId = event.customerId,
        recipeId = event.recipeId.ifBlank { null },
        potionId = event.potionId,
        orderedAt = event.metadata.occurredAt
    )

internal fun flavourState(event: OrderLifecycleEvent, response: OrderFlavourResponse): OrderFlavourState =
    OrderFlavourState(
        orderId = event.orderId,
        flavourProfileId = response.flavourProfileId,
        palette = response.palette,
        etaCorrectionFactor = response.etaCorrectionFactor,
        moonPhase = response.moonPhase,
        modelRequestId = response.requestId,
        updatedAt = event.metadata.occurredAt
    )
