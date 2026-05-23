package avh.ckc.demo.service

import avh.ckc.demo.modelclient.flavour.OrderFlavourRequest
import avh.ckc.demo.modelclient.flavour.OrderFlavourResponse
import avh.ckc.demo.modelclient.flavour.SuspendOrderFlavourModelClient
import avh.ckc.demo.modelclient.flavour.SyncOrderFlavourModelClient
import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.proto.OrderLifecycleEventType
import avh.ckc.demo.repository.OrderFlavourState
import avh.ckc.demo.repository.OrderState
import avh.ckc.demo.repository.SuspendBrewingStateRepository
import avh.ckc.demo.repository.SyncBrewingStateRepository
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

@Service
class SyncOrderLifecycleService(
    private val brewingStateRepository: SyncBrewingStateRepository,
    private val flavourModelClient: SyncOrderFlavourModelClient
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun apply(event: OrderLifecycleEvent) {
        try {
            val existingOrder = brewingStateRepository.findOrder(event.orderId)
            brewingStateRepository.saveOrder(mergeOrderState(event, existingOrder))

            if (event.eventType == OrderLifecycleEventType.ORDER_CREATED) {
                val flavour = flavourModelClient.analyse(flavourRequest(event))
                brewingStateRepository.saveOrderFlavour(flavourState(event, flavour))
            }
        } catch (error: Throwable) {
            logger.error(
                "Spring Kafka order processing failed for orderId={}, eventType={}",
                event.orderId,
                event.eventType.name,
                error
            )
            throw error
        }
    }
}

@Service
class SuspendOrderLifecycleService(
    private val brewingStateRepository: SuspendBrewingStateRepository,
    private val flavourModelClient: SuspendOrderFlavourModelClient
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    suspend fun apply(event: OrderLifecycleEvent) {
        try {
            val existingOrder = brewingStateRepository.findOrder(event.orderId)
            brewingStateRepository.saveOrder(mergeOrderState(event, existingOrder))

            if (event.eventType == OrderLifecycleEventType.ORDER_CREATED) {
                val flavour = flavourModelClient.analyse(flavourRequest(event))
                brewingStateRepository.saveOrderFlavour(flavourState(event, flavour))
            }
        } catch (error: Throwable) {
            logger.error(
                "CKC order processing failed for orderId={}, eventType={}",
                event.orderId,
                event.eventType.name,
                error
            )
            throw error
        }
    }
}

private fun mergeOrderState(event: OrderLifecycleEvent, existing: OrderState?): OrderState =
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

private fun flavourRequest(event: OrderLifecycleEvent): OrderFlavourRequest =
    OrderFlavourRequest(
        orderId = event.orderId,
        customerId = event.customerId,
        recipeId = event.recipeId.ifBlank { null },
        potionId = event.potionId,
        orderedAt = event.metadata.occurredAt
    )

private fun flavourState(event: OrderLifecycleEvent, response: OrderFlavourResponse): OrderFlavourState =
    OrderFlavourState(
        orderId = event.orderId,
        flavourProfileId = response.flavourProfileId,
        palette = response.palette,
        etaCorrectionFactor = response.etaCorrectionFactor,
        moonPhase = response.moonPhase,
        modelRequestId = response.requestId,
        updatedAt = event.metadata.occurredAt
    )
