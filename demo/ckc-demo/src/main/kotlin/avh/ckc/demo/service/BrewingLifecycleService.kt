package avh.ckc.demo.service

import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.proto.OrderLifecycleEventType
import avh.ckc.demo.repository.BatchState
import avh.ckc.demo.repository.OrderState
import avh.ckc.demo.repository.SuspendBrewingStateRepository
import avh.ckc.demo.repository.SyncBrewingStateRepository
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

@Service
class SyncBrewingLifecycleService(
    private val brewingStateRepository: SyncBrewingStateRepository
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun applyLifecycleEvent(event: OrderLifecycleEvent) {
        try {
            val existingOrder = brewingStateRepository.findOrder(event.orderId)
            brewingStateRepository.saveOrder(mergeOrderState(event, existingOrder))

            val batchId = event.batchId.takeIf(String::isNotBlank) ?: return
            val existingBatch = brewingStateRepository.findBatch(batchId)
            brewingStateRepository.saveBatch(mergeBatchState(event, batchId, existingBatch))
            updateActiveBatch(event, batchId)
        } catch (error: Throwable) {
            logger.error(
                "Spring Kafka lifecycle processing failed for orderId={}, eventType={}",
                event.orderId,
                event.eventType.name,
                error
            )
            throw error
        }
    }

    private fun updateActiveBatch(event: OrderLifecycleEvent, batchId: String) {
        val cauldronId = event.cauldronId.takeIf(String::isNotBlank) ?: return
        when (event.eventType) {
            OrderLifecycleEventType.CAULDRON_ASSIGNED,
            OrderLifecycleEventType.BREWING_STARTED -> brewingStateRepository.saveActiveBatchId(cauldronId, batchId)

            OrderLifecycleEventType.BREWING_COMPLETED -> {
                val activeBatchId = brewingStateRepository.findActiveBatchId(cauldronId)
                if (activeBatchId == batchId) {
                    brewingStateRepository.deleteActiveBatchId(cauldronId)
                }
            }

            else -> Unit
        }
    }
}

@Service
class SuspendBrewingLifecycleService(
    private val brewingStateRepository: SuspendBrewingStateRepository
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    suspend fun applyLifecycleEvent(event: OrderLifecycleEvent) {
        try {
            val existingOrder = brewingStateRepository.findOrder(event.orderId)
            brewingStateRepository.saveOrder(mergeOrderState(event, existingOrder))

            val batchId = event.batchId.takeIf(String::isNotBlank) ?: return
            val existingBatch = brewingStateRepository.findBatch(batchId)
            brewingStateRepository.saveBatch(mergeBatchState(event, batchId, existingBatch))
            updateActiveBatch(event, batchId)
        } catch (error: Throwable) {
            logger.error(
                "CKC lifecycle processing failed for orderId={}, eventType={}",
                event.orderId,
                event.eventType.name,
                error
            )
            throw error
        }
    }

    private suspend fun updateActiveBatch(event: OrderLifecycleEvent, batchId: String) {
        val cauldronId = event.cauldronId.takeIf(String::isNotBlank) ?: return
        when (event.eventType) {
            OrderLifecycleEventType.CAULDRON_ASSIGNED,
            OrderLifecycleEventType.BREWING_STARTED -> brewingStateRepository.saveActiveBatchId(cauldronId, batchId)

            OrderLifecycleEventType.BREWING_COMPLETED -> {
                val activeBatchId = brewingStateRepository.findActiveBatchId(cauldronId)
                if (activeBatchId == batchId) {
                    brewingStateRepository.deleteActiveBatchId(cauldronId)
                }
            }

            else -> Unit
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
        cauldronId = event.cauldronId.ifBlank { existing?.cauldronId },
        status = event.eventType.name,
        updatedAt = event.metadata.occurredAt
    )

private fun mergeBatchState(event: OrderLifecycleEvent, batchId: String, existing: BatchState?): BatchState {
    val orderIds = (existing?.orderIds.orEmpty() + event.orderId)
        .filter(String::isNotBlank)
        .distinct()

    return BatchState(
        batchId = batchId,
        recipeId = event.recipeId.ifBlank { existing?.recipeId },
        potionId = event.potionId.ifBlank { existing?.potionId },
        cauldronId = event.cauldronId.ifBlank { existing?.cauldronId },
        status = event.eventType.name,
        orderIds = orderIds,
        updatedAt = event.metadata.occurredAt
    )
}
