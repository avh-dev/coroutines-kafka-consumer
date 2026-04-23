package avh.ckc.demo.service

import avh.ckc.demo.proto.OrderLifecycleEvent
import avh.ckc.demo.proto.OrderLifecycleEventType
import avh.ckc.demo.repository.BatchState
import avh.ckc.demo.repository.BrewingStateRepository
import avh.ckc.demo.repository.OrderState
import org.springframework.stereotype.Service
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

@Service
class BrewingLifecycleService(
    private val brewingStateRepository: BrewingStateRepository
) {
    fun applyLifecycleEvent(event: OrderLifecycleEvent): CompletionStage<Void> {
        val saveOrder = brewingStateRepository.findOrder(event.orderId).thenCompose { existing ->
            brewingStateRepository.saveOrder(mergeOrderState(event, existing))
        }

        val saveBatch = event.batchId.takeIf(String::isNotBlank)?.let { batchId ->
            brewingStateRepository.findBatch(batchId).thenCompose { existing ->
                brewingStateRepository.saveBatch(mergeBatchState(event, batchId, existing))
                    .thenCompose { updateActiveBatch(event, batchId) }
            }
        } ?: completedVoid()

        return CompletableFuture.allOf(saveOrder.toCompletableFuture(), saveBatch.toCompletableFuture())
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

    private fun updateActiveBatch(event: OrderLifecycleEvent, batchId: String): CompletionStage<Void> {
        val cauldronId = event.cauldronId.takeIf(String::isNotBlank) ?: return completedVoid()
        return when (event.eventType) {
            OrderLifecycleEventType.CAULDRON_ASSIGNED,
            OrderLifecycleEventType.BREWING_STARTED -> brewingStateRepository.saveActiveBatchId(cauldronId, batchId)

            OrderLifecycleEventType.BREWING_COMPLETED -> brewingStateRepository.findActiveBatchId(cauldronId).thenCompose { activeBatchId ->
                if (activeBatchId == batchId) {
                    brewingStateRepository.deleteActiveBatchId(cauldronId)
                } else {
                    completedVoid()
                }
            }

            else -> completedVoid()
        }
    }

    private fun completedVoid(): CompletionStage<Void> = CompletableFuture.completedFuture(null)
}
