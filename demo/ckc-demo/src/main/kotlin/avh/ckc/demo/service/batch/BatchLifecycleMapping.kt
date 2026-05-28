package avh.ckc.demo.service.batch

import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.BatchLifecycleEventType
import avh.ckc.demo.model.Batch
import avh.ckc.demo.repository.SuspendBrewingStateRepository
import avh.ckc.demo.repository.SyncBrewingStateRepository

internal fun mergeBatch(event: BatchLifecycleEvent, existing: Batch?): Batch =
    Batch(
        batchId = event.batchId,
        recipeId = event.recipeId.ifBlank { existing?.recipeId },
        potionId = event.potionId.ifBlank { existing?.potionId },
        cauldronId = event.cauldronId.ifBlank { existing?.cauldronId },
        status = batchStatus(event),
        orderIds = (existing?.orderIds.orEmpty() + event.orderIdsList).filter(String::isNotBlank).distinct(),
        updatedAt = event.metadata.occurredAt
    )

private fun batchStatus(event: BatchLifecycleEvent): String =
    when (event.eventType) {
        BatchLifecycleEventType.BATCH_CREATED -> "CREATED"
        BatchLifecycleEventType.BATCH_REAGENTS_PREPARATION_STARTED -> "REAGENTS_PREPARING"
        BatchLifecycleEventType.BATCH_REAGENTS_PREPARED -> "REAGENTS_PREPARED"
        BatchLifecycleEventType.BATCH_CAULDRON_REQUESTED -> "WAITING_FOR_CAULDRON"
        BatchLifecycleEventType.BATCH_CAULDRON_ASSIGNED -> "CAULDRON_ASSIGNED"
        BatchLifecycleEventType.BATCH_BREWING_STARTED -> "BREWING"
        BatchLifecycleEventType.BATCH_BREWING_STEP_COMPLETED -> "BREWING"
        BatchLifecycleEventType.BATCH_BREWING_COMPLETED -> "WAITING_FOR_BOTTLING"
        BatchLifecycleEventType.BATCH_BOTTLING_STARTED -> "BOTTLING"
        BatchLifecycleEventType.BATCH_BOTTLING_COMPLETED -> "BOTTLING_COMPLETED"
        BatchLifecycleEventType.BATCH_FAILED -> "FAILED"
        BatchLifecycleEventType.BATCH_CANCELLED -> "CANCELLED"
        else -> event.eventType.name
    }

internal fun updateActiveBatch(event: BatchLifecycleEvent, repository: SyncBrewingStateRepository) {
    val cauldronId = event.cauldronId.takeIf(String::isNotBlank) ?: return
    when (event.eventType) {
        BatchLifecycleEventType.BATCH_CAULDRON_ASSIGNED,
        BatchLifecycleEventType.BATCH_BREWING_STARTED -> repository.saveActiveBatchId(cauldronId, event.batchId)

        else -> Unit
    }
}

internal suspend fun updateActiveBatch(event: BatchLifecycleEvent, repository: SuspendBrewingStateRepository) {
    val cauldronId = event.cauldronId.takeIf(String::isNotBlank) ?: return
    when (event.eventType) {
        BatchLifecycleEventType.BATCH_CAULDRON_ASSIGNED,
        BatchLifecycleEventType.BATCH_BREWING_STARTED -> repository.saveActiveBatchId(cauldronId, event.batchId)

        else -> Unit
    }
}
