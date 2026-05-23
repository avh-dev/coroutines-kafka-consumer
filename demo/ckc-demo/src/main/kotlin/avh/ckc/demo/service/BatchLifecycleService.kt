package avh.ckc.demo.service

import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.BatchLifecycleEventType
import avh.ckc.demo.repository.BatchState
import avh.ckc.demo.repository.SuspendBrewingStateRepository
import avh.ckc.demo.repository.SyncBrewingStateRepository
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

@Service
class SyncBatchLifecycleService(
    private val brewingStateRepository: SyncBrewingStateRepository
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun apply(event: BatchLifecycleEvent) {
        try {
            val existingBatch = brewingStateRepository.findBatch(event.batchId)
            brewingStateRepository.saveBatch(mergeBatchState(event, existingBatch))
            updateActiveBatch(event)
        } catch (error: Throwable) {
            logger.error(
                "Spring Kafka batch processing failed for batchId={}, eventType={}",
                event.batchId,
                event.eventType.name,
                error
            )
            throw error
        }
    }

    private fun updateActiveBatch(event: BatchLifecycleEvent) {
        val cauldronId = event.cauldronId.takeIf(String::isNotBlank) ?: return
        when (event.eventType) {
            BatchLifecycleEventType.BATCH_CAULDRON_ASSIGNED,
            BatchLifecycleEventType.BATCH_BREWING_STARTED -> brewingStateRepository.saveActiveBatchId(cauldronId, event.batchId)

            BatchLifecycleEventType.BATCH_BOTTLING_COMPLETED,
            BatchLifecycleEventType.BATCH_FAILED,
            BatchLifecycleEventType.BATCH_CANCELLED -> {
                val activeBatchId = brewingStateRepository.findActiveBatchId(cauldronId)
                if (activeBatchId == event.batchId) {
                    brewingStateRepository.deleteActiveBatchId(cauldronId)
                }
            }

            else -> Unit
        }
    }
}

@Service
class SuspendBatchLifecycleService(
    private val brewingStateRepository: SuspendBrewingStateRepository
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    suspend fun apply(event: BatchLifecycleEvent) {
        try {
            val existingBatch = brewingStateRepository.findBatch(event.batchId)
            brewingStateRepository.saveBatch(mergeBatchState(event, existingBatch))
            updateActiveBatch(event)
        } catch (error: Throwable) {
            logger.error(
                "CKC batch processing failed for batchId={}, eventType={}",
                event.batchId,
                event.eventType.name,
                error
            )
            throw error
        }
    }

    private suspend fun updateActiveBatch(event: BatchLifecycleEvent) {
        val cauldronId = event.cauldronId.takeIf(String::isNotBlank) ?: return
        when (event.eventType) {
            BatchLifecycleEventType.BATCH_CAULDRON_ASSIGNED,
            BatchLifecycleEventType.BATCH_BREWING_STARTED -> brewingStateRepository.saveActiveBatchId(cauldronId, event.batchId)

            BatchLifecycleEventType.BATCH_BOTTLING_COMPLETED,
            BatchLifecycleEventType.BATCH_FAILED,
            BatchLifecycleEventType.BATCH_CANCELLED -> {
                val activeBatchId = brewingStateRepository.findActiveBatchId(cauldronId)
                if (activeBatchId == event.batchId) {
                    brewingStateRepository.deleteActiveBatchId(cauldronId)
                }
            }

            else -> Unit
        }
    }
}

private fun mergeBatchState(event: BatchLifecycleEvent, existing: BatchState?): BatchState =
    BatchState(
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
