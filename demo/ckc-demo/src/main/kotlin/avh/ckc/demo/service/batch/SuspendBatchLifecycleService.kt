package avh.ckc.demo.service.batch

import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.repository.SuspendBrewingStateRepository
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

@Service
class SuspendBatchLifecycleService(
    private val brewingStateRepository: SuspendBrewingStateRepository
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    suspend fun apply(event: BatchLifecycleEvent) {
        try {
            val existingBatch = brewingStateRepository.findBatch(event.batchId)
            brewingStateRepository.saveBatch(mergeBatchState(event, existingBatch))
            updateActiveBatch(event, brewingStateRepository)
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
}
