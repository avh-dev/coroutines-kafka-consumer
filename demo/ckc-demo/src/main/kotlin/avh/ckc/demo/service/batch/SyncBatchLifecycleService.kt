package avh.ckc.demo.service.batch

import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.repository.SyncBrewingStateRepository
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service

@Service
@Profile("spring-kafka", "confluent-parallel")
class SyncBatchLifecycleService(
    private val brewingStateRepository: SyncBrewingStateRepository
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun apply(event: BatchLifecycleEvent) {
        try {
            val existingBatch = brewingStateRepository.findBatch(event.batchId)
            brewingStateRepository.saveBatch(mergeBatch(event, existingBatch))
            updateActiveBatch(event, brewingStateRepository)
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
}
