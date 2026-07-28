package avh.ckc.demo.service.batch

import avh.ckc.demo.proto.BatchLifecycleEvent
import avh.ckc.demo.proto.BatchLifecycleEventType
import avh.ckc.demo.registry.SyncBrewingStepRegistryClient
import avh.ckc.demo.repository.SyncBrewingStateRepository
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service

@Service
@Profile("spring-kafka", "spring-kafka-thread-pool", "confluent-parallel", "ckc-sync")
class SyncBatchLifecycleService(
    private val brewingStateRepository: SyncBrewingStateRepository,
    private val registryClient: SyncBrewingStepRegistryClient
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun apply(event: BatchLifecycleEvent) {
        try {
            val existingBatch = brewingStateRepository.findBatch(event.batchId)
            brewingStateRepository.saveBatch(mergeBatch(event, existingBatch))
            updateActiveBatch(event, brewingStateRepository)
            reportBrewingStepIfNeeded(event)
        } catch (error: Throwable) {
            logger.error(
                "Sync batch processing failed for batchId={}, eventType={}",
                event.batchId,
                event.eventType.name,
                error
            )
            throw error
        }
    }

    private fun reportBrewingStepIfNeeded(event: BatchLifecycleEvent) {
        if (event.eventType != BatchLifecycleEventType.BATCH_BREWING_STEP_COMPLETED) {
            return
        }
        val response = registryClient.reportStep(registryRequest(event))
        brewingStateRepository.saveBrewingStepReceipt(brewingStepReceipt(event, response))
    }
}
